from __future__ import annotations

import json
import re
import time
from typing import Any

import ray

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import (
    atomic_write_bytes,
    atomic_write_json,
    jsonl_bytes,
)
from clean_qa.k12_clean_qa_pipeline.common.hashing import canonical_sha256, stable_id
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.stage2_qa import PROMPT_VERSION, STAGE2_VERSION
from clean_qa.k12_clean_qa_pipeline.stage2_qa.helpers import (
    deduplicate,
    merge_adjacent_blocks,
    rule_prefilter,
    select_units_by_chapter,
)
from clean_qa.k12_clean_qa_pipeline.stage2_qa.prompts import (
    generation_messages,
    judge_batch_messages,
)
from clean_qa.k12_clean_qa_pipeline.stage2_qa.validation import validate_mcq, validate_qa


OUTPUT_NAMES = (
    "eligibility.jsonl",
    "extracted_facts.jsonl",
    "qa_candidates.jsonl",
    "qa_verified.jsonl",
    "mcq_candidates.jsonl",
    "mcq_verified.jsonl",
    "textbook_exercise_solutions.jsonl",
    "rejected.jsonl",
    "dedup_report.json",
    "generation_report.json",
    "sft_messages.jsonl",
    "alpaca_format.jsonl",
)


def parse_jsonl(body: bytes) -> list[dict[str, Any]]:
    return [
        json.loads(line)
        for line in body.decode("utf-8").splitlines()
        if line.strip()
    ]


def _base_item(
    document_id: str,
    block: dict[str, Any],
    kind: str,
    index: int,
    source: dict[str, Any],
    model: str,
) -> dict[str, Any]:
    normalized = dict(source)
    if kind == "mcq":
        options = normalized.get("options", [])
        correct = normalized.get("correct_index")
        if isinstance(options, list) and isinstance(correct, int) and 0 <= correct < len(options):
            normalized.setdefault("answer", str(options[correct]))
            normalized.setdefault("final_answer", str(options[correct]))
        normalized.setdefault("question_type", "multiple_choice")
        normalized.setdefault("difficulty", "medium")
    return {
        "item_id": stable_id(
            document_id,
            block["block_id"],
            kind,
            index,
            canonical_sha256(source),
            prefix="item",
        ),
        "document_id": document_id,
        "block_id": block["block_id"],
        "source_type": (
            "textbook_original_with_generated_solution"
            if block["block_type"] == "exercise"
            else "generated_from_textbook"
        ),
        "chapter_path": block.get("chapter_path", []),
        "knowledge_points": source.get("knowledge_points", []),
        "requires_image": False,
        "generator_model": model,
        "prompt_version": PROMPT_VERSION,
        "quality_status": "candidate",
        "source_block_ids": block.get("source_block_ids", [block["block_id"]]),
        "generation_unit_id": block.get("generation_unit_id", block["block_id"]),
        **normalized,
    }


def _write_jsonl(
    store: ObjectStore,
    bucket: str,
    key: str,
    rows: list[dict[str, Any]],
) -> None:
    atomic_write_bytes(store, bucket, key, jsonl_bytes(rows), "application/x-ndjson")


@ray.remote(num_cpus=1, max_retries=0)
def process_document(
    document_id: str,
    stage1_bucket: str,
    stage1_prefix: str,
    output_bucket: str,
    output_prefix: str,
    coordinator,
    model: str,
    block_inflight: int,
    microbatch_size: int,
    max_blocks_per_document: int,
    merge_max_chars: int,
    merge_max_blocks: int,
    chapter_max_units: int,
    document_max_units: int,
    judge_batch_size: int,
    resume: bool,
    judge_enabled: bool = True,
    routing_telemetry: bool = False,
) -> dict[str, Any]:
    started = time.monotonic()
    store = ObjectStore()
    source = f"{stage1_prefix.rstrip('/')}/{document_id}"
    target = f"{output_prefix.rstrip('/')}/{document_id}"
    stage1_success = store.read_json(stage1_bucket, f"{source}/_SUCCESS.json")
    contract = {
        "stage1_artifact_sha256": stage1_success["artifact_sha256"],
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
        "model": model,
        "pipeline_config": {
            "microbatch_size": microbatch_size,
            "max_blocks_per_document": max_blocks_per_document,
            "merge_max_chars": merge_max_chars,
            "merge_max_blocks": merge_max_blocks,
            "chapter_max_units": chapter_max_units,
            "document_max_units": document_max_units,
            "judge_batch_size": judge_batch_size,
            "judge_enabled": judge_enabled,
        },
    }
    marker_key = f"{target}/_SUCCESS.json"
    observation_key = f"{target}/_OBSERVABILITY.json"
    observation = {
        "document_id": document_id,
        "updated_at": time.time(),
        "stages": {},
        "qwen_assignments": [],
    }

    def observe(stage: str, status: str, **details: Any) -> None:
        if not routing_telemetry:
            return
        now = time.time()
        previous = observation["stages"].get(stage, {})
        observation["stages"][stage] = {
            **previous,
            "status": status,
            "started_at": previous.get("started_at", now),
            "updated_at": now,
            **details,
        }
        if status in {"completed", "failed"}:
            observation["stages"][stage]["completed_at"] = now
            observation["stages"][stage]["processing_time"] = round(
                now - observation["stages"][stage]["started_at"], 3
            )
        observation["updated_at"] = now
        atomic_write_json(
            store, output_bucket, observation_key, observation
        )

    if resume and store.exists(output_bucket, marker_key):
        marker = store.read_json(output_bucket, marker_key)
        if marker.get("input_contract_sha256") == canonical_sha256(contract):
            return {
                "document_id": document_id,
                "status": "skipped",
                "metrics": marker["metrics"],
            }

    observe("qa_mcq", "busy")
    blocks = parse_jsonl(store.read_bytes(stage1_bucket, f"{source}/blocks.jsonl"))
    exercises = parse_jsonl(
        store.read_bytes(stage1_bucket, f"{source}/exercises.jsonl")
    )
    quarantine = parse_jsonl(
        store.read_bytes(stage1_bucket, f"{source}/quarantine.jsonl")
    )
    store.read_bytes(stage1_bucket, f"{source}/book_metadata.json")
    store.read_bytes(stage1_bucket, f"{source}/image_manifest.jsonl")
    quarantined = {row["block_id"] for row in quarantine}
    eligibility: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    eligible_blocks: list[dict[str, Any]] = []
    for block in blocks:
        reason = rule_prefilter(block, quarantined)
        eligibility.append(
            {
                "document_id": document_id,
                "block_id": block["block_id"],
                "eligible": reason is None,
                "reason": reason,
                "stage": "rule_prefilter",
            }
        )
        if reason:
            rejected.append(
                {
                    "document_id": document_id,
                    "block_id": block["block_id"],
                    "rejection_reason": reason,
                    "stage": "rule_prefilter",
                }
            )
        else:
            eligible_blocks.append(block)
    all_eligible_blocks = eligible_blocks
    eligible_blocks = []
    selected_ids: set[str] = set()
    for block_type in (
        "exercise",
        "definition",
        "worked_example",
        "formula",
        "table",
        "concept",
    ):
        candidate = next(
            (
                row
                for row in all_eligible_blocks
                if row["block_type"] == block_type
                and row["block_id"] not in selected_ids
            ),
            None,
        )
        if candidate:
            eligible_blocks.append(candidate)
            selected_ids.add(candidate["block_id"])
        if len(eligible_blocks) == max_blocks_per_document:
            break
    for candidate in all_eligible_blocks:
        if len(eligible_blocks) == max_blocks_per_document:
            break
        if candidate["block_id"] not in selected_ids:
            eligible_blocks.append(candidate)
            selected_ids.add(candidate["block_id"])
    generation_units = merge_adjacent_blocks(
        eligible_blocks,
        max_chars=merge_max_chars,
        max_blocks=merge_max_blocks,
    )
    generation_units = select_units_by_chapter(
        generation_units,
        chapter_max_units=chapter_max_units,
        document_max_units=document_max_units,
    )
    block_by_id = {row["block_id"]: row for row in generation_units}

    batches: list[list[dict[str, Any]]] = []
    cursor = 0
    while cursor < len(generation_units):
        first = generation_units[cursor]
        mergeable = (
            first["block_type"] in {"definition", "formula", "concept", "property"}
            and len(first["clean_text"]) < 1800
        )
        size = microbatch_size if mergeable else 1
        batch = [first]
        for candidate in generation_units[cursor + 1 : cursor + size]:
            if (
                candidate["block_type"]
                in {"definition", "formula", "concept", "property"}
                and len(candidate["clean_text"]) < 1800
            ):
                batch.append(candidate)
            else:
                break
        batches.append(batch)
        cursor += len(batch)

    pending: dict[Any, list[dict[str, Any]]] = {}
    generated: list[dict[str, Any]] = []
    qwen_assignments: list[dict[str, Any]] = []
    batch_iter = iter(batches)

    def submit_one() -> bool:
        try:
            batch = next(batch_iter)
        except StopIteration:
            return False
        request_args: list[Any] = [
            "generation",
            generation_messages(batch),
            1200,
        ]
        if routing_telemetry:
            request_args.append(
                {
                    "document_id": document_id,
                    "block_ids": [row["block_id"] for row in batch],
                    "request_kind": "qa_mcq_generation",
                }
            )
        ref = coordinator.request.remote(*request_args)
        pending[ref] = batch
        return True

    for _ in range(min(block_inflight, len(batches))):
        submit_one()
    while pending:
        ready, _ = ray.wait(list(pending), num_returns=1)
        for ref in ready:
            batch = pending.pop(ref)
            try:
                response = ray.get(ref)
                rows = response["data"].get("results", [])
                if not isinstance(rows, list):
                    raise ValueError("results is not a list")
                if response.get("routing"):
                    qwen_assignments.append(response["routing"])
                generated.extend(rows)
                for row in rows:
                    block_id = str(row.get("block_id", "unknown"))
                    atomic_write_json(
                        store,
                        output_bucket,
                        f"{target}/_shards/{block_id}.json",
                        row,
                    )
            except Exception as exc:
                for block in batch:
                    rejected.append(
                        {
                            "document_id": document_id,
                            "block_id": block["block_id"],
                            "rejection_reason": "source_not_answerable",
                            "stage": "qwen_generation",
                            "error": repr(exc),
                        }
                    )
            submit_one()

    observation["qwen_assignments"] = qwen_assignments
    observe(
        "qa_mcq",
        "completed",
        generation_batches=len(batches),
        generated_results=len(generated),
        qwen_assignment_count=len(qwen_assignments),
    )
    observe("schema_validate", "busy")
    facts: list[dict[str, Any]] = []
    qa_candidates: list[dict[str, Any]] = []
    mcq_candidates: list[dict[str, Any]] = []
    for result in generated:
        block_id = str(result.get("block_id", ""))
        block = block_by_id.get(block_id)
        if not block:
            continue
        if not result.get("eligible"):
            rejected.append(
                {
                    "document_id": document_id,
                    "block_id": block_id,
                    "rejection_reason": "source_not_answerable",
                    "stage": "qwen_eligibility",
                }
            )
            continue
        for fact in result.get("facts", [])[:3]:
            facts.append(
                {
                    "document_id": document_id,
                    "block_id": block_id,
                    **fact,
                }
            )
        for index, item in enumerate(result.get("qa", [])[:2]):
            qa_candidates.append(
                _base_item(document_id, block, "qa", index, item, model)
            )
        for index, item in enumerate(result.get("mcq", [])[:1]):
            mcq_candidates.append(
                _base_item(document_id, block, "mcq", index, item, model)
            )

    valid_candidates: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for kind, candidates, validator in (
        ("qa", qa_candidates, validate_qa),
        ("mcq", mcq_candidates, validate_mcq),
    ):
        for item in candidates:
            block = block_by_id[item["block_id"]]
            valid, reason = validator(item, block["clean_text"])
            if valid:
                valid_candidates.append((kind, item, block))
            else:
                rejected.append({**item, "rejection_reason": reason, "stage": "program_validation"})

    judge_batches = (
        [
            valid_candidates[index : index + judge_batch_size]
            for index in range(0, len(valid_candidates), judge_batch_size)
        ]
        if judge_enabled
        else []
    )
    judge_refs: dict[Any, list[tuple[str, dict[str, Any], dict[str, Any]]]] = {}
    judge_iter = iter(judge_batches)

    def submit_judge_batch() -> bool:
        try:
            batch = next(judge_iter)
        except StopIteration:
            return False
        max_tokens = min(2400, 180 + 190 * len(batch))
        ref = coordinator.request.remote(
            "judge",
            judge_batch_messages(
                [(item, block["clean_text"]) for _, item, block in batch]
            ),
            max_tokens,
        )
        judge_refs[ref] = batch
        return True

    qa_verified: list[dict[str, Any]] = []
    mcq_verified: list[dict[str, Any]] = []
    if judge_enabled:
        for _ in range(min(block_inflight, len(judge_batches))):
            submit_judge_batch()
    else:
        for kind, item, _ in valid_candidates:
            validated = {
                **item,
                "quality_status": "schema_valid_unjudged",
                "judge_enabled": False,
                "validation_status": "schema_valid_unjudged",
                "semantic_verified": False,
            }
            (qa_verified if kind == "qa" else mcq_verified).append(validated)
    while judge_refs:
        ready, _ = ray.wait(list(judge_refs), num_returns=1)
        for ref in ready:
            batch = judge_refs.pop(ref)
            batch_error = "missing_batch_result"
            try:
                rows = ray.get(ref)["data"].get("results", [])
                if not isinstance(rows, list):
                    raise ValueError("judge results is not a list")
                result_ids = [
                    str(row.get("item_id"))
                    for row in rows
                    if isinstance(row, dict) and row.get("item_id")
                ]
                expected_ids = {item["item_id"] for _, item, _ in batch}
                if len(result_ids) != len(set(result_ids)):
                    raise ValueError("judge results contains duplicate item_id")
                if set(result_ids) != expected_ids:
                    raise ValueError("judge results item_id set mismatch")
                by_id = {
                    str(row["item_id"]): row
                    for row in rows
                    if isinstance(row, dict) and row.get("item_id")
                }
            except Exception as exc:
                by_id = {}
                batch_error = repr(exc)
            for kind, item, _ in batch:
                judged = by_id.get(item["item_id"])
                checks = judged.get("checks", {}) if judged else {}
                accepted = bool(judged and judged.get("accept")) and checks and all(
                    checks.get(name) is True
                    for name in (
                        "grounded",
                        "question_clear",
                        "answer_correct",
                        "analysis_correct",
                        "age_appropriate",
                        "single_correct_option",
                        "source_supported",
                    )
                )
                if accepted:
                    verified = {
                        **item,
                        "quality_status": "verified",
                        "judge": judged,
                    }
                    (qa_verified if kind == "qa" else mcq_verified).append(verified)
                else:
                    rejected.append(
                        {
                            **item,
                            "rejection_reason": "judge_rejected",
                            "stage": "qwen_judge",
                            "judge": judged
                            or {
                                "accept": False,
                                "reason": batch_error,
                            },
                        }
                    )
            submit_judge_batch()

    qa_verified, qa_duplicates = deduplicate(qa_verified)
    mcq_verified, mcq_duplicates = deduplicate(mcq_verified)
    rejected.extend(qa_duplicates)
    rejected.extend(mcq_duplicates)
    exercise_ids = {row["block_id"] for row in exercises}
    textbook_solutions = [
        row for row in qa_verified if row["block_id"] in exercise_ids
    ]
    sft = [
        {
            "messages": [
                {"role": "user", "content": row["question"]},
                {
                    "role": "assistant",
                    "content": f"{row['analysis']}\n\n答案：{row['final_answer']}",
                },
            ],
            "item_id": row["item_id"],
        }
        for row in qa_verified + mcq_verified
    ]
    alpaca = [
        {
            "instruction": row["question"],
            "input": "",
            "output": f"{row['analysis']}\n\n答案：{row.get('final_answer', row.get('answer', ''))}",
            "item_id": row["item_id"],
        }
        for row in qa_verified + mcq_verified
    ]
    observe(
        "schema_validate",
        "completed",
        qa_candidates=len(qa_candidates),
        mcq_candidates=len(mcq_candidates),
        qa_schema_valid_unjudged=(
            len(qa_verified) if not judge_enabled else 0
        ),
        mcq_schema_valid_unjudged=(
            len(mcq_verified) if not judge_enabled else 0
        ),
    )
    observe("minio_write", "busy")
    artifacts: dict[str, Any] = {
        "eligibility.jsonl": eligibility,
        "extracted_facts.jsonl": facts,
        "qa_candidates.jsonl": qa_candidates,
        "mcq_candidates.jsonl": mcq_candidates,
        "textbook_exercise_solutions.jsonl": textbook_solutions,
        "rejected.jsonl": rejected,
        "dedup_report.json": {
            "exact_duplicate_count": len(qa_duplicates) + len(mcq_duplicates),
            "near_duplicate_rate": 0,
            "near_duplicate_threshold": 0.9,
        },
    }
    if judge_enabled:
        artifacts["qa_verified.jsonl"] = qa_verified
        artifacts["mcq_verified.jsonl"] = mcq_verified
        artifacts["sft_messages.jsonl"] = sft
        artifacts["alpaca_format.jsonl"] = alpaca
    else:
        artifacts["qa_schema_valid_unjudged.jsonl"] = qa_verified
        artifacts["mcq_schema_valid_unjudged.jsonl"] = mcq_verified
        artifacts["sft_messages_unjudged.jsonl"] = sft
        artifacts["alpaca_format_unjudged.jsonl"] = alpaca
    metrics = {
        "block_count": len(blocks),
        "eligible_block_count": len(all_eligible_blocks),
        "generation_unit_count": len(generation_units),
        "merged_source_block_count": sum(
            int(row.get("merged_block_count", 1)) for row in generation_units
        ),
        "judge_batch_count": len(judge_batches),
        "judge_candidate_count": (
            len(valid_candidates) if judge_enabled else 0
        ),
        "schema_valid_candidate_count": len(valid_candidates),
        "qa_candidates": len(qa_candidates),
        "qa_verified": len(qa_verified) if judge_enabled else 0,
        "mcq_candidates": len(mcq_candidates),
        "mcq_verified": len(mcq_verified) if judge_enabled else 0,
        "rejected": len(rejected),
        "textbook_exercise_solutions": len(textbook_solutions),
        "judge_enabled": judge_enabled,
        "qa_schema_valid_unjudged": 0 if judge_enabled else len(qa_verified),
        "mcq_schema_valid_unjudged": 0 if judge_enabled else len(mcq_verified),
        "elapsed_seconds": round(time.monotonic() - started, 3),
    }
    artifacts["generation_report.json"] = {
        "document_id": document_id,
        "metrics": metrics,
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
        "model": model,
        "judge_enabled": judge_enabled,
        "validation_status": (
            "judge_verified" if judge_enabled else "schema_valid_unjudged"
        ),
    }
    artifact_hashes: dict[str, str] = {}
    for name, value in artifacts.items():
        body = (
            jsonl_bytes(value)
            if name.endswith(".jsonl")
            else json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        )
        atomic_write_bytes(
            store,
            output_bucket,
            f"{target}/{name}",
            body,
            "application/x-ndjson" if name.endswith(".jsonl") else "application/json",
        )
        artifact_hashes[name] = canonical_sha256(value)
    marker = {
        "document_id": document_id,
        "input_contract_sha256": canonical_sha256(contract),
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
        "model": model,
        "judge_enabled": judge_enabled,
        "validation_status": (
            "judge_verified" if judge_enabled else "schema_valid_unjudged"
        ),
        "semantic_verified": bool(judge_enabled),
        "artifact_sha256": artifact_hashes,
        "metrics": metrics,
    }
    observe(
        "minio_write",
        "completed",
        artifact_count=len(artifacts),
        output_prefix=f"s3://{output_bucket}/{target}",
    )
    atomic_write_json(store, output_bucket, marker_key, marker)
    return {
        "document_id": document_id,
        "status": "success",
        "metrics": metrics,
        "qwen_assignments": qwen_assignments,
    }
