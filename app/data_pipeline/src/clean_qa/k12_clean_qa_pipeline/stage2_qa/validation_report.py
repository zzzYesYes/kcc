from __future__ import annotations

import json
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.stage2_qa.core import OUTPUT_NAMES, parse_jsonl
from clean_qa.k12_clean_qa_pipeline.stage2_qa.validation import validate_mcq, validate_qa


def validate_batch(
    store: ObjectStore,
    bucket: str,
    prefix: str,
    stage1_bucket: str,
    stage1_prefix: str,
    document_ids: list[str],
    expected_document_count: int | None = None,
) -> dict[str, Any]:
    item_ids: set[str] = set()
    failures: list[str] = []
    qa_total = qa_evidence = mcq_total = textbook_solution_total = 0
    for document_id in document_ids:
        base = f"{prefix.rstrip('/')}/{document_id}"
        for name in (*OUTPUT_NAMES, "_SUCCESS.json"):
            if not store.exists(bucket, f"{base}/{name}"):
                failures.append(f"{document_id}:missing:{name}")
        qa = parse_jsonl(store.read_bytes(bucket, f"{base}/qa_verified.jsonl"))
        mcq = parse_jsonl(store.read_bytes(bucket, f"{base}/mcq_verified.jsonl"))
        source_base = f"{stage1_prefix.rstrip('/')}/{document_id}"
        blocks = {
            row["block_id"]: row
            for row in parse_jsonl(
                store.read_bytes(stage1_bucket, f"{source_base}/blocks.jsonl")
            )
        }
        quarantined = {
            row["block_id"]
            for row in parse_jsonl(
                store.read_bytes(stage1_bucket, f"{source_base}/quarantine.jsonl")
            )
        }
        candidates = parse_jsonl(
            store.read_bytes(bucket, f"{base}/qa_candidates.jsonl")
        )
        mcq_candidates = parse_jsonl(
            store.read_bytes(bucket, f"{base}/mcq_candidates.jsonl")
        )
        textbook_solution_total += len(
            parse_jsonl(
                store.read_bytes(
                    bucket, f"{base}/textbook_exercise_solutions.jsonl"
                )
            )
        )
        if not candidates:
            failures.append(f"{document_id}:qa_candidates_empty")
        if not mcq_candidates:
            failures.append(f"{document_id}:mcq_candidates_empty")
        for item in qa + mcq:
            if item["item_id"] in item_ids:
                failures.append(f"duplicate_item_id:{item['item_id']}")
            item_ids.add(item["item_id"])
            if item.get("quality_status") != "verified":
                failures.append(f"unverified_in_final:{item['item_id']}")
            block = blocks.get(item.get("block_id"))
            if not block:
                failures.append(f"invalid_block_id:{item['item_id']}")
                continue
            source_block_ids = item.get("source_block_ids", [item["block_id"]])
            source_blocks = [blocks.get(block_id) for block_id in source_block_ids]
            if any(source_block is None for source_block in source_blocks):
                failures.append(f"invalid_block_id:{item['item_id']}")
                continue
            if any(block_id in quarantined for block_id in source_block_ids):
                failures.append(f"quarantine_leak:{item['item_id']}")
            validator = validate_mcq if "options" in item else validate_qa
            valid, reason = validator(
                item,
                "\n\n".join(
                    str(source_block["clean_text"]) for source_block in source_blocks
                ),
            )
            if not valid:
                failures.append(f"program_validation:{item['item_id']}:{reason}")
        qa_total += len(qa)
        qa_evidence += sum(bool(row.get("evidence")) for row in qa)
        mcq_total += len(mcq)
        for row in mcq:
            options = row.get("options", [])
            if len(options) != 4 or len(set(options)) != 4:
                failures.append(f"mcq_not_unique:{row['item_id']}")
        sft = parse_jsonl(store.read_bytes(bucket, f"{base}/sft_messages.jsonl"))
        verified_ids = {row["item_id"] for row in qa + mcq}
        if any(row.get("item_id") not in verified_ids for row in sft):
            failures.append(f"{document_id}:unverified_sft")
        json.loads(store.read_bytes(bucket, f"{base}/generation_report.json"))
        json.loads(store.read_bytes(bucket, f"{base}/dedup_report.json"))
    expected = expected_document_count or len(document_ids)
    checks = {
        f"document_count_{expected}": len(document_ids) == expected,
        "all_outputs_present": not any(":missing:" in value for value in failures),
        "json_jsonl_parse_rate_100": True,
        "item_id_unique_rate_100": not any(
            value.startswith("duplicate_item_id") for value in failures
        ),
        "verified_only_training_exports": not any(
            value.endswith("unverified_sft") for value in failures
        ),
        "block_id_reference_rate_100": not any(
            value.startswith("invalid_block_id") for value in failures
        ),
        "math_program_recheck_rate_100": not any(
            value.startswith("program_validation") for value in failures
        ),
        "quarantine_leak_count_0": not any(
            value.startswith("quarantine_leak") for value in failures
        ),
        "mcq_single_correct_structure_100": not any(
            value.startswith("mcq_not_unique") for value in failures
        ),
        "evidence_presence_rate_at_least_98": (
            qa_total == 0 or qa_evidence / qa_total >= 0.98
        ),
        "qa_candidates_for_all_documents": not any(
            value.endswith("qa_candidates_empty") for value in failures
        ),
        "mcq_candidates_for_all_documents": not any(
            value.endswith("mcq_candidates_empty") for value in failures
        ),
        "textbook_original_solution_path_exercised": textbook_solution_total > 0,
    }
    return {
        "status": "pass" if all(checks.values()) and not failures else "fail",
        "checks": checks,
        "failures": failures,
        "qa_verified": qa_total,
        "mcq_verified": mcq_total,
        "textbook_exercise_solutions": textbook_solution_total,
    }
