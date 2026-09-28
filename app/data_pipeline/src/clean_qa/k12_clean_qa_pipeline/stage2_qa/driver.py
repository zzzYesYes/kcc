from __future__ import annotations

import argparse
import json
import time
import uuid
from typing import Any

import ray

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import (
    atomic_write_bytes,
    atomic_write_json,
    jsonl_bytes,
)
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import ProgressTracker
from clean_qa.k12_clean_qa_pipeline.stage2_qa import PROMPT_VERSION, STAGE2_VERSION
from clean_qa.k12_clean_qa_pipeline.stage2_qa.core import process_document
from clean_qa.k12_clean_qa_pipeline.stage2_qa.qwen import QwenRequestCoordinator
from clean_qa.k12_clean_qa_pipeline.stage2_qa.validation_report import validate_batch


def run_documents(
    document_ids: list[str],
    stage1_bucket: str,
    stage1_prefix: str,
    output_bucket: str,
    output_prefix: str,
    coordinators: list,
    model: str,
    config: dict[str, int],
    resume: bool,
) -> list[dict[str, Any]]:
    store = ObjectStore()
    tracker = ProgressTracker(
        store, output_bucket, output_prefix, len(document_ids), "stage2"
    )
    pending = iter(document_ids)
    active: dict[Any, str] = {}
    results: list[dict[str, Any]] = []
    coordinator_cursor = 0

    def submit_one() -> bool:
        nonlocal coordinator_cursor
        try:
            document_id = next(pending)
        except StopIteration:
            return False
        coordinator = coordinators[coordinator_cursor % len(coordinators)]
        coordinator_cursor += 1
        tracker.start(document_id)
        ref = process_document.remote(
            document_id,
            stage1_bucket,
            stage1_prefix,
            output_bucket,
            output_prefix,
            coordinator,
            model,
            config["block_inflight"],
            config["microbatch_size"],
            config["max_blocks_per_document"],
            config["merge_max_chars"],
            config["merge_max_blocks"],
            config["chapter_max_units"],
            config["document_max_units"],
            config["judge_batch_size"],
            resume,
        )
        active[ref] = document_id
        return True

    for _ in range(min(config["document_inflight"], len(document_ids))):
        submit_one()
    while active:
        ready, _ = ray.wait(list(active), num_returns=1, timeout=2)
        if not ready:
            continue
        for ref in ready:
            document_id = active.pop(ref)
            try:
                result = ray.get(ref)
            except Exception as exc:
                result = {
                    "document_id": document_id,
                    "status": "failed",
                    "error": repr(exc),
                    "metrics": {},
                }
            results.append(result)
            tracker.finish(document_id, result["status"], result.get("metrics", {}))
            submit_one()
    return sorted(results, key=lambda row: row["document_id"])


def run_experiment(
    name: str,
    document_ids: list[str],
    args,
    output_prefix: str,
    config: dict[str, int],
) -> dict[str, Any]:
    started = time.monotonic()
    api_bases = [
        value.strip()
        for value in args.qwen_api_bases.split(",")
        if value.strip()
    ]
    if not api_bases:
        api_bases = [args.qwen_api_base]
    coordinators = [
        QwenRequestCoordinator.options(
            name=f"qwen-coordinator-{name}-{index}-{uuid.uuid4().hex[:8]}",
            lifetime="non_detached",
        ).remote(
            api_base,
            args.qwen_model,
            config["generation_max_inflight"],
            config["judge_max_inflight"],
            config["http_pool_size"],
            args.qwen_timeout_seconds,
            args.qwen_max_retries,
        )
        for index, api_base in enumerate(api_bases)
    ]
    health = ray.get([coordinator.health.remote() for coordinator in coordinators])
    unhealthy = [
        {"api_base": api_base, "health": state}
        for api_base, state in zip(api_bases, health)
        if not state["healthy"]
    ]
    if unhealthy:
        raise RuntimeError(f"Qwen services unavailable: {unhealthy}")
    results = run_documents(
        document_ids,
        args.stage1_bucket,
        args.stage1_prefix,
        args.output_bucket,
        output_prefix,
        coordinators,
        args.qwen_model,
        config,
        args.resume,
    )
    elapsed = time.monotonic() - started
    service_stats = ray.get(
        [coordinator.snapshot.remote() for coordinator in coordinators]
    )
    stats = {
        "service_count": len(service_stats),
        "services": [
            {"api_base": api_base, **snapshot}
            for api_base, snapshot in zip(api_bases, service_stats)
        ],
    }
    for key in ("requests", "retries", "errors", "input_tokens", "output_tokens"):
        stats[key] = sum(int(snapshot.get(key, 0)) for snapshot in service_stats)
    progress_key = f"{output_prefix.rstrip('/')}/_PROGRESS.json"
    progress = ObjectStore().read_json(args.output_bucket, progress_key)
    progress["eligible_blocks"] = progress.get("eligible_block_count", 0)
    progress["qwen_requests"] = stats["requests"]
    progress["qwen_retries"] = stats["retries"]
    atomic_write_json(ObjectStore(), args.output_bucket, progress_key, progress)
    failed = [row for row in results if row["status"] == "failed"]
    metrics = {
        key: sum(int(row.get("metrics", {}).get(key, 0)) for row in results)
        for key in (
            "block_count",
            "eligible_block_count",
            "generation_unit_count",
            "merged_source_block_count",
            "judge_batch_count",
            "judge_candidate_count",
            "qa_candidates",
            "qa_verified",
            "mcq_candidates",
            "mcq_verified",
            "rejected",
            "textbook_exercise_solutions",
        )
    }
    accepted = metrics["qa_verified"] + metrics["mcq_verified"]
    return {
        "name": name,
        "status": "success" if not failed else "failed",
        "config": config,
        "output_prefix": output_prefix,
        "elapsed_seconds": round(elapsed, 3),
        "documents_per_hour": round(len(document_ids) * 3600 / elapsed, 3),
        "accepted_items_per_second": round(accepted / elapsed, 4),
        "candidate_items_per_second": round(
            (metrics["qa_candidates"] + metrics["mcq_candidates"]) / elapsed, 4
        ),
        "metrics": metrics,
        "qwen": stats,
        "results": results,
    }


def throughput_markdown(experiments: list[dict[str, Any]], winner: str) -> str:
    lines = [
        "# Stage 2 Throughput Optimization Report",
        "",
        "| Config | Wall seconds | Docs/hour | Candidates/s | Verified/s | Requests | Retries |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for row in experiments:
        lines.append(
            f"| {row['name']} | {row['elapsed_seconds']} | {row['documents_per_hour']} | "
            f"{row['candidate_items_per_second']} | {row['accepted_items_per_second']} | "
            f"{row['qwen']['requests']} | {row['qwen']['retries']} |"
        )
    lines += ["", f"Selected default: `{winner}`.", ""]
    return "\n".join(lines)


def validation_markdown(
    validation: dict[str, Any], output_uri: str, document_count: int
) -> str:
    lines = [
        f"# Stage 2 {document_count} Automated Validation Report",
        "",
        f"- Status: `{validation['status']}`",
        f"- Output: `{output_uri}`",
        f"- QA verified: `{validation['qa_verified']}`",
        f"- MCQ verified: `{validation['mcq_verified']}`",
        "",
        "## Quality Gates",
        "",
    ]
    lines += [
        f"- {'PASS' if passed else 'FAIL'} `{name}`"
        for name, passed in validation["checks"].items()
    ]
    if validation["failures"]:
        lines += ["", "## Failures", ""] + [
            f"- `{value}`" for value in validation["failures"]
        ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage1-bucket", default="k12-cleaned-corpus")
    parser.add_argument("--stage1-prefix", required=True)
    parser.add_argument("--output-bucket", default="k12-cleaned-corpus")
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--qwen-api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--qwen-api-bases", default="")
    parser.add_argument("--qwen-model", default="qwen3.6-35b-a3b")
    parser.add_argument("--qwen-timeout-seconds", type=int, default=180)
    parser.add_argument("--qwen-max-retries", type=int, default=3)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--automated-validation", action="store_true")
    parser.add_argument("--benchmark", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--document-inflight", type=int, default=3)
    parser.add_argument("--block-inflight", type=int, default=8)
    parser.add_argument("--generation-max-inflight", type=int, default=8)
    parser.add_argument("--judge-max-inflight", type=int, default=4)
    parser.add_argument("--http-pool-size", type=int, default=16)
    parser.add_argument("--microbatch-size", type=int, default=2)
    parser.add_argument("--max-blocks-per-document", type=int, default=6)
    parser.add_argument("--merge-max-chars", type=int, default=3200)
    parser.add_argument("--merge-max-blocks", type=int, default=8)
    parser.add_argument("--chapter-max-units", type=int, default=12)
    parser.add_argument("--document-max-units", type=int, default=48)
    parser.add_argument("--judge-batch-size", type=int, default=8)
    args = parser.parse_args()
    store = ObjectStore()
    stage1_run = store.read_json(
        args.stage1_bucket,
        f"{args.stage1_prefix.rstrip('/')}/_RUN_MANIFEST.json",
    )
    document_ids = [
        row["document_id"] for row in stage1_run["documents"][: args.limit or None]
    ]
    run_manifest = {
        "stage": "stage2",
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
        "stage1_bucket": args.stage1_bucket,
        "stage1_prefix": args.stage1_prefix,
        "output_bucket": args.output_bucket,
        "output_prefix": args.output_prefix,
        "qwen_api_base": args.qwen_api_base,
        "qwen_api_bases": [
            value.strip()
            for value in args.qwen_api_bases.split(",")
            if value.strip()
        ]
        or [args.qwen_api_base],
        "qwen_model": args.qwen_model,
        "documents": document_ids,
        "dry_run": args.dry_run,
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_RUN_MANIFEST.json",
        run_manifest,
    )
    if args.dry_run:
        print(json.dumps(run_manifest, ensure_ascii=False, indent=2))
        return
    ray.init(address="auto", log_to_driver=True)
    optimized = {
        "document_inflight": args.document_inflight,
        "block_inflight": args.block_inflight,
        "generation_max_inflight": args.generation_max_inflight,
        "judge_max_inflight": args.judge_max_inflight,
        "http_pool_size": args.http_pool_size,
        "microbatch_size": args.microbatch_size,
        "max_blocks_per_document": args.max_blocks_per_document,
        "merge_max_chars": args.merge_max_chars,
        "merge_max_blocks": args.merge_max_blocks,
        "chapter_max_units": args.chapter_max_units,
        "document_max_units": args.document_max_units,
        "judge_batch_size": args.judge_batch_size,
    }
    experiments: list[dict[str, Any]] = []
    if args.benchmark:
        conservative = {
            **optimized,
            "document_inflight": 1,
            "block_inflight": 2,
            "generation_max_inflight": 2,
            "judge_max_inflight": 1,
            "http_pool_size": 4,
            "microbatch_size": 1,
        }
        experiments.append(
            run_experiment(
                "conservative",
                document_ids,
                args,
                f"{args.output_prefix.rstrip('/')}/_benchmark/conservative",
                conservative,
            )
        )
    experiments.append(
        run_experiment(
            "optimized",
            document_ids,
            args,
            args.output_prefix,
            optimized,
        )
    )
    result = experiments[-1]
    validation = (
        validate_batch(
            store,
            args.output_bucket,
            args.output_prefix,
            args.stage1_bucket,
            args.stage1_prefix,
            document_ids,
            args.limit or len(document_ids),
        )
        if args.automated_validation and result["status"] == "success"
        else None
    )
    winner = max(
        (row for row in experiments if row["status"] == "success"),
        key=lambda row: row["accepted_items_per_second"],
    )["name"]
    atomic_write_bytes(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/STAGE2_THROUGHPUT_OPTIMIZATION_REPORT.md",
        throughput_markdown(experiments, winner).encode(),
        "text/markdown; charset=utf-8",
    )
    if validation:
        atomic_write_json(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/_AUTOMATED_VALIDATION.json",
            validation,
        )
        atomic_write_bytes(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/"
            f"STAGE2_{len(document_ids)}_AUTOMATED_VALIDATION_REPORT.md",
            validation_markdown(
                validation,
                f"s3://{args.output_bucket}/{args.output_prefix}",
                len(document_ids),
            ).encode(),
            "text/markdown; charset=utf-8",
        )
    summary = {
        "status": (
            "success"
            if result["status"] == "success"
            and (validation is None or validation["status"] == "pass")
            else "failed"
        ),
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
        "default_config": optimized,
        "selected_default": winner,
        "experiments": experiments,
        "validation": validation,
        "capacity_projection": {
            "document_count": 2595,
            "target_hours": 64,
            "projected_hours": round(
                2595 / result["documents_per_hour"], 3
            ),
            "meets_target": result["documents_per_hour"] >= 2595 / 64,
            "required_documents_per_hour": round(2595 / 64, 3),
        },
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_SUMMARY.json",
        summary,
    )
    failures = [
        row
        for experiment in experiments
        for row in experiment["results"]
        if row["status"] == "failed"
    ]
    atomic_write_bytes(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_FAILED.jsonl",
        jsonl_bytes(failures),
        "application/x-ndjson",
    )
    if summary["status"] != "success":
        raise SystemExit(1)
    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
