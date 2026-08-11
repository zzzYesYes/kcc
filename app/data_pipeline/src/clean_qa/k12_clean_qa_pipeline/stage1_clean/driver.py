from __future__ import annotations

import argparse
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any

import ray

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import (
    atomic_write_bytes,
    atomic_write_json,
    jsonl_bytes,
)
from clean_qa.k12_clean_qa_pipeline.common.hashing import canonical_sha256, sha256_bytes, sha256_text
from clean_qa.k12_clean_qa_pipeline.common.manifests import resolve_manifest
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import ProgressTracker, utc_now
from clean_qa.k12_clean_qa_pipeline.stage1_clean import STAGE1_VERSION
from clean_qa.k12_clean_qa_pipeline.stage1_clean.core import build_stage1
from clean_qa.k12_clean_qa_pipeline.stage1_clean.validation import (
    REQUIRED,
    validate_document_artifacts,
)


OUTPUT_CONTENT_TYPES = {
    "clean.md": "text/markdown; charset=utf-8",
    "book_metadata.json": "application/json",
    "blocks.jsonl": "application/x-ndjson",
    "exercises.jsonl": "application/x-ndjson",
    "image_manifest.jsonl": "application/x-ndjson",
    "quarantine.jsonl": "application/x-ndjson",
    "cleaning_report.json": "application/json",
}


def encode_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")


def output_bodies(result: dict[str, Any]) -> dict[str, bytes]:
    return {
        "clean.md": result["clean_md"].encode("utf-8"),
        "book_metadata.json": encode_json(result["book_metadata"]),
        "blocks.jsonl": jsonl_bytes(result["blocks"]),
        "exercises.jsonl": jsonl_bytes(result["exercises"]),
        "image_manifest.jsonl": jsonl_bytes(result["images"]),
        "quarantine.jsonl": jsonl_bytes(result["quarantine"]),
        "cleaning_report.json": encode_json(result["report"]),
    }


def success_matches(
    store: ObjectStore,
    bucket: str,
    document_prefix: str,
    source_sha256: str,
) -> bool:
    marker_key = f"{document_prefix}/_SUCCESS.json"
    if not store.exists(bucket, marker_key):
        return False
    marker = store.read_json(bucket, marker_key)
    if (
        marker.get("stage1_version") != STAGE1_VERSION
        or marker.get("source_sha256") != source_sha256
    ):
        return False
    return all(
        store.exists(bucket, f"{document_prefix}/{name}")
        for name in OUTPUT_CONTENT_TYPES
    )


def process_document(
    document: dict[str, Any],
    output_bucket: str,
    output_prefix: str,
    resume: bool,
) -> dict[str, Any]:
    started = time.time()
    store = ObjectStore()
    document_id = document["document_id"]
    target = f"{output_prefix.rstrip('/')}/{document_id}"
    markdown_bytes = store.read_bytes(document["source_bucket"], document["markdown_key"])
    markdown = markdown_bytes.decode("utf-8", "replace")
    source_sha = sha256_bytes(markdown_bytes)
    if resume and success_matches(store, output_bucket, target, source_sha):
        marker = store.read_json(output_bucket, f"{target}/_SUCCESS.json")
        return {
            "document_id": document_id,
            "status": "skipped",
            "elapsed_seconds": round(time.time() - started, 3),
            "source_sha256": source_sha,
            "metrics": marker.get("metrics", {}),
            "artifact_sha256": marker.get("artifact_sha256", {}),
        }
    try:
        content_list = store.read_json(
            document["source_bucket"], document["content_list_key"]
        )
        mineru_success = store.read_json(
            document["source_bucket"], document["mineru_success_key"]
        )
        result = build_stage1(document_id, markdown, content_list, mineru_success)
        if result["source_sha256"] != source_sha:
            raise RuntimeError("source SHA changed while processing")
        bodies = output_bodies(result)
        artifact_hashes = {name: sha256_bytes(body) for name, body in bodies.items()}
        for name, body in bodies.items():
            atomic_write_bytes(
                store,
                output_bucket,
                f"{target}/{name}",
                body,
                OUTPUT_CONTENT_TYPES[name],
            )
        marker = {
            "document_id": document_id,
            "status": "success",
            "stage1_version": STAGE1_VERSION,
            "source_bucket": document["source_bucket"],
            "source_markdown_key": document["markdown_key"],
            "source_sha256": source_sha,
            "source_etag": document["source_etag"],
            "artifact_sha256": artifact_hashes,
            "metrics": result["report"],
            "completed_at": utc_now(),
        }
        atomic_write_json(
            store,
            output_bucket,
            f"{target}/_SUCCESS.json",
            marker,
        )
        return {
            "document_id": document_id,
            "status": "success",
            "elapsed_seconds": round(time.time() - started, 3),
            "source_sha256": source_sha,
            "artifact_sha256": artifact_hashes,
            "metrics": result["report"],
        }
    except Exception as exc:
        return {
            "document_id": document_id,
            "status": "failed",
            "elapsed_seconds": round(time.time() - started, 3),
            "source_sha256": source_sha,
            "error": repr(exc),
            "metrics": {},
        }


@ray.remote(num_cpus=1, max_retries=1)
def process_document_remote(
    document: dict[str, Any],
    output_bucket: str,
    output_prefix: str,
    resume: bool,
) -> dict[str, Any]:
    return process_document(document, output_bucket, output_prefix, resume)


def run_pass(
    documents: list[dict[str, Any]],
    output_bucket: str,
    output_prefix: str,
    max_inflight: int,
    resume: bool,
    progress: ProgressTracker | None,
) -> list[dict[str, Any]]:
    pending = iter(documents)
    active: dict[Any, dict[str, Any]] = {}
    results: list[dict[str, Any]] = []

    def submit_next() -> bool:
        try:
            document = next(pending)
        except StopIteration:
            return False
        if progress:
            progress.start(document["document_id"])
        reference = process_document_remote.remote(
            document,
            output_bucket,
            output_prefix,
            resume,
        )
        active[reference] = document
        return True

    for _ in range(min(max_inflight, len(documents))):
        submit_next()
    while active:
        ready, _ = ray.wait(list(active), num_returns=1, timeout=2)
        if not ready:
            continue
        for reference in ready:
            document = active.pop(reference)
            try:
                result = ray.get(reference)
            except Exception as exc:
                result = {
                    "document_id": document["document_id"],
                    "status": "failed",
                    "error": f"Ray task failed: {exc!r}",
                    "metrics": {},
                }
            results.append(result)
            if progress:
                metrics = result.get("metrics", {})
                progress.finish(
                    document["document_id"],
                    result["status"],
                    {
                        "kept_blocks": metrics.get("kept_block_count", 0),
                        "removed_blocks": metrics.get("removed_block_count", 0),
                        "quarantine_blocks": metrics.get("quarantine_block_count", 0),
                        "formula_repairs": metrics.get("formula_repair_count", 0),
                    },
                )
            submit_next()
    return sorted(results, key=lambda row: row["document_id"])


def load_bodies(
    store: ObjectStore,
    bucket: str,
    prefix: str,
    document_id: str,
) -> dict[str, bytes]:
    target = f"{prefix.rstrip('/')}/{document_id}"
    return {
        name: store.read_bytes(bucket, f"{target}/{name}")
        for name in REQUIRED
    }


def validate_batch(
    documents: list[dict[str, Any]],
    initial_results: list[dict[str, Any]],
    output_bucket: str,
    output_prefix: str,
    max_inflight: int,
) -> dict[str, Any]:
    store = ObjectStore()
    first_hashes = {
        row["document_id"]: row["artifact_sha256"]
        for row in initial_results
        if row["status"] in {"success", "skipped"}
    }
    document_checks = {}
    source_etags_before = {
        row["document_id"]: store.head(row["source_bucket"], row["markdown_key"])[
            "ETag"
        ].strip('"')
        for row in documents
    }
    for document in documents:
        document_id = document["document_id"]
        bodies = load_bodies(store, output_bucket, output_prefix, document_id)
        document_checks[document_id] = validate_document_artifacts(
            bodies,
            sha256_bytes(
                store.read_bytes(document["source_bucket"], document["markdown_key"])
            ),
        )
    second = run_pass(
        documents,
        output_bucket,
        output_prefix,
        max_inflight,
        True,
        None,
    )
    all_skipped = len(second) == len(documents) and all(
        row["status"] == "skipped" for row in second
    )
    probe = documents[0]["document_id"]
    store.delete(
        output_bucket,
        f"{output_prefix.rstrip('/')}/{probe}/_SUCCESS.json",
    )
    third = run_pass(
        documents,
        output_bucket,
        output_prefix,
        max_inflight,
        True,
        None,
    )
    reprocessed = [row["document_id"] for row in third if row["status"] == "success"]
    skipped = [row["document_id"] for row in third if row["status"] == "skipped"]
    final_hashes = {
        row["document_id"]: json.loads(
            store.read_bytes(
                output_bucket,
                f"{output_prefix.rstrip('/')}/{row['document_id']}/_SUCCESS.json",
            )
        )["artifact_sha256"]
        for row in documents
    }
    source_etags_after = {
        row["document_id"]: store.head(row["source_bucket"], row["markdown_key"])[
            "ETag"
        ].strip('"')
        for row in documents
    }
    checks = {
        "document_count_10": len(documents) == 10,
        "all_required_outputs": len(document_checks) == 10,
        "all_document_quality_checks_pass": all(
            row["status"] == "pass" for row in document_checks.values()
        ),
        "second_run_skipped_all": all_skipped,
        "single_marker_reprocessed_one": reprocessed == [probe],
        "single_marker_skipped_nine": len(skipped) == len(documents) - 1,
        "content_hash_stable": first_hashes == final_hashes,
        "source_etag_unchanged": source_etags_before == source_etags_after,
    }
    failed = [key for key, passed in checks.items() if not passed]
    return {
        "status": "pass" if not failed else "fail",
        "stage1_version": STAGE1_VERSION,
        "checks": checks,
        "failed_checks": failed,
        "document_checks": document_checks,
        "idempotency": {
            "second_pass": second,
            "recovery_probe": probe,
            "third_pass_reprocessed": reprocessed,
            "third_pass_skipped_count": len(skipped),
        },
    }


def report_markdown(report: dict[str, Any], output_uri: str) -> str:
    lines = [
        "# Stage 1 10 Automated Validation Report",
        "",
        f"- Status: `{report['status']}`",
        f"- Stage 1 version: `{report['stage1_version']}`",
        f"- Output: `{output_uri}`",
        "",
        "## Quality Gates",
        "",
    ]
    lines.extend(
        f"- {'PASS' if passed else 'FAIL'} `{name}`"
        for name, passed in report["checks"].items()
    )
    lines.extend(["", "## Documents", ""])
    lines.extend(
        f"- `{document_id}`: {result['status']}, blocks={result['block_count']}, "
        f"exercises={result['exercise_count']}, quarantine={result['quarantine_count']}"
        for document_id, result in sorted(report["document_checks"].items())
    )
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source-bucket", default="k12-mineru-output")
    parser.add_argument("--source-prefix", required=True)
    parser.add_argument("--output-bucket", default="k12-cleaned-corpus")
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--selection-manifest-key")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--max-document-inflight", type=int, default=8)
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--automated-validation", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    started = time.time()
    store = ObjectStore()
    requested = None
    if args.selection_manifest_key:
        requested = store.read_json(
            args.output_bucket,
            args.selection_manifest_key,
        )["documents"]
    documents = resolve_manifest(
        store,
        args.source_bucket,
        args.source_prefix,
        requested,
        args.limit,
    )
    run_manifest = {
        "stage": "stage1",
        "stage1_version": STAGE1_VERSION,
        "created_at": utc_now(),
        "source_bucket": args.source_bucket,
        "source_prefix": args.source_prefix,
        "output_bucket": args.output_bucket,
        "output_prefix": args.output_prefix,
        "resume": args.resume,
        "max_document_inflight": args.max_document_inflight,
        "documents": documents,
        "manifest_sha256": canonical_sha256(documents),
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
    tracker = ProgressTracker(
        store,
        args.output_bucket,
        args.output_prefix,
        len(documents),
        "stage1",
    )
    results = run_pass(
        documents,
        args.output_bucket,
        args.output_prefix,
        args.max_document_inflight,
        args.resume,
        tracker,
    )
    failures = [row for row in results if row["status"] == "failed"]
    validation = None
    if not failures and args.automated_validation:
        validation = validate_batch(
            documents,
            results,
            args.output_bucket,
            args.output_prefix,
            args.max_document_inflight,
        )
        atomic_write_json(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/_AUTOMATED_VALIDATION.json",
            validation,
        )
        atomic_write_bytes(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/STAGE1_10_AUTOMATED_VALIDATION_REPORT.md",
            report_markdown(
                validation,
                f"s3://{args.output_bucket}/{args.output_prefix.rstrip('/')}",
            ).encode("utf-8"),
            "text/markdown; charset=utf-8",
        )
    summary = {
        "status": (
            "success"
            if not failures and (validation is None or validation["status"] == "pass")
            else "failed"
        ),
        "stage1_version": STAGE1_VERSION,
        "created_at": utc_now(),
        "total_documents": len(documents),
        "success_documents": sum(row["status"] == "success" for row in results),
        "skipped_documents": sum(row["status"] == "skipped" for row in results),
        "failed_documents": len(failures),
        "elapsed_seconds": round(time.time() - started, 3),
        "metrics": {
            key: sum(int(row.get("metrics", {}).get(source, 0)) for row in results)
            for key, source in (
                ("kept_blocks", "kept_block_count"),
                ("removed_blocks", "removed_block_count"),
                ("quarantine_blocks", "quarantine_block_count"),
                ("formula_repairs", "formula_repair_count"),
                ("exercises", "exercise_count"),
                ("images", "image_count"),
            )
        },
        "validation": validation,
        "results": results,
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_SUMMARY.json",
        summary,
    )
    atomic_write_bytes(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_FAILED.jsonl",
        jsonl_bytes(failures),
        "application/x-ndjson",
    )
    print("STAGE1_SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    if summary["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()

