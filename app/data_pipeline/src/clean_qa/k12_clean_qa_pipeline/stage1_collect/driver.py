from __future__ import annotations

import argparse
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import atomic_write_bytes, atomic_write_json, jsonl_bytes
from clean_qa.k12_clean_qa_pipeline.common.hashing import canonical_sha256, sha256_bytes
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import utc_now


COLLECTION_VERSION = "clean-md-collection-v1"


def key(prefix: str, suffix: str) -> str:
    return f"{prefix.rstrip('/')}/{suffix}"


def discover_documents(store: ObjectStore, bucket: str, source_prefix: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    marker_suffix = "/_SUCCESS.json"
    for item in store.iter_objects(bucket, f"{source_prefix.rstrip('/')}/"):
        marker_key = item["Key"]
        if not marker_key.endswith(marker_suffix):
            continue
        document_id = marker_key[: -len(marker_suffix)].rsplit("/", 1)[-1]
        marker = store.read_json(bucket, marker_key)
        clean_sha = marker.get("artifact_sha256", {}).get("clean.md")
        if marker.get("status") != "success" or not clean_sha:
            continue
        documents.append(
            {
                "document_id": document_id,
                "source_key": key(source_prefix, f"{document_id}/clean.md"),
                "source_success_key": marker_key,
                "source_sha256": clean_sha,
            }
        )
    return sorted(documents, key=lambda item: item["document_id"])


def copy_document(document: dict[str, Any], bucket: str, output_prefix: str, resume: bool) -> dict[str, Any]:
    started = time.time()
    store = ObjectStore()
    document_id = document["document_id"]
    target_key = key(output_prefix, f"documents/{document_id}.md")
    source_bytes = store.read_bytes(bucket, document["source_key"])
    source_sha = sha256_bytes(source_bytes)
    if source_sha != document["source_sha256"]:
        return {
            "document_id": document_id,
            "status": "failed",
            "source_key": document["source_key"],
            "target_key": target_key,
            "error": "source clean.md hash differs from its Stage 1 success marker",
        }
    if store.exists(bucket, target_key):
        target_sha = sha256_bytes(store.read_bytes(bucket, target_key))
        if target_sha == source_sha and resume:
            return {
                "document_id": document_id,
                "status": "skipped",
                "source_key": document["source_key"],
                "target_key": target_key,
                "sha256": source_sha,
                "bytes": len(source_bytes),
                "elapsed_seconds": round(time.time() - started, 3),
            }
        if target_sha != source_sha:
            return {
                "document_id": document_id,
                "status": "failed",
                "source_key": document["source_key"],
                "target_key": target_key,
                "error": "target exists with a different SHA256; refusing overwrite",
            }
    store.copy(bucket, document["source_key"], target_key)
    target_bytes = store.read_bytes(bucket, target_key)
    target_sha = sha256_bytes(target_bytes)
    if target_sha != source_sha:
        return {
            "document_id": document_id,
            "status": "failed",
            "source_key": document["source_key"],
            "target_key": target_key,
            "error": "post-copy SHA256 verification failed",
        }
    return {
        "document_id": document_id,
        "status": "success",
        "source_key": document["source_key"],
        "target_key": target_key,
        "sha256": source_sha,
        "bytes": len(source_bytes),
        "elapsed_seconds": round(time.time() - started, 3),
    }


def write_progress(store: ObjectStore, bucket: str, output_prefix: str, total: int, results: list[dict[str, Any]]) -> None:
    counts = {status: sum(row["status"] == status for row in results) for status in ("success", "skipped", "failed")}
    atomic_write_json(
        store,
        bucket,
        key(output_prefix, "_PROGRESS.json"),
        {
            "collection_version": COLLECTION_VERSION,
            "total_documents": total,
            "completed_documents": len(results),
            "success_documents": counts["success"],
            "skipped_documents": counts["skipped"],
            "failed_documents": counts["failed"],
            "pending_documents": total - len(results),
            "bytes_verified": sum(row.get("bytes", 0) for row in results if row["status"] != "failed"),
            "updated_at": utc_now(),
        },
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect verified Stage 1 clean.md files")
    parser.add_argument("--bucket", default="k12-cleaned-corpus")
    parser.add_argument("--source-prefix", default="stage1/full/stage1-v1.0.2")
    parser.add_argument("--output-prefix", default="stage1/clean-md-collection-v1")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.output_prefix.rstrip("/").startswith(args.source_prefix.rstrip("/") + "/"):
        raise SystemExit("output prefix must not be nested under the source prefix")

    started = time.time()
    store = ObjectStore()
    documents = discover_documents(store, args.bucket, args.source_prefix)
    if not documents:
        raise SystemExit("no successful Stage 1 documents discovered")
    run_manifest = {
        "collection_version": COLLECTION_VERSION,
        "created_at": utc_now(),
        "bucket": args.bucket,
        "source_prefix": args.source_prefix,
        "output_prefix": args.output_prefix,
        "workers": args.workers,
        "resume": args.resume,
        "total_documents": len(documents),
        "documents": documents,
        "manifest_sha256": canonical_sha256(documents),
    }
    atomic_write_json(store, args.bucket, key(args.output_prefix, "_RUN_MANIFEST.json"), run_manifest)
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(copy_document, document, args.bucket, args.output_prefix, args.resume): document
            for document in documents
        }
        for future in as_completed(futures):
            document = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {"document_id": document["document_id"], "status": "failed", "error": repr(exc)}
            results.append(result)
            if len(results) % 10 == 0 or len(results) == len(documents):
                write_progress(store, args.bucket, args.output_prefix, len(documents), results)
                print(f"progress={len(results)}/{len(documents)} success={sum(row['status'] == 'success' for row in results)} skipped={sum(row['status'] == 'skipped' for row in results)} failed={sum(row['status'] == 'failed' for row in results)}", flush=True)
    results.sort(key=lambda item: item["document_id"])
    failures = [row for row in results if row["status"] == "failed"]
    manifest_rows = [row for row in results if row["status"] != "failed"]
    atomic_write_bytes(store, args.bucket, key(args.output_prefix, "manifest.jsonl"), jsonl_bytes(manifest_rows), "application/x-ndjson")
    atomic_write_bytes(store, args.bucket, key(args.output_prefix, "_FAILED.jsonl"), jsonl_bytes(failures), "application/x-ndjson")
    write_progress(store, args.bucket, args.output_prefix, len(documents), results)
    summary = {
        "status": "success" if not failures else "failed",
        "collection_version": COLLECTION_VERSION,
        "created_at": utc_now(),
        "bucket": args.bucket,
        "source_prefix": args.source_prefix,
        "output_prefix": args.output_prefix,
        "total_documents": len(documents),
        "success_documents": sum(row["status"] == "success" for row in results),
        "skipped_documents": sum(row["status"] == "skipped" for row in results),
        "failed_documents": len(failures),
        "bytes_verified": sum(row.get("bytes", 0) for row in manifest_rows),
        "elapsed_seconds": round(time.time() - started, 3),
    }
    atomic_write_json(store, args.bucket, key(args.output_prefix, "_SUMMARY.json"), summary)
    if not failures:
        atomic_write_json(
            store,
            args.bucket,
            key(args.output_prefix, "_SUCCESS.json"),
            {**summary, "completed_at": utc_now(), "manifest_sha256": canonical_sha256(manifest_rows)},
        )
    print("COLLECTION_SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
