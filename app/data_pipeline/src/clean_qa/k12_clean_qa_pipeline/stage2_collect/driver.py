from __future__ import annotations

import argparse
import json
import re
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import atomic_write_bytes, atomic_write_json, jsonl_bytes
from clean_qa.k12_clean_qa_pipeline.common.hashing import canonical_sha256, sha256_bytes
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import utc_now


COLLECTION_VERSION = "training-jsonl-collection-v1"
LABELS = ("A", "B", "C", "D")


def key(prefix: str, suffix: str) -> str:
    return f"{prefix.rstrip('/')}/{suffix}"


def clean_string(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    value = unicodedata.normalize("NFC", value)
    value = "".join(char for char in value if char >= " " or char in "\n\t")
    return " ".join(value.split())


def parse_jsonl(body: bytes, source_key: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(body.decode("utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"{source_key}:{number} is not valid JSON") from exc
        if not isinstance(row, dict):
            raise ValueError(f"{source_key}:{number} is not a JSON object")
        rows.append(row)
    return rows


def evidence_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    seen: set[str] = set()
    result: list[str] = []
    for entry in value:
        normalized = clean_string(entry)
        if normalized and normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
    return result


def require_verified(row: dict[str, Any], kind: str) -> None:
    if row.get("quality_status") != "verified" or row.get("judge", {}).get("accept") is not True:
        raise ValueError(f"{kind} row is not verified and judge-accepted")


def qa_payload(row: dict[str, Any]) -> dict[str, Any]:
    require_verified(row, "qa")
    payload = {
        "type": "qa",
        "question": clean_string(row.get("question")),
        "analysis": clean_string(row.get("analysis")),
        "evidence": evidence_list(row.get("evidence")),
        "answer": clean_string(row.get("answer")) or clean_string(row.get("final_answer")),
    }
    if not all((payload["question"], payload["analysis"], payload["evidence"], payload["answer"])):
        raise ValueError("qa payload has an empty required field")
    return payload


def mcq_payload(row: dict[str, Any]) -> dict[str, Any]:
    require_verified(row, "mcq")
    options = row.get("options")
    correct_index = row.get("correct_index")
    if not isinstance(options, list) or len(options) != 4 or not isinstance(correct_index, int) or correct_index not in range(4):
        raise ValueError("mcq requires four options and a valid correct_index")
    option_values = [
        re.sub(r"^[A-D][.)、:：]\s*", "", clean_string(option))
        for option in options
    ]
    if any(not option for option in option_values) or len(set(option_values)) != 4:
        raise ValueError("mcq options must be non-empty and distinct")
    payload = {
        "type": "mcq",
        "question": clean_string(row.get("question")),
        "options": dict(zip(LABELS, option_values, strict=True)),
        "analysis": clean_string(row.get("analysis")),
        "evidence": evidence_list(row.get("evidence")),
        "answer": LABELS[correct_index],
    }
    if not all((payload["question"], payload["analysis"], payload["evidence"])):
        raise ValueError("mcq payload has an empty required field")
    return payload


def encode_records(document_id: str, kind: str, rows: list[dict[str, Any]]) -> tuple[bytes, list[dict[str, str]]]:
    builder = qa_payload if kind == "qa" else mcq_payload
    ordered = sorted(rows, key=lambda row: str(row.get("item_id", "")))
    records: list[dict[str, str]] = []
    rejected: list[dict[str, str]] = []
    for row in ordered:
        try:
            payload = builder(row)
        except ValueError as exc:
            rejected.append(
                {
                    "document_id": document_id,
                    "kind": kind,
                    "source_item_id": str(row.get("item_id", "")),
                    "rejection_reason": str(exc),
                }
            )
            continue
        index = len(records) + 1
        record = {
            "id": f"{kind}_{document_id}_item-{index}",
            "text": json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
        }
        if set(record) != {"id", "text"} or not record["id"] or not isinstance(record["text"], str):
            raise ValueError("training record violates strict top-level schema")
        decoded = json.loads(record["text"])
        if decoded != payload:
            raise ValueError("training text JSON round-trip failed")
        records.append(record)
    return jsonl_bytes(records), rejected


def discover_documents(store: ObjectStore, bucket: str, stage2_prefix: str, clean_prefix: str) -> list[dict[str, Any]]:
    documents: list[dict[str, Any]] = []
    suffix = "/_SUCCESS.json"
    for item in store.iter_objects(bucket, f"{stage2_prefix.rstrip('/')}/"):
        marker_key = item["Key"]
        if not marker_key.endswith(suffix):
            continue
        document_id = marker_key[: -len(suffix)].rsplit("/", 1)[-1]
        marker = store.read_json(bucket, marker_key)
        hashes = marker.get("artifact_sha256", {})
        if marker.get("status") not in (None, "success") or not hashes.get("qa_verified.jsonl") or not hashes.get("mcq_verified.jsonl"):
            continue
        clean_key = key(clean_prefix, f"documents/{document_id}.md")
        if not store.exists(bucket, clean_key):
            raise ValueError(f"matching clean.md is missing for {document_id}")
        documents.append(
            {
                "document_id": document_id,
                "qa_key": key(stage2_prefix, f"{document_id}/qa_verified.jsonl"),
                "mcq_key": key(stage2_prefix, f"{document_id}/mcq_verified.jsonl"),
                "clean_key": clean_key,
                "source_success_key": marker_key,
                "stage2_marker_hashes": {name: hashes[name] for name in ("qa_verified.jsonl", "mcq_verified.jsonl")},
            }
        )
    return sorted(documents, key=lambda item: item["document_id"])


def target_keys(output_prefix: str, document_id: str) -> dict[str, str]:
    return {
        "qa": key(output_prefix, f"qa/{document_id}—QA.jsonl"),
        "mcq": key(output_prefix, f"mcq/{document_id}—mcq.jsonl"),
    }


def process_document(document: dict[str, Any], bucket: str, output_prefix: str, resume: bool) -> dict[str, Any]:
    started = time.time()
    store = ObjectStore()
    document_id = document["document_id"]
    source_bodies = {kind: store.read_bytes(bucket, document[f"{kind}_key"]) for kind in ("qa", "mcq")}
    source_hashes = {kind: sha256_bytes(body) for kind, body in source_bodies.items()}
    encoded = {
        kind: encode_records(document_id, kind, parse_jsonl(source_bodies[kind], document[f"{kind}_key"]))
        for kind in ("qa", "mcq")
    }
    outputs = {kind: encoded[kind][0] for kind in ("qa", "mcq")}
    rejected_items = encoded["qa"][1] + encoded["mcq"][1]
    record_counts = {kind: len(parse_jsonl(outputs[kind], document[f"{kind}_key"])) for kind in ("qa", "mcq")}
    targets = target_keys(output_prefix, document_id)
    output_hashes = {kind: sha256_bytes(body) for kind, body in outputs.items()}
    existing = {kind: store.exists(bucket, target) for kind, target in targets.items()}
    if all(existing.values()):
        existing_hashes = {kind: sha256_bytes(store.read_bytes(bucket, target)) for kind, target in targets.items()}
        if existing_hashes == output_hashes and resume:
            return {"document_id": document_id, "status": "skipped", "targets": targets, "artifact_sha256": output_hashes, "source_artifact_sha256": source_hashes, "stage2_marker_hashes": document["stage2_marker_hashes"], "qa_items": record_counts["qa"], "mcq_items": record_counts["mcq"], "rejected_items": rejected_items, "elapsed_seconds": round(time.time() - started, 3)}
        if existing_hashes != output_hashes:
            raise ValueError(f"{document_id} target exists with different content; refusing overwrite")
    elif any(existing.values()):
        raise ValueError(f"{document_id} has an incomplete target pair; refusing overwrite")
    for kind in ("qa", "mcq"):
        atomic_write_bytes(store, bucket, targets[kind], outputs[kind], "application/x-ndjson")
    return {"document_id": document_id, "status": "success", "targets": targets, "artifact_sha256": output_hashes, "source_artifact_sha256": source_hashes, "stage2_marker_hashes": document["stage2_marker_hashes"], "qa_items": record_counts["qa"], "mcq_items": record_counts["mcq"], "rejected_items": rejected_items, "elapsed_seconds": round(time.time() - started, 3)}


def write_progress(store: ObjectStore, bucket: str, output_prefix: str, total: int, results: list[dict[str, Any]]) -> None:
    atomic_write_json(store, bucket, key(output_prefix, "_PROGRESS.json"), {
        "collection_version": COLLECTION_VERSION,
        "total_documents": total,
        "completed_documents": len(results),
        "success_documents": sum(row["status"] == "success" for row in results),
        "skipped_documents": sum(row["status"] == "skipped" for row in results),
        "failed_documents": sum(row["status"] == "failed" for row in results),
        "pending_documents": total - len(results),
        "qa_items": sum(row.get("qa_items", 0) for row in results if row["status"] != "failed"),
        "mcq_items": sum(row.get("mcq_items", 0) for row in results if row["status"] != "failed"),
        "rejected_items": sum(len(row.get("rejected_items", [])) for row in results),
        "updated_at": utc_now(),
    })


def main() -> None:
    parser = argparse.ArgumentParser(description="Export strict id/text QA and MCQ JSONL")
    parser.add_argument("--bucket", default="k12-cleaned-corpus")
    parser.add_argument("--stage2-prefix", default="stage2/full/stage2-v1.1.0-8npu")
    parser.add_argument("--clean-prefix", default="stage1/clean-md-collection-v1")
    parser.add_argument("--output-prefix", default="stage2/training-jsonl-collection-v1")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()
    if args.output_prefix.rstrip("/").startswith(args.stage2_prefix.rstrip("/") + "/"):
        raise SystemExit("output prefix must not be nested under the Stage 2 source prefix")
    store = ObjectStore()
    documents = discover_documents(store, args.bucket, args.stage2_prefix, args.clean_prefix)
    if args.limit:
        documents = documents[: args.limit]
    if not documents:
        raise SystemExit("no complete Stage 2 documents discovered")
    started = time.time()
    run_manifest = {"collection_version": COLLECTION_VERSION, "created_at": utc_now(), "bucket": args.bucket, "stage2_prefix": args.stage2_prefix, "clean_prefix": args.clean_prefix, "output_prefix": args.output_prefix, "workers": args.workers, "resume": args.resume, "total_documents": len(documents), "documents": documents, "manifest_sha256": canonical_sha256(documents)}
    atomic_write_json(store, args.bucket, key(args.output_prefix, "_RUN_MANIFEST.json"), run_manifest)
    results: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_document, document, args.bucket, args.output_prefix, args.resume): document for document in documents}
        for future in as_completed(futures):
            document = futures[future]
            try:
                result = future.result()
            except Exception as exc:
                result = {"document_id": document["document_id"], "status": "failed", "error": repr(exc)}
            results.append(result)
            if len(results) % 10 == 0 or len(results) == len(documents):
                write_progress(store, args.bucket, args.output_prefix, len(documents), results)
                print(f"progress={len(results)}/{len(documents)} qa={sum(row.get('qa_items', 0) for row in results)} mcq={sum(row.get('mcq_items', 0) for row in results)} failed={sum(row['status'] == 'failed' for row in results)}", flush=True)
    results.sort(key=lambda item: item["document_id"])
    failures = [row for row in results if row["status"] == "failed"]
    successes = [row for row in results if row["status"] != "failed"]
    rejected_items = [item for row in results for item in row.get("rejected_items", [])]
    atomic_write_bytes(store, args.bucket, key(args.output_prefix, "manifest.jsonl"), jsonl_bytes(successes), "application/x-ndjson")
    atomic_write_bytes(store, args.bucket, key(args.output_prefix, "_FAILED.jsonl"), jsonl_bytes(failures), "application/x-ndjson")
    atomic_write_bytes(store, args.bucket, key(args.output_prefix, "_REJECTED_ITEMS.jsonl"), jsonl_bytes(rejected_items), "application/x-ndjson")
    write_progress(store, args.bucket, args.output_prefix, len(documents), results)
    summary = {"status": "success" if not failures else "failed", "collection_version": COLLECTION_VERSION, "created_at": utc_now(), "bucket": args.bucket, "stage2_prefix": args.stage2_prefix, "clean_prefix": args.clean_prefix, "output_prefix": args.output_prefix, "total_documents": len(documents), "success_documents": sum(row["status"] == "success" for row in results), "skipped_documents": sum(row["status"] == "skipped" for row in results), "failed_documents": len(failures), "qa_items": sum(row.get("qa_items", 0) for row in successes), "mcq_items": sum(row.get("mcq_items", 0) for row in successes), "rejected_items": len(rejected_items), "elapsed_seconds": round(time.time() - started, 3)}
    atomic_write_json(store, args.bucket, key(args.output_prefix, "_SUMMARY.json"), summary)
    if not failures:
        atomic_write_json(store, args.bucket, key(args.output_prefix, "_SUCCESS.json"), {**summary, "completed_at": utc_now(), "manifest_sha256": canonical_sha256(successes)})
    print("TRAINING_COLLECTION_SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    if failures:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
