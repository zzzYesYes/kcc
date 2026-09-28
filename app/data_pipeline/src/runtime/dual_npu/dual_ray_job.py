"""Head-side S3 manifest scan and sticky dual-service Ray coordinator."""

from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import ray
from botocore.config import Config

from .dual_service_actor import MinerUServiceActor


OUTPUT_BUCKET = os.environ.get("MINERU_OUTPUT_BUCKET", "k12-mineru-output")
SMOKE_IDS = {"doc-03", "doc-10", "doc-12", "doc-18", "doc-21", "doc-23"}


def positive_count(value: str) -> int:
    count = int(value)
    if count < 1:
        raise argparse.ArgumentTypeError("count must be a positive integer")
    return count


def scheduling_pages(document: dict[str, Any]) -> int:
    return max(
        1,
        int(document.get("page_count") or document.get("estimated_page_count") or 1),
    )


def emit_dagster_event(event: str, **payload: Any) -> None:
    row = {
        "event": event,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    print(
        f"DAGSTER_EVENT {json.dumps(row, ensure_ascii=False, separators=(',', ':'))}",
        flush=True,
    )


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def scan_manifest(manifest_key: str, output_prefix: str, count: int) -> dict[str, Any]:
    s3 = s3_client()
    source = json.loads(
        s3.get_object(Bucket=OUTPUT_BUCKET, Key=manifest_key)["Body"].read()
    )
    source_ids = {document["document_id"] for document in source["documents"]}
    use_fixed_smoke_set = count == 6 and SMOKE_IDS.issubset(source_ids)
    seen: set[tuple[str, str]] = set()
    documents = []
    for document in source["documents"]:
        if use_fixed_smoke_set and document["document_id"] not in SMOKE_IDS:
            continue
        identity = (document["object_key"], document.get("etag", ""))
        if identity in seen:
            continue
        seen.add(identity)
        success_key = f"{output_prefix}/{document['document_id']}/_SUCCESS.json"
        try:
            s3.head_object(Bucket=OUTPUT_BUCKET, Key=success_key)
            continue
        except s3.exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") != 404:
                raise
        documents.append(document)
        if len(documents) == count:
            break
    return {
        "created_at": datetime.now(timezone.utc).isoformat(),
        "source_manifest": f"s3://{OUTPUT_BUCKET}/{manifest_key}",
        "output_prefix": output_prefix,
        "requested_count": count,
        "pending_count": len(documents),
        "documents": documents,
    }


def service_score(status: dict[str, Any]) -> float:
    window_penalty = status.get("scheduler", {}).get("active_slots", 0) * 64
    return status["estimated_remaining_seconds"] + (
        window_penalty * status["seconds_per_page"]
    )


def coordinator_loop(actors: dict[str, Any], documents: list[dict[str, Any]], log_path: Path):
    pending = sorted(documents, key=scheduling_pages, reverse=True)
    results: list[dict[str, Any]] = []
    assignments: list[dict[str, Any]] = []
    unhealthy_since: dict[str, float | None] = {name: None for name in actors}

    while pending or any(ray.get(actor.status.remote())["inflight_documents"] for actor in actors.values()):
        statuses = {name: ray.get(actor.status.remote()) for name, actor in actors.items()}
        for name, actor in actors.items():
            completed = ray.get(actor.drain_completed.remote())
            results.extend(completed)
            if statuses[name]["healthy"]:
                unhealthy_since[name] = None
            else:
                unhealthy_since[name] = unhealthy_since[name] or time.time()

        while pending:
            candidates = [
                (service_score(status), name)
                for name, status in statuses.items()
                if status["healthy"]
                and status["inflight_documents"] < status["document_capacity"]
            ]
            if not candidates:
                break
            _, name = min(candidates)
            document = pending[0]
            accepted = ray.get(actors[name].submit.remote(document))
            if not accepted["accepted"]:
                statuses[name] = ray.get(actors[name].status.remote())
                break
            pending.pop(0)
            statuses[name]["inflight_documents"] += 1
            statuses[name]["estimated_remaining_pages"] += scheduling_pages(document)
            statuses[name]["estimated_remaining_seconds"] = (
                statuses[name]["estimated_remaining_pages"] * statuses[name]["seconds_per_page"]
            )
            assignment = {
                "ts": time.time(),
                "document_id": document["document_id"],
                "page_count": document.get("page_count"),
                "estimated_page_count": scheduling_pages(document),
                "service": name,
                "score_after_assignment": service_score(statuses[name]),
            }
            assignments.append(assignment)
            emit_dagster_event(
                "DOCUMENT_ASSIGNED",
                document_id=document["document_id"],
                input_key=document["object_key"],
                worker_ip=statuses[name].get("worker_ip"),
                service=name,
                actor_pid=statuses[name].get("actor_pid"),
                npu_logical_id=statuses[name].get("npu_logical_id"),
                assigned_at=datetime.now(timezone.utc).isoformat(),
            )

        row = {
            "ts": time.time(),
            "pending": len(pending),
            "completed": len(results),
            "statuses": statuses,
        }
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")

        if pending and all(
            value is not None and time.time() - value > 30
            for value in unhealthy_since.values()
        ):
            raise RuntimeError("both MinerU services unhealthy for more than 30 seconds")
        time.sleep(2)

    for actor in actors.values():
        results.extend(ray.get(actor.drain_completed.remote()))
    return results, assignments


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--count", type=positive_count, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--mapping-file", type=Path, required=True)
    parser.add_argument("--batch-id", default="")
    parser.add_argument("--ray-job-id", default="")
    parser.add_argument("--service-count", type=int, choices=(1, 2), default=2)
    parser.add_argument("--inference-slots", type=positive_count, default=4)
    parser.add_argument("--document-inflight", type=positive_count, default=5)
    parser.add_argument("--window-prefetch", type=positive_count, default=1)
    parser.add_argument("--download-workers", type=positive_count, default=2)
    parser.add_argument("--upload-workers", type=positive_count, default=4)
    parser.add_argument("--block-prepare-workers", type=positive_count, default=12)
    parser.add_argument("--render-workers", type=positive_count, default=6)
    parser.add_argument("--finalize-workers", type=positive_count, default=3)
    parser.add_argument("--archive-workers", type=positive_count, default=2)
    parser.add_argument("--multipart-chunksize-mib", type=positive_count, default=16)
    parser.add_argument("--multipart-max-concurrency", type=positive_count, default=4)
    args = parser.parse_args()
    if not args.batch_id:
        args.batch_id = args.output_prefix.rstrip("/").rsplit("/", 1)[-1]
    args.run_dir.mkdir(parents=True, exist_ok=True)

    manifest = scan_manifest(args.manifest_key, args.output_prefix, args.count)
    manifest_path = args.run_dir / "head-manifest.json"
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    s3_client().upload_file(
        str(manifest_path), OUTPUT_BUCKET, f"{args.output_prefix}/_HEAD_MANIFEST.json"
    )

    mapping = json.loads(args.mapping_file.read_text())["devices"]
    ray.init(address="auto", log_to_driver=True)
    actors = {}
    service_specs = (
        ("A", "14", 30001, "0-31"),
        ("B", "15", 30002, "32-63"),
    )
    for name, physical, port, cpu_set in service_specs[: args.service_count]:
        device = mapping[physical]
        config = {
            "name": name,
            "physical_npu": int(physical),
            "npu_id": device["npu_id"],
            "chip_id": device["chip_id"],
            "logical_id": device["logical_id"],
            "server_url": f"http://127.0.0.1:{port}",
            "cpu_set": cpu_set,
            "run_dir": str(args.run_dir),
            "output_prefix": args.output_prefix,
            "batch_id": args.batch_id,
            "vllm_pid_file": f"/tmp/mineru-dual/vllm-{name}.pid",
            "inference_slots": args.inference_slots,
            "document_inflight": args.document_inflight,
            "window_prefetch": args.window_prefetch,
            "download_workers": args.download_workers,
            "upload_workers": args.upload_workers,
            "block_prepare_workers": args.block_prepare_workers,
            "render_workers": args.render_workers,
            "finalize_workers": args.finalize_workers,
            "archive_workers": args.archive_workers,
            "multipart_chunksize_mib": args.multipart_chunksize_mib,
            "multipart_max_concurrency": args.multipart_max_concurrency,
        }
        actors[name] = MinerUServiceActor.options(
            name=f"mineru-dual-{name}-{int(time.time())}",
            num_cpus=32,
            resources={"NPU": 1, "MINERU_NPU": 1},
        ).remote(config)

    started = ray.get([actor.start.remote() for actor in actors.values()])
    run_started = time.time()
    try:
        results, assignments = coordinator_loop(
            actors, manifest["documents"], args.run_dir / "coordinator.jsonl"
        )
    finally:
        ray.get([actor.shutdown.remote() for actor in actors.values()])

    by_id = {row["document_id"]: row for row in manifest["documents"]}
    validation = []
    for result in results:
        raw_expected = by_id[result["document_id"]].get("page_count")
        expected = int(raw_expected) if raw_expected is not None else None
        actual = int(result.get("page_count", 0))
        validation.append(
            {
                "document_id": result["document_id"],
                "expected_pages": expected,
                "actual_pages": actual,
                "page_count_matches": expected is None or expected == actual,
            }
        )
    elapsed = time.time() - run_started
    total_pages = sum(row.get("page_count", 0) for row in results)
    summary = {
        "status": "success" if (
            len(results) == len(manifest["documents"])
            and all(row.get("status") == "success" for row in results)
            and all(row["page_count_matches"] for row in validation)
        ) else "partial",
        "elapsed_seconds": round(elapsed, 3),
        "pdf_count": len(results),
        "success_count": sum(row.get("status") == "success" for row in results),
        "page_count": total_pages,
        "pages_per_second": total_pages / elapsed if elapsed else 0.0,
        "services_started": started,
        "assignments": assignments,
        "validation": validation,
        "results": sorted(results, key=lambda row: row["document_id"]),
        "run_config": {
            "service_count": args.service_count,
            "inference_slots": args.inference_slots,
            "document_inflight_per_service": args.document_inflight,
            "window_prefetch": args.window_prefetch,
            "download_workers": args.download_workers,
            "upload_workers": args.upload_workers,
            "block_prepare_workers": args.block_prepare_workers,
            "render_workers": args.render_workers,
            "finalize_workers": args.finalize_workers,
            "archive_workers": args.archive_workers,
            "multipart_chunksize_mib": args.multipart_chunksize_mib,
            "multipart_max_concurrency": args.multipart_max_concurrency,
        },
    }
    summary_path = args.run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    s3_client().upload_file(
        str(summary_path), OUTPUT_BUCKET, f"{args.output_prefix}/_SUMMARY.json"
    )
    emit_dagster_event(
        "BATCH_SUCCEEDED" if summary["status"] == "success" else "BATCH_FAILED",
        batch_id=args.batch_id,
        ray_job_id=args.ray_job_id,
        output_prefix=args.output_prefix,
        pdf_count=summary["pdf_count"],
        success_count=summary["success_count"],
        page_count=summary["page_count"],
        elapsed_seconds=summary["elapsed_seconds"],
        pages_per_second=summary["pages_per_second"],
    )
    print(json.dumps({key: summary[key] for key in (
        "status", "elapsed_seconds", "pdf_count", "success_count", "page_count", "pages_per_second"
    )}, ensure_ascii=False, indent=2))
    if summary["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
