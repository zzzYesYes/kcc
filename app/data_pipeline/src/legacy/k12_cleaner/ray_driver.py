from __future__ import annotations

import argparse
import json
import os
import socket
import time
from datetime import datetime, timezone
from pathlib import PurePosixPath

import daft
import ray
from daft.io import IOConfig, S3Config

from .core import CLEANER_VERSION, canonical_hash, parse_s3_uri, run_stage, s3_client


def emit(event: str, **payload) -> None:
    value = {"event": event, "timestamp": datetime.now(timezone.utc).isoformat(), **payload}
    print("DAGSTER_EVENT " + json.dumps(value, ensure_ascii=False, separators=(",", ":")), flush=True)


def read_json(uri: str) -> dict:
    bucket, key = parse_s3_uri(uri)
    return json.loads(s3_client().get_object(Bucket=bucket, Key=key)["Body"].read())


def write_json(uri: str, value: dict) -> None:
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(value, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )


def daft_io_config() -> IOConfig:
    return IOConfig(
        s3=S3Config(
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
            access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            force_virtual_addressing=False,
        )
    )


def scan_manifest(args: argparse.Namespace) -> None:
    started = time.time()
    glob_uri = f"s3://{args.parsed_bucket}/{args.parsed_prefix.strip('/')}/{args.markdown_glob.lstrip('/')}"
    emit("MANIFEST_SCAN_STARTED", glob_uri=glob_uri, count=args.count)
    frame = daft.from_glob_path(glob_uri, io_config=daft_io_config())
    selected = frame.select("path", "size").sort("path")
    rows = (selected.limit(args.count) if args.count > 0 else selected).to_pylist()
    documents = []
    for item in rows:
        path = item["path"]
        key_parts = PurePosixPath(parse_s3_uri(path)[1]).parts
        document_id = next((part for part in key_parts if part.startswith("pdf-")), PurePosixPath(path).stem)
        head = s3_client().head_object(Bucket=args.parsed_bucket, Key=parse_s3_uri(path)[1])
        documents.append(
            {
                "document_id": document_id,
                "source_uri": path,
                "current_uri": path,
                "source_etag": head["ETag"].strip('"'),
                "source_size_bytes": int(item["size"]),
                "output_prefix": f"s3://{args.output_bucket}/{args.output_prefix.strip('/')}",
            }
        )
    if args.count > 0 and len(documents) != args.count:
        raise RuntimeError(f"requested {args.count} Markdown documents, Daft found {len(documents)}")
    manifest = {
        "batch_id": args.batch_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "cleaner_version": CLEANER_VERSION,
        "source_glob": glob_uri,
        "document_count": len(documents),
        "documents": documents,
    }
    write_json(args.manifest_uri, manifest)
    emit(
        "MANIFEST_SCAN_SUCCEEDED",
        document_count=len(documents),
        input_bytes=sum(row["source_size_bytes"] for row in documents),
        elapsed_seconds=round(time.time() - started, 3),
        manifest_uri=args.manifest_uri,
    )


def stage_input_rows(manifest: dict, stage: str) -> list[dict]:
    rows = []
    for source in manifest["documents"]:
        row = dict(source)
        output_prefix = source["output_prefix"]
        if stage == "step_job2":
            row["current_uri"] = f"{output_prefix}/_stages/step_job1/{source['document_id']}.md"
        elif stage == "step_job3":
            row["current_uri"] = f"{output_prefix}/_stages/step_job2/{source['document_id']}.md"
            row["quality_uri"] = f"{output_prefix}/_stages/step_job2/{source['document_id']}.quality.json"
        rows.append(row)
    return rows


@ray.remote
def process_document(stage: str, row: dict, config: dict, batch_id: str, run_id: str) -> dict:
    started = time.time()
    host = socket.gethostname()
    emit("DOCUMENT_STAGE_STARTED", stage=stage, document_id=row["document_id"], worker=host, pid=os.getpid())
    try:
        result = run_stage(stage, row, config, batch_id, run_id).to_dict()
        emit(
            "DOCUMENT_STAGE_SUCCEEDED",
            stage=stage,
            document_id=row["document_id"],
            worker=host,
            input_count=result["input_count"],
            output_count=result["output_count"],
            elapsed_seconds=result["elapsed_seconds"],
        )
        return result
    except Exception as exc:
        result = {
            "status": "failed",
            "stage": stage,
            "document_id": row["document_id"],
            "input_uri": row["current_uri"],
            "error": repr(exc),
            "elapsed_seconds": round(time.time() - started, 3),
            "run_id": run_id,
        }
        output_prefix = row["output_prefix"].rstrip("/")
        write_json(
            f"{output_prefix}/_control/{batch_id}/stages/{stage}/{row['document_id']}.json",
            result | {"completed_at": datetime.now(timezone.utc).isoformat()},
        )
        emit("DOCUMENT_STAGE_FAILED", **result)
        return result


def run_parallel_stage(args: argparse.Namespace) -> None:
    started = time.time()
    manifest = read_json(args.manifest_uri)
    config = json.loads(args.stage_config_json)
    rows = stage_input_rows(manifest, args.stage)
    ray.init(address="auto", log_to_driver=True)
    remote = process_document.options(num_cpus=args.cpus_per_task, max_retries=args.max_retries)
    pending_rows = iter(rows)
    inflight: list[ray.ObjectRef] = []
    results: list[dict] = []
    emit(
        "STAGE_STARTED",
        stage=args.stage,
        document_count=len(rows),
        parallelism=args.parallelism,
        cpus_per_task=args.cpus_per_task,
        config=config,
    )

    while len(inflight) < args.parallelism:
        try:
            row = next(pending_rows)
        except StopIteration:
            break
        inflight.append(remote.remote(args.stage, row, config, manifest["batch_id"], args.run_id))
    while inflight:
        ready, inflight = ray.wait(inflight, num_returns=1, timeout=args.task_timeout_seconds)
        if not ready:
            for ref in inflight:
                ray.cancel(ref, force=True)
            raise TimeoutError(f"{args.stage} made no completion progress for {args.task_timeout_seconds}s")
        results.append(ray.get(ready[0]))
        try:
            row = next(pending_rows)
            inflight.append(remote.remote(args.stage, row, config, manifest["batch_id"], args.run_id))
        except StopIteration:
            pass

    results.sort(key=lambda result: result["document_id"])
    success_count = sum(row["status"] == "success" for row in results)
    elapsed = round(time.time() - started, 3)
    summary = {
        "status": "success" if success_count == len(results) else "partial",
        "stage": args.stage,
        "batch_id": manifest["batch_id"],
        "run_id": args.run_id,
        "document_count": len(results),
        "success_count": success_count,
        "failed_count": len(results) - success_count,
        "input_count": sum(int(row.get("input_count", 0)) for row in results),
        "output_count": sum(int(row.get("output_count", 0)) for row in results),
        "elapsed_seconds": elapsed,
        "documents_per_second": round(len(results) / max(elapsed, 0.001), 6),
        "parallelism": args.parallelism,
        "cpus_per_task": args.cpus_per_task,
        "config": config,
        "config_hash": canonical_hash(config),
        "results": results,
    }
    output_prefix = manifest["documents"][0]["output_prefix"]
    summary_uri = f"{output_prefix}/_control/{manifest['batch_id']}/stages/{args.stage}/_SUMMARY.json"
    write_json(summary_uri, summary)
    if args.stage == "step_job3":
        write_json(f"{output_prefix}/_SUMMARY.json", summary | {"cleaner_version": CLEANER_VERSION})
    emit(
        "STAGE_SUCCEEDED" if summary["status"] == "success" else "STAGE_FAILED",
        stage=args.stage,
        document_count=len(results),
        success_count=success_count,
        input_count=summary["input_count"],
        output_count=summary["output_count"],
        elapsed_seconds=elapsed,
        summary_uri=summary_uri,
    )
    if summary["status"] != "success":
        raise SystemExit(1)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description="Daft + Ray staged cleaner for MinerU Markdown.")
    commands = root.add_subparsers(dest="command", required=True)
    scan = commands.add_parser("scan")
    scan.add_argument("--batch-id", required=True)
    scan.add_argument("--count", type=int, required=True)
    scan.add_argument("--parsed-bucket", required=True)
    scan.add_argument("--parsed-prefix", required=True)
    scan.add_argument("--output-bucket", required=True)
    scan.add_argument("--output-prefix", required=True)
    scan.add_argument("--markdown-glob", default="**/*.md")
    scan.add_argument("--manifest-uri", required=True)
    stage = commands.add_parser("stage")
    stage.add_argument("--stage", choices=("step_job1", "step_job2", "step_job3"), required=True)
    stage.add_argument("--manifest-uri", required=True)
    stage.add_argument("--run-id", required=True)
    stage.add_argument("--parallelism", type=int, required=True)
    stage.add_argument("--cpus-per-task", type=float, required=True)
    stage.add_argument("--max-retries", type=int, required=True)
    stage.add_argument("--task-timeout-seconds", type=float, required=True)
    stage.add_argument("--stage-config-json", required=True)
    return root


def main() -> None:
    args = parser().parse_args()
    if args.command == "scan":
        scan_manifest(args)
    else:
        run_parallel_stage(args)


if __name__ == "__main__":
    main()
