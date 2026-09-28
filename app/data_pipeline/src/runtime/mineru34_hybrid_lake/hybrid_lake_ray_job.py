"""Run MinerU 3.4 Hybrid high over an S3 manifest using two sticky NPU services.

Each actor is bound to the dual-NPU worker node and processes one PDF at a
time.  This preserves one official Hybrid parser per vLLM service while the
two services, S3 transfers, packaging and coordinator bookkeeping overlap.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import shutil
import subprocess
import tarfile
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import ray
import zstandard
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


IMAGE_SUFFIXES = {".bmp", ".jpeg", ".jpg", ".png", ".tif", ".tiff", ".webp"}
TEXT_SUFFIXES = {".json", ".md", ".txt"}
MULTIPART_SIZE = 16 * 1024 * 1024


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}, retries={"max_attempts": 4, "mode": "standard"}),
    )


def transfer_config() -> TransferConfig:
    return TransferConfig(
        multipart_threshold=MULTIPART_SIZE,
        multipart_chunksize=MULTIPART_SIZE,
        max_concurrency=4,
        use_threads=True,
    )


def archive_images(output_dir: Path, package_dir: Path) -> tuple[Path | None, int]:
    images = [path for path in sorted(output_dir.rglob("*")) if path.is_file() and path.suffix.lower() in IMAGE_SUFFIXES]
    if not images:
        return None, 0
    archive = package_dir / "images.tar.zst"
    with archive.open("wb") as raw:
        with zstandard.ZstdCompressor(level=3, threads=2).stream_writer(raw) as compressed:
            with tarfile.open(fileobj=compressed, mode="w|") as tar:
                for image in images:
                    tar.add(image, arcname=image.relative_to(output_dir).as_posix())
    return archive, len(images)


def count_pages(parsed_dir: Path) -> int | None:
    for path in parsed_dir.rglob("*.json"):
        try:
            payload = json.loads(path.read_text(errors="replace"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(payload, dict) and isinstance(payload.get("pdf_info"), list):
            return len(payload["pdf_info"])
    return None


@ray.remote(num_cpus=32, resources={"NPU": 1, "MINERU_NPU": 1})
class HybridLakeActor:
    """One sequential official Hybrid client pinned to one local vLLM service."""

    def __init__(self, name: str, logical_id: int, endpoint: str, cpu_ids: list[int]) -> None:
        self.name = name
        self.logical_id = logical_id
        self.endpoint = endpoint
        self.cpu_ids = cpu_ids
        self.s3 = s3_client()
        self.upload_config = transfer_config()
        try:
            os.sched_setaffinity(0, set(cpu_ids))
        except (AttributeError, OSError):
            pass

    def health(self) -> dict[str, Any]:
        import urllib.request

        started = time.time()
        with urllib.request.urlopen(f"{self.endpoint}/health", timeout=10) as response:
            response.read()
        return {
            "service": self.name,
            "endpoint": self.endpoint,
            "logical_id": self.logical_id,
            "node_id": ray.get_runtime_context().get_node_id(),
            "health_seconds": round(time.time() - started, 3),
        }

    def parse(self, document: dict[str, Any], input_bucket: str, output_bucket: str, output_prefix: str) -> dict[str, Any]:
        started = time.time()
        document_id = document["document_id"]
        root = Path(tempfile.mkdtemp(prefix=f"mineru34-hybrid-{self.name}-{document_id}-"))
        source = root / "input.pdf"
        parsed = root / "parsed"
        package = root / "package"
        package.mkdir()
        target = f"{output_prefix.rstrip('/')}/{document_id}"
        result: dict[str, Any] = {
            "document_id": document_id,
            "input": {"bucket": input_bucket, "key": document["object_key"], "etag": document.get("etag")},
            "output_prefix": target,
            "service": self.name,
            "logical_id": self.logical_id,
            "endpoint": self.endpoint,
            "started_at": utc_now(),
            "status": "failed",
            "timings": {},
        }
        try:
            phase = time.time()
            self.s3.download_file(input_bucket, document["object_key"], str(source))
            result["timings"]["download_seconds"] = round(time.time() - phase, 3)

            env = os.environ.copy()
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
                env.pop(key, None)
            no_proxy = os.environ.get(
                "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
            )
            env.update({
                "ASCEND_VISIBLE_DEVICES": str(self.logical_id),
                "ASCEND_RT_VISIBLE_DEVICES": str(self.logical_id),
                "ASCEND_DEVICE_ID": str(self.logical_id),
                "MINERU_MODEL_SOURCE": "local",
                "NO_PROXY": no_proxy,
                "no_proxy": no_proxy,
            })
            command = [
                "mineru", "--path", str(source), "--output", str(parsed),
                "--backend", "hybrid-http-client", "--url", self.endpoint,
                "--method", "auto", "--effort", "high", "--image-analysis", "true",
                "--client-side-output-generation", "false",
            ]
            phase = time.time()
            completed = subprocess.run(command, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=7200, check=False)
            result["timings"]["hybrid_parse_seconds"] = round(time.time() - phase, 3)
            result["mineru_command"] = command
            result["mineru_returncode"] = completed.returncode
            result["mineru_log_tail"] = completed.stdout[-6000:]
            if completed.returncode:
                raise RuntimeError(f"MinerU exited with {completed.returncode}")

            markdown_text = "\n".join(path.read_text(errors="replace") for path in parsed.rglob("*.md"))
            result["image_analysis"] = {
                "markdown_count": len(list(parsed.rglob("*.md"))),
                "details_block_count": markdown_text.count("<details>"),
                "verified": "<details>" in markdown_text,
            }
            result["page_count"] = count_pages(parsed)

            phase = time.time()
            archive, image_count = archive_images(parsed, package)
            artifacts: list[tuple[Path, str]] = []
            for path in sorted(parsed.rglob("*")):
                if path.is_file() and path.suffix.lower() in TEXT_SUFFIXES:
                    artifacts.append((path, f"artifacts/{path.relative_to(parsed).as_posix()}"))
            if archive:
                artifacts.append((archive, "artifacts/images.tar.zst"))
            result["timings"]["archive_seconds"] = round(time.time() - phase, 3)
            result["image_count"] = image_count
            result["artifact_count"] = len(artifacts)

            phase = time.time()
            def upload_one(item: tuple[Path, str]) -> None:
                path, relative_key = item
                self.s3.upload_file(str(path), output_bucket, f"{target}/{relative_key}", Config=self.upload_config)
            with ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"upload-{self.name}") as pool:
                list(pool.map(upload_one, artifacts))
            result["timings"]["upload_seconds"] = round(time.time() - phase, 3)
            result["status"] = "success"
        except Exception as exc:
            result["error"] = repr(exc)
        finally:
            result["elapsed_seconds"] = round(time.time() - started, 3)
            result["finished_at"] = utc_now()
            marker = "_SUCCESS.json" if result["status"] == "success" else "_FAILED.json"
            self.s3.put_object(Bucket=output_bucket, Key=f"{target}/{marker}", Body=json.dumps(result, ensure_ascii=False, indent=2).encode(), ContentType="application/json")
            shutil.rmtree(root, ignore_errors=True)
        return result

    def parse_pair(
        self,
        documents: list[dict[str, Any]],
        input_bucket: str,
        output_bucket: str,
        output_prefix: str,
        inference_slots: int,
        queue_size: int,
        timeout_seconds: int,
    ) -> list[dict[str, Any]]:
        """Run a bounded document batch through one shared Hybrid window scheduler."""
        root = Path(tempfile.mkdtemp(prefix=f"mineru34-hybrid-pipeline-{self.name}-"))
        sources = root / "sources"
        parsed = root / "parsed"
        sources.mkdir()
        specs = []
        download_seconds: dict[str, float] = {}
        pair_started = time.time()
        try:
            def download(document: dict[str, Any]) -> dict[str, str]:
                started = time.time()
                source = sources / f"{document['document_id']}.pdf"
                self.s3.download_file(input_bucket, document["object_key"], str(source))
                download_seconds[document["document_id"]] = round(time.time() - started, 3)
                return {"document_id": document["document_id"], "source": str(source)}

            with ThreadPoolExecutor(max_workers=len(documents), thread_name_prefix=f"download-{self.name}") as pool:
                specs = list(pool.map(download, documents))
            specs_path = root / "specs.json"
            specs_path.write_text(json.dumps(specs, ensure_ascii=False), encoding="utf-8")
            result_path = root / "pipeline-result.json"
            profile_path = root / "window-profile.jsonl"
            env = os.environ.copy()
            for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
                env.pop(key, None)
            env.update({
                "ASCEND_VISIBLE_DEVICES": str(self.logical_id),
                "ASCEND_RT_VISIBLE_DEVICES": str(self.logical_id),
                "ASCEND_DEVICE_ID": str(self.logical_id),
                "MINERU_MODEL_SOURCE": "local",
                "NO_PROXY": os.environ.get(
                    "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
                ),
                "no_proxy": os.environ.get(
                    "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
                ),
            })
            command = [
                "python3", "-m", "runtime.mineru34_hybrid_lake.hybrid_pair_runner",
                "--specs", str(specs_path), "--output", str(parsed),
                "--result", str(result_path), "--profile", str(profile_path),
                "--server-url", self.endpoint, "--inference-slots", str(inference_slots),
                "--queue-size", str(queue_size), "--cpu-workers", "16",
            ]
            parse_started = time.time()
            process = subprocess.Popen(
                command,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                start_new_session=True,
            )
            try:
                stdout, _ = process.communicate(timeout=timeout_seconds)
            except subprocess.TimeoutExpired as exc:
                # The runner starts multiprocessing children.  Killing only its
                # parent leaves those children resident after a timeout.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    stdout, _ = process.communicate(timeout=30)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    stdout, _ = process.communicate()
                raise TimeoutError(
                    f"Hybrid pipeline exceeded {timeout_seconds}s: {stdout[-8000:]}"
                ) from exc
            parse_seconds = round(time.time() - parse_started, 3)
            if process.returncode:
                raise RuntimeError(f"Hybrid pipeline exited with {process.returncode}: {stdout[-8000:]}")
            pipeline_rows = {row["document_id"]: row for row in json.loads(result_path.read_text())}
            profile_text = profile_path.read_text(errors="replace") if profile_path.exists() else ""
            results = []
            for document in documents:
                document_id = document["document_id"]
                output_dir = Path(pipeline_rows[document_id]["output_dir"])
                package = root / f"package-{document_id}"
                package.mkdir()
                target = f"{output_prefix.rstrip('/')}/{document_id}"
                result = {
                    "document_id": document_id, "status": "failed", "service": self.name,
                    "logical_id": self.logical_id, "endpoint": self.endpoint,
                    "input": {"bucket": input_bucket, "key": document["object_key"], "etag": document.get("etag")},
                    "output_prefix": target, "started_at": utc_now(),
                    "timings": {"download_seconds": download_seconds[document_id], "hybrid_pipeline_pair_seconds": parse_seconds},
                    "page_count": pipeline_rows[document_id]["page_count"],
                    "pipeline": {"inference_slots": inference_slots, "document_inflight": len(documents), "window_prefetch": 1, "queue_size": queue_size},
                }
                try:
                    markdown_text = "\n".join(path.read_text(errors="replace") for path in output_dir.rglob("*.md"))
                    result["image_analysis"] = {"markdown_count": len(list(output_dir.rglob("*.md"))), "details_block_count": markdown_text.count("<details>"), "verified": "<details>" in markdown_text}
                    phase = time.time()
                    archive, image_count = archive_images(output_dir, package)
                    artifacts = [(path, f"artifacts/{path.relative_to(output_dir).as_posix()}") for path in sorted(output_dir.rglob("*")) if path.is_file() and path.suffix.lower() in TEXT_SUFFIXES]
                    if archive:
                        artifacts.append((archive, "artifacts/images.tar.zst"))
                    profile_copy = package / "window-profile.jsonl"
                    profile_copy.write_text(profile_text, encoding="utf-8")
                    artifacts.append((profile_copy, "artifacts/window-profile.jsonl"))
                    result["timings"]["archive_seconds"] = round(time.time() - phase, 3)
                    result["image_count"] = image_count
                    phase = time.time()
                    def upload_one(item: tuple[Path, str]) -> None:
                        path, relative_key = item
                        self.s3.upload_file(str(path), output_bucket, f"{target}/{relative_key}", Config=self.upload_config)
                    with ThreadPoolExecutor(max_workers=4, thread_name_prefix=f"upload-{self.name}") as pool:
                        list(pool.map(upload_one, artifacts))
                    result["timings"]["upload_seconds"] = round(time.time() - phase, 3)
                    result["status"] = "success"
                except Exception as exc:
                    result["error"] = repr(exc)
                result["elapsed_seconds"] = round(time.time() - pair_started, 3)
                result["finished_at"] = utc_now()
                marker = "_SUCCESS.json" if result["status"] == "success" else "_FAILED.json"
                self.s3.put_object(Bucket=output_bucket, Key=f"{target}/{marker}", Body=json.dumps(result, ensure_ascii=False, indent=2).encode(), ContentType="application/json")
                results.append(result)
            return results
        except Exception as exc:
            results = []
            for document in documents:
                target = f"{output_prefix.rstrip('/')}/{document['document_id']}"
                result = {"document_id": document["document_id"], "status": "failed", "service": self.name, "error": repr(exc), "elapsed_seconds": round(time.time()-pair_started, 3)}
                self.s3.put_object(Bucket=output_bucket, Key=f"{target}/_FAILED.json", Body=json.dumps(result, ensure_ascii=False, indent=2).encode(), ContentType="application/json")
                results.append(result)
            return results
        finally:
            shutil.rmtree(root, ignore_errors=True)


def load_manifest(output_bucket: str, manifest_key: str) -> list[dict[str, Any]]:
    payload = s3_client().get_object(Bucket=output_bucket, Key=manifest_key)["Body"].read()
    return json.loads(payload)["documents"]


def is_successful(output_bucket: str, output_prefix: str, document_id: str) -> bool:
    try:
        s3_client().head_object(Bucket=output_bucket, Key=f"{output_prefix.rstrip('/')}/{document_id}/_SUCCESS.json")
        return True
    except s3_client().exceptions.ClientError as exc:
        if exc.response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}:
            return False
        raise


def write_progress(run_dir: Path, state: dict[str, Any]) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "progress.json").write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
    with (run_dir / "progress.jsonl").open("a", encoding="utf-8") as output:
        output.write(json.dumps(state, ensure_ascii=False) + "\n")


def dual_worker_node_id() -> str:
    candidates = [node for node in ray.nodes() if node["Alive"] and node.get("Resources", {}).get("MINERU_NPU", 0) >= 2]
    if len(candidates) != 1:
        raise RuntimeError(f"expected exactly one live dual MinerU node, found {len(candidates)}")
    return candidates[0]["NodeID"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--input-bucket", required=True)
    parser.add_argument("--output-bucket", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--run-dir", required=True, type=Path)
    parser.add_argument("--service-a-logical-id", type=int, default=14)
    parser.add_argument("--service-b-logical-id", type=int, default=15)
    parser.add_argument("--pipeline", action="store_true")
    parser.add_argument("--inference-slots", type=int, default=4)
    parser.add_argument("--document-inflight", type=int, default=5)
    parser.add_argument("--queue-size", type=int, default=6)
    parser.add_argument("--pair-timeout-seconds", type=int, default=1200)
    parser.add_argument("--batch-timeout-seconds", type=int, default=1320)
    parser.add_argument("--max-document-retries", type=int, default=2)
    args = parser.parse_args()
    if args.inference_slots < 1 or args.document_inflight < 1:
        parser.error("inference-slots and document-inflight must be positive")
    if args.queue_size < args.inference_slots:
        parser.error("queue-size must be at least inference-slots")
    if args.pair_timeout_seconds < 60:
        parser.error("pair-timeout-seconds must be at least 60")
    if args.batch_timeout_seconds <= args.pair_timeout_seconds:
        parser.error("batch-timeout-seconds must exceed pair-timeout-seconds")
    if args.max_document_retries < 0:
        parser.error("max-document-retries cannot be negative")

    ray.init(address="auto", log_to_driver=True)
    documents = load_manifest(args.output_bucket, args.manifest_key)
    skipped = [doc for doc in documents if is_successful(args.output_bucket, args.output_prefix, doc["document_id"])]
    pending = [doc for doc in documents if doc not in skipped]
    service_pending: dict[str, list[dict[str, Any]]] = {"A": [], "B": []}
    estimated_load = {"A": 0, "B": 0}
    for document in sorted(pending, key=lambda row: row.get("estimated_page_count", 1), reverse=True):
        service_name = min(estimated_load, key=estimated_load.get)
        service_pending[service_name].append(document)
        estimated_load[service_name] += document.get("estimated_page_count", 1)
    node_id = dual_worker_node_id()
    affinity = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
    actor_config = {
        "A": (args.service_a_logical_id, "http://127.0.0.1:30001", list(range(0, 32))),
        "B": (args.service_b_logical_id, "http://127.0.0.1:30002", list(range(32, 64))),
    }

    def create_actor(service_name: str) -> Any:
        logical_id, endpoint, cpu_ids = actor_config[service_name]
        actor = HybridLakeActor.options(scheduling_strategy=affinity).remote(
            service_name, logical_id, endpoint, cpu_ids
        )
        ray.get(actor.health.remote(), timeout=20)
        return actor

    actors = {service_name: create_actor(service_name) for service_name in ("A", "B")}
    health = ray.get([actors[service_name].health.remote() for service_name in ("A", "B")])
    active: dict[Any, tuple[Any, str, list[dict[str, Any]], float]] = {}
    results: list[dict[str, Any]] = []
    retries: dict[str, int] = {}
    run_started = time.time()

    def submit_next(actor: Any, service_name: str) -> bool:
        """Keep an idle service busy, including documents retried from its peer."""
        service_queue = service_pending[service_name]
        if not service_queue:
            service_queue = service_pending["B" if service_name == "A" else "A"]
        if not service_queue:
            return False
        batch_size = args.document_inflight if args.pipeline else 1
        documents_for_actor = [service_queue.pop(0) for _ in range(min(batch_size, len(service_queue)))]
        if args.pipeline:
            reference = actor.parse_pair.remote(
                documents_for_actor,
                args.input_bucket,
                args.output_bucket,
                args.output_prefix,
                args.inference_slots,
                args.queue_size,
                args.pair_timeout_seconds,
            )
        else:
            reference = actor.parse.remote(documents_for_actor[0], args.input_bucket, args.output_bucket, args.output_prefix)
        active[reference] = (actor, service_name, documents_for_actor, time.monotonic())
        return True

    for service_name in ("A", "B"):
        submit_next(actors[service_name], service_name)
    while active:
        current_state = {
            "updated_at": utc_now(), "total": len(documents), "skipped": len(skipped), "completed": len(results),
            "pending": sum(len(queue) for queue in service_pending.values()),
            "active": [
                {"service": service_name, "document_id": document["document_id"], "key": document["object_key"]}
                for _, (_, service_name, active_documents, _) in active.items()
                for document in active_documents
            ],
            "queued_by_service": {service_name: len(queue) for service_name, queue in service_pending.items()},
            "retry_counts": retries,
        }
        write_progress(args.run_dir, current_state)
        ready, _ = ray.wait(list(active), num_returns=1, timeout=2)
        for ref in ready:
            actor, service_name, submitted_documents, _ = active.pop(ref)
            try:
                completed_result = ray.get(ref)
            except Exception as exc:
                completed_result = [
                    {
                        "document_id": document["document_id"],
                        "status": "failed",
                        "service": service_name,
                        "error": f"Ray actor task failed: {exc!r}",
                    }
                    for document in submitted_documents
                ]
            completed_rows = completed_result if args.pipeline else [completed_result]
            rows_by_document = {row["document_id"]: row for row in completed_rows}
            for document in submitted_documents:
                row = rows_by_document.get(document["document_id"])
                if row and row.get("status") == "success":
                    results.append(row)
                    continue
                attempts = retries.get(document["document_id"], 0)
                if attempts < args.max_document_retries:
                    retries[document["document_id"]] = attempts + 1
                    alternate = "B" if service_name == "A" else "A"
                    service_pending[alternate].append(document)
                else:
                    results.append(row or {
                        "document_id": document["document_id"],
                        "status": "failed",
                        "service": service_name,
                        "error": "missing result after retry limit",
                    })
            submit_next(actor, service_name)

        now = time.monotonic()
        expired = [
            (ref, metadata)
            for ref, metadata in active.items()
            if now - metadata[3] > args.batch_timeout_seconds
        ]
        for ref, (actor, service_name, submitted_documents, _) in expired:
            active.pop(ref, None)
            ray.cancel(ref, force=True, recursive=True)
            ray.kill(actor, no_restart=True)
            alternate = "B" if service_name == "A" else "A"
            for document in submitted_documents:
                attempts = retries.get(document["document_id"], 0)
                if attempts < args.max_document_retries:
                    retries[document["document_id"]] = attempts + 1
                    service_pending[alternate].append(document)
                else:
                    results.append({
                        "document_id": document["document_id"],
                        "status": "failed",
                        "service": service_name,
                        "error": f"batch exceeded {args.batch_timeout_seconds}s timeout",
                    })
            actors[service_name] = create_actor(service_name)
            submit_next(actors[service_name], service_name)

    results.sort(key=lambda row: row["document_id"])
    successful = [row for row in results if row["status"] == "success"]
    parsed_pages = sum(row.get("page_count") or 0 for row in successful)
    elapsed = time.time() - run_started
    summary = {
        "created_at": utc_now(), "status": "success" if len(successful) + len(skipped) == len(documents) else "partial",
        "backend": "hybrid-http-client", "effort": "high", "image_analysis": True,
        "pipeline": args.pipeline, "inference_slots": args.inference_slots,
        "document_inflight": args.document_inflight, "queue_size": args.queue_size,
        "pair_timeout_seconds": args.pair_timeout_seconds,
        "batch_timeout_seconds": args.batch_timeout_seconds,
        "max_document_retries": args.max_document_retries,
        "pdf_count": len(documents), "skipped_count": len(skipped), "success_count": len(successful), "failed_count": len(results) - len(successful),
        "image_analysis_verified_count": sum(row.get("image_analysis", {}).get("verified", False) for row in successful),
        "page_count": parsed_pages, "elapsed_seconds": round(elapsed, 3), "pages_per_second": round(parsed_pages / elapsed, 4) if elapsed and parsed_pages else None,
        "health": health, "results": results,
    }
    args.run_dir.mkdir(parents=True, exist_ok=True)
    (args.run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    s3_client().put_object(Bucket=args.output_bucket, Key=f"{args.output_prefix.rstrip('/')}/_SUMMARY.json", Body=json.dumps(summary, ensure_ascii=False, indent=2).encode(), ContentType="application/json")
    print(json.dumps(summary, ensure_ascii=False, indent=2), flush=True)
    if summary["status"] != "success":
        raise SystemExit(1)


if __name__ == "__main__":
    main()
