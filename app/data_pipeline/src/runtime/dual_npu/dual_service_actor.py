"""Ray actor for one sticky-PDF MinerU service on a dual-NPU worker."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import tempfile
import time
from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ray


def parse_cpu_set(value: str) -> set[int]:
    cpus: set[int] = set()
    for item in value.split(","):
        if "-" in item:
            start, end = (int(part) for part in item.split("-", 1))
            cpus.update(range(start, end + 1))
        else:
            cpus.add(int(item))
    return cpus


def read_rss_bytes(pid: int) -> int | None:
    try:
        for line in Path(f"/proc/{pid}/status").read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
    except (FileNotFoundError, ProcessLookupError):
        return None
    return None


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


@ray.remote(max_concurrency=32)
class MinerUServiceActor:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.name = config["name"]
        self.server_url = config["server_url"]
        self.worker_ip = socket.gethostbyname(socket.gethostname())
        self.actor_pid = os.getpid()
        self.npu_logical_id = int(config["logical_id"])
        self.cpu_set = parse_cpu_set(config["cpu_set"])
        os.sched_setaffinity(0, self.cpu_set)
        self.run_dir = Path(config["run_dir"]) / f"service-{self.name}"
        self.run_dir.mkdir(parents=True, exist_ok=True)
        self.tasks: dict[str, asyncio.Task[dict[str, Any]]] = {}
        self.documents: dict[str, dict[str, Any]] = {}
        self.results: list[dict[str, Any]] = []
        self.healthy = False
        self.health_failures = 0
        self.started_at = time.time()
        self.total_pages = 0
        self.total_parse_seconds = 0.0
        self.monitor_stop = asyncio.Event()
        self.monitor_task: asyncio.Task[None] | None = None
        self.scheduler = None
        self.pools = None
        self.predictor = None

    async def start(self) -> dict[str, Any]:
        from mineru.backend.vlm.vlm_analyze import _get_model_async

        from runtime.mineru_pipeline.production_30_runner import PipelineExecutors, fetch_text, run_in_pool
        from runtime.mineru_pipeline.official_window_pipeline import (
            GlobalWindowScheduler,
            install_layout_content_gap_tracer,
        )

        await asyncio.to_thread(fetch_text, f"{self.server_url}/health", 10.0)
        pool_args = Namespace(
            download_workers=self.config["download_workers"],
            upload_workers=self.config["upload_workers"],
            block_prepare_workers=self.config["block_prepare_workers"],
            render_workers=self.config["render_workers"],
            finalize_workers=self.config["finalize_workers"],
            archive_workers=self.config["archive_workers"],
        )
        self.pools = PipelineExecutors(pool_args)
        self.predictor = await _get_model_async("http-client", None, self.server_url)
        self.predictor.executor = self.pools.block_prepare
        if hasattr(self.predictor, "helper"):
            self.predictor.helper.executor = self.pools.block_prepare
        install_layout_content_gap_tracer(self.predictor, self.pools.metrics)
        self.scheduler = GlobalWindowScheduler(
            inference_slots=self.config["inference_slots"],
            max_inference_slots=self.config["inference_slots"],
            queue_size=self.config["document_inflight"],
        )
        await self.scheduler.__aenter__()
        self.healthy = True
        self.monitor_task = asyncio.create_task(self._monitor())
        return {
            "service": self.name,
            "server_url": self.server_url,
            "cpu_affinity": sorted(os.sched_getaffinity(0)),
            "worker_ip": self.worker_ip,
            "actor_pid": self.actor_pid,
            "npu_logical_id": self.npu_logical_id,
            "healthy": True,
            "run_config": {
                key: self.config[key]
                for key in (
                    "inference_slots",
                    "document_inflight",
                    "window_prefetch",
                    "download_workers",
                    "upload_workers",
                    "block_prepare_workers",
                    "render_workers",
                    "finalize_workers",
                    "archive_workers",
                    "multipart_chunksize_mib",
                    "multipart_max_concurrency",
                )
            },
        }

    async def submit(self, document: dict[str, Any]) -> dict[str, Any]:
        if not self.healthy:
            return {"accepted": False, "reason": "service_unhealthy"}
        inflight = sum(not task.done() for task in self.tasks.values())
        if inflight >= self.config["document_inflight"]:
            return {"accepted": False, "reason": "document_capacity_full"}
        document_id = document["document_id"]
        if document_id in self.tasks:
            return {"accepted": False, "reason": "duplicate_document_id"}
        self.documents[document_id] = document
        self.tasks[document_id] = asyncio.create_task(self._process_document(document))
        return {"accepted": True, "service": self.name, "document_id": document_id}

    async def status(self) -> dict[str, Any]:
        active_documents = [
            document_id for document_id, task in self.tasks.items() if not task.done()
        ]
        remaining_pages = sum(
            scheduling_pages(self.documents[document_id])
            for document_id in active_documents
        )
        seconds_per_page = (
            self.total_parse_seconds / self.total_pages if self.total_pages else 0.65
        )
        scheduler = self.scheduler.snapshot() if self.scheduler is not None else {}
        return {
            "service": self.name,
            "healthy": self.healthy,
            "health_failures": self.health_failures,
            "worker_ip": self.worker_ip,
            "actor_pid": self.actor_pid,
            "npu_logical_id": self.npu_logical_id,
            "inflight_documents": len(active_documents),
            "document_capacity": self.config["document_inflight"],
            "active_document_ids": active_documents,
            "estimated_remaining_pages": remaining_pages,
            "seconds_per_page": seconds_per_page,
            "estimated_remaining_seconds": remaining_pages * seconds_per_page,
            "completed_documents": len(self.results),
            "completed_pages": self.total_pages,
            "pages_per_second": (
                self.total_pages / self.total_parse_seconds if self.total_parse_seconds else 0.0
            ),
            "scheduler": scheduler,
            "cpu_pool": self.pools.metrics.snapshot() if self.pools is not None else {},
        }

    async def drain_completed(self) -> list[dict[str, Any]]:
        completed: list[dict[str, Any]] = []
        for document_id, task in list(self.tasks.items()):
            if not task.done():
                continue
            try:
                result = task.result()
            except BaseException as exc:
                result = {
                    "document_id": document_id,
                    "service": self.name,
                    "status": "failed",
                    "error": repr(exc),
                }
            completed.append(result)
            self.results.append(result)
            del self.tasks[document_id]
            self.documents.pop(document_id, None)
        return completed

    async def shutdown(self) -> dict[str, Any]:
        if self.tasks:
            await asyncio.gather(*self.tasks.values(), return_exceptions=True)
        self.monitor_stop.set()
        if self.monitor_task is not None:
            await self.monitor_task
        if self.scheduler is not None:
            await self.scheduler.__aexit__(None, None, None)
        if self.pools is not None:
            self.pools.shutdown()
        return {"service": self.name, "results": len(self.results)}

    async def _process_document(self, document: dict[str, Any]) -> dict[str, Any]:
        from mineru.cli.common import _process_output, prepare_env
        from mineru.data.data_reader_writer import FileBasedDataWriter
        from mineru.utils.enum_class import MakeMode

        from runtime.mineru_pipeline.official_concurrent_runner import (
            INPUT_BUCKET,
            OUTPUT_BUCKET,
            prepare_artifacts,
            s3_client,
            upload_one,
        )
        from runtime.mineru_pipeline.official_window_pipeline import aio_doc_analyze_window_pipeline
        from runtime.mineru_pipeline.production_30_runner import run_in_pool

        document_id = document["document_id"]
        started = time.time()
        root = Path(tempfile.mkdtemp(prefix=f"mineru-dual-{self.name}-{document_id}-"))
        source = root / f"{document_id}.pdf"
        output_dir = root / "output"
        package_dir = root / "package"
        package_dir.mkdir()
        result: dict[str, Any] = {
            "document_id": document_id,
            "service": self.name,
            "worker_ip": self.worker_ip,
            "actor_pid": self.actor_pid,
            "npu_logical_id": self.npu_logical_id,
            "status": "failed",
            "input_key": document["object_key"],
            "timings": {},
        }
        output_prefix = f"{self.config['output_prefix']}/{document_id}"
        event_base = {
            "batch_id": self.config["batch_id"],
            "document_id": document_id,
            "input_key": document["object_key"],
            "worker_ip": self.worker_ip,
            "service": self.name,
            "actor_pid": self.actor_pid,
            "npu_logical_id": self.npu_logical_id,
        }
        try:
            emit_dagster_event("DOCUMENT_STARTED", **event_base)
            phase = time.time()
            await run_in_pool(
                self.pools.download,
                s3_client().download_file,
                INPUT_BUCKET,
                document["object_key"],
                str(source),
            )
            result["timings"]["download_seconds"] = round(time.time() - phase, 3)

            phase = time.time()
            pdf_bytes = await run_in_pool(self.pools.download, source.read_bytes)
            image_dir, markdown_dir = prepare_env(str(output_dir), document_id, "vlm")
            image_writer = FileBasedDataWriter(image_dir)
            markdown_writer = FileBasedDataWriter(markdown_dir)
            profile = self.run_dir / f"profile-{document_id}.jsonl"
            middle_json, extracts = await aio_doc_analyze_window_pipeline(
                pdf_bytes,
                image_writer=image_writer,
                predictor=self.predictor,
                server_url=self.server_url,
                window_prefetch=self.config["window_prefetch"],
                global_scheduler=self.scheduler,
                profile_jsonl=profile,
                document_id=document_id,
                render_executor=self.pools.render,
                finalize_executor=self.pools.finalize,
                profile_executor=self.pools.control,
            )
            await run_in_pool(
                self.pools.finalize,
                _process_output,
                middle_json["pdf_info"], pdf_bytes, document_id,
                markdown_dir, image_dir, markdown_writer,
                False, False, False, True, True, True, True,
                MakeMode.MM_MD, middle_json, extracts, "vlm",
            )
            parse_seconds = time.time() - phase
            page_count = len(extracts)
            result["timings"]["parse_and_output_seconds"] = round(parse_seconds, 3)
            result["page_count"] = page_count
            self.total_pages += page_count
            self.total_parse_seconds += parse_seconds
            emit_dagster_event(
                "DOCUMENT_PARSED",
                **event_base,
                page_count=page_count,
                parse_seconds=round(parse_seconds, 3),
            )
            del pdf_bytes, middle_json, extracts

            phase = time.time()
            artifacts, artifact_profile = await run_in_pool(
                self.pools.archive, prepare_artifacts, output_dir, package_dir
            )
            result["timings"]["archive_seconds"] = round(time.time() - phase, 3)
            result["artifacts"] = artifact_profile

            phase = time.time()
            emit_dagster_event(
                "DOCUMENT_UPLOAD_STARTED",
                **event_base,
                artifact_count=artifact_profile["artifact_count"],
            )
            uploaded = await asyncio.gather(
                *(
                    run_in_pool(
                        self.pools.upload,
                        upload_one,
                        path,
                        OUTPUT_BUCKET,
                        f"{output_prefix}/artifacts/{relative}",
                        self.config["multipart_chunksize_mib"] * 1024 * 1024,
                        self.config["multipart_max_concurrency"],
                    )
                    for path, relative in artifacts
                )
            )
            result["timings"]["upload_seconds"] = round(time.time() - phase, 3)
            result["uploaded"] = uploaded
            emit_dagster_event(
                "DOCUMENT_UPLOADED",
                **event_base,
                upload_seconds=result["timings"]["upload_seconds"],
                artifact_count=len(uploaded),
            )
            result["status"] = "success"
            success = {
                "status": "success",
                "document_id": document_id,
                "service": self.name,
                "page_count": page_count,
                "input": {
                    "bucket": INPUT_BUCKET,
                    "key": document["object_key"],
                    "etag": document.get("etag"),
                },
                "finished_at": datetime.now(timezone.utc).isoformat(),
            }
            await run_in_pool(
                self.pools.upload,
                s3_client().put_object,
                Bucket=OUTPUT_BUCKET,
                Key=f"{output_prefix}/_SUCCESS.json",
                Body=json.dumps(success, ensure_ascii=False, indent=2).encode(),
                ContentType="application/json",
            )
            emit_dagster_event(
                "DOCUMENT_SUCCEEDED",
                **event_base,
                page_count=page_count,
                parse_seconds=result["timings"]["parse_and_output_seconds"],
                upload_seconds=result["timings"]["upload_seconds"],
                artifact_count=len(uploaded),
                output_uri=f"s3://{OUTPUT_BUCKET}/{output_prefix}/",
                status="success",
            )
        except Exception as exc:
            result["error"] = repr(exc)
            emit_dagster_event(
                "DOCUMENT_FAILED",
                **event_base,
                error=repr(exc),
                status="failed",
            )
        finally:
            result["elapsed_seconds"] = round(time.time() - started, 3)
            result["finished_at"] = datetime.now(timezone.utc).isoformat()
            try:
                await run_in_pool(
                    self.pools.upload,
                    s3_client().put_object,
                    Bucket=OUTPUT_BUCKET,
                    Key=f"{output_prefix}/_RESULT.json",
                    Body=json.dumps(result, ensure_ascii=False, indent=2).encode(),
                    ContentType="application/json",
                )
                await run_in_pool(
                    self.pools.upload,
                    s3_client().put_object,
                    Bucket=OUTPUT_BUCKET,
                    Key=(
                        f"_control/{self.config['batch_id']}/documents/"
                        f"{document_id}.json"
                    ),
                    Body=json.dumps(result, ensure_ascii=False, indent=2).encode(),
                    ContentType="application/json",
                )
            finally:
                await run_in_pool(self.pools.control, shutil.rmtree, root, True)
        return result

    async def _monitor(self) -> None:
        from runtime.mineru_pipeline.production_30_runner import (
            _append_text,
            fetch_text,
            parse_vllm_metrics,
            read_npu_usage,
            run_in_pool,
        )

        monitor_path = self.run_dir / "service-monitor.jsonl"
        while not self.monitor_stop.is_set():
            now = time.time()
            vllm: dict[str, Any] = {}
            npu: dict[str, Any] = {}
            try:
                await run_in_pool(
                    self.pools.control, fetch_text, f"{self.server_url}/health", 3.0
                )
                metrics_text = await run_in_pool(
                    self.pools.control, fetch_text, f"{self.server_url}/metrics", 5.0
                )
                vllm = parse_vllm_metrics(metrics_text)
                self.health_failures = 0
                self.healthy = True
            except Exception as exc:
                self.health_failures += 1
                vllm["error"] = repr(exc)
                if self.health_failures >= 2:
                    self.healthy = False
            try:
                npu = await run_in_pool(
                    self.pools.control,
                    read_npu_usage,
                    "npu-smi",
                    int(self.config["npu_id"]),
                    int(self.config["chip_id"]),
                )
            except Exception as exc:
                npu["error"] = repr(exc)
            service_pid = int(Path(self.config["vllm_pid_file"]).read_text())
            row = {
                "ts": now,
                "service": self.name,
                "physical_npu": self.config["physical_npu"],
                "healthy": self.healthy,
                "vllm": vllm,
                "npu": npu,
                "actor_rss_bytes": read_rss_bytes(os.getpid()),
                "vllm_rss_bytes": read_rss_bytes(service_pid),
                "status": await self.status(),
            }
            await run_in_pool(
                self.pools.control,
                _append_text,
                monitor_path,
                json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n",
            )
            try:
                await asyncio.wait_for(self.monitor_stop.wait(), timeout=1.0)
            except asyncio.TimeoutError:
                pass
