"""Thirty-document MinerU pipeline with adaptive global window concurrency."""

from __future__ import annotations

import argparse
import asyncio
import collections
import functools
import json
import os
import re
import shutil
import subprocess
import tempfile
import threading
import time
import urllib.request
from contextlib import asynccontextmanager, suppress
from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from mineru.backend.vlm.vlm_analyze import _get_model_async
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
from runtime.mineru_pipeline.official_window_pipeline import (
    GlobalWindowScheduler,
    aio_doc_analyze_window_pipeline,
    install_layout_content_gap_tracer,
)


GIB = 1024**3


class PipelineMetrics:
    """Thread-safe measurements for the isolated MinerU CPU helper pool."""

    def __init__(self, sample_limit: int = 4096) -> None:
        self._lock = threading.Lock()
        self._queued = 0
        self._active = 0
        self._completed = 0
        self._queue_waits: collections.deque[float] = collections.deque(maxlen=sample_limit)
        self._service_times: collections.deque[float] = collections.deque(maxlen=sample_limit)
        self._content_delays: collections.deque[float] = collections.deque(maxlen=sample_limit)
        self._last_started_at = 0.0

    def submitted(self) -> None:
        with self._lock:
            self._queued += 1

    def started(self, queued_at: float) -> None:
        with self._lock:
            self._queued -= 1
            self._active += 1
            self._last_started_at = time.time()
            self._queue_waits.append(time.perf_counter() - queued_at)

    def finished(self, started_at: float) -> None:
        with self._lock:
            self._active -= 1
            self._completed += 1
            self._service_times.append(time.perf_counter() - started_at)

    def record_content_first_request_delay(self, seconds: float) -> None:
        with self._lock:
            self._content_delays.append(seconds)

    @staticmethod
    def _percentile(values: list[float], fraction: float) -> float | None:
        if not values:
            return None
        ordered = sorted(values)
        return ordered[min(len(ordered) - 1, int((len(ordered) - 1) * fraction))]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            waits = list(self._queue_waits)
            services = list(self._service_times)
            delays = list(self._content_delays)
            return {
                "block_prepare_active_workers": self._active,
                "block_prepare_queue_depth": self._queued,
                "block_prepare_completed": self._completed,
                "block_prepare_recent": time.time() - self._last_started_at <= 1.5,
                "block_prepare_queue_wait_avg_seconds": (
                    sum(waits) / len(waits) if waits else None
                ),
                "block_prepare_queue_wait_p95_seconds": self._percentile(waits, 0.95),
                "block_prepare_service_avg_seconds": (
                    sum(services) / len(services) if services else None
                ),
                "block_prepare_service_p95_seconds": self._percentile(services, 0.95),
                "content_first_request_delay_avg_seconds": (
                    sum(delays) / len(delays) if delays else None
                ),
                "content_first_request_delay_p95_seconds": self._percentile(delays, 0.95),
            }


class ObservableThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args: Any, metrics: PipelineMetrics, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.metrics = metrics

    def submit(self, fn, /, *args, **kwargs) -> Future:
        queued_at = time.perf_counter()
        self.metrics.submitted()

        def measured():
            self.metrics.started(queued_at)
            started_at = time.perf_counter()
            try:
                return fn(*args, **kwargs)
            finally:
                self.metrics.finished(started_at)

        return super().submit(measured)


class PipelineExecutors:
    def __init__(self, args: argparse.Namespace) -> None:
        self.metrics = PipelineMetrics()
        self.control = ThreadPoolExecutor(max_workers=2, thread_name_prefix="control")
        self.download = ThreadPoolExecutor(
            max_workers=args.download_workers, thread_name_prefix="download"
        )
        self.block_prepare = ObservableThreadPoolExecutor(
            max_workers=args.block_prepare_workers,
            thread_name_prefix="block-prepare",
            metrics=self.metrics,
        )
        self.render = ThreadPoolExecutor(
            max_workers=args.render_workers, thread_name_prefix="render"
        )
        self.finalize = ThreadPoolExecutor(
            max_workers=args.finalize_workers, thread_name_prefix="finalize"
        )
        self.archive = ThreadPoolExecutor(
            max_workers=args.archive_workers, thread_name_prefix="archive"
        )
        self.upload = ThreadPoolExecutor(
            max_workers=args.upload_workers, thread_name_prefix="upload"
        )

    def shutdown(self) -> None:
        for executor in (
            self.control,
            self.download,
            self.block_prepare,
            self.render,
            self.finalize,
            self.archive,
            self.upload,
        ):
            executor.shutdown(wait=True, cancel_futures=True)


async def run_in_pool(executor, function, /, *args, **kwargs):
    call = functools.partial(function, *args, **kwargs)
    return await asyncio.get_running_loop().run_in_executor(executor, call)


class DynamicLimiter:
    def __init__(self, limit: int, max_limit: int) -> None:
        self.limit = limit
        self.max_limit = max_limit
        self.active = 0
        self.waiting = 0
        self._condition = asyncio.Condition()

    @asynccontextmanager
    async def slot(self):
        async with self._condition:
            self.waiting += 1
            try:
                await self._condition.wait_for(lambda: self.active < self.limit)
                self.active += 1
            finally:
                self.waiting -= 1
        try:
            yield
        finally:
            async with self._condition:
                self.active -= 1
                self._condition.notify_all()

    async def set_limit(self, limit: int) -> None:
        if not 1 <= limit <= self.max_limit:
            raise ValueError(f"document limit must be between 1 and {self.max_limit}")
        async with self._condition:
            self.limit = limit
            self._condition.notify_all()

    def snapshot(self) -> dict[str, int]:
        return {"limit": self.limit, "active": self.active, "waiting": self.waiting}


def read_int(path: str) -> int | None:
    try:
        return int(Path(path).read_text().strip())
    except (FileNotFoundError, ValueError):
        return None


def fetch_text(url: str, timeout: float = 2.0) -> str:
    request = urllib.request.Request(url)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read().decode("utf-8", errors="replace")


def parse_vllm_metrics(text: str) -> dict[str, float]:
    wanted = {
        "vllm:num_requests_running": "running_requests",
        "vllm:num_requests_waiting": "waiting_requests",
        "vllm:kv_cache_usage_perc": "kv_cache_usage",
    }
    values: dict[str, float] = {}
    for line in text.splitlines():
        if not line or line.startswith("#"):
            continue
        metric = line.split("{", 1)[0].split()[0]
        if metric not in wanted:
            continue
        with suppress(ValueError, IndexError):
            values[wanted[metric]] = values.get(wanted[metric], 0.0) + float(line.rsplit(None, 1)[1])
    return values


def parse_histogram_buckets(text: str, metric_name: str) -> dict[float, float]:
    buckets: dict[float, float] = {}
    prefix = f"{metric_name}_bucket"
    for line in text.splitlines():
        if not line.startswith(prefix):
            continue
        match = re.search(r'le="([^"]+)"', line)
        if match is None:
            continue
        try:
            boundary = float(match.group(1))
            value = float(line.rsplit(None, 1)[1])
        except ValueError:
            continue
        buckets[boundary] = buckets.get(boundary, 0.0) + value
    return buckets


class RollingHistogram:
    def __init__(self, window_seconds: float = 30.0) -> None:
        self.window_seconds = window_seconds
        self.previous: dict[float, float] | None = None
        self.samples: collections.deque[tuple[float, dict[float, float]]] = collections.deque()

    def update(self, now: float, cumulative: dict[float, float]) -> float | None:
        if self.previous is not None and cumulative:
            delta = {
                boundary: max(0.0, value - self.previous.get(boundary, 0.0))
                for boundary, value in cumulative.items()
            }
            self.samples.append((now, delta))
        self.previous = cumulative
        while self.samples and now - self.samples[0][0] > self.window_seconds:
            self.samples.popleft()
        if not self.samples:
            return None
        totals: dict[float, float] = {}
        for _, sample in self.samples:
            for boundary, value in sample.items():
                totals[boundary] = totals.get(boundary, 0.0) + value
        count = totals.get(float("inf"), 0.0)
        if count <= 0:
            return None
        target = count * 0.95
        for boundary in sorted(totals):
            if totals[boundary] >= target:
                return boundary if boundary != float("inf") else None
        return None


def read_npu_usage(executable: str, device: int, chip: int) -> dict[str, float]:
    completed = subprocess.run(
        [executable, "info", "-t", "usages", "-i", str(device), "-c", str(chip)],
        capture_output=True,
        text=True,
        timeout=3,
        check=True,
    )
    values: dict[str, float] = {}
    for line in completed.stdout.splitlines():
        match = re.search(r":\s*(-?\d+(?:\.\d+)?)\s*$", line)
        if match is None:
            continue
        lowered = line.lower()
        if "aicore" in lowered or "ai core" in lowered:
            values["aicore_percent"] = float(match.group(1))
        elif "hbm usage rate" in lowered:
            values["hbm_percent"] = float(match.group(1))
    return values


async def adaptive_monitor(
    scheduler: GlobalWindowScheduler,
    documents: DynamicLimiter,
    args: argparse.Namespace,
    pools: PipelineExecutors,
    stop: asyncio.Event,
) -> None:
    log_path = args.local_output / "adaptive-monitor.jsonl"
    memory_limit = 110 * GIB
    last_failures = 0
    consecutive_health_failures = 0
    last_control_mtime = 0
    initial_memory_failcnt = read_int("/sys/fs/cgroup/memory/memory.failcnt") or 0
    started_at = time.time()
    waiting_overload_since: float | None = None
    latency = RollingHistogram(args.api_latency_window_seconds)
    latency_baseline: float | None = None
    cached_npu: dict[str, float] = {}
    next_npu_sample = 0.0
    while not stop.is_set():
        now = time.time()
        memory_current = read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
        memory_peak = read_int("/sys/fs/cgroup/memory/memory.max_usage_in_bytes")
        memory_failcnt = read_int("/sys/fs/cgroup/memory/memory.failcnt")
        cpu_usage_ns = read_int("/sys/fs/cgroup/cpuacct/cpuacct.usage")
        health = False
        metrics_available = False
        metrics: dict[str, float] = {}
        try:
            await run_in_pool(pools.control, fetch_text, f"{args.server_url}/health", 3.0)
            health = True
            consecutive_health_failures = 0
        except Exception:
            consecutive_health_failures += 1
        try:
            metrics_text = await run_in_pool(
                pools.control, fetch_text, f"{args.server_url}/metrics", 5.0
            )
            metrics_available = True
            metrics = parse_vllm_metrics(metrics_text)
            p95 = latency.update(
                now,
                parse_histogram_buckets(metrics_text, "vllm:e2e_request_latency_seconds"),
            )
            if p95 is not None:
                metrics["api_e2e_p95_seconds"] = p95
                if (
                    latency_baseline is None
                    and now - started_at >= args.elastic_warmup_seconds / 2
                ):
                    latency_baseline = p95
        except Exception:
            metrics["metrics_available"] = 0.0

        if now >= next_npu_sample:
            try:
                cached_npu = await run_in_pool(
                    pools.control,
                    read_npu_usage,
                    args.npu_smi_path,
                    args.npu_device,
                    args.npu_chip,
                )
            except Exception as exc:
                cached_npu = {"error": repr(exc)}
            next_npu_sample = now + args.npu_sample_seconds

        new_failures = scheduler.failed_windows - last_failures
        last_failures = scheduler.failed_windows
        emergency_reason = None
        if memory_current is not None and memory_current >= memory_limit:
            emergency_reason = "memory_current_at_least_110GiB"
        elif memory_failcnt is not None and memory_failcnt > initial_memory_failcnt:
            emergency_reason = "memory_failcnt_increased"
        elif consecutive_health_failures >= 3:
            emergency_reason = "vllm_health_failed_three_times"
        elif new_failures > 0:
            emergency_reason = "inference_window_failed"

        if emergency_reason:
            await scheduler.set_inference_slots(2)
            await documents.set_limit(3)
            scheduler.suppress_elastic(emergency_reason, args.elastic_cooldown_seconds)

        running = metrics.get("running_requests", 0.0)
        waiting = metrics.get("waiting_requests", 0.0)
        if waiting > args.elastic_waiting_fallback:
            waiting_overload_since = waiting_overload_since or now
        else:
            waiting_overload_since = None

        latency_p95 = metrics.get("api_e2e_p95_seconds")
        latency_spike = bool(
            latency_p95 is not None
            and latency_baseline is not None
            and latency_p95 >= max(
                latency_baseline * args.api_latency_spike_factor,
                latency_baseline + args.api_latency_spike_seconds,
            )
        )
        fallback_reason = None
        if waiting_overload_since and now - waiting_overload_since >= 5.0:
            fallback_reason = "waiting_above_fallback_for_5s"
        elif running >= args.elastic_running_fallback:
            fallback_reason = "running_near_max_num_seqs"
        elif consecutive_health_failures >= 2:
            fallback_reason = "health_timeout_twice"
        elif latency_spike:
            fallback_reason = "api_p95_spike"
        elastic_was_active = bool(scheduler.snapshot()["elastic_active"])
        if fallback_reason and elastic_was_active:
            scheduler.suppress_elastic(fallback_reason, args.elastic_cooldown_seconds)

        scheduler_state = scheduler.snapshot()
        pool_state = pools.metrics.snapshot()
        elastic_checks = {
            "warm": now - started_at >= args.elastic_warmup_seconds,
            "metrics_available": metrics_available,
            "running_below_limit": running < args.elastic_running_limit,
            "waiting_below_limit": waiting < args.elastic_waiting_limit,
            "waiting_is_zero": waiting == 0,
            "ready_queue_nonempty": scheduler_state["ready_queue_depth"] > 0,
            "health": health,
            "memory_below_limit": (
                memory_current is None or memory_current < args.elastic_memory_gib * GIB
            ),
            "hbm_below_limit": (
                isinstance(cached_npu.get("hbm_percent"), (int, float))
                and cached_npu["hbm_percent"] < args.elastic_hbm_percent
            ),
            "block_prepare_active_or_recent": bool(
                pool_state["block_prepare_active_workers"]
                or pool_state["block_prepare_recent"]
            ),
            "not_in_cooldown": now >= scheduler.elastic_suppressed_until,
        }
        elastic_granted = False
        if all(elastic_checks.values()) and not fallback_reason and not emergency_reason:
            elastic_granted = scheduler.grant_elastic_once()

        try:
            mtime = args.control_file.stat().st_mtime_ns
            if mtime != last_control_mtime:
                control = json.loads(args.control_file.read_text())
                requested_slots = int(control.get("inference_slots", scheduler.inference_slots))
                requested_documents = int(control.get("document_inflight", documents.limit))
                promotion_safe = (
                    health
                    and not emergency_reason
                    and (memory_current is None or memory_current < 100 * GIB)
                )
                if requested_slots <= scheduler.inference_slots or promotion_safe:
                    await scheduler.set_inference_slots(requested_slots)
                    await documents.set_limit(requested_documents)
                last_control_mtime = mtime
        except FileNotFoundError:
            pass
        except Exception as exc:
            metrics["control_error"] = repr(exc)

        row: dict[str, Any] = {
            "ts": now,
            "health": health,
            "consecutive_health_failures": consecutive_health_failures,
            "memory_current_bytes": memory_current,
            "memory_peak_bytes": memory_peak,
            "memory_failcnt": memory_failcnt,
            "cpu_usage_ns": cpu_usage_ns,
            "emergency_reason": emergency_reason,
            "scheduler": scheduler.snapshot(),
            "documents": documents.snapshot(),
            "vllm": metrics,
            "npu": cached_npu,
            "cpu_pools": pool_state,
            "elastic": {
                "checks": elastic_checks,
                "granted": elastic_granted,
                "fallback_reason": fallback_reason,
                "fallback_armed": elastic_was_active,
                "latency_baseline_seconds": latency_baseline,
            },
        }
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        await run_in_pool(pools.control, _append_text, log_path, line)
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.monitor_interval_seconds)
        except asyncio.TimeoutError:
            pass


def _append_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(text)


def load_manifest(bucket: str, key: str) -> list[dict[str, Any]]:
    body = s3_client().get_object(Bucket=bucket, Key=key)["Body"].read()
    manifest = json.loads(body)
    return manifest["documents"]


async def _run_pipeline(args: argparse.Namespace, pools: PipelineExecutors) -> None:
    args.local_output.mkdir(parents=True, exist_ok=True)
    asyncio.get_running_loop().set_default_executor(pools.block_prepare)
    documents = await run_in_pool(
        pools.control, load_manifest, OUTPUT_BUCKET, args.manifest_key
    )
    if len(documents) != args.expected_pdf_count:
        raise ValueError(
            f"expected {args.expected_pdf_count}-document manifest, got {len(documents)}"
        )

    predictor = await _get_model_async("http-client", None, args.server_url)
    predictor.executor = pools.block_prepare
    if hasattr(predictor, "helper"):
        predictor.helper.executor = pools.block_prepare
    install_layout_content_gap_tracer(predictor, pools.metrics)
    document_limiter = DynamicLimiter(args.document_inflight, args.max_document_inflight)
    download_limiter = asyncio.Semaphore(args.download_workers)
    upload_limiter = asyncio.Semaphore(args.upload_workers)
    stop_monitor = asyncio.Event()
    run_started = time.time()
    results: list[dict[str, Any]] = []

    async with GlobalWindowScheduler(
        inference_slots=args.inference_slots,
        max_inference_slots=args.max_inference_slots,
        queue_size=max(args.max_document_inflight, args.max_inference_slots),
    ) as scheduler:
        monitor = asyncio.create_task(
            adaptive_monitor(
                scheduler,
                document_limiter,
                args,
                pools,
                stop_monitor,
            )
        )

        async def process_one(document: dict[str, Any]) -> dict[str, Any]:
            document_id = document["document_id"]
            started = time.time()
            root = Path(tempfile.mkdtemp(prefix=f"mineru-production-{document_id}-"))
            source = root / f"{document_id}.pdf"
            output_dir = root / "output"
            package_dir = root / "package"
            package_dir.mkdir()
            result: dict[str, Any] = {
                "document_id": document_id,
                "status": "failed",
                "input_key": document["object_key"],
                "timings": {},
            }
            try:
                async with download_limiter:
                    phase = time.time()
                    await run_in_pool(
                        pools.download,
                        s3_client().download_file,
                        INPUT_BUCKET,
                        document["object_key"],
                        str(source),
                    )
                    result["timings"]["download_seconds"] = round(time.time() - phase, 3)

                parse_wait = time.time()
                async with document_limiter.slot():
                    result["timings"]["parse_wait_seconds"] = round(
                        time.time() - parse_wait, 3
                    )
                    phase = time.time()
                    pdf_bytes = await run_in_pool(pools.download, source.read_bytes)
                    image_dir, markdown_dir = prepare_env(str(output_dir), document_id, "vlm")
                    image_writer = FileBasedDataWriter(image_dir)
                    markdown_writer = FileBasedDataWriter(markdown_dir)
                    profile = args.local_output / f"profile-{document_id}.jsonl"
                    middle_json, extracts = await aio_doc_analyze_window_pipeline(
                        pdf_bytes,
                        image_writer=image_writer,
                        predictor=predictor,
                        server_url=args.server_url,
                        window_prefetch=args.window_prefetch,
                        global_scheduler=scheduler,
                        profile_jsonl=profile,
                        document_id=document_id,
                        render_executor=pools.render,
                        finalize_executor=pools.finalize,
                        profile_executor=pools.control,
                    )
                    await run_in_pool(
                        pools.finalize,
                        _process_output,
                        middle_json["pdf_info"], pdf_bytes, document_id,
                        markdown_dir, image_dir, markdown_writer,
                        False, False, False, True, True, True, True,
                        MakeMode.MM_MD, middle_json, extracts, "vlm",
                    )
                    result["timings"]["parse_and_output_seconds"] = round(
                        time.time() - phase, 3
                    )
                    result["page_count"] = len(extracts)
                    del pdf_bytes, middle_json, extracts

                phase = time.time()
                artifacts, artifact_profile = await run_in_pool(
                    pools.archive,
                    prepare_artifacts, output_dir, package_dir
                )
                result["timings"]["archive_seconds"] = round(time.time() - phase, 3)
                result["artifacts"] = artifact_profile

                phase = time.time()
                uploaded = []
                for path, relative in artifacts:
                    async with upload_limiter:
                        uploaded.append(
                            await run_in_pool(
                                pools.upload,
                                upload_one,
                                path,
                                OUTPUT_BUCKET,
                                f"{args.output_prefix}/{document_id}/artifacts/{relative}",
                                16 * 1024 * 1024,
                                4,
                            )
                        )
                result["timings"]["upload_seconds"] = round(time.time() - phase, 3)
                result["uploaded"] = uploaded
                result["status"] = "success"
            except Exception as exc:
                result["error"] = repr(exc)
            finally:
                result["elapsed_seconds"] = round(time.time() - started, 3)
                result["finished_at"] = datetime.now(timezone.utc).isoformat()
                await run_in_pool(
                    pools.upload,
                    s3_client().put_object,
                    Bucket=OUTPUT_BUCKET,
                    Key=f"{args.output_prefix}/{document_id}/_RESULT.json",
                    Body=json.dumps(result, ensure_ascii=False, indent=2).encode(),
                    ContentType="application/json",
                )
                await run_in_pool(pools.control, shutil.rmtree, root, True)
            return result

        try:
            results = list(await asyncio.gather(*(process_one(doc) for doc in documents)))
        finally:
            stop_monitor.set()
            await monitor

    summary = {
        "status": "success" if all(row["status"] == "success" for row in results) else "partial",
        "elapsed_seconds": round(time.time() - run_started, 3),
        "pdf_count": len(results),
        "success_count": sum(row["status"] == "success" for row in results),
        "page_count": sum(row.get("page_count", 0) for row in results),
        "initial_inference_slots": args.inference_slots,
        "initial_document_inflight": args.document_inflight,
        "max_inference_slots": args.max_inference_slots,
        "max_document_inflight": args.max_document_inflight,
        "window_prefetch": args.window_prefetch,
        "results": sorted(results, key=lambda row: row["document_id"]),
    }
    summary_path = args.local_output / "summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2))
    await run_in_pool(
        pools.upload,
        s3_client().put_object,
        Bucket=OUTPUT_BUCKET,
        Key=f"{args.output_prefix}/_SUMMARY.json",
        Body=json.dumps(summary, ensure_ascii=False, indent=2).encode(),
        ContentType="application/json",
    )


async def main_async(args: argparse.Namespace) -> None:
    pools = PipelineExecutors(args)
    try:
        await _run_pipeline(args, pools)
    finally:
        pools.shutdown()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--output-prefix", required=True)
    parser.add_argument("--local-output", required=True, type=Path)
    parser.add_argument("--control-file", required=True, type=Path)
    parser.add_argument("--server-url", default="http://127.0.0.1:30001")
    parser.add_argument("--inference-slots", type=int, default=3)
    parser.add_argument("--max-inference-slots", type=int, default=4)
    parser.add_argument("--document-inflight", type=int, default=4)
    parser.add_argument("--max-document-inflight", type=int, default=5)
    parser.add_argument("--window-prefetch", type=int, default=1)
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--upload-workers", type=int, default=4)
    parser.add_argument("--block-prepare-workers", type=int, default=12)
    parser.add_argument("--render-workers", type=int, default=6)
    parser.add_argument("--finalize-workers", type=int, default=3)
    parser.add_argument("--archive-workers", type=int, default=2)
    parser.add_argument("--expected-pdf-count", type=int, default=30)
    parser.add_argument("--monitor-interval-seconds", type=float, default=5.0)
    parser.add_argument("--elastic-warmup-seconds", type=float, default=120.0)
    parser.add_argument("--elastic-cooldown-seconds", type=float, default=60.0)
    parser.add_argument("--elastic-running-limit", type=float, default=220.0)
    parser.add_argument("--elastic-waiting-limit", type=float, default=16.0)
    parser.add_argument("--elastic-waiting-fallback", type=float, default=96.0)
    parser.add_argument("--elastic-running-fallback", type=float, default=280.0)
    parser.add_argument("--elastic-memory-gib", type=float, default=90.0)
    parser.add_argument("--elastic-hbm-percent", type=float, default=80.0)
    parser.add_argument("--api-latency-window-seconds", type=float, default=30.0)
    parser.add_argument("--api-latency-spike-factor", type=float, default=2.0)
    parser.add_argument("--api-latency-spike-seconds", type=float, default=2.0)
    parser.add_argument("--npu-smi-path", default="/tmp/npu-smi")
    parser.add_argument("--npu-device", type=int, default=7)
    parser.add_argument("--npu-chip", type=int, default=0)
    parser.add_argument("--npu-sample-seconds", type=float, default=5.0)
    return parser.parse_args()


if __name__ == "__main__":
    if os.environ.get("MINERU_PRODUCTION_PARENT") != "1":
        os.environ["MINERU_PRODUCTION_PARENT"] = "1"
        asyncio.run(main_async(parse_args()))
