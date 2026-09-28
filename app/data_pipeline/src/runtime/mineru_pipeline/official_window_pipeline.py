"""Flash-inspired, bounded processing-window overlap for MinerU HTTP client.

The implementation deliberately leaves MinerU inference and output semantics
alone.  It overlaps three existing operations across consecutive official
processing windows: PDF rendering, ``aio_concurrent_two_step_extract`` and
ordered middle-json assembly.
"""

from __future__ import annotations

import asyncio
import contextvars
import functools
import json
import time
from concurrent.futures import Executor
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from types import MethodType
from typing import Any

import pypdfium2 as pdfium

from mineru.backend.vlm.model_output_to_middle_json import (
    append_page_blocks_to_middle_json,
    finalize_middle_json,
    init_middle_json,
)
from mineru.backend.vlm.vlm_analyze import (
    _get_model_async,
    _maybe_enable_serial_execution,
    aio_predictor_execution_guard,
)
from mineru.utils.config_reader import get_processing_window_size
from mineru.utils.enum_class import ImageType
from mineru.utils.pdf_image_tools import aio_load_images_from_pdf_bytes_range
from mineru.utils.pdf_image_tools import _load_images_from_pdf_bytes_range
from mineru.utils.pdfium_guard import (
    close_pdfium_document,
    get_pdfium_document_page_count,
    open_pdfium_document,
)
from mineru_vl_utils import MinerUClient
from mineru_vl_utils.structs import ExtractResult


@dataclass
class _RenderedWindow:
    start_page_id: int
    end_page_id: int
    images: list[dict[str, Any]]
    render_started: float
    render_finished: float
    image_bytes_estimate: int


@dataclass
class _InferredWindow:
    rendered: _RenderedWindow
    results: list[ExtractResult]
    infer_started: float
    infer_finished: float


@dataclass
class _ReadyWindow:
    rendered: _RenderedWindow
    predictor: MinerUClient
    image_analysis: bool
    profiler: "WindowProfiler"
    result: asyncio.Future[_InferredWindow]


class GlobalWindowScheduler:
    """Cross-document ready queue whose workers own only VLM inference time."""

    def __init__(
        self,
        inference_slots: int = 2,
        queue_size: int = 3,
        max_inference_slots: int | None = None,
    ) -> None:
        max_slots = max_inference_slots or inference_slots
        if inference_slots < 1 or max_slots < inference_slots or queue_size < max_slots:
            raise ValueError("queue_size must cover max_inference_slots")
        self.queue: asyncio.Queue[_ReadyWindow] = asyncio.Queue(queue_size)
        self.inference_slots = inference_slots
        self.max_inference_slots = max_slots
        self._workers: dict[int, asyncio.Task[None]] = {}
        self._active_workers = 0
        self._elastic_task: asyncio.Task[None] | None = None
        self._elastic_active = False
        self.elastic_admissions = 0
        self.elastic_suppressed_until = 0.0
        self.elastic_suppression_reason: str | None = None
        self.completed_windows = 0
        self.failed_windows = 0
        self._closing = False

    async def __aenter__(self) -> "GlobalWindowScheduler":
        await self.set_inference_slots(self.inference_slots)
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        self._closing = True
        for task in self._workers.values():
            task.cancel()
        if self._elastic_task is not None:
            self._elastic_task.cancel()
        await asyncio.gather(*self._workers.values(), return_exceptions=True)
        if self._elastic_task is not None:
            await asyncio.gather(self._elastic_task, return_exceptions=True)
        while not self.queue.empty():
            ready = self.queue.get_nowait()
            _close_images(ready.rendered.images)
            if not ready.result.done():
                ready.result.cancel()

    async def set_inference_slots(self, inference_slots: int) -> None:
        if not 1 <= inference_slots <= self.max_inference_slots:
            raise ValueError(
                f"inference_slots must be between 1 and {self.max_inference_slots}"
            )
        self.inference_slots = inference_slots
        for worker_id in range(inference_slots):
            task = self._workers.get(worker_id)
            if task is None or task.done():
                self._workers[worker_id] = asyncio.create_task(self._worker(worker_id))

    def snapshot(self) -> dict[str, Any]:
        return {
            "desired_slots": self.inference_slots,
            "active_slots": self._active_workers,
            "ready_queue_depth": self.queue.qsize(),
            "completed_windows": self.completed_windows,
            "failed_windows": self.failed_windows,
            "elastic_active": int(self._elastic_active),
            "elastic_admissions": self.elastic_admissions,
            "elastic_suppressed_until": self.elastic_suppressed_until,
            "elastic_suppression_reason": self.elastic_suppression_reason,
        }

    def grant_elastic_once(self) -> bool:
        if self.max_inference_slots <= self.inference_slots:
            return False
        if time.time() < self.elastic_suppressed_until:
            return False
        if self._elastic_task is not None and not self._elastic_task.done():
            return False
        if self.queue.empty():
            return False
        self.elastic_admissions += 1
        self.elastic_suppression_reason = None
        self._elastic_task = asyncio.create_task(
            self._elastic_worker(self.max_inference_slots - 1)
        )
        return True

    def suppress_elastic(self, reason: str, cooldown_seconds: float) -> None:
        self.elastic_suppressed_until = max(
            self.elastic_suppressed_until, time.time() + cooldown_seconds
        )
        self.elastic_suppression_reason = reason
        if (
            self._elastic_task is not None
            and not self._elastic_task.done()
            and not self._elastic_active
        ):
            self._elastic_task.cancel()

    async def submit(
        self,
        rendered: _RenderedWindow,
        predictor: MinerUClient,
        image_analysis: bool,
        profiler: "WindowProfiler",
    ) -> _InferredWindow:
        loop = asyncio.get_running_loop()
        future: asyncio.Future[_InferredWindow] = loop.create_future()
        queued_at = time.time()
        await self.queue.put(_ReadyWindow(rendered, predictor, image_analysis, profiler, future))
        await profiler.emit(
            "ready_queue_enter",
            page_start=rendered.start_page_id,
            page_end=rendered.end_page_id,
            queue_depth=self.queue.qsize(),
        )
        inferred = await future
        await profiler.emit(
            "ready_queue_leave",
            page_start=rendered.start_page_id,
            page_end=rendered.end_page_id,
            wait_seconds=round(inferred.infer_started - queued_at, 6),
        )
        return inferred

    async def _worker(self, worker_id: int) -> None:
        while not self._closing:
            if worker_id >= self.inference_slots:
                return
            try:
                ready = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            await self._execute_ready(ready, worker_id)

    async def _elastic_worker(self, worker_id: int) -> None:
        try:
            ready = self.queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        self._elastic_active = True
        try:
            await self._execute_ready(ready, worker_id)
        finally:
            self._elastic_active = False

    async def _execute_ready(self, ready: _ReadyWindow, worker_id: int) -> None:
        self._active_workers += 1
        started = time.time()
        await ready.profiler.emit(
            "infer_start",
            page_start=ready.rendered.start_page_id,
            page_end=ready.rendered.end_page_id,
            worker_id=worker_id,
        )
        token = _active_profiler.set(ready.profiler)
        try:
            async with aio_predictor_execution_guard(ready.predictor):
                results = await ready.predictor.aio_concurrent_two_step_extract(
                    images=[image_dict["img_pil"] for image_dict in ready.rendered.images],
                    image_analysis=ready.image_analysis,
                    priority=list(
                        range(
                            ready.rendered.start_page_id,
                            ready.rendered.end_page_id + 1,
                        )
                    ),
                )
            finished = time.time()
            inferred = _InferredWindow(ready.rendered, results, started, finished)
            await ready.profiler.emit(
                "infer_end",
                page_start=ready.rendered.start_page_id,
                page_end=ready.rendered.end_page_id,
                seconds=round(finished - started, 6),
                pages=len(results),
                worker_id=worker_id,
            )
            if not ready.result.done():
                ready.result.set_result(inferred)
            self.completed_windows += 1
        except BaseException as exc:
            _close_images(ready.rendered.images)
            if not ready.result.done():
                ready.result.set_exception(exc)
            if not isinstance(exc, asyncio.CancelledError):
                self.failed_windows += 1
        finally:
            _active_profiler.reset(token)
            self._active_workers -= 1


class WindowProfiler:
    """Small JSONL profiler kept outside MinerU's installed package."""

    def __init__(
        self,
        path: str | Path | None,
        document_id: str | None = None,
        io_executor: Executor | None = None,
    ) -> None:
        self.path = Path(path) if path else None
        self.document_id = document_id
        self.io_executor = io_executor
        self._lock = asyncio.Lock()
        self._buffered_rows: list[dict[str, Any]] = []

    async def emit(self, event: str, **fields: Any) -> None:
        if self.path is None:
            return
        row = {"event": event, "ts": time.time(), "document_id": self.document_id, **fields}
        line = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        async with self._lock:
            await asyncio.get_running_loop().run_in_executor(
                self.io_executor, self._append, line
            )

    def mark(self, event: str, **fields: Any) -> None:
        """Record a low-overhead boundary without adding file I/O to the gap."""
        if self.path is None:
            return
        self._buffered_rows.append(
            {
                "event": event,
                "ts": time.time(),
                "monotonic_ns": time.perf_counter_ns(),
                "document_id": self.document_id,
                **fields,
            }
        )

    async def flush_marks(self) -> None:
        if self.path is None or not self._buffered_rows:
            return
        rows, self._buffered_rows = self._buffered_rows, []
        lines = "".join(
            json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            for row in rows
        )
        async with self._lock:
            await asyncio.get_running_loop().run_in_executor(
                self.io_executor, self._append, lines
            )

    def _append(self, line: str) -> None:
        assert self.path is not None
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line)


_active_profiler: contextvars.ContextVar[WindowProfiler | None] = contextvars.ContextVar(
    "mineru_active_profiler", default=None
)
_active_page_id: contextvars.ContextVar[int | None] = contextvars.ContextVar(
    "mineru_active_page_id", default=None
)
_block_prepare_finished_at: contextvars.ContextVar[float | None] = contextvars.ContextVar(
    "mineru_block_prepare_finished_at", default=None
)


def _mark_gap(event: str, **fields: Any) -> None:
    profiler = _active_profiler.get()
    if profiler is not None:
        profiler.mark(event, page_id=_active_page_id.get(), **fields)


def install_layout_content_gap_tracer(predictor: MinerUClient, metrics: Any = None) -> None:
    """Install opt-in tracing on one client instance, leaving MinerU untouched."""
    if getattr(predictor, "_mineru_gap_tracer_installed", False):
        return

    original_two_step = predictor.aio_two_step_extract
    original_predict = predictor._aio_predict
    original_batch_predict = predictor._aio_batch_predict
    helper = predictor.helper
    original_parse_layout = helper.aio_parse_layout_output
    original_prepare_extract = helper.aio_prepare_for_extract
    original_post_process = helper.aio_post_process
    layout_prompt = predictor.prompts.get("[layout]")

    async def traced_two_step(self, *args: Any, **kwargs: Any):
        page_id = kwargs.get("priority")
        token = _active_page_id.set(page_id if isinstance(page_id, int) else None)
        try:
            return await original_two_step(*args, **kwargs)
        finally:
            _active_page_id.reset(token)

    async def traced_predict(self, image, prompt, *args: Any, **kwargs: Any):
        is_layout = prompt == layout_prompt
        if is_layout:
            _mark_gap("layout_request_start")
        try:
            return await original_predict(image, prompt, *args, **kwargs)
        finally:
            if is_layout:
                _mark_gap("layout_response_end")

    async def traced_batch_predict(self, images, prompts, *args: Any, **kwargs: Any):
        _mark_gap("content_request_start", request_count=len(images))
        block_finished_at = _block_prepare_finished_at.get()
        if block_finished_at is not None and metrics is not None:
            metrics.record_content_first_request_delay(time.time() - block_finished_at)
            _block_prepare_finished_at.set(None)
        try:
            return await original_batch_predict(images, prompts, *args, **kwargs)
        finally:
            _mark_gap("content_response_end", request_count=len(images))

    async def traced_parse_layout(self, *args: Any, **kwargs: Any):
        _mark_gap("layout_parse_start")
        try:
            return await original_parse_layout(*args, **kwargs)
        finally:
            _mark_gap("layout_parse_end")

    async def traced_prepare_extract(self, *args: Any, **kwargs: Any):
        _mark_gap("block_prepare_start")
        try:
            return await original_prepare_extract(*args, **kwargs)
        finally:
            _mark_gap("block_prepare_end")
            _block_prepare_finished_at.set(time.time())

    async def traced_post_process(self, *args: Any, **kwargs: Any):
        _mark_gap("page_postprocess_start")
        try:
            return await original_post_process(*args, **kwargs)
        finally:
            _mark_gap("page_postprocess_end")

    predictor.aio_two_step_extract = MethodType(traced_two_step, predictor)
    predictor._aio_predict = MethodType(traced_predict, predictor)
    predictor._aio_batch_predict = MethodType(traced_batch_predict, predictor)
    helper.aio_parse_layout_output = MethodType(traced_parse_layout, helper)
    helper.aio_prepare_for_extract = MethodType(traced_prepare_extract, helper)
    helper.aio_post_process = MethodType(traced_post_process, helper)
    predictor._mineru_gap_tracer_installed = True


def _estimate_image_bytes(images: list[dict[str, Any]]) -> int:
    total = 0
    for image_dict in images:
        image = image_dict.get("img_pil")
        if image is None:
            continue
        channels = len(image.getbands())
        total += image.width * image.height * channels
    return total


def _close_images(images: list[dict[str, Any]]) -> None:
    for image_dict in images:
        image = image_dict.get("img_pil")
        if image is not None:
            with suppress(Exception):
                image.close()


async def aio_doc_analyze_window_pipeline(
    pdf_bytes: bytes,
    image_writer: Any,
    predictor: MinerUClient | None = None,
    backend: str = "http-client",
    model_path: str | None = None,
    server_url: str | None = None,
    image_analysis: bool = True,
    *,
    window_prefetch: int = 1,
    global_scheduler: GlobalWindowScheduler | None = None,
    profile_jsonl: str | Path | None = None,
    document_id: str | None = None,
    client_side_output_generation: bool = False,
    render_executor: Executor | None = None,
    finalize_executor: Executor | None = None,
    profile_executor: Executor | None = None,
    **model_kwargs: Any,
) -> tuple[dict[str, Any], list[ExtractResult]]:
    """Preserve official window semantics while overlapping CPU and NPU work.

    Each inference item uses the same page range and the same official
    ``aio_concurrent_two_step_extract`` call that the HTTP client uses today.
    A single assembler owns ``pdf_doc`` so output pages remain strictly ordered.
    """
    if window_prefetch < 1:
        raise ValueError("window_prefetch must be positive")

    profiler = WindowProfiler(
        profile_jsonl,
        document_id=document_id,
        io_executor=profile_executor,
    )
    if predictor is None:
        predictor = await _get_model_async(backend, model_path, server_url, **model_kwargs)
    predictor = _maybe_enable_serial_execution(predictor, backend)

    pdf_doc = open_pdfium_document(pdfium.PdfDocument, pdf_bytes)
    middle_json = init_middle_json()
    results: list[ExtractResult] = []
    rendered_queue: asyncio.Queue[_RenderedWindow | None] = asyncio.Queue(window_prefetch)
    inferred_queue: asyncio.Queue[_InferredWindow | None] = asyncio.Queue(1)
    document_closed = False

    async def loader(page_count: int, window_size: int) -> None:
        try:
            for start_page_id in range(0, page_count, window_size):
                end_page_id = min(page_count - 1, start_page_id + window_size - 1)
                started = time.time()
                await profiler.emit(
                    "render_start",
                    page_start=start_page_id,
                    page_end=end_page_id,
                )
                if render_executor is None:
                    images = await aio_load_images_from_pdf_bytes_range(
                        pdf_bytes,
                        start_page_id=start_page_id,
                        end_page_id=end_page_id,
                        image_type=ImageType.PIL,
                    )
                else:
                    images = await asyncio.get_running_loop().run_in_executor(
                        render_executor,
                        functools.partial(
                            _load_images_from_pdf_bytes_range,
                            pdf_bytes,
                            start_page_id=start_page_id,
                            end_page_id=end_page_id,
                            image_type=ImageType.PIL,
                        ),
                    )
                finished = time.time()
                window = _RenderedWindow(
                    start_page_id=start_page_id,
                    end_page_id=end_page_id,
                    images=images,
                    render_started=started,
                    render_finished=finished,
                    image_bytes_estimate=_estimate_image_bytes(images),
                )
                await profiler.emit(
                    "render_end",
                    page_start=start_page_id,
                    page_end=end_page_id,
                    seconds=round(finished - started, 6),
                    image_bytes_estimate=window.image_bytes_estimate,
                    pages=len(images),
                )
                await rendered_queue.put(window)
        finally:
            await rendered_queue.put(None)

    async def infer(scheduler: GlobalWindowScheduler) -> None:
        try:
            while True:
                rendered = await rendered_queue.get()
                if rendered is None:
                    return
                try:
                    inferred = await scheduler.submit(rendered, predictor, image_analysis, profiler)
                except BaseException:
                    _close_images(rendered.images)
                    raise
                await inferred_queue.put(inferred)
        finally:
            await inferred_queue.put(None)

    try:
        page_count = get_pdfium_document_page_count(pdf_doc)
        configured_window_size = get_processing_window_size(default=64)
        effective_window_size = min(page_count, configured_window_size) if page_count else 1
        await profiler.emit(
            "document_start",
            page_count=page_count,
            window_size=effective_window_size,
            window_prefetch=window_prefetch,
            batching_mode=getattr(predictor, "batching_mode", None),
        )

        owned_scheduler = global_scheduler is None
        scheduler = global_scheduler or GlobalWindowScheduler(inference_slots=1, queue_size=1)
        if owned_scheduler:
            await scheduler.__aenter__()
        loader_task = asyncio.create_task(loader(page_count, effective_window_size))
        infer_task = asyncio.create_task(infer(scheduler))
        expected_page_start = 0
        while True:
            inferred = await inferred_queue.get()
            if inferred is None:
                break
            if inferred.rendered.start_page_id != expected_page_start:
                raise RuntimeError(
                    "window order changed: "
                    f"expected {expected_page_start}, got {inferred.rendered.start_page_id}"
                )
            started = time.time()
            await profiler.emit(
                "append_start",
                page_start=inferred.rendered.start_page_id,
                page_end=inferred.rendered.end_page_id,
            )
            try:
                await asyncio.get_running_loop().run_in_executor(
                    finalize_executor,
                    functools.partial(
                        append_page_blocks_to_middle_json,
                        middle_json,
                        inferred.results,
                        inferred.rendered.images,
                        pdf_doc,
                        image_writer,
                        inferred.rendered.start_page_id,
                    ),
                )
                results.extend(inferred.results)
                expected_page_start = inferred.rendered.end_page_id + 1
            finally:
                _close_images(inferred.rendered.images)
            await profiler.emit(
                "append_end",
                page_start=inferred.rendered.start_page_id,
                page_end=inferred.rendered.end_page_id,
                seconds=round(time.time() - started, 6),
            )

        await loader_task
        await infer_task
        if owned_scheduler:
            await scheduler.__aexit__(None, None, None)
        if expected_page_start != page_count:
            raise RuntimeError(f"expected {page_count} assembled pages, got {expected_page_start}")
        if not client_side_output_generation:
            started = time.time()
            await profiler.emit("finalize_start")
            await asyncio.get_running_loop().run_in_executor(
                finalize_executor, finalize_middle_json, middle_json["pdf_info"]
            )
            await profiler.emit("finalize_end", seconds=round(time.time() - started, 6))
        await profiler.emit("document_end", pages=len(results))
        await profiler.flush_marks()
        close_pdfium_document(pdf_doc)
        document_closed = True
        return middle_json, results
    finally:
        for task in (locals().get("loader_task"), locals().get("infer_task")):
            if task is not None and not task.done():
                task.cancel()
        for task in (locals().get("loader_task"), locals().get("infer_task")):
            if task is not None:
                with suppress(asyncio.CancelledError, Exception):
                    await task
        while not rendered_queue.empty():
            item = rendered_queue.get_nowait()
            if item is not None:
                _close_images(item.images)
        while not inferred_queue.empty():
            item = inferred_queue.get_nowait()
            if item is not None:
                _close_images(item.rendered.images)
        if not document_closed:
            close_pdfium_document(pdf_doc)
