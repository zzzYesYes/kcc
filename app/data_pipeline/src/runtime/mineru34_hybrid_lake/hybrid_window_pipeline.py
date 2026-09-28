"""Bounded cross-document window pipeline for MinerU 3.4 Hybrid high.

The recognition and output functions are the installed MinerU implementation.
Only rendering, ready-window admission and CPU postprocessing are scheduled
independently so another document can feed vLLM while one is on the CPU.
"""

from __future__ import annotations

import asyncio
import json
import time
from concurrent.futures import Executor
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

import pypdfium2 as pdfium
from mineru.backend.hybrid import hybrid_analyze as official
from mineru.utils.enum_class import ImageType


@dataclass
class RenderedWindow:
    start: int
    end: int
    images: list[dict[str, Any]]
    rendered_at: float


@dataclass
class ReadyCall:
    document_id: str
    start: int
    end: int
    invoke: Callable[[], Awaitable[list[Any]]]
    future: asyncio.Future[list[Any]]
    queued_at: float


class HybridWindowScheduler:
    def __init__(self, inference_slots: int = 2, queue_size: int = 4, profile_path: Path | None = None) -> None:
        if inference_slots < 1 or queue_size < inference_slots:
            raise ValueError("queue_size must cover inference_slots")
        self.inference_slots = inference_slots
        self.queue: asyncio.Queue[ReadyCall] = asyncio.Queue(queue_size)
        self.profile_path = profile_path
        self.workers: list[asyncio.Task[None]] = []
        self.active = 0

    async def __aenter__(self):
        self.workers = [asyncio.create_task(self._worker(index)) for index in range(self.inference_slots)]
        return self

    async def __aexit__(self, *_):
        for task in self.workers:
            task.cancel()
        await asyncio.gather(*self.workers, return_exceptions=True)

    async def emit(self, event: str, **values: Any) -> None:
        if self.profile_path is None:
            return
        payload = {"timestamp": time.time(), "event": event, "active_slots": self.active, "ready_queue": self.queue.qsize(), **values}
        line = json.dumps(payload, ensure_ascii=False) + "\n"
        await asyncio.to_thread(self._append, line)

    def _append(self, line: str) -> None:
        self.profile_path.parent.mkdir(parents=True, exist_ok=True)
        with self.profile_path.open("a", encoding="utf-8") as output:
            output.write(line)

    async def submit(self, document_id: str, start: int, end: int, invoke: Callable[[], Awaitable[list[Any]]]) -> list[Any]:
        future = asyncio.get_running_loop().create_future()
        call = ReadyCall(document_id, start, end, invoke, future, time.time())
        await self.queue.put(call)
        await self.emit("ready", document_id=document_id, page_start=start, page_end=end)
        return await future

    async def _worker(self, slot: int) -> None:
        while True:
            call = await self.queue.get()
            self.active += 1
            started = time.time()
            await self.emit("inference_start", document_id=call.document_id, page_start=call.start, page_end=call.end, slot=slot, queue_wait_seconds=started-call.queued_at)
            try:
                result = await call.invoke()
                if not call.future.done():
                    call.future.set_result(result)
                await self.emit("inference_end", document_id=call.document_id, page_start=call.start, page_end=call.end, slot=slot, seconds=time.time()-started)
            except BaseException as exc:
                if not call.future.done():
                    call.future.set_exception(exc)
            finally:
                self.active -= 1
                self.queue.task_done()


async def aio_doc_analyze_hybrid_pipeline(
    pdf_bytes: bytes,
    image_writer: Any,
    predictor: Any,
    scheduler: HybridWindowScheduler,
    document_id: str,
    *,
    parse_method: str = "auto",
    image_analysis: bool = True,
    effort: str = "high",
    window_prefetch: int = 1,
    render_executor: Executor | None = None,
    cpu_executor: Executor | None = None,
) -> tuple[dict[str, Any], list[Any]]:
    if effort != "high":
        raise ValueError("this pipeline currently supports effort=high only")
    effective_image_analysis = official._resolve_effective_image_analysis(effort, image_analysis)
    predictor = official._maybe_enable_serial_execution(predictor, "http-client")
    ocr_enabled = official.ocr_classify(pdf_bytes, parse_method=parse_method)
    pdf_doc = official.open_pdfium_document(pdfium.PdfDocument, pdf_bytes)
    middle_json = official.init_middle_json(ocr_enabled, effort=effort)
    model_list: list[Any] = []
    rendered_queue: asyncio.Queue[RenderedWindow | None] = asyncio.Queue(window_prefetch)
    closed = False

    async def render_windows(page_count: int, window_size: int) -> None:
        try:
            for start in range(0, page_count, window_size):
                end = min(page_count - 1, start + window_size - 1)
                images = await official.aio_load_images_from_pdf_bytes_range(
                    pdf_bytes, start_page_id=start, end_page_id=end, image_type=ImageType.PIL
                )
                await rendered_queue.put(RenderedWindow(start, end, images, time.time()))
                await scheduler.emit("render_ready", document_id=document_id, page_start=start, page_end=end)
        finally:
            await rendered_queue.put(None)

    try:
        page_count = official.get_pdfium_document_page_count(pdf_doc)
        window_size = min(page_count, official.get_processing_window_size(default=64)) if page_count else 1
        device = official.get_device()
        batch_ratio = official.get_batch_ratio(device) if not ocr_enabled else 1
        renderer = asyncio.create_task(render_windows(page_count, window_size))
        hybrid_pipeline_model = None
        expected_start = 0
        while True:
            rendered = await rendered_queue.get()
            if rendered is None:
                break
            images = rendered.images
            pil_images = [item["img_pil"] for item in images]
            page_sizes = [official._normalize_page_size(image) for image in pil_images]
            cpu_started = time.time()
            layout_results, hybrid_pipeline_model = await asyncio.get_running_loop().run_in_executor(
                cpu_executor,
                official._predict_layout_for_window,
                pil_images,
                True,
                batch_ratio,
                ocr_enabled,
            )
            await scheduler.emit("layout_ready", document_id=document_id, page_start=rendered.start, page_end=rendered.end, seconds=time.time()-cpu_started)

            async def infer_window(pil_images=pil_images):
                async with official.aio_predictor_execution_guard(predictor):
                    return await predictor.aio_batch_two_step_extract(
                        images=pil_images,
                        not_extract_list=None if ocr_enabled else official.not_extract_list,
                        image_analysis=effective_image_analysis,
                    )

            window_models = await scheduler.submit(document_id, rendered.start, rendered.end, infer_window)
            post_started = time.time()
            if ocr_enabled:
                await asyncio.get_running_loop().run_in_executor(
                    cpu_executor,
                    lambda: official._apply_vlm_ocr_det_sidecars_for_window(
                        pil_images,
                        window_models,
                        batch_ratio,
                        images_layout_res=layout_results,
                        hybrid_pipeline_model=hybrid_pipeline_model,
                    ),
                )
            else:
                window_models = await asyncio.to_thread(
                    official._process_ocr_and_formulas,
                    pil_images,
                    window_models,
                    True,
                    batch_ratio=batch_ratio,
                    images_layout_res=layout_results,
                    hybrid_pipeline_model=hybrid_pipeline_model,
                )
            await asyncio.to_thread(official._apply_layout_title_split, window_models, layout_results, page_sizes)
            if rendered.start != expected_start:
                raise RuntimeError(f"window order changed: expected {expected_start}, got {rendered.start}")
            official.append_page_model_list_to_middle_json(
                middle_json, window_models, images, pdf_doc, image_writer,
                page_start_index=rendered.start, _ocr_enable=ocr_enabled, progress_bar=None,
            )
            model_list.extend(window_models)
            expected_start = rendered.end + 1
            official._close_images(images)
            await scheduler.emit("cpu_postprocess_end", document_id=document_id, page_start=rendered.start, page_end=rendered.end, seconds=time.time()-post_started)
        await renderer
        if expected_start != page_count:
            raise RuntimeError(f"expected {page_count} pages, assembled {expected_start}")
        await asyncio.to_thread(
            official.finalize_middle_json,
            middle_json["pdf_info"], hybrid_pipeline_model, ocr_enabled, effort=effort,
        )
        official.close_pdfium_document(pdf_doc)
        closed = True
        return middle_json, model_list
    finally:
        renderer_task = locals().get("renderer")
        if renderer_task is not None and not renderer_task.done():
            renderer_task.cancel()
            with suppress(asyncio.CancelledError):
                await renderer_task
        while not rendered_queue.empty():
            item = rendered_queue.get_nowait()
            if item is not None:
                official._close_images(item.images)
        if not closed:
            official.close_pdfium_document(pdf_doc)
