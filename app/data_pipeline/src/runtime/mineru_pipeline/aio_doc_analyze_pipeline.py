"""Bounded microbatch VLM pipeline for the Flash30 MinerU experiment.

This module deliberately owns only scheduling.  It keeps MinerU's model client,
block preparation, post-processing, middle-json assembly, and document-level
finalization intact so it can be removed without changing the installed package.
"""

from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
import time
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

from PIL import Image

from mineru.backend.vlm.model_output_to_middle_json import (
    append_page_blocks_to_middle_json,
    finalize_middle_json,
    init_middle_json,
)
from mineru.backend.vlm.vlm_analyze import _get_model_async, _maybe_enable_serial_execution
from mineru.utils.enum_class import ImageType
from mineru.utils.pdf_image_tools import aio_load_images_from_pdf_bytes_range
from mineru.utils.pdfium_guard import (
    close_pdfium_document,
    get_pdfium_document_page_count,
    open_pdfium_document,
)
from mineru_vl_utils import MinerUClient
from mineru_vl_utils.structs import ExtractResult


Stage = Literal[
    "pdf_render",
    "layout_inference",
    "block_prepare",
    "content_inference",
    "page_postprocess",
    "cross_page_table_merge",
    "ordered_append",
]


class PipelineProfiler:
    """Writes one JSON object per stage boundary without global monkey patches."""

    _write_lock = threading.Lock()

    def __init__(self, path: str | Path | None, document_id: str | None = None):
        self.path = Path(path) if path else None
        self.document_id = document_id
        self._lock = asyncio.Lock()

    async def emit(self, event: str, **payload: Any) -> None:
        if self.path is None:
            return
        row = {
            "event": event,
            "ts": time.time(),
            "document_id": self.document_id,
            **payload,
        }
        encoded = json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
        async with self._lock:
            await asyncio.to_thread(self._append, encoded)

    def _append(self, encoded: str) -> None:
        assert self.path is not None
        with self._write_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(encoded)

    @asynccontextmanager
    async def stage(self, stage: Stage, page_ids: list[int] | None = None, **extra: Any):
        started = time.time()
        await self.emit(
            "stage_start",
            stage=stage,
            start=started,
            page_ids=page_ids or [],
            **extra,
        )
        try:
            yield
        finally:
            ended = time.time()
            await self.emit(
                "stage_end",
                stage=stage,
                start=started,
                end=ended,
                duration=round(ended - started, 6),
                page_ids=page_ids or [],
                **extra,
            )


class _InferenceGate:
    """A shared HTTP-request limit with priority for pending layout work."""

    def __init__(self, total: int, layout_reserved_slots: int, content_soft_limit: int):
        if total < 1:
            raise ValueError("total_inference_inflight must be positive")
        if not 0 < layout_reserved_slots < total:
            raise ValueError("layout_reserved_slots must be between 1 and total - 1")
        self.total = total
        self.layout_reserved_slots = layout_reserved_slots
        self.content_soft_limit = min(content_soft_limit, total - layout_reserved_slots)
        self._condition = asyncio.Condition()
        self._active_total = 0
        self._active_content = 0
        self._layout_waiters = 0

    async def acquire(self, kind: Literal["layout", "content"]) -> None:
        async with self._condition:
            if kind == "layout":
                self._layout_waiters += 1
                try:
                    await self._condition.wait_for(lambda: self._active_total < self.total)
                    self._active_total += 1
                finally:
                    self._layout_waiters -= 1
                    self._condition.notify_all()
                return

            def content_allowed() -> bool:
                if self._active_total >= self.total:
                    return False
                # When layout is pending, preserve capacity for it.  With no
                # layout demand, Content may borrow every available slot.
                if self._layout_waiters:
                    return self._active_content < self.content_soft_limit
                return True

            await self._condition.wait_for(content_allowed)
            self._active_total += 1
            self._active_content += 1

    async def release(self, kind: Literal["layout", "content"]) -> None:
        async with self._condition:
            self._active_total -= 1
            if kind == "content":
                self._active_content -= 1
            self._condition.notify_all()


class _StageSemaphore:
    """Semaphore-compatible adapter used by MinerU's HTTP client."""

    def __init__(self, gate: _InferenceGate, kind: Literal["layout", "content"], limit: int | None = None):
        self.gate = gate
        self.kind = kind
        self.limit = asyncio.Semaphore(limit) if limit else None

    async def __aenter__(self) -> "_StageSemaphore":
        if self.limit:
            await self.limit.acquire()
        try:
            await self.gate.acquire(self.kind)
        except BaseException:
            if self.limit:
                self.limit.release()
            raise
        return self

    async def __aexit__(self, exc_type, exc, traceback) -> None:
        await self.gate.release(self.kind)
        if self.limit:
            self.limit.release()


@dataclass
class _CompletedPage:
    page_id: int
    result: ExtractResult
    image_path: Path
    scale: float


def _block_counts(blocks: list[Any]) -> dict[str, int]:
    counts = {"text_blocks": 0, "image_blocks": 0, "table_blocks": 0, "formula_blocks": 0}
    for block in blocks:
        block_type = getattr(block, "type", "")
        if block_type == "text":
            counts["text_blocks"] += 1
        elif block_type in {"image", "chart", "image_block"}:
            counts["image_blocks"] += 1
        elif block_type == "table":
            counts["table_blocks"] += 1
        elif block_type in {"equation", "equation_block", "inline_formula"}:
            counts["formula_blocks"] += 1
    return counts


def _spool_image(image_dict: dict[str, Any], image_path: Path) -> float:
    image = image_dict["img_pil"]
    image_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        image.save(image_path, format="PNG")
        return float(image_dict["scale"])
    finally:
        image.close()


def _load_spooled_image(image_path: Path, scale: float) -> dict[str, Any]:
    with Image.open(image_path) as image:
        return {"img_pil": image.copy(), "scale": scale}


def _close_image_dict(image_dict: dict[str, Any]) -> None:
    image = image_dict.get("img_pil")
    if image is not None:
        image.close()


def _page_count_from_bytes(pdf_bytes: bytes) -> int:
    import pypdfium2 as pdfium

    document = open_pdfium_document(pdfium.PdfDocument, pdf_bytes)
    try:
        return get_pdfium_document_page_count(document)
    finally:
        close_pdfium_document(document)


async def _run_cross_page_merge(
    predictor: MinerUClient,
    results: list[ExtractResult],
    content_semaphore: _StageSemaphore,
) -> None:
    if not predictor.helper.enable_cross_page_table_merge:
        return

    from mineru_vl_utils.post_process.cross_page_table import aio_detect_cross_page_cell_merge

    params = predictor.sampling_params.get("[cross_page_table_merge]")

    async def batch_predict(prompts: list[str]) -> list[str]:
        return await predictor.client.aio_batch_predict(
            [None] * len(prompts),
            prompts,
            [params] * len(prompts),
            semaphore=content_semaphore,
        )

    await aio_detect_cross_page_cell_merge(results, batch_predict)


async def aio_doc_analyze_pipeline(
    pdf_bytes: bytes,
    image_writer: Any,
    predictor: MinerUClient | None = None,
    backend: str = "http-client",
    model_path: str | None = None,
    server_url: str | None = None,
    image_analysis: bool = True,
    *,
    render_workers: int = 2,
    render_microbatch: int = 2,
    global_page_buffer: int = 6,
    layout_inflight: int = 4,
    content_inflight: int = 12,
    total_inference_inflight: int = 16,
    layout_reserved_slots: int = 4,
    profile_jsonl: str | Path | None = None,
    document_id: str | None = None,
    client_side_output_generation: bool = False,
    **model_kwargs: Any,
) -> tuple[dict[str, Any], list[ExtractResult]]:
    """Run MinerU VLM analysis with bounded render/Layout/Content overlap.

    Images are spooled after page-level work.  This keeps the page buffer
    bounded while retaining all page results for MinerU's document-level
    cross-page-table merge before ordered middle-json assembly.
    """
    if render_microbatch < 1 or global_page_buffer < render_microbatch:
        raise ValueError("global_page_buffer must be at least render_microbatch")
    if render_workers < 1 or layout_inflight < 1 or content_inflight < 1:
        raise ValueError("pipeline concurrency values must be positive")

    profiler = PipelineProfiler(profile_jsonl, document_id)
    if predictor is None:
        predictor = await _get_model_async(backend, model_path, server_url, **model_kwargs)
    predictor = _maybe_enable_serial_execution(predictor, backend)

    gate = _InferenceGate(total_inference_inflight, layout_reserved_slots, content_inflight)
    layout_semaphore = _StageSemaphore(gate, "layout", layout_inflight)
    content_semaphore = _StageSemaphore(gate, "content")
    page_slots = asyncio.Semaphore(global_page_buffer)
    render_queue: asyncio.Queue[tuple[int, dict[str, Any]] | None] = asyncio.Queue(global_page_buffer)
    spool_dir = Path(tempfile.mkdtemp(prefix="mineru-flash30-pages-"))
    completed_pages: dict[int, _CompletedPage] = {}
    page_tasks: list[asyncio.Task[_CompletedPage]] = []
    pdf_doc = None

    async def render_worker(ranges: asyncio.Queue[tuple[int, int] | None]) -> None:
        while True:
            page_range = await ranges.get()
            if page_range is None:
                await render_queue.put(None)
                return
            start_page_id, end_page_id = page_range
            images_list: list[dict[str, Any]] = []
            try:
                async with profiler.stage(
                    "pdf_render",
                    list(range(start_page_id, end_page_id + 1)),
                    microbatch_size=end_page_id - start_page_id + 1,
                ):
                    images_list = await aio_load_images_from_pdf_bytes_range(
                        pdf_bytes,
                        start_page_id=start_page_id,
                        end_page_id=end_page_id,
                        image_type=ImageType.PIL,
                    )
                for offset, image_dict in enumerate(images_list):
                    await page_slots.acquire()
                    await render_queue.put((start_page_id + offset, image_dict))
                images_list = []
            finally:
                for image_dict in images_list:
                    _close_image_dict(image_dict)

    async def process_page(page_id: int, image_dict: dict[str, Any], layout_result: Any) -> _CompletedPage:
        try:
            blocks = layout_result
            await profiler.emit(
                "page_block_counts",
                page_id=page_id,
                **_block_counts(blocks),
            )
            async with profiler.stage("block_prepare", [page_id]):
                prepared = await predictor.helper.aio_prepare_for_extract(
                    predictor.executor,
                    image_dict["img_pil"],
                    blocks,
                    image_analysis=image_analysis,
                )
            block_images, prompts, params, indices = prepared
            await profiler.emit("content_request_count", page_id=page_id, count=len(prompts))
            if prompts:
                async with profiler.stage("content_inference", [page_id], request_count=len(prompts)):
                    outputs = await predictor._aio_batch_predict(
                        block_images,
                        prompts,
                        params,
                        page_id,
                        content_semaphore,
                        None,
                        use_tqdm=False,
                        tqdm_desc=None,
                    )
                for block_index, output in zip(indices, outputs):
                    blocks[block_index].content = output.text
                    blocks[block_index].scored = output.scored
            async with profiler.stage("page_postprocess", [page_id]):
                processed = await predictor.helper.aio_post_process(predictor.executor, blocks)
            image_path = spool_dir / f"page-{page_id:06d}.png"
            scale = await asyncio.to_thread(_spool_image, image_dict, image_path)
            return _CompletedPage(
                page_id=page_id,
                result=ExtractResult(processed, layout_result.layout_scored),
                image_path=image_path,
                scale=scale,
            )
        except BaseException:
            _close_image_dict(image_dict)
            raise
        finally:
            page_slots.release()

    try:
        page_count = await asyncio.to_thread(_page_count_from_bytes, pdf_bytes)
        # The short-lived document above is not shared with render workers or
        # ordered assembly; each MinerU range loader owns its rendering handle.
        ranges: asyncio.Queue[tuple[int, int] | None] = asyncio.Queue()
        for start_page_id in range(0, page_count, render_microbatch):
            await ranges.put((start_page_id, min(page_count - 1, start_page_id + render_microbatch - 1)))
        for _ in range(render_workers):
            await ranges.put(None)
        render_tasks = [asyncio.create_task(render_worker(ranges)) for _ in range(render_workers)]

        render_workers_finished = 0
        while render_workers_finished < render_workers:
            first = await render_queue.get()
            if first is None:
                render_workers_finished += 1
                continue
            batch = [first]
            while len(batch) < render_microbatch:
                try:
                    next_item = await asyncio.wait_for(render_queue.get(), timeout=0.01)
                except asyncio.TimeoutError:
                    break
                if next_item is None:
                    render_workers_finished += 1
                    break
                batch.append(next_item)

            page_ids = [item[0] for item in batch]
            images = [item[1]["img_pil"] for item in batch]
            async with profiler.stage("layout_inference", page_ids, microbatch_size=len(batch)):
                layout_results = await predictor.aio_batch_layout_detect(
                    images,
                    priority=page_ids,
                    semaphore=layout_semaphore,
                    scored=None,
                )
            for (page_id, image_dict), layout_result in zip(batch, layout_results):
                task = asyncio.create_task(process_page(page_id, image_dict, layout_result))
                page_tasks.append(task)

        await asyncio.gather(*render_tasks)
        for completed in await asyncio.gather(*page_tasks):
            completed_pages[completed.page_id] = completed

        results = [completed_pages[page_id].result for page_id in range(page_count)]
        async with profiler.stage("cross_page_table_merge", list(range(page_count))):
            await _run_cross_page_merge(predictor, results, content_semaphore)

        pdf_doc = open_pdfium_document(__import__("pypdfium2").PdfDocument, pdf_bytes)
        middle_json = init_middle_json()
        for page_id, result in enumerate(results):
            completed = completed_pages[page_id]
            image_dict = await asyncio.to_thread(_load_spooled_image, completed.image_path, completed.scale)
            try:
                async with profiler.stage("ordered_append", [page_id]):
                    append_page_blocks_to_middle_json(
                        middle_json,
                        [result],
                        [image_dict],
                        pdf_doc,
                        image_writer,
                        page_start_index=page_id,
                    )
            finally:
                _close_image_dict(image_dict)
        if not client_side_output_generation:
            await asyncio.to_thread(finalize_middle_json, middle_json["pdf_info"])
        return middle_json, results
    finally:
        if pdf_doc is not None:
            close_pdfium_document(pdf_doc)
        shutil.rmtree(spool_dir, ignore_errors=True)
