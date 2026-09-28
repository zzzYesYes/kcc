"""Run two Hybrid documents concurrently against one persistent HTTP predictor."""

from __future__ import annotations

import argparse
import asyncio
import json
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from mineru.backend.hybrid import hybrid_analyze as official
from mineru.cli.common import _process_output, prepare_env
from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.utils.enum_class import MakeMode

from .hybrid_window_pipeline import HybridWindowScheduler, aio_doc_analyze_hybrid_pipeline


async def process_document(spec: dict, predictor, scheduler, output_root: Path, cpu_pool, finalize_pool):
    document_id = spec["document_id"]
    pdf_bytes = await asyncio.to_thread(Path(spec["source"]).read_bytes)
    image_dir, markdown_dir = prepare_env(str(output_root / document_id), document_id, "hybrid_auto")
    image_writer = FileBasedDataWriter(image_dir)
    markdown_writer = FileBasedDataWriter(markdown_dir)
    middle_json, extracts = await aio_doc_analyze_hybrid_pipeline(
        pdf_bytes,
        image_writer,
        predictor,
        scheduler,
        document_id,
        parse_method="auto",
        image_analysis=True,
        effort="high",
        window_prefetch=1,
        cpu_executor=cpu_pool,
    )
    await asyncio.get_running_loop().run_in_executor(
        finalize_pool,
        lambda: _process_output(
            middle_json["pdf_info"], pdf_bytes, document_id,
            markdown_dir, image_dir, markdown_writer,
            False, False, False, True, True, True, True,
            MakeMode.MM_MD, middle_json, extracts, "vlm",
        ),
    )
    return {"document_id": document_id, "page_count": len(extracts), "output_dir": str(output_root / document_id)}


async def async_main(args) -> None:
    specs = json.loads(args.specs.read_text())
    predictor = await official._get_model_async("http-client", None, args.server_url)
    predictor = official._maybe_enable_serial_execution(predictor, "http-client")
    cpu_pool = ThreadPoolExecutor(max_workers=args.cpu_workers, thread_name_prefix="hybrid-cpu")
    finalize_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="hybrid-finalize")
    try:
        async with HybridWindowScheduler(args.inference_slots, args.queue_size, args.profile) as scheduler:
            results = await asyncio.gather(*[
                process_document(spec, predictor, scheduler, args.output, cpu_pool, finalize_pool)
                for spec in specs
            ])
        args.result.write_text(json.dumps(results, ensure_ascii=False, indent=2))
    finally:
        cpu_pool.shutdown(wait=True, cancel_futures=True)
        finalize_pool.shutdown(wait=True, cancel_futures=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--specs", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--result", required=True, type=Path)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--server-url", required=True)
    parser.add_argument("--inference-slots", type=int, default=2)
    parser.add_argument("--queue-size", type=int, default=4)
    parser.add_argument("--cpu-workers", type=int, default=16)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    asyncio.run(async_main(args))


if __name__ == "__main__":
    main()
