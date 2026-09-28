"""CLI entry point for the packaged official MinerU window pipeline."""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from mineru.backend.vlm.vlm_analyze import _get_model_async
from mineru.cli.common import _process_output, prepare_env
from mineru.data.data_reader_writer import FileBasedDataWriter
from mineru.utils.enum_class import MakeMode

from .official_window_pipeline import (
    GlobalWindowScheduler,
    aio_doc_analyze_window_pipeline,
    install_layout_content_gap_tracer,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", action="append", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--url", required=True)
    parser.add_argument("--profile-json", required=True, type=Path)
    parser.add_argument("--window-prefetch", type=int, default=1)
    parser.add_argument("--document-inflight", type=int, default=3)
    parser.add_argument("--inference-slots", type=int, default=2)
    parser.add_argument("--profile-layout-content-gap", action="store_true")
    return parser.parse_args()


async def main_async(args: argparse.Namespace) -> None:
    pdf_paths = [path.resolve() for path in args.path]
    missing = [path for path in pdf_paths if not path.is_file()]
    if missing:
        raise FileNotFoundError(missing)

    args.output.mkdir(parents=True, exist_ok=True)
    predictor = await _get_model_async("http-client", None, args.url)
    if args.profile_layout_content_gap:
        install_layout_content_gap_tracer(predictor)
    document_slots = asyncio.Semaphore(args.document_inflight)

    async def run_one(pdf_path: Path, scheduler: GlobalWindowScheduler) -> None:
        async with document_slots:
            image_dir, markdown_dir = prepare_env(str(args.output), pdf_path.stem, "vlm")
            image_writer = FileBasedDataWriter(image_dir)
            markdown_writer = FileBasedDataWriter(markdown_dir)
            pdf_bytes = await asyncio.to_thread(pdf_path.read_bytes)
            profile = args.profile_json.with_name(
                f"{args.profile_json.stem}-{pdf_path.stem}.json"
            )
            middle_json, results = await aio_doc_analyze_window_pipeline(
                pdf_bytes,
                image_writer=image_writer,
                predictor=predictor,
                server_url=args.url,
                window_prefetch=args.window_prefetch,
                global_scheduler=scheduler,
                profile_jsonl=profile,
                document_id=pdf_path.stem,
            )
            await asyncio.to_thread(
                _process_output,
                middle_json["pdf_info"],
                pdf_bytes,
                pdf_path.stem,
                markdown_dir,
                image_dir,
                markdown_writer,
                False,
                False,
                False,
                True,
                True,
                True,
                True,
                MakeMode.MM_MD,
                middle_json,
                results,
                "vlm",
            )

    async with GlobalWindowScheduler(
        inference_slots=args.inference_slots,
        queue_size=max(args.document_inflight, args.inference_slots),
    ) as scheduler:
        await asyncio.gather(*(run_one(path, scheduler) for path in pdf_paths))


def main() -> None:
    asyncio.run(main_async(parse_args()))


if __name__ == "__main__":
    main()
