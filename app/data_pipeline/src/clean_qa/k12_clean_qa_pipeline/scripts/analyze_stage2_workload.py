from __future__ import annotations

import argparse

from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.stage2_qa.core import parse_jsonl
from clean_qa.k12_clean_qa_pipeline.stage2_qa.helpers import (
    merge_adjacent_blocks,
    rule_prefilter,
    select_units_by_chapter,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default="k12-cleaned-corpus")
    parser.add_argument("--prefix", default="stage1/full/stage1-v1.0.2")
    parser.add_argument("--count", type=int, default=16)
    parser.add_argument("--merge-max-chars", type=int, default=3200)
    parser.add_argument("--merge-max-blocks", type=int, default=8)
    parser.add_argument("--chapter-max-units", type=int, default=12)
    parser.add_argument("--document-max-units", type=int, default=48)
    args = parser.parse_args()

    store = ObjectStore()
    manifest = store.read_json(
        args.bucket, f"{args.prefix.rstrip('/')}/_RUN_MANIFEST.json"
    )
    totals = [0, 0, 0]
    for document in manifest["documents"][: args.count or None]:
        document_id = document["document_id"]
        base = f"{args.prefix.rstrip('/')}/{document_id}"
        blocks = parse_jsonl(store.read_bytes(args.bucket, f"{base}/blocks.jsonl"))
        quarantine = parse_jsonl(
            store.read_bytes(args.bucket, f"{base}/quarantine.jsonl")
        )
        quarantined = {row["block_id"] for row in quarantine}
        eligible = [
            block
            for block in blocks
            if rule_prefilter(block, quarantined) is None
        ]
        units = merge_adjacent_blocks(
            eligible, args.merge_max_chars, args.merge_max_blocks
        )
        selected = select_units_by_chapter(
            units, args.chapter_max_units, args.document_max_units
        )
        counts = (len(eligible), len(units), len(selected))
        totals = [left + right for left, right in zip(totals, counts)]
        print(document_id, *counts)

    compression = round(totals[0] / totals[2], 2) if totals[2] else 0
    print(
        "TOTAL",
        *totals,
        f"eligible_to_selected_compression={compression}",
    )


if __name__ == "__main__":
    main()
