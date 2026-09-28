from __future__ import annotations

import json
import re
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.hashing import sha256_bytes


REQUIRED = (
    "clean.md",
    "book_metadata.json",
    "blocks.jsonl",
    "exercises.jsonl",
    "image_manifest.jsonl",
    "quarantine.jsonl",
    "cleaning_report.json",
    "_SUCCESS.json",
)
FORBIDDEN = (
    "<details>",
    "责任编辑",
    "定价：",
    "定价:",
    "监督电话",
)


def parse_jsonl(body: bytes) -> list[dict[str, Any]]:
    if not body.strip():
        return []
    return [json.loads(line) for line in body.decode("utf-8").splitlines() if line.strip()]


def validate_document_artifacts(
    bodies: dict[str, bytes],
    source_sha256: str,
) -> dict[str, Any]:
    missing = [name for name in REQUIRED if name not in bodies]
    if missing:
        raise ValueError(f"missing outputs: {missing}")
    clean = bodies["clean.md"].decode("utf-8")
    blocks = parse_jsonl(bodies["blocks.jsonl"])
    exercises = parse_jsonl(bodies["exercises.jsonl"])
    images = parse_jsonl(bodies["image_manifest.jsonl"])
    quarantine = parse_jsonl(bodies["quarantine.jsonl"])
    report = json.loads(bodies["cleaning_report.json"])
    success = json.loads(bodies["_SUCCESS.json"])
    failures: list[str] = []
    if not clean.strip():
        failures.append("clean_md_empty")
    failures.extend(f"forbidden:{value}" for value in FORBIDDEN if value in clean)
    if re.search(r"!\[[^\]]*\]\(\s*\)", clean):
        failures.append("empty_image_link")
    if re.search(r"^\s*你能提出什么问题[？?]?\s*$", clean, re.MULTILINE):
        failures.append("floating_question_prompt")
    if re.search(r"(?:如右图|如下图|如图所示)", clean) and not any(
        block.get("image_required") and block.get("keep_in_clean_md") for block in blocks
    ):
        failures.append("dangling_image_reference")
    if not blocks:
        failures.append("blocks_empty")
    if not exercises:
        failures.append("exercise_count_zero")
    ids = [row["block_id"] for row in blocks]
    if len(ids) != len(set(ids)):
        failures.append("duplicate_block_id")
    if any(not isinstance(row.get("chapter_path"), list) for row in blocks):
        failures.append("invalid_chapter_path")
    if any(len({len(line) for line in table}) > 1 for row in blocks for table in row["tables"]):
        failures.append("inconsistent_table_width")
    if any(
        repair.get("source_formula") is None or repair.get("normalized_formula") is None
        for row in blocks
        for repair in row["formulas"]
    ):
        failures.append("untraceable_formula")
    if source_sha256 != report.get("source_sha256") or source_sha256 != success.get("source_sha256"):
        failures.append("source_sha_mismatch")
    if success.get("artifact_sha256", {}).get("clean.md") != sha256_bytes(bodies["clean.md"]):
        failures.append("clean_hash_mismatch")
    if failures:
        raise ValueError(f"validation failures: {failures}")
    return {
        "status": "pass",
        "block_count": len(blocks),
        "exercise_count": len(exercises),
        "image_count": len(images),
        "quarantine_count": len(quarantine),
        "checks": {
            "json_jsonl_parse": True,
            "clean_md_non_empty": True,
            "details_removed": True,
            "forbidden_noise_removed": True,
            "chapter_hierarchy_valid": True,
            "table_width_valid": True,
            "formula_traceable": True,
            "exercise_count_positive": True,
            "source_sha_unchanged": True,
        },
    }

