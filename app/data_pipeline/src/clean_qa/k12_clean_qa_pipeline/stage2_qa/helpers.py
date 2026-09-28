from __future__ import annotations

import json
import re
from collections import OrderedDict
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.hashing import stable_id


MERGEABLE_BLOCK_TYPES = {
    "chapter_intro",
    "concept",
    "definition",
    "property",
    "formula",
    "derivation",
    "image_text",
}

UNIT_TYPE_PRIORITY = {
    "exercise": 0,
    "worked_example": 1,
    "definition": 2,
    "formula": 3,
    "table": 4,
    "concept": 5,
    "property": 6,
    "derivation": 7,
}


def json_from_content(content: str) -> dict[str, Any]:
    value = content.strip()
    if value.startswith("```"):
        value = value.split("\n", 1)[1].rsplit("```", 1)[0].strip()
    start = value.find("{")
    end = value.rfind("}")
    if start < 0 or end < start:
        raise ValueError("Qwen response does not contain a JSON object")
    parsed = json.loads(value[start : end + 1])
    if not isinstance(parsed, dict):
        raise ValueError("Qwen response JSON is not an object")
    return parsed


def rule_prefilter(block: dict[str, Any], quarantined: set[str]) -> str | None:
    text = str(block.get("clean_text", "")).strip()
    if block["block_id"] in quarantined:
        return "ocr_corruption"
    if block.get("image_required"):
        return "missing_image"
    if not block.get("qa_eligible_candidate"):
        return "source_not_answerable"
    if len(re.sub(r"\s+", "", text)) < 24:
        return "source_not_answerable"
    if "�" in text:
        return "ocr_corruption"
    if re.search(r"(自我评价|家长评价|同学评价|你能提出什么问题)", text):
        return "open_ended_question"
    return None


def _chapter_key(block: dict[str, Any]) -> tuple[str, ...]:
    path = tuple(str(value).strip() for value in block.get("chapter_path", []) if str(value).strip())
    if path:
        return path
    # Documents without recovered headings still need bounded, distributed coverage.
    source_order = int(block.get("source_order", 0))
    return (f"__root_window_{source_order // 200:04d}",)


def merge_adjacent_blocks(
    blocks: list[dict[str, Any]],
    max_chars: int,
    max_blocks: int,
) -> list[dict[str, Any]]:
    """Build traceable semantic units without changing Stage 1 artifacts."""
    if max_chars <= 0 or max_blocks <= 0:
        raise ValueError("merge limits must be positive")
    units: list[dict[str, Any]] = []
    current: list[dict[str, Any]] = []

    def flush() -> None:
        if not current:
            return
        anchor = current[0]
        source_block_ids = [str(row["block_id"]) for row in current]
        unit = {
            **anchor,
            "block_id": str(anchor["block_id"]),
            "source_block_ids": source_block_ids,
            "generation_unit_id": stable_id(
                str(anchor.get("document_id", "")),
                *source_block_ids,
                prefix="unit",
            ),
            "clean_text": "\n\n".join(str(row.get("clean_text", "")).strip() for row in current),
            "source_text": "\n\n".join(str(row.get("source_text", "")).strip() for row in current),
            "merged_block_count": len(current),
            "formulas": [
                value for row in current for value in row.get("formulas", [])
            ],
            "tables": [value for row in current for value in row.get("tables", [])],
            "images": [value for row in current for value in row.get("images", [])],
        }
        units.append(unit)
        current.clear()

    for block in sorted(blocks, key=lambda row: int(row.get("source_order", 0))):
        text = str(block.get("clean_text", "")).strip()
        mergeable = block.get("block_type") in MERGEABLE_BLOCK_TYPES
        if not mergeable:
            flush()
            current.append(block)
            flush()
            continue
        if current:
            previous = current[-1]
            same_chapter = _chapter_key(previous) == _chapter_key(block)
            adjacent = int(block.get("source_order", 0)) - int(
                previous.get("source_order", 0)
            ) <= 2
            combined_chars = sum(
                len(str(row.get("clean_text", "")).strip()) for row in current
            ) + len(text)
            if (
                not same_chapter
                or not adjacent
                or len(current) >= max_blocks
                or combined_chars > max_chars
            ):
                flush()
        current.append(block)
    flush()
    return units


def select_units_by_chapter(
    units: list[dict[str, Any]],
    chapter_max_units: int,
    document_max_units: int,
) -> list[dict[str, Any]]:
    """Apply per-chapter quotas, then round-robin chapters for document fairness."""
    if chapter_max_units <= 0:
        raise ValueError("chapter_max_units must be positive")
    chapters: OrderedDict[tuple[str, ...], list[dict[str, Any]]] = OrderedDict()
    for unit in sorted(units, key=lambda row: int(row.get("source_order", 0))):
        chapters.setdefault(_chapter_key(unit), []).append(unit)

    limited: list[list[dict[str, Any]]] = []
    for chapter_units in chapters.values():
        ranked = sorted(
            chapter_units,
            key=lambda row: (
                UNIT_TYPE_PRIORITY.get(str(row.get("block_type")), 99),
                int(row.get("source_order", 0)),
            ),
        )[:chapter_max_units]
        limited.append(sorted(ranked, key=lambda row: int(row.get("source_order", 0))))

    target = document_max_units if document_max_units > 0 else sum(map(len, limited))
    selected: list[dict[str, Any]] = []
    cursor = 0
    while len(selected) < target:
        added = False
        for chapter_units in limited:
            if cursor < len(chapter_units):
                selected.append(chapter_units[cursor])
                added = True
                if len(selected) == target:
                    break
        if not added:
            break
        cursor += 1
    return sorted(selected, key=lambda row: int(row.get("source_order", 0)))


def deduplicate(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    unique: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    seen: set[str] = set()
    signatures: list[set[str]] = []
    for row in rows:
        key = re.sub(r"\W+", "", row["question"]).lower()
        if key in seen:
            rejected.append({**row, "rejection_reason": "duplicate_item"})
            continue
        signature = {key[index : index + 2] for index in range(max(1, len(key) - 1))}
        if any(
            signature
            and prior
            and len(signature & prior) / len(signature | prior) >= 0.9
            for prior in signatures
        ):
            rejected.append({**row, "rejection_reason": "duplicate_item"})
            continue
        seen.add(key)
        signatures.append(signature)
        unique.append(row)
    return unique, rejected
