from __future__ import annotations

import html
import json
import re
import unicodedata
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any

from clean_qa.k12_clean_qa_pipeline.common.hashing import sha256_text, stable_id
from clean_qa.k12_clean_qa_pipeline.stage1_clean import STAGE1_VERSION


DETAIL_RE = re.compile(
    r"!\[[^\]]*\]\((?P<path>images/[^)\n]+)\)\s*"
    r"<details\b[^>]*>\s*<summary>\s*(?P<kind>[^<\n]+)"
    r"\s*</summary>(?P<body>.*?)</details>",
    re.IGNORECASE | re.DOTALL,
)
STANDALONE_DETAIL_RE = re.compile(
    r"<details\b[^>]*>\s*<summary>\s*(?P<kind>[^<\n]+)"
    r"\s*</summary>(?P<body>.*?)</details>",
    re.IGNORECASE | re.DOTALL,
)
IMAGE_RE = re.compile(r"!\[[^\]]*\]\((?P<path>[^)\n]*)\)")
HTML_TABLE_RE = re.compile(r"<table\b[^>]*>.*?</table>", re.IGNORECASE | re.DOTALL)
FORMULA_RE = re.compile(r"\$\$.*?\$\$|\\\[.*?\\\]|(?<!\\)\$(?!\$).*?(?<!\\)\$", re.DOTALL)
HEADING_RE = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
QUESTION_RE = re.compile(
    r"^(?:例\s*\d+|练习|习题|复习参考题|复习巩固|综合运用|拓广探索|"
    r"\d{1,3}[.、)]|\([一二三四五六七八九十0-9]+\))"
)
OCR_RE = re.compile(r"�|([^\s])\1{7,}")
FLOATING_IMAGE_REF_RE = re.compile(r"(?:如|见|观察)(?:右|左|下|上)?图|如下图|上图|下图")
FLOATING_QUESTION_RE = re.compile(r"^\s*你能提出什么问题[？?]?\s*$")
NOISE_RE = re.compile(
    r"(责任编辑|美术编辑|责任校对|责任印制|出版发行|印刷厂|"
    r"北京市?.{0,20}(?:路|街|大街|号|邮编)|定价[:：]|开本[:：]|印张[:：]|"
    r"监督电话|ISBN|CIP|版权所有|侵权必究|网址\s*https?://)",
    re.IGNORECASE,
)
METADATA_RE = re.compile(
    r"(?:书名|学科|年级|册次|出版社|版次|ISBN|普通高中教科书|义务教育教科书)"
)
DROP_SECTION_RE = re.compile(
    r"^(?:目录|编者的话|后记|自我评价|问题口袋|丰收园|"
    r"老师评价|家长评价|同学评价)$"
)
DECORATION_RE = re.compile(r"(decorative|cartoon|icon|logo|插画|装饰)", re.IGNORECASE)
TABLEISH_RE = re.compile(r"(table|bar|pie|line|funnel|area|chart|统计图|表格)", re.IGNORECASE)


@dataclass
class RawBlock:
    source_order: int
    source_line_start: int
    source_line_end: int
    heading_level: int | None
    source_text: str
    chapter_path: list[str]


class TableParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[list[str]] = []
        self.row: list[str] | None = None
        self.cell: list[str] | None = None

    def handle_starttag(self, tag: str, attrs) -> None:
        if tag.lower() == "tr":
            self.row = []
        elif tag.lower() in {"td", "th"}:
            self.cell = []
        elif tag.lower() == "br" and self.cell is not None:
            self.cell.append("<br>")

    def handle_data(self, data: str) -> None:
        if self.cell is not None:
            self.cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() in {"td", "th"} and self.cell is not None and self.row is not None:
            self.row.append(
                re.sub(r"\s+", " ", "".join(self.cell)).strip().replace("|", "\\|")
            )
            self.cell = None
        elif tag.lower() == "tr" and self.row is not None:
            if self.row:
                self.rows.append(self.row)
            self.row = None


def html_table_to_markdown(value: str) -> tuple[str, list[list[str]]]:
    parser = TableParser()
    parser.feed(value)
    if not parser.rows:
        plain = re.sub(r"<[^>]+>", " ", value)
        return re.sub(r"\s+", " ", plain).strip(), []
    width = max(len(row) for row in parser.rows)
    rows = [row + [""] * (width - len(row)) for row in parser.rows]
    rendered = ["| " + " | ".join(row) + " |" for row in rows]
    rendered.insert(1, "| " + " | ".join(["---"] * width) + " |")
    return "\n".join(rendered), rows


def normalize_formula(value: str) -> tuple[str, list[str]]:
    rules: list[str] = []
    normalized = value

    def join_digits(match: re.Match[str]) -> str:
        rules.append("digit_spacing_repair")
        return match.group(0).replace(" ", "")

    normalized = re.sub(r"(?<![A-Za-z\\])\d(?:\s+\d)+(?:\s*\.\s*\d+)?", join_digits, normalized)
    before = normalized
    normalized = re.sub(r"(?<=\d)\.\s+(?=\d)", ".", normalized)
    if normalized != before:
        rules.append("decimal_spacing_repair")
    before = normalized
    normalized = normalized.replace("％", r"\%").replace("×", r"\times ").replace("÷", r"\div ")
    if normalized != before:
        rules.append("math_symbol_normalization")
    return normalized, sorted(set(rules))


def normalize_text(value: str) -> tuple[str, list[dict[str, Any]], list[list[list[str]]]]:
    tables: list[list[list[str]]] = []

    def table_replace(match: re.Match[str]) -> str:
        rendered, rows = html_table_to_markdown(match.group(0))
        if rows:
            tables.append(rows)
        return rendered

    text = HTML_TABLE_RE.sub(table_replace, value)
    repairs: list[dict[str, Any]] = []

    def formula_replace(match: re.Match[str]) -> str:
        source = match.group(0)
        normalized, rules = normalize_formula(source)
        if rules:
            repairs.append(
                {
                    "source_formula": source,
                    "normalized_formula": normalized,
                    "normalization_rules": rules,
                }
            )
        return normalized

    text = FORMULA_RE.sub(formula_replace, text)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    lines = [re.sub(r"[ \t]+", " ", line).rstrip() for line in text.splitlines()]
    compact: list[str] = []
    for line in lines:
        if not line and (not compact or not compact[-1]):
            continue
        compact.append(line)
    return "\n".join(compact).strip(), repairs, tables


def structured_index(content_list: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for item in content_list:
        text = str(item.get("text") or item.get("content") or "").strip()
        if text:
            key = re.sub(r"\s+", "", text)[:160]
            index.setdefault(key, []).append(
                {
                    "page_idx": item.get("page_idx"),
                    "bbox": item.get("bbox"),
                    "type": item.get("type"),
                    "text_level": item.get("text_level"),
                }
            )
    return index


def split_raw_blocks(markdown: str) -> list[RawBlock]:
    markdown = re.sub(
        r"<details\b[^>]*>.*?</details>",
        lambda match: re.sub(r"\n\s*\n", "\n", match.group(0)),
        markdown,
        flags=re.IGNORECASE | re.DOTALL,
    )
    lines = markdown.splitlines()
    chapter: list[str] = []
    blocks: list[RawBlock] = []
    start = 0
    current: list[str] = []
    current_level: int | None = None
    current_path: list[str] = []

    def flush(end: int) -> None:
        nonlocal current, start, current_level, current_path
        value = "\n".join(current).strip()
        if value:
            blocks.append(
                RawBlock(
                    source_order=len(blocks),
                    source_line_start=start + 1,
                    source_line_end=end,
                    heading_level=current_level,
                    source_text=value,
                    chapter_path=list(current_path),
                )
            )
        current = []
        current_level = None

    for index, line in enumerate(lines):
        heading = HEADING_RE.match(line)
        boundary = heading or (not line.strip() and current)
        if boundary:
            flush(index)
        if heading:
            level = len(heading.group(1))
            title = heading.group(2).strip()
            chapter = chapter[: level - 1]
            while len(chapter) < level - 1:
                chapter.append("")
            chapter.append(title)
            start = index
            current = [line]
            current_level = level
            current_path = [part for part in chapter if part]
        elif line.strip():
            if not current:
                start = index
                current_path = list(chapter)
            current.append(line)
    flush(len(lines))
    return blocks


def classify_block(text: str, chapter_path: list[str], heading_level: int | None) -> str:
    plain = re.sub(r"^#{1,6}\s+", "", text).strip()
    context = " / ".join(chapter_path + [plain[:80]])
    if DROP_SECTION_RE.match(plain):
        return "toc" if plain == "目录" else "self_evaluation"
    if "目录" in context and re.search(r"……|\.\.\.|…\s*\d+$", plain):
        return "toc"
    if re.search(r"(版权|出版|责任编辑|主编|ISBN|CIP)", plain, re.IGNORECASE):
        return "front_matter"
    if re.search(r"(定义|叫做|称为)", plain):
        return "definition"
    if re.search(r"(性质|定理|法则)", plain):
        return "property"
    if re.search(r"(推导|证明|因为|所以)", plain):
        return "derivation"
    if re.search(r"^(?:#+\s*)?例\s*\d+", plain):
        return "worked_example"
    if QUESTION_RE.match(plain) or any(re.search(r"(练习|习题|复习题)", item) for item in chapter_path):
        return "exercise"
    if "$" in plain or r"\[" in plain:
        return "formula"
    if re.search(r"(活动|探究)", context):
        return "activity"
    if re.search(r"(回顾|复习|小结)", context):
        return "review"
    if re.search(r"(反思|思考)", context):
        return "reflection"
    if re.search(r"^\|.+\|$", plain, re.MULTILINE):
        return "table"
    if heading_level is not None and heading_level <= 2:
        return "chapter_intro"
    if len(re.sub(r"\s+", "", plain)) >= 20:
        return "concept"
    return "unknown"


def image_records(text: str, document_id: str, block_id: str) -> tuple[list[dict[str, Any]], str]:
    records: list[dict[str, Any]] = []

    def replace(match: re.Match[str]) -> str:
        kind = match.group("kind").lower()
        body = re.sub(r"<[^>]+>", " ", html.unescape(match.group("body")))
        body = "\n".join(line.strip() for line in body.splitlines() if line.strip())
        path = match.group("path")
        spatial = len(body) < 80 and bool(re.search(r"(左|右|上|下|方向|位置|图)", body))
        corrupted = bool(OCR_RE.search(body))
        if kind == "natural_image":
            if DECORATION_RE.search(body):
                classification = "decorative"
            elif FLOATING_IMAGE_REF_RE.search(text):
                classification = "question_required"
            elif re.search(r"(图\d|坐标|几何|示意|流程|结构)", text):
                classification = "instructional"
            else:
                classification = "contextual"
        elif kind != "text_image":
            classification = "quarantine"
        elif corrupted or not re.search(r"[\w\u4e00-\u9fff]", body) or spatial:
            classification = "quarantine"
        elif TABLEISH_RE.search(body):
            classification = "instructional"
        else:
            classification = "text_extracted"
        record = {
            "image_id": stable_id(document_id, path, prefix="img"),
            "document_id": document_id,
            "block_id": block_id,
            "path": path,
            "mineru_type": kind,
            "classification": classification,
            "description": body,
            "quality_flags": [
                flag
                for flag, enabled in (
                    ("spatial_relation_unverifiable", spatial),
                    ("ocr_corruption", corrupted),
                )
                if enabled
            ],
        }
        records.append(record)
        if classification == "text_extracted":
            return "\n" + body + "\n"
        return ""

    cleaned = DETAIL_RE.sub(replace, text)

    def replace_standalone(match: re.Match[str]) -> str:
        kind = match.group("kind").lower()
        body = re.sub(r"<[^>]+>", " ", html.unescape(match.group("body")))
        body = "\n".join(line.strip() for line in body.splitlines() if line.strip())
        corrupted = bool(OCR_RE.search(body))
        spatial = len(body) < 80 and bool(re.search(r"(左|右|上|下|方向|位置|图)", body))
        usable_text = (
            kind == "text_image"
            and not corrupted
            and not spatial
            and bool(re.search(r"[\w\u4e00-\u9fff]", body))
        )
        synthetic_path = f"unlinked-details/{stable_id(document_id, block_id, body, prefix='img')}"
        classification = (
            "text_extracted"
            if usable_text
            else "decorative"
            if kind == "natural_image" and DECORATION_RE.search(body)
            else "quarantine"
        )
        records.append(
            {
                "image_id": stable_id(document_id, synthetic_path, prefix="img"),
                "document_id": document_id,
                "block_id": block_id,
                "path": None,
                "mineru_type": kind,
                "classification": classification,
                "description": body,
                "quality_flags": [
                    flag
                    for flag, enabled in (
                        ("missing_image_path", True),
                        ("spatial_relation_unverifiable", spatial),
                        ("ocr_corruption", corrupted),
                    )
                    if enabled
                ],
            }
        )
        return f"\n{body}\n" if usable_text else ""

    cleaned = STANDALONE_DETAIL_RE.sub(replace_standalone, cleaned)
    cleaned = re.sub(
        r"</?(?:details|summary)\b[^>]*>",
        "",
        cleaned,
        flags=re.IGNORECASE,
    )
    for match in IMAGE_RE.finditer(cleaned):
        path = match.group("path")
        if not path:
            continue
        records.append(
            {
                "image_id": stable_id(document_id, path, prefix="img"),
                "document_id": document_id,
                "block_id": block_id,
                "path": path,
                "mineru_type": "unknown",
                "classification": "quarantine",
                "description": "",
                "quality_flags": ["missing_image_analysis"],
            }
        )
    cleaned = IMAGE_RE.sub("", cleaned)
    return records, cleaned


def extract_book_metadata(markdown: str, mineru_success: dict[str, Any]) -> dict[str, Any]:
    front = "\n".join(markdown.splitlines()[:160])
    title_lines = [
        re.sub(r"^#{1,6}\s+", "", line).strip()
        for line in front.splitlines()
        if line.strip() and not line.startswith("!")
    ]
    input_key = mineru_success.get("input", {}).get("key", "")
    filename = input_key.rsplit("/", 1)[-1].removesuffix(".pdf")
    isbn = re.search(r"ISBN\s*([0-9Xx-]{10,})", front)
    publisher = next(
        (line for line in title_lines if line.endswith("出版社")),
        None,
    )
    return {
        "document_id": mineru_success.get("document_id"),
        "title": filename or (title_lines[0] if title_lines else ""),
        "publisher": publisher,
        "isbn": isbn.group(1) if isbn else None,
        "source_input": mineru_success.get("input", {}),
        "page_count": mineru_success.get("page_count"),
        "image_count": mineru_success.get("image_count"),
    }


def build_stage1(
    document_id: str,
    markdown: str,
    content_list: list[dict[str, Any]],
    mineru_success: dict[str, Any],
) -> dict[str, Any]:
    source_sha = sha256_text(markdown)
    structured = structured_index(content_list)
    blocks: list[dict[str, Any]] = []
    images: list[dict[str, Any]] = []
    exercises: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    repairs_total = 0
    noise_count = 0
    drop_section_level: int | None = None

    for raw in split_raw_blocks(markdown):
        block_id = stable_id(
            document_id,
            STAGE1_VERSION,
            raw.source_order,
            sha256_text(raw.source_text),
            prefix="blk",
        )
        block_images, without_images = image_records(raw.source_text, document_id, block_id)
        images.extend(block_images)
        normalized, repairs, tables = normalize_text(without_images)
        normalized = "\n".join(
            line for line in normalized.splitlines() if not FLOATING_QUESTION_RE.match(line)
        ).strip()
        repairs_total += len(repairs)
        plain = re.sub(r"^#{1,6}\s+", "", normalized).strip()
        block_type = classify_block(normalized, raw.chapter_path, raw.heading_level)
        if raw.heading_level is not None and DROP_SECTION_RE.match(plain):
            drop_section_level = raw.heading_level
        elif raw.heading_level is not None and drop_section_level is not None and raw.heading_level <= drop_section_level:
            drop_section_level = None
        severe_ocr = bool(OCR_RE.search(normalized))
        noise = bool(NOISE_RE.search(normalized)) or block_type in {
            "front_matter",
            "toc",
            "self_evaluation",
            "decoration",
        }
        if drop_section_level is not None:
            noise = True
        dangling = bool(FLOATING_IMAGE_REF_RE.search(normalized)) and not any(
            image["classification"] in {"instructional", "question_required", "text_extracted"}
            for image in block_images
        )
        image_required = any(
            image["classification"] == "question_required" for image in block_images
        ) or dangling
        quality_flags = [
            flag
            for flag, enabled in (
                ("ocr_corruption", severe_ocr),
                ("source_noise", noise),
                ("missing_required_image", dangling),
            )
            if enabled
        ]
        keep = bool(normalized) and not noise and not severe_ocr and not dangling
        if noise:
            noise_count += 1
        key = re.sub(r"\s+", "", plain)[:160]
        location = structured.get(key, [{}])[0]
        block = {
            "document_id": document_id,
            "block_id": block_id,
            "source_order": raw.source_order,
            "chapter_path": raw.chapter_path,
            "block_type": block_type,
            "source_text": raw.source_text,
            "clean_text": normalized if keep else "",
            "source_line_start": raw.source_line_start,
            "source_line_end": raw.source_line_end,
            "page_idx": location.get("page_idx"),
            "bbox": location.get("bbox"),
            "formulas": repairs,
            "tables": tables,
            "images": [image["image_id"] for image in block_images],
            "image_required": image_required,
            "keep_in_clean_md": keep,
            "qa_eligible_candidate": keep and block_type not in {"reflection", "unknown"},
            "mcq_eligible_candidate": keep and block_type in {
                "concept",
                "definition",
                "property",
                "formula",
                "worked_example",
                "exercise",
                "table",
            },
            "quality_flags": quality_flags,
            "source_sha256": sha256_text(raw.source_text),
            "clean_sha256": sha256_text(normalized if keep else ""),
        }
        blocks.append(block)
        if block_type == "exercise":
            exercises.append(
                {
                    "document_id": document_id,
                    "block_id": block_id,
                    "chapter_path": raw.chapter_path,
                    "source_text": raw.source_text,
                    "clean_text": normalized,
                    "image_required": image_required,
                    "quality_flags": quality_flags,
                }
            )
        if not keep and normalized:
            quarantine.append(
                {
                    "document_id": document_id,
                    "block_id": block_id,
                    "reason": (
                        "missing_image"
                        if dangling
                        else "ocr_corruption"
                        if severe_ocr
                        else "source_noise"
                    ),
                    "source_text": raw.source_text,
                    "candidate_text": normalized,
                    "quality_flags": quality_flags,
                }
            )
    clean_md = render_clean_markdown(blocks)
    report = {
        "document_id": document_id,
        "stage1_version": STAGE1_VERSION,
        "source_sha256": source_sha,
        "source_block_count": len(blocks),
        "kept_block_count": sum(row["keep_in_clean_md"] for row in blocks),
        "removed_block_count": noise_count,
        "quarantine_block_count": len(quarantine),
        "exercise_count": len(exercises),
        "image_count": len(images),
        "formula_repair_count": repairs_total,
        "clean_character_count": len(re.sub(r"\s+", "", clean_md)),
    }
    return {
        "source_sha256": source_sha,
        "book_metadata": extract_book_metadata(markdown, mineru_success),
        "blocks": blocks,
        "exercises": exercises,
        "images": images,
        "quarantine": quarantine,
        "clean_md": clean_md,
        "report": report,
    }


def render_clean_markdown(blocks: list[dict[str, Any]]) -> str:
    values = [
        block["clean_text"].strip()
        for block in sorted(blocks, key=lambda row: row["source_order"])
        if block["keep_in_clean_md"] and block["clean_text"].strip()
    ]
    return "\n\n".join(values).strip() + "\n"
