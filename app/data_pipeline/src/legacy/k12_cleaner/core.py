from __future__ import annotations

import hashlib
import html
import json
import os
import re
import time
import unicodedata
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

CLEANER_VERSION = "2.0.0"
IMAGE_RE = re.compile(r"!?\[[^\]]*\]\([^\n)]*\)(?:[ \t]*\n)?")
DETAILS_RE = re.compile(
    r"<details\b[^>]*>\s*<summary>\s*(natural_image|text_image)\s*</summary>(.*?)</details>",
    re.IGNORECASE | re.DOTALL,
)
HTML_TABLE_RE = re.compile(r"<table\b[^>]*>.*?</table>", re.IGNORECASE | re.DOTALL)
MATH_RE = re.compile(r"\$\$.*?\$\$|\\\[.*?\\\]|(?<!\\)\$(?!\$).*?(?<!\\)\$", re.DOTALL)
HEADING_RE = re.compile(r"^#{1,6}\s+\S")
LIST_RE = re.compile(r"^(?:[-*+]\s+|\d+[.)、]\s*)")
ROMAN_COVER_RE = re.compile(r"^[A-Z][A-Z\s'-]{5,}$")
CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
REPEATED_CHAR_RE = re.compile(r"(.)\1{7,}")


@dataclass(frozen=True)
class StageResult:
    status: str
    stage: str
    document_id: str
    input_uri: str
    output_uri: str
    input_count: int
    output_count: int
    elapsed_seconds: float
    metrics: dict[str, Any]
    skipped: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_s3_uri(uri: str) -> tuple[str, str]:
    parsed = urlparse(uri)
    if parsed.scheme != "s3" or not parsed.netloc:
        raise ValueError(f"expected s3 URI, got {uri!r}")
    return parsed.netloc, parsed.path.lstrip("/").rstrip("/")


def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def canonical_hash(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode()).hexdigest()


def _read_text(uri: str) -> tuple[str, str]:
    bucket, key = parse_s3_uri(uri)
    client = s3_client()
    response = client.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8", "replace"), response["ETag"].strip('"')


def _write_bytes(uri: str, body: bytes, content_type: str) -> None:
    bucket, key = parse_s3_uri(uri)
    s3_client().put_object(Bucket=bucket, Key=key, Body=body, ContentType=content_type)


def _write_json(uri: str, value: Any) -> None:
    _write_bytes(uri, json.dumps(value, ensure_ascii=False, indent=2).encode(), "application/json")


def _line_count(text: str) -> int:
    return sum(bool(line.strip()) for line in text.splitlines())


def _strip_details(match: re.Match[str], config: dict[str, Any], metrics: dict[str, int]) -> str:
    detail_type, body = match.group(1).lower(), match.group(2)
    if detail_type == "natural_image":
        metrics["natural_image_details_removed"] += 1
        return ""
    text = re.sub(r"<[^>]+>", " ", html.unescape(body))
    text = "\n".join(line.strip() for line in text.splitlines() if line.strip())
    min_chars = int(config.get("text_image_min_chars", 4))
    policy = config.get("text_image_policy", "classify")
    meaningful = len(re.sub(r"\s+", "", text)) >= min_chars and bool(re.search(r"[\w\u4e00-\u9fff]", text))
    if policy == "keep" or (policy == "classify" and meaningful):
        metrics["text_image_details_kept"] += 1
        return f"\n{text}\n"
    metrics["text_image_details_removed"] += 1
    return ""


def clean_structure(markdown: str, config: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    metrics = {
        "image_references_removed": len(IMAGE_RE.findall(markdown)),
        "natural_image_details_removed": 0,
        "text_image_details_kept": 0,
        "text_image_details_removed": 0,
        "repeated_margin_lines_removed": 0,
        "watermark_lines_removed": 0,
        "roman_cover_lines_removed": 0,
        "isolated_lines_removed": 0,
    }
    text = IMAGE_RE.sub("", markdown) if config.get("remove_image_markdown", True) else markdown

    def details_replacement(match: re.Match[str]) -> str:
        if match.group(1).lower() == "natural_image" and not config.get("remove_natural_image_details", True):
            return match.group(0)
        return _strip_details(match, config, metrics)

    text = DETAILS_RE.sub(details_replacement, text)
    lines = text.splitlines()
    normalized = [re.sub(r"\s+", " ", line.strip()).casefold() for line in lines]
    frequencies: dict[str, int] = {}
    for line in normalized:
        if line and len(line) <= int(config.get("repeated_line_max_chars", 80)):
            frequencies[line] = frequencies.get(line, 0) + 1
    repeated_min = int(config.get("repeated_line_min_occurrences", 3))
    watermark_pattern = config.get("watermark_regex", r"(?:仅供|内部|试读|样书|水印|www\.)")
    watermark_re = re.compile(watermark_pattern, re.IGNORECASE) if watermark_pattern else None
    cover_lines = int(config.get("cover_scan_lines", 120))
    remove_roman = bool(config.get("remove_romanized_cover", True))
    isolated_max = int(config.get("isolated_text_max_chars", 1))
    output: list[str] = []
    for index, line in enumerate(lines):
        stripped = line.strip()
        key = normalized[index]
        protected = bool(HEADING_RE.match(stripped) or LIST_RE.match(stripped) or re.search(r"[?？=]", stripped))
        if key and frequencies.get(key, 0) >= repeated_min and not protected:
            metrics["repeated_margin_lines_removed"] += 1
            continue
        if stripped and watermark_re and watermark_re.search(stripped):
            metrics["watermark_lines_removed"] += 1
            continue
        if remove_roman and index < cover_lines and ROMAN_COVER_RE.fullmatch(stripped) and len(stripped.split()) >= 2:
            metrics["roman_cover_lines_removed"] += 1
            continue
        compact = re.sub(r"\s+", "", stripped)
        if compact and len(compact) <= isolated_max and not protected and not re.search(r"[。！？.!?]", stripped):
            metrics["isolated_lines_removed"] += 1
            continue
        output.append(line.rstrip())
    return "\n".join(output).strip() + "\n", metrics


class _TableParser(HTMLParser):
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
            self.row.append(re.sub(r"\s+", " ", "".join(self.cell)).strip().replace("|", "\\|"))
            self.cell = None
        elif tag.lower() == "tr" and self.row is not None:
            if self.row:
                self.rows.append(self.row)
            self.row = None


def html_table_to_markdown(value: str) -> str:
    parser = _TableParser()
    parser.feed(value)
    if not parser.rows:
        return re.sub(r"<[^>]+>", " ", value).strip()
    width = max(len(row) for row in parser.rows)
    rows = [row + [""] * (width - len(row)) for row in parser.rows]
    rendered = ["| " + " | ".join(row) + " |" for row in rows]
    rendered.insert(1, "| " + " | ".join(["---"] * width) + " |")
    return "\n".join(rendered)


def _protect_math(text: str) -> tuple[str, list[str]]:
    values: list[str] = []

    def replace(match: re.Match[str]) -> str:
        values.append(match.group(0))
        return f"MINERUMATHPLACEHOLDER{len(values) - 1}END"

    return MATH_RE.sub(replace, text), values


def _restore_math(text: str, values: list[str]) -> str:
    for index, value in enumerate(values):
        text = text.replace(f"MINERUMATHPLACEHOLDER{index}END", value)
    return text


def normalize_and_quality(markdown: str, config: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    table_count = len(HTML_TABLE_RE.findall(markdown))
    text = (
        HTML_TABLE_RE.sub(lambda match: html_table_to_markdown(match.group(0)), markdown)
        if config.get("convert_html_tables", True)
        else markdown
    )
    text, math_values = _protect_math(text) if config.get("preserve_latex", True) else (text, [])
    unicode_form = str(config.get("unicode_form", "NFKC"))
    text = unicodedata.normalize(unicode_form, CONTROL_RE.sub("", text))
    text = text.replace("\u00a0", " ").replace("\u3000", " ")
    lines: list[str] = []
    ocr_suspect_lines = 0
    dropped_ocr_lines = 0
    max_repeat = int(config.get("ocr_max_repeated_char_run", 8))
    drop_ocr = bool(config.get("drop_severe_ocr_lines", False))
    for line in text.splitlines():
        line = re.sub(r"[ \t]+", " ", line).rstrip()
        suspicious = "�" in line or bool(REPEATED_CHAR_RE.search(line))
        if suspicious:
            ocr_suspect_lines += 1
            longest = max((len(match.group(0)) for match in re.finditer(r"(.)\1+", line)), default=0)
            if drop_ocr and ("�" in line or longest >= max_repeat):
                dropped_ocr_lines += 1
                continue
        lines.append(line)
    max_blank = int(config.get("max_consecutive_blank_lines", 1))
    collapsed: list[str] = []
    blank_count = 0
    for line in lines:
        if line:
            blank_count = 0
            collapsed.append(line)
        else:
            blank_count += 1
            if blank_count <= max_blank:
                collapsed.append("")
    result = _restore_math("\n".join(collapsed).strip() + "\n", math_values)
    compact = re.sub(r"\s+", "", result)
    suspect_ratio = ocr_suspect_lines / max(1, _line_count(result))
    return result, {
        "html_tables_converted": table_count,
        "latex_formula_count": len(math_values),
        "ocr_suspect_line_count": ocr_suspect_lines,
        "ocr_dropped_line_count": dropped_ocr_lines,
        "replacement_character_count": result.count("�"),
        "quality_score": round(max(0.0, 1.0 - ocr_suspect_lines / max(1, _line_count(result))), 6),
        "ocr_warning": suspect_ratio >= float(config.get("ocr_warn_line_ratio", 0.02)),
        "character_count": len(compact),
    }


def split_training_samples(markdown: str, document_id: str, source_uri: str, config: dict[str, Any]) -> list[dict[str, Any]]:
    min_chars = int(config.get("sample_min_chars", 200))
    max_chars = int(config.get("sample_max_chars", 8000))
    heading_levels = int(config.get("chapter_heading_max_level", 3))
    heading = re.compile(rf"^#{{1,{heading_levels}}}\s+.+$")
    sections: list[list[str]] = []
    current: list[str] = []
    for line in markdown.splitlines():
        if heading.match(line) and current:
            sections.append(current)
            current = []
        current.append(line)
    if current:
        sections.append(current)
    chunks: list[str] = []
    pending = ""
    for section in sections:
        value = "\n".join(section).strip()
        if not value:
            continue
        if pending:
            value = pending + "\n\n" + value
            pending = ""
        while len(value) > max_chars:
            split_at = value.rfind("\n", 0, max_chars)
            if split_at < max_chars // 2:
                split_at = max_chars
            chunks.append(value[:split_at].strip())
            value = value[split_at:].strip()
        if len(value) < min_chars:
            pending = value
        else:
            chunks.append(value)
    if pending:
        if chunks:
            chunks[-1] = chunks[-1] + "\n\n" + pending
        else:
            chunks.append(pending)
    overlap = int(config.get("sample_overlap_chars", 0))
    if overlap > 0:
        for index in range(1, len(chunks)):
            chunks[index] = chunks[index - 1][-overlap:] + "\n" + chunks[index]
    include_source = bool(config.get("include_source_uri", True))
    return [
        {
            "id": f"{document_id}-chapter-{index:04d}",
            "document_id": document_id,
            "chapter_index": index,
            "text": chunk,
            "character_count": len(re.sub(r"\s+", "", chunk)),
            **({"source_uri": source_uri} if include_source else {}),
        }
        for index, chunk in enumerate(chunks, start=1)
    ]


def run_stage(
    stage: str,
    row: dict[str, Any],
    config: dict[str, Any],
    batch_id: str,
    run_id: str,
) -> StageResult:
    started = time.time()
    document_id = row["document_id"]
    input_uri = row["current_uri"]
    source_text, source_etag = _read_text(input_uri)
    output_prefix = row["output_prefix"].rstrip("/")
    config_hash = canonical_hash(config)
    stage_prefix = f"{output_prefix}/_stages/{stage}"
    stage_uri = f"{stage_prefix}/{document_id}.md"
    audit_uri = f"{output_prefix}/_control/{batch_id}/stages/{stage}/{document_id}.json"
    input_count = _line_count(source_text)

    if stage == "step_job1":
        output_text, metrics = clean_structure(source_text, config)
        outputs = {stage_uri: (output_text.encode(), "text/markdown; charset=utf-8")}
    elif stage == "step_job2":
        output_text, metrics = normalize_and_quality(source_text, config)
        outputs = {
            stage_uri: (output_text.encode(), "text/markdown; charset=utf-8"),
            f"{stage_prefix}/{document_id}.quality.json": (
                json.dumps(metrics, ensure_ascii=False, indent=2).encode(),
                "application/json",
            ),
        }
    elif stage == "step_job3":
        output_text = source_text.strip() + "\n"
        samples = split_training_samples(output_text, document_id, row["source_uri"], config)
        jsonl = "".join(json.dumps(sample, ensure_ascii=False) + "\n" for sample in samples)
        quality_uri = row.get("quality_uri")
        quality = {}
        if quality_uri:
            quality, _ = _read_text(quality_uri)
            quality = json.loads(quality)
        metrics = {
            "sample_count": len(samples),
            "character_count": len(re.sub(r"\s+", "", output_text)),
            "empty_document": not bool(output_text.strip()),
            "quality": quality,
        }
        final_prefix = f"{output_prefix}/{document_id}"
        provenance = {
            "document_id": document_id,
            "source_uri": row["source_uri"],
            "source_etag": row.get("source_etag", source_etag),
            "cleaner_version": CLEANER_VERSION,
            "stage_config_hash": config_hash,
            "completed_at": utc_now(),
        }
        outputs = {
            f"{final_prefix}/cleaned.md": (output_text.encode(), "text/markdown; charset=utf-8"),
            f"{final_prefix}/pretrain.jsonl": (jsonl.encode(), "application/x-ndjson"),
            f"{final_prefix}/quality_report.json": (json.dumps(metrics, ensure_ascii=False, indent=2).encode(), "application/json"),
            f"{final_prefix}/provenance.json": (json.dumps(provenance, ensure_ascii=False, indent=2).encode(), "application/json"),
        }
    else:
        raise ValueError(f"unknown cleaning stage: {stage}")

    for uri, (body, content_type) in outputs.items():
        _write_bytes(uri, body, content_type)
    elapsed = round(time.time() - started, 3)
    result = StageResult(
        status="success",
        stage=stage,
        document_id=document_id,
        input_uri=input_uri,
        output_uri=stage_uri if stage != "step_job3" else f"{output_prefix}/{document_id}",
        input_count=input_count,
        output_count=len(samples) if stage == "step_job3" else _line_count(output_text),
        elapsed_seconds=elapsed,
        metrics=metrics,
    )
    audit = result.to_dict() | {
        "run_id": run_id,
        "source_etag": source_etag,
        "config_hash": config_hash,
        "completed_at": utc_now(),
    }
    _write_json(audit_uri, audit)
    if stage == "step_job3":
        _write_json(f"{output_prefix}/{document_id}/_SUCCESS.json", audit)
    return result
