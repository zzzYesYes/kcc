"""Build a stable manifest for MinerU 3.4 Hybrid data-lake runs.

The Head only lists objects.  PDF bytes are intentionally downloaded by the
NPU Worker after Ray has assigned the document to a service.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from datetime import timezone
from pathlib import Path
from typing import Any

import boto3
from botocore.config import Config


def positive_int(value: str) -> int:
    value_as_int = int(value)
    if value_as_int < 1:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return value_as_int


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=os.environ["S3_ENDPOINT_URL"],
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def document_id(bucket: str, key: str, etag: str) -> str:
    digest = hashlib.sha1(f"{bucket}/{key}/{etag}".encode()).hexdigest()[:20]
    return f"pdf-{digest}"


def list_pdfs(bucket: str, prefix: str, limit: int | None, bytes_per_page: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for page in s3_client().get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            key = item["Key"]
            if not key.lower().endswith(".pdf"):
                continue
            etag = item.get("ETag", "").strip('"')
            size = int(item["Size"])
            rows.append(
                {
                    "document_id": document_id(bucket, key, etag),
                    "object_key": key,
                    "etag": etag,
                    "size_bytes": size,
                    "estimated_page_count": max(1, math.ceil(size / bytes_per_page)),
                    "last_modified": item["LastModified"].astimezone(timezone.utc).isoformat(),
                }
            )
    rows.sort(key=lambda row: row["object_key"])
    return rows if limit is None else rows[:limit]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-bucket", required=True)
    parser.add_argument("--input-prefix", default="")
    parser.add_argument("--output-bucket", required=True)
    parser.add_argument("--output-key", required=True)
    parser.add_argument("--local-output", type=Path, required=True)
    parser.add_argument("--limit", type=positive_int)
    parser.add_argument("--estimated-bytes-per-page", type=positive_int, default=458752)
    args = parser.parse_args()

    documents = list_pdfs(args.input_bucket, args.input_prefix, args.limit, args.estimated_bytes_per_page)
    if not documents:
        raise RuntimeError(f"no PDF objects under s3://{args.input_bucket}/{args.input_prefix}")
    manifest = {
        "source": {"bucket": args.input_bucket, "prefix": args.input_prefix},
        "selection": {"mode": "count" if args.limit else "all", "requested_limit": args.limit, "pdf_count": len(documents)},
        "documents": documents,
    }
    args.local_output.parent.mkdir(parents=True, exist_ok=True)
    args.local_output.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    s3_client().upload_file(str(args.local_output), args.output_bucket, args.output_key)
    print(json.dumps({"pdf_count": len(documents), "manifest_key": args.output_key}, ensure_ascii=False))


if __name__ == "__main__":
    main()
