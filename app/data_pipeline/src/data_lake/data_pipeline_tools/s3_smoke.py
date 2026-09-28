from __future__ import annotations

import argparse
import json
import os
import uuid

import boto3
from botocore.config import Config


DEFAULT_BUCKETS = (
    "k12-textbook-raw",
    "k12-textbook-meta",
    "k12-mineru-output",
    "k12-cleaned-corpus",
    "k12-vector-artifacts",
    "k12-rejected",
)


def client():
    endpoint = os.environ.get("S3_ENDPOINT_URL")
    if not endpoint:
        raise ValueError("S3_ENDPOINT_URL is required")
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def expected_buckets() -> list[str]:
    raw = os.environ.get("EXPECTED_BUCKETS", ",".join(DEFAULT_BUCKETS))
    return [value.strip() for value in raw.split(",") if value.strip()]


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate K12 MinIO buckets and optional writes.")
    parser.add_argument("--write-probe", action="store_true")
    args = parser.parse_args()

    s3 = client()
    available = {item["Name"] for item in s3.list_buckets().get("Buckets", [])}
    expected = expected_buckets()
    missing = sorted(set(expected) - available)
    result = {"expected": expected, "available": sorted(available), "missing": missing}
    if missing:
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 2

    if args.write_probe:
        bucket = "k12-textbook-meta"
        key = f"_control/smoke/{uuid.uuid4().hex}.json"
        body = json.dumps({"status": "ok", "key": key}).encode()
        try:
            s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json")
            returned = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
            if returned != body:
                raise RuntimeError("S3 write probe content mismatch")
            result["write_probe"] = "passed"
        finally:
            s3.delete_object(Bucket=bucket, Key=key)

    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
