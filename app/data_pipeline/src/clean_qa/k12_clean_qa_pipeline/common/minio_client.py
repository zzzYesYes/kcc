from __future__ import annotations

import json
import os
from collections.abc import Iterator
from typing import Any


class ObjectStore:
    def __init__(self):
        import boto3
        from botocore.config import Config

        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT_URL"],
            region_name=os.environ.get("AWS_DEFAULT_REGION", "us-east-1"),
            config=Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                retries={"max_attempts": 5, "mode": "standard"},
            ),
        )

    def iter_objects(self, bucket: str, prefix: str) -> Iterator[dict[str, Any]]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            yield from page.get("Contents", [])

    def list_keys(self, bucket: str, prefix: str) -> list[str]:
        return [item["Key"] for item in self.iter_objects(bucket, prefix)]

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except self.client.exceptions.ClientError as exc:
            status = exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if status == 404:
                return False
            raise

    def head(self, bucket: str, key: str) -> dict[str, Any]:
        return self.client.head_object(Bucket=bucket, Key=key)

    def read_bytes(self, bucket: str, key: str) -> bytes:
        return self.client.get_object(Bucket=bucket, Key=key)["Body"].read()

    def read_text(self, bucket: str, key: str) -> str:
        return self.read_bytes(bucket, key).decode("utf-8", "replace")

    def read_json(self, bucket: str, key: str) -> Any:
        return json.loads(self.read_bytes(bucket, key))

    def put_bytes(
        self,
        bucket: str,
        key: str,
        body: bytes,
        content_type: str = "application/octet-stream",
    ) -> None:
        self.client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
        )

    def put_json(self, bucket: str, key: str, value: Any) -> None:
        body = json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8")
        self.put_bytes(bucket, key, body, "application/json")

    def delete(self, bucket: str, key: str) -> None:
        self.client.delete_object(Bucket=bucket, Key=key)

    def copy(self, bucket: str, source_key: str, target_key: str) -> None:
        self.client.copy_object(
            Bucket=bucket,
            Key=target_key,
            CopySource={"Bucket": bucket, "Key": source_key},
        )

