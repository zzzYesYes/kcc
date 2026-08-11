from __future__ import annotations

import json
from typing import Any

import boto3
from botocore.config import Config
from dagster import ConfigurableResource, EnvVar


class S3Resource(ConfigurableResource):
    endpoint_url: str = EnvVar("S3_ENDPOINT_URL")
    region_name: str = "us-east-1"

    def client(self):
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            region_name=self.region_name,
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
        )

    def read_json(self, bucket: str, key: str) -> Any:
        body = self.client().get_object(Bucket=bucket, Key=key)["Body"].read()
        return json.loads(body)

    def write_json(self, bucket: str, key: str, value: Any) -> None:
        self.client().put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(value, ensure_ascii=False, indent=2).encode(),
            ContentType="application/json",
        )

    def exists(self, bucket: str, key: str) -> bool:
        try:
            self.client().head_object(Bucket=bucket, Key=key)
            return True
        except self.client().exceptions.ClientError as exc:
            if exc.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 404:
                return False
            raise

    def list_prefix(self, bucket: str, prefix: str):
        paginator = self.client().get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            yield from page.get("Contents", [])
