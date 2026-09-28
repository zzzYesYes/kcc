from __future__ import annotations

import json
import uuid
from typing import Any

from .minio_client import ObjectStore


def atomic_write_bytes(
    store: ObjectStore,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> None:
    temp_key = f"{key}.tmp-{uuid.uuid4().hex}"
    store.put_bytes(bucket, temp_key, body, content_type)
    store.copy(bucket, temp_key, key)
    store.delete(bucket, temp_key)


def atomic_write_json(
    store: ObjectStore,
    bucket: str,
    key: str,
    value: Any,
) -> None:
    atomic_write_bytes(
        store,
        bucket,
        key,
        json.dumps(value, ensure_ascii=False, indent=2).encode("utf-8"),
        "application/json",
    )


def jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    if not rows:
        return b""
    return (
        "\n".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True)
            for row in rows
        )
        + "\n"
    ).encode("utf-8")

