from __future__ import annotations

import hashlib
import json
from typing import Any


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def sha256_text(value: str) -> str:
    return sha256_bytes(value.encode("utf-8"))


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return sha256_bytes(canonical_json(value))


def stable_id(*parts: object, prefix: str) -> str:
    payload = "\x1f".join(str(part) for part in parts)
    return f"{prefix}-{sha256_text(payload)[:24]}"

