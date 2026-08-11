from __future__ import annotations

from typing import Any

from .hashing import canonical_sha256


def prefixes_overlap(
    left_bucket: str,
    left_prefix: str,
    right_bucket: str,
    right_prefix: str,
) -> bool:
    if left_bucket != right_bucket:
        return False
    left = left_prefix.strip("/")
    right = right_prefix.strip("/")
    return left == right or left.startswith(right + "/") or right.startswith(left + "/")


def resume_contract_matches(marker: dict[str, Any], contract: dict[str, Any]) -> bool:
    return marker.get("input_contract_sha256") == canonical_sha256(contract)

