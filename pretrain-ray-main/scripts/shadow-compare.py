#!/usr/bin/env python3
"""Fail-closed comparison of normalized legacy and release run evidence."""

from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Mapping


FIELDS = (
    "status",
    "workers",
    "worldSize",
    "rankTableSha256",
    "checkpointStep",
    "checkpointSha256",
    "outputSha256",
)


def load(path: Path) -> Mapping[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, Mapping):
        raise ValueError(f"{path} is not a JSON object")
    missing = [field for field in FIELDS if field not in value]
    if missing:
        raise ValueError(f"{path} is missing {', '.join(missing)}")
    return value


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print("usage: shadow-compare.py LEGACY.json RELEASE.json", file=sys.stderr)
        return 2
    legacy, release = load(Path(argv[1])), load(Path(argv[2]))
    differences = {
        field: {"legacy": legacy[field], "release": release[field]}
        for field in FIELDS
        if legacy[field] != release[field]
    }
    print(json.dumps({"pass": not differences, "differences": differences}, sort_keys=True))
    return 0 if not differences else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
