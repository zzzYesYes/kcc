"""Stable v1 runtime entrypoint with idempotent artifact publication."""

from __future__ import annotations

from typing import Sequence

from kcc_training.artifact_publish_v1 import publish_directory

from . import coordinator_release


def main(argv: Sequence[str] | None = None) -> int:
    return coordinator_release.main(argv, publish_fn=publish_directory)


if __name__ == "__main__":
    raise SystemExit(main())
