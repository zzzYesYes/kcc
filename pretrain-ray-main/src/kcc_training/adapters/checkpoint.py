"""Bounded checkpoint evidence reader for controller-side recovery decisions."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping, Sequence


MAX_EVIDENCE_BYTES = 1024 * 1024


class CheckpointEvidenceError(RuntimeError):
    pass


class CheckpointEvidenceAdapter:
    def __init__(self, evidence_root: Path) -> None:
        self._root = evidence_root

    def _read(self, worker: str) -> Mapping[str, Any]:
        if Path(worker).name != worker or worker in {".", ".."}:
            raise CheckpointEvidenceError(f"unsafe worker identity: {worker!r}")
        path = self._root / f"{worker}.json"
        try:
            metadata = path.stat()
            if not path.is_file() or path.is_symlink():
                raise CheckpointEvidenceError(f"checkpoint evidence is not regular: {path}")
            if metadata.st_size > MAX_EVIDENCE_BYTES:
                raise CheckpointEvidenceError(f"checkpoint evidence is too large: {path}")
            value = json.loads(path.read_text(encoding="utf-8"))
        except CheckpointEvidenceError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise CheckpointEvidenceError(f"cannot read checkpoint evidence {path}: {error}") from error
        if not isinstance(value, Mapping) or value.get("status") != "PASS":
            raise CheckpointEvidenceError(f"checkpoint evidence is not PASS: {path}")
        iteration = value.get("iteration")
        digest = value.get("snapshotSha256")
        if (
            isinstance(iteration, bool)
            or not isinstance(iteration, int)
            or iteration <= 0
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
        ):
            raise CheckpointEvidenceError(f"checkpoint evidence fields are invalid: {path}")
        return value

    def _views(self, workers: Sequence[str]) -> tuple[Mapping[str, Any], ...]:
        if not workers:
            raise CheckpointEvidenceError("checkpoint worker list is empty")
        return tuple(self._read(worker) for worker in workers)

    def committed_iteration(self, workers: Sequence[str]) -> int | None:
        try:
            views = self._views(workers)
        except CheckpointEvidenceError:
            return None
        snapshots = {(view["iteration"], view["snapshotSha256"]) for view in views}
        return int(views[0]["iteration"]) if len(snapshots) == 1 else None

    def views_are_consistent(self, workers: Sequence[str]) -> bool:
        return self.committed_iteration(workers) is not None

