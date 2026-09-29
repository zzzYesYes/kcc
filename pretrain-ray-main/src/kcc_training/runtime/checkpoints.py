from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import shutil
from typing import Any, Mapping, Sequence


TRACKER = "latest_checkpointed_iteration.txt"
MAX_TRACKER_BYTES = 4096
MAX_FILES = 100000
SAMPLE_BYTES = 64 * 1024
SAMPLE_BLOCKS = 3
ITERATION_DIRECTORY = re.compile(r"^iter_([0-9]{7,})$")


class CheckpointError(RuntimeError):
    pass


class CheckpointUnavailable(CheckpointError):
    """No checkpoint has been committed yet."""


def discard_uncommitted(
    checkpoint_root: Path,
    keep_iteration: int | None,
) -> dict[str, Any]:
    """Delete checkpoints created after the last controller-approved baseline."""
    if keep_iteration is not None and keep_iteration <= 0:
        raise CheckpointError("retained checkpoint iteration must be positive")
    if not checkpoint_root.is_absolute():
        raise CheckpointError("checkpoint cleanup root must be absolute")
    if checkpoint_root.is_symlink():
        raise CheckpointError("checkpoint cleanup root cannot be a symbolic link")
    if not checkpoint_root.exists():
        return {"removedEntries": 0, "retainedIteration": keep_iteration}
    if not checkpoint_root.is_dir():
        raise CheckpointError("checkpoint cleanup root is not a directory")

    removed = 0
    if keep_iteration is None:
        removed = sum(1 for _ in checkpoint_root.iterdir())
        shutil.rmtree(checkpoint_root)
        return {"removedEntries": removed, "retainedIteration": None}

    retained = checkpoint_root / f"iter_{keep_iteration:07d}"
    if not retained.is_dir() or retained.is_symlink():
        raise CheckpointError("retained checkpoint directory is missing or unsafe")
    for candidate in list(checkpoint_root.iterdir()):
        match = ITERATION_DIRECTORY.fullmatch(candidate.name)
        if match and int(match.group(1)) <= keep_iteration:
            continue
        if candidate.name == TRACKER:
            continue
        if candidate.is_symlink() or candidate.is_file():
            candidate.unlink()
        elif candidate.is_dir():
            shutil.rmtree(candidate)
        else:
            raise CheckpointError("checkpoint cleanup entry has unsupported type")
        removed += 1
    temporary = checkpoint_root / f".{TRACKER}.tmp"
    temporary.write_text(f"{keep_iteration}\n", encoding="utf-8")
    os.replace(temporary, checkpoint_root / TRACKER)
    return {"removedEntries": removed, "retainedIteration": keep_iteration}


def _sample_sha256(path: Path, size: int) -> str:
    digest = hashlib.sha256()
    digest.update(b"kcc-checkpoint-sampled-v1\0")
    digest.update(str(size).encode("ascii"))
    with path.open("rb") as stream:
        if size <= SAMPLE_BYTES * SAMPLE_BLOCKS:
            for chunk in iter(lambda: stream.read(SAMPLE_BYTES), b""):
                digest.update(chunk)
        else:
            offsets = (0, (size - SAMPLE_BYTES) // 2, size - SAMPLE_BYTES)
            for offset in offsets:
                stream.seek(offset)
                chunk = stream.read(SAMPLE_BYTES)
                digest.update(offset.to_bytes(8, "big"))
                digest.update(chunk)
    return digest.hexdigest()


def snapshot(checkpoint_root: Path) -> dict[str, Any]:
    tracker = checkpoint_root / TRACKER
    try:
        if not tracker.exists():
            raise CheckpointUnavailable("no checkpoint has been committed")
        if tracker.is_symlink() or not tracker.is_file():
            raise CheckpointError("checkpoint tracker is not a regular file")
        if tracker.stat().st_size > MAX_TRACKER_BYTES:
            raise CheckpointError("checkpoint tracker is too large")
        tracker_bytes = tracker.read_bytes()
        value = tracker_bytes.decode("utf-8").strip()
    except CheckpointError:
        raise
    except (OSError, UnicodeError) as error:
        raise CheckpointError(f"cannot read checkpoint tracker: {error}") from error
    if value == "release":
        raise CheckpointError("release checkpoint is not resumable")
    try:
        iteration = int(value)
    except ValueError as error:
        raise CheckpointError("checkpoint tracker is not an iteration") from error
    if iteration <= 0:
        raise CheckpointError("checkpoint iteration must be positive")
    selected = checkpoint_root / f"iter_{iteration:07d}"
    if not selected.is_dir() or selected.is_symlink():
        raise CheckpointError("committed checkpoint directory is missing or unsafe")

    files: list[tuple[str, int, int, str]] = []
    for candidate in sorted(selected.rglob("*")):
        if candidate.is_symlink():
            raise CheckpointError("checkpoint contains a symbolic link")
        if not candidate.is_file():
            continue
        try:
            initial = candidate.stat()
            size = initial.st_size
            content_sha256 = _sample_sha256(candidate, size)
            final = candidate.stat()
        except OSError as error:
            raise CheckpointError(f"cannot inspect checkpoint shard: {error}") from error
        if (initial.st_size, initial.st_mtime_ns) != (
            final.st_size,
            final.st_mtime_ns,
        ):
            raise CheckpointError("checkpoint shard changed while it was inspected")
        files.append(
            (str(candidate.relative_to(checkpoint_root)), size, initial.st_mtime_ns, content_sha256)
        )
        if len(files) > MAX_FILES:
            raise CheckpointError("checkpoint contains too many files")
    if not files:
        raise CheckpointError("checkpoint contains no shards")

    comparable = {
        "iteration": iteration,
        "trackerSha256": hashlib.sha256(tracker_bytes).hexdigest(),
        "hashMode": "sampled-v1",
        "sampleBytesPerFile": SAMPLE_BYTES * SAMPLE_BLOCKS,
        "files": files,
    }
    digest = hashlib.sha256(
        json.dumps(comparable, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        **comparable,
        "available": True,
        "snapshotSha256": digest,
        "selectedDir": str(selected),
    }


def require_consistent(
    views: Sequence[Mapping[str, Any]], workers: int
) -> Mapping[str, Any] | None:
    if len(views) != workers or not views:
        raise CheckpointError("checkpoint view is missing from a worker")
    availability = [view.get("available", True) is True for view in views]
    if not any(availability):
        return None
    if not all(availability):
        raise CheckpointError("workers disagree on checkpoint availability")
    digests = {view.get("snapshotSha256") for view in views}
    if len(digests) != 1 or None in digests:
        raise CheckpointError("workers see different committed checkpoints")
    return views[0]
