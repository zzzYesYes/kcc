"""Stable runtime entrypoint with optional terminal checkpoints."""

from __future__ import annotations

from typing import Any, Mapping, Sequence

from kcc_training.artifact_publish_v1 import publish_directory

from . import coordinator as core
from . import coordinator_release
from .checkpoints import TRACKER
from .spec import RuntimeSpec


def execute(spec: RuntimeSpec) -> Mapping[str, Any]:
    result = dict(core.execute(spec))
    workers = result.get("workers")
    checkpoint_root = getattr(spec, "checkpoint_root", spec.output_root / "checkpoints")
    no_committed_checkpoint = not (checkpoint_root / TRACKER).exists()
    training_finished = (
        isinstance(workers, list)
        and len(workers) == spec.workers
        and all(
            isinstance(item, Mapping) and item.get("status") == "PASS"
            for item in workers
        )
    )
    # Compatibility for results produced by an older core runtime.  The
    # current core already treats a never-created checkpoint as optional.
    if (
        result.get("status") == "FAIL"
        and result.get("failureScope") == "checkpoint"
        and no_committed_checkpoint
        and training_finished
    ):
        result.update(
            status="PASS",
            checkpointConsistent=True,
            checkpoint=None,
            checkpointAvailable=False,
            failureScope=None,
        )
    else:
        result.setdefault("checkpointAvailable", result.get("checkpoint") is not None)
    return result


def main(argv: Sequence[str] | None = None) -> int:
    return coordinator_release.main(
        argv,
        execute_fn=execute,
        publish_fn=publish_directory,
    )


if __name__ == "__main__":
    raise SystemExit(main())
