from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any, Mapping


MAX_SPEC_BYTES = 1024 * 1024


class RuntimeSpecError(ValueError):
    pass


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise RuntimeSpecError(f"{label} must be an object")
    return value


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise RuntimeSpecError(f"{label} must be a non-empty trimmed string")
    return value


def _integer(value: Any, label: str, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RuntimeSpecError(f"{label} must be an integer >= {minimum}")
    return value


@dataclass(frozen=True)
class RuntimeSpec:
    run_name: str
    namespace: str
    run_uid: str
    attempt: int
    workers: int
    nodes: tuple[str, ...]
    devices_per_node: int
    resource_name: str
    ranktable_path: Path
    command: tuple[str, ...]
    working_directory: Path
    environment: Mapping[str, str]
    no_progress_seconds: int
    output_root: Path
    checkpoint_root: Path
    artifact_provider: str

    @classmethod
    def load(cls, path: Path) -> "RuntimeSpec":
        try:
            if path.stat().st_size > MAX_SPEC_BYTES:
                raise RuntimeSpecError("runtime spec is too large")
            document = json.loads(path.read_text(encoding="utf-8"))
        except RuntimeSpecError:
            raise
        except (OSError, UnicodeError, json.JSONDecodeError) as error:
            raise RuntimeSpecError(f"cannot load runtime spec: {error}") from error
        root = _mapping(document, "runtime spec")
        if root.get("schemaVersion") != "kcc-runtime/v1":
            raise RuntimeSpecError("unsupported runtime spec schema")
        run = _mapping(root.get("run"), "run")
        topology = _mapping(root.get("topology"), "topology")
        training = _mapping(root.get("training"), "training")
        artifacts = _mapping(root.get("artifacts"), "artifacts")
        artifact_provider = _text(
            artifacts.get("provider", "gateway"), "artifacts.provider"
        )
        if artifact_provider not in {"gateway", "workspace"}:
            raise RuntimeSpecError("artifacts.provider must be gateway or workspace")
        workers = _integer(topology.get("workers"), "workers", 1)
        raw_nodes = topology.get("nodes")
        if not isinstance(raw_nodes, list):
            raise RuntimeSpecError("nodes must be a list")
        nodes = tuple(_text(item, "node") for item in raw_nodes)
        if len(nodes) != workers or len(set(nodes)) != workers:
            raise RuntimeSpecError("nodes must match workers and be unique")
        raw_command = training.get("command")
        if not isinstance(raw_command, list) or not raw_command:
            raise RuntimeSpecError("training command must be a non-empty list")
        command = tuple(_text(item, "command item") for item in raw_command)
        raw_environment = _mapping(training.get("environment"), "environment")
        if not all(isinstance(key, str) and isinstance(value, str) for key, value in raw_environment.items()):
            raise RuntimeSpecError("environment must contain strings")
        ranktable = Path(_text(topology.get("rankTablePath"), "rankTablePath"))
        cwd = Path(_text(training.get("workingDirectory"), "workingDirectory"))
        output = Path(_text(artifacts.get("outputRoot"), "outputRoot"))
        raw_checkpoint = artifacts.get("checkpointRoot")
        checkpoint = (
            output / "checkpoints"
            if raw_checkpoint is None
            else Path(_text(raw_checkpoint, "checkpointRoot"))
        )
        if not all(path.is_absolute() for path in (ranktable, cwd, output, checkpoint)):
            raise RuntimeSpecError("runtime paths must be absolute")
        return cls(
            run_name=_text(run.get("name"), "run.name"),
            namespace=_text(run.get("namespace"), "run.namespace"),
            run_uid=_text(run.get("uid"), "run.uid"),
            attempt=_integer(run.get("attempt"), "run.attempt"),
            workers=workers,
            nodes=nodes,
            devices_per_node=_integer(topology.get("devicesPerNode"), "devicesPerNode", 1),
            resource_name=_text(topology.get("resourceName"), "resourceName"),
            ranktable_path=ranktable,
            command=command,
            working_directory=cwd,
            environment=dict(raw_environment),
            no_progress_seconds=_integer(training.get("noProgressSeconds"), "noProgressSeconds"),
            output_root=output,
            checkpoint_root=checkpoint,
            artifact_provider=artifact_provider,
        )

