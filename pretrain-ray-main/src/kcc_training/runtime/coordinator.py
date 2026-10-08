"""HCCL-gated, whole-node Ray training coordinator."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

from kcc_training.kube_api import KubernetesApi, core_namespaced_path, utc_now
from kcc_training.raycluster import CONTROL_SCHEMA, attempt_name

from .checkpoints import (
    TRACKER,
    CheckpointError,
    CheckpointUnavailable,
    discard_uncommitted,
    require_consistent,
    snapshot,
)
from .evaluation import EvaluationConfig
from .evaluation_coordinator import EvaluationSupervisor
from .spec import RuntimeSpec
from .worker import StructuredWorker


DEFAULT_PROBE_BINARY = "/opt/kcc-hccl/bin/ranktable_allreduce_probe"
CONTROL_FILE = Path("/etc/kcc/control/control.json")
MAX_CONTROL_BYTES = 64 * 1024
PROGRESS_SCHEMA = "kcc-runtime-progress/v1"
MAX_PROGRESS_HISTORY = 500


def _progress_history(
    current: Mapping[str, Any] | None,
    progress: Mapping[str, Any],
) -> list[Mapping[str, Any]]:
    """Keep stage milestones while coalescing repetitive training heartbeats."""
    history: list[Mapping[str, Any]] = []
    data = current.get("data") if isinstance(current, Mapping) else None
    raw = data.get("history.json") if isinstance(data, Mapping) else None
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            parsed = None
        if isinstance(parsed, list):
            history = [item for item in parsed if isinstance(item, Mapping)]

    if history and all(
        history[-1].get(key) == progress.get(key)
        for key in ("stage", "status", "message")
    ):
        history[-1] = dict(progress)
    else:
        history.append(dict(progress))

    if len(history) > MAX_PROGRESS_HISTORY:
        # Early entries contain HCCL/RankTable evidence; keep those as well as
        # the newest runtime activity.
        history = history[:100] + history[-(MAX_PROGRESS_HISTORY - 100) :]
    return history


def _training_output_chunk(path: Path, offset: int, max_bytes: int) -> tuple[str, int]:
    if not path.is_file():
        return "", offset
    with path.open("rb") as stream:
        stream.seek(max(offset, 0))
        payload = stream.read(max_bytes)
        next_offset = stream.tell()
    return payload.decode("utf-8", errors="replace"), next_offset


def publish_progress(
    spec: RuntimeSpec,
    stage: str,
    status: str,
    message: str,
    **details: Any,
) -> None:
    """Emit human-visible progress and publish the latest structured stage."""
    progress = {
        "schemaVersion": PROGRESS_SCHEMA,
        "runName": spec.run_name,
        "runUid": spec.run_uid,
        "attempt": spec.attempt,
        "stage": stage,
        "status": status,
        "message": message,
        "updatedAt": utc_now(),
        "details": details,
    }
    print(
        "KCC_PROGRESS " + json.dumps(progress, ensure_ascii=False, sort_keys=True),
        flush=True,
    )
    cluster_name = attempt_name(spec.run_name, spec.attempt)
    name = f"{cluster_name}-progress"
    item_path = core_namespaced_path(spec.namespace, "configmaps", name)
    try:
        api = KubernetesApi()
    except Exception as error:
        print(f"KCC_PROGRESS publish warning: {error}", file=sys.stderr, flush=True)
        return
    try:
        current = api.get(item_path)
    except Exception as error:
        current = None
        print(
            f"KCC_PROGRESS history read warning: {error}",
            file=sys.stderr,
            flush=True,
        )
    history = _progress_history(current, progress)
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": spec.namespace,
            "labels": {"training.kcc.io/run": spec.run_name},
            "annotations": {
                "training.kcc.io/run-uid": spec.run_uid,
                "training.kcc.io/attempt": str(spec.attempt),
            },
            "ownerReferences": [
                {
                    "apiVersion": "training.kcc.io/v1beta1",
                    "kind": "TrainingRun",
                    "name": spec.run_name,
                    "uid": spec.run_uid,
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "data": {
            "progress.json": json.dumps(
                progress, ensure_ascii=False, sort_keys=True, default=str
            ),
            "history.json": json.dumps(
                history, ensure_ascii=False, sort_keys=True, default=str
            ),
        },
    }
    try:
        api.upsert(
            core_namespaced_path(spec.namespace, "configmaps"),
            item_path,
            document,
        )
    except Exception as error:
        print(f"KCC_PROGRESS publish warning: {error}", file=sys.stderr, flush=True)


class CoordinatorError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        scope: str = "infrastructure",
        failed_nodes: Sequence[str] = (),
    ) -> None:
        super().__init__(message)
        self.scope = scope
        self.failed_nodes = tuple(sorted(set(failed_nodes)))


def load_runtime_control(path: Path, spec: RuntimeSpec) -> Mapping[str, Any] | None:
    if not path.exists():
        return None
    try:
        if not path.is_file() or path.stat().st_size > MAX_CONTROL_BYTES:
            raise CoordinatorError("runtime control file is missing, invalid, or too large")
        value = json.loads(path.read_text(encoding="utf-8"))
    except CoordinatorError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise CoordinatorError(f"cannot read runtime control request: {error}") from error
    generation = value.get("requestGeneration") if isinstance(value, Mapping) else None
    if (
        not isinstance(value, Mapping)
        or value.get("schemaVersion") != CONTROL_SCHEMA
        or value.get("runName") != spec.run_name
        or value.get("runUid") != spec.run_uid
        or value.get("attempt") != spec.attempt
        or value.get("action") not in {"Continue", "StopAfterCheckpoint", "StopImmediate"}
        or isinstance(generation, bool)
        or not isinstance(generation, int)
        or generation < 1
    ):
        raise CoordinatorError("runtime control request identity or fields are invalid")
    return dict(value)


def consistent_checkpoint_iteration(
    views: Sequence[Mapping[str, Any]], workers: int
) -> int | None:
    if len(views) != workers or not views:
        raise CheckpointError("checkpoint tracker view is missing from a worker")
    availability = [view.get("available") is True for view in views]
    if not any(availability):
        return None
    if not all(availability):
        raise CheckpointError("workers disagree on checkpoint tracker availability")
    iterations = {
        view.get("iteration")
        for view in views
        if isinstance(view.get("iteration"), int)
        and not isinstance(view.get("iteration"), bool)
        and int(view["iteration"]) > 0
    }
    if len(iterations) != 1 or len(iterations) != len(
        {view.get("iteration") for view in views}
    ):
        raise CheckpointError("workers see different committed checkpoint iterations")
    return int(next(iter(iterations)))


def wait_ranktable(path: Path, timeout_seconds: int = 600) -> str:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            if path.is_file() and path.stat().st_size > 0:
                payload = path.read_bytes()
                value = json.loads(payload)
                if isinstance(value, Mapping):
                    return hashlib.sha256(payload).hexdigest()
        except (OSError, json.JSONDecodeError):
            pass
        time.sleep(2)
    raise CoordinatorError("RankTable did not become readable before timeout")


def _pipeline_stage(result: Mapping[str, Any], name: str) -> Mapping[str, Any] | None:
    stages = result.get("stages")
    if not isinstance(stages, list):
        return None
    for stage in stages:
        if isinstance(stage, Mapping) and stage.get("name") == name:
            return stage
    return None


def _failed_hccl_nodes(result: Mapping[str, Any]) -> tuple[str, ...]:
    stage = _pipeline_stage(result, "hccl")
    payload = stage.get("result") if stage is not None else None
    if not isinstance(payload, Mapping):
        return ()
    preflight = payload.get("preflight")
    preparations = preflight.get("workers") if isinstance(preflight, Mapping) else None
    rank_to_node: dict[int, str] = {}
    if isinstance(preparations, list):
        for item in preparations:
            if not isinstance(item, Mapping):
                continue
            rank_start = item.get("rank_start")
            node_name = item.get("node_name")
            if isinstance(rank_start, int) and isinstance(node_name, str) and node_name:
                rank_to_node[rank_start] = node_name
    workers = payload.get("workers")
    if not isinstance(workers, list):
        return ()
    failed: set[str] = set()
    for item in workers:
        if not isinstance(item, Mapping) or item.get("status") == "PASS":
            continue
        rank_start = item.get("rank_start")
        if "stop requested" in str(item.get("failure", "")).lower():
            continue
        if isinstance(rank_start, int) and rank_start in rank_to_node:
            failed.add(rank_to_node[rank_start])
    return tuple(sorted(failed))


def _node_ranks_from_hccl(
    result: Mapping[str, Any], spec: RuntimeSpec
) -> dict[str, int]:
    stage = _pipeline_stage(result, "hccl")
    payload = stage.get("result") if stage is not None else None
    preflight = payload.get("preflight") if isinstance(payload, Mapping) else None
    preparations = preflight.get("workers") if isinstance(preflight, Mapping) else None
    if not isinstance(preparations, list):
        raise CoordinatorError("HCCL result lacks worker rank evidence")
    node_ranks: dict[str, int] = {}
    for item in preparations:
        if not isinstance(item, Mapping):
            continue
        node_name = item.get("node_name")
        rank_start = item.get("rank_start")
        if not isinstance(node_name, str) or not node_name or not isinstance(rank_start, int):
            continue
        if rank_start % spec.devices_per_node:
            raise CoordinatorError("HCCL rank_start is not aligned to devicesPerNode")
        node_ranks[node_name] = rank_start // spec.devices_per_node
    if set(node_ranks) != set(spec.nodes):
        raise CoordinatorError("HCCL worker identities differ from frozen topology")
    if set(node_ranks.values()) != set(range(spec.workers)):
        raise CoordinatorError("HCCL rank_start values do not define all training node ranks")
    return node_ranks


def run_hccl_gate(spec: RuntimeSpec, attempt_root: Path) -> Mapping[str, Any]:
    output_dir = attempt_root / "hccl"
    probe_binary = os.environ.get("HCCL_CHECK_PROBE", DEFAULT_PROBE_BINARY)
    command = [
        sys.executable,
        "-m",
        "hccl_check",
        "--execute",
        "--output-dir",
        str(output_dir),
        "--namespace",
        spec.namespace,
        "--raycluster",
        attempt_name(spec.run_name, spec.attempt),
        "--ray-address",
        "auto",
        "--resource",
        "NPU",
        "--device-ids",
        ",".join(str(item) for item in range(spec.devices_per_node)),
        "--rank-table-path",
        str(spec.ranktable_path),
        "--probe-binary",
        probe_binary,
        "--expected-workers",
        str(spec.workers),
        "--expected-world-size",
        str(spec.workers * spec.devices_per_node),
        "--run-id",
        attempt_name(spec.run_name, spec.attempt),
    ]
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            shell=False,
            timeout=3600,
        )
    except subprocess.TimeoutExpired as error:
        raise CoordinatorError("HCCL gate timed out", scope="infrastructure") from error
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as error:
        detail = (completed.stderr or completed.stdout)[-4000:]
        raise CoordinatorError(
            f"HCCL gate returned invalid JSON: {detail}", scope="infrastructure"
        ) from error
    if not isinstance(result, Mapping):
        raise CoordinatorError("HCCL gate result is not an object")
    if completed.returncode != 0 or result.get("status") != "PASS":
        failed_stage = str(result.get("failed_stage") or "unknown")
        detail = str(result.get("failure") or completed.stderr or "pipeline failed")[-4000:]
        failed_nodes = _failed_hccl_nodes(result)
        infrastructure_markers = (
            "probe binary",
            "no module named",
            "could not start stage",
            "missing or not executable",
            "permission denied",
        )
        if any(marker in detail.lower() for marker in infrastructure_markers):
            scope = "infrastructure"
        elif failed_stage == "ping":
            scope = "network"
        elif failed_stage == "hccl" and failed_nodes:
            scope = "hardware"
        elif failed_stage == "hccl":
            scope = "network"
        else:
            scope = "infrastructure"
        raise CoordinatorError(
            f"HCCL gate failed in {failed_stage}: {detail}",
            scope=scope,
            failed_nodes=failed_nodes,
        )
    return result


MAX_RESULT_CONFIGMAP_BYTES = 768 * 1024
MAX_RESULT_TEXT_BYTES = 16 * 1024


def _bounded_text(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    payload = value.encode("utf-8")
    if len(payload) <= MAX_RESULT_TEXT_BYTES:
        return value
    excerpt = payload[:MAX_RESULT_TEXT_BYTES].decode("utf-8", errors="ignore")
    return f"{excerpt}\n...[truncated, originalBytes={len(payload)}]"


def _compact_checkpoint(value: Any) -> Any:
    if not isinstance(value, Mapping):
        return value
    files = value.get("files")
    file_count = value.get("fileCount", 0)
    total_bytes = value.get("totalBytes", 0)
    if not isinstance(file_count, int) or isinstance(file_count, bool):
        file_count = 0
    if not isinstance(total_bytes, int) or isinstance(total_bytes, bool):
        total_bytes = 0
    if isinstance(files, list):
        file_count = len(files)
        total_bytes = 0
        for item in files:
            if (
                isinstance(item, (list, tuple))
                and len(item) >= 2
                and isinstance(item[1], int)
            ):
                total_bytes += item[1]
    return {
        key: value[key]
        for key in (
            "available",
            "iteration",
            "trackerSha256",
            "hashMode",
            "sampleBytesPerFile",
            "snapshotSha256",
            "selectedDir",
        )
        if key in value
    } | {"fileCount": file_count, "totalBytes": total_bytes}


def _compact_result(result: Mapping[str, Any]) -> dict[str, Any]:
    compact = dict(result)
    if "failure" in compact:
        compact["failure"] = _bounded_text(compact["failure"])
    if "checkpoint" in compact:
        compact["checkpoint"] = _compact_checkpoint(compact.get("checkpoint"))
    workers = compact.get("workers")
    if isinstance(workers, list):
        compact["workers"] = [
            {
                key: _bounded_text(item[key])
                for key in (
                    "status",
                    "failureScope",
                    "failure",
                    "stopReason",
                    "nodeRank",
                    "nodeName",
                    "returncode",
                    "durationSeconds",
                    "stdout",
                    "stderr",
                )
                if isinstance(item, Mapping) and key in item
            }
            for item in workers
            if isinstance(item, Mapping)
        ]
    preflights = compact.get("preflights")
    if isinstance(preflights, list):
        compact["preflights"] = [
            {
                "status": item.get("status"),
                "identity": item.get("identity"),
            }
            for item in preflights
            if isinstance(item, Mapping)
        ]
    hccl = compact.get("hccl")
    if isinstance(hccl, Mapping):
        compact["hccl"] = {
            key: _bounded_text(hccl[key])
            for key in (
                "schema_version",
                "run_id",
                "status",
                "failure",
                "failed_stage",
                "output_dir",
                "plan_sha256",
                "ranktable_sha256",
            )
            if key in hccl
        }
    return compact


def _result_payload(result: Mapping[str, Any]) -> tuple[str, bool, str]:
    full = json.dumps(dict(result), ensure_ascii=False, sort_keys=True, default=str)
    digest = hashlib.sha256(full.encode("utf-8")).hexdigest()
    compact = _compact_result(result)
    payload = json.dumps(compact, ensure_ascii=False, sort_keys=True, default=str)
    if len(payload.encode("utf-8")) <= MAX_RESULT_CONFIGMAP_BYTES:
        return payload, compact != dict(result), digest
    essential_keys = (
        "schemaVersion",
        "runName",
        "namespace",
        "runUid",
        "attempt",
        "status",
        "trainingStatus",
        "checkpointConsistent",
        "checkpointAvailable",
        "checkpoint",
        "stopReason",
        "stopRequestGeneration",
        "stopBaselineIteration",
        "failureScope",
        "failedNodes",
        "failure",
        "outputArtifact",
        "publicationRetryable",
        "rankTableSha256",
        "evaluation",
    )
    summary = {key: compact[key] for key in essential_keys if key in compact}
    summary.update(diagnosticsTruncated=True, fullResultSha256=digest)
    payload = json.dumps(summary, ensure_ascii=False, sort_keys=True, default=str)
    if len(payload.encode("utf-8")) > MAX_RESULT_CONFIGMAP_BYTES:
        raise CoordinatorError("runtime result summary exceeds ConfigMap limit")
    return payload, True, digest


def _publish(result: Mapping[str, Any]) -> None:
    name = f"{attempt_name(str(result['runName']), int(result['attempt']))}-result"
    payload, truncated, result_sha256 = _result_payload(result)
    namespace = str(result["namespace"])
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"training.kcc.io/run": str(result["runName"])},
            "annotations": {
                "training.kcc.io/run-uid": str(result["runUid"]),
                "training.kcc.io/attempt": str(result["attempt"]),
                "training.kcc.io/result-sha256": result_sha256,
                "training.kcc.io/diagnostics-truncated": str(truncated).lower(),
            },
            "ownerReferences": [
                {
                    "apiVersion": "training.kcc.io/v1beta1",
                    "kind": "TrainingRun",
                    "name": str(result["runName"]),
                    "uid": str(result["runUid"]),
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "data": {"result.json": payload},
    }
    api = KubernetesApi()
    api.upsert(
        core_namespaced_path(namespace, "configmaps"),
        core_namespaced_path(namespace, "configmaps", name),
        document,
    )


def _checkpoint_evidence(
    checkpoint_root: Path,
) -> tuple[bool, Mapping[str, Any] | None, bool]:
    try:
        checkpoint = snapshot(checkpoint_root)
    except CheckpointUnavailable:
        return True, None, False
    except CheckpointError:
        return False, None, True
    return True, checkpoint, True


def _error_scope(error: Exception) -> str:
    if isinstance(error, CoordinatorError):
        return error.scope
    if isinstance(error, CheckpointError):
        return "checkpoint"
    if isinstance(error, (ConnectionError, OSError, subprocess.TimeoutExpired, TimeoutError)):
        return "infrastructure"
    if type(error).__module__.startswith("ray"):
        return "infrastructure"
    return "software"


def failure_result(
    spec: RuntimeSpec,
    error: Exception,
    *,
    base: Mapping[str, Any] | None = None,
    scope: str | None = None,
) -> dict[str, Any]:
    result = dict(base or {})
    if "checkpointConsistent" not in result:
        consistent, checkpoint, available = _checkpoint_evidence(spec.checkpoint_root)
        result.update(
            checkpointConsistent=consistent,
            checkpoint=checkpoint,
            checkpointAvailable=available,
        )
    result.update(
        schemaVersion="kcc-runtime-result/v1",
        runName=spec.run_name,
        namespace=spec.namespace,
        runUid=spec.run_uid,
        attempt=spec.attempt,
        status="FAIL",
        failureScope=scope or _error_scope(error),
        failedNodes=list(getattr(error, "failed_nodes", ())),
        failure=f"{type(error).__name__}: {error}",
    )
    return result


def _select_failure_scope(outcomes: Sequence[Mapping[str, Any]]) -> str:
    scopes = {
        str(item.get("failureScope"))
        for item in outcomes
        if item.get("status") not in {"PASS", "STOPPED"}
        and item.get("failureScope") is not None
    }
    for scope in ("hardware", "infrastructure", "network", "global-stall", "software"):
        if scope in scopes:
            return scope
    return "software"


def execute(spec: RuntimeSpec) -> Mapping[str, Any]:
    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except ImportError as error:
        raise CoordinatorError(f"Ray runtime is unavailable: {error}") from error
    expected_command = os.environ.get("KCC_COMMAND_JSON")
    if expected_command is not None:
        try:
            submitted_command = tuple(json.loads(expected_command))
        except (json.JSONDecodeError, TypeError) as error:
            raise CoordinatorError("submitted command binding is invalid") from error
        if submitted_command != spec.command:
            raise CoordinatorError("submitted command differs from mounted immutable spec")
    evaluation_config = EvaluationConfig.from_environment(
        spec.environment,
        spec.devices_per_node,
    )

    attempt_root = spec.output_root / f"attempt-{spec.attempt:02d}"
    attempt_root.mkdir(parents=True, exist_ok=False)
    log_root = attempt_root / "training"
    log_root.mkdir()

    publish_progress(
        spec,
        "CoordinatorStarting",
        "Running",
        "training coordinator started",
        workers=spec.workers,
        devicesPerNode=spec.devices_per_node,
    )

    # The pipeline owns RankTable preparation.  Waiting before it runs creates
    # a producer/consumer deadlock on a fresh attempt.
    hccl_started = time.monotonic()
    publish_progress(
        spec,
        "HcclTest",
        "Running",
        "discovering workers, generating RankTable, and running HCCL AllReduce",
        expectedWorkers=spec.workers,
        expectedRanks=spec.workers * spec.devices_per_node,
    )
    hccl_result = run_hccl_gate(spec, attempt_root)
    hccl_stage = _pipeline_stage(hccl_result, "hccl")
    hccl_payload = hccl_stage.get("result") if hccl_stage is not None else None
    single_rank_no_collective = (
        isinstance(hccl_payload, Mapping)
        and hccl_payload.get("single_rank_no_collective") is True
    )
    publish_progress(
        spec,
        "HcclTest",
        "Passed",
        (
            "single rank; no inter-rank HCCL collective is required"
            if single_rank_no_collective
            else "HCCL AllReduce passed on every rank"
        ),
        durationSeconds=round(time.monotonic() - hccl_started, 3),
        rankTableSha256=hccl_result.get("ranktable_sha256"),
        singleRankNoCollective=single_rank_no_collective,
    )
    raw_projection_timeout = os.environ.get(
        "KCC_RANKTABLE_PROJECTION_TIMEOUT_SECONDS", "120"
    )
    try:
        projection_timeout = int(raw_projection_timeout)
    except ValueError as error:
        raise CoordinatorError(
            "KCC_RANKTABLE_PROJECTION_TIMEOUT_SECONDS must be an integer"
        ) from error
    if projection_timeout <= 0:
        raise CoordinatorError(
            "KCC_RANKTABLE_PROJECTION_TIMEOUT_SECONDS must be positive"
        )
    ranktable_sha256 = wait_ranktable(spec.ranktable_path, projection_timeout)
    if hccl_result.get("ranktable_sha256") != ranktable_sha256:
        raise CoordinatorError("projected RankTable differs from HCCL evidence")
    node_ranks = _node_ranks_from_hccl(hccl_result, spec)
    publish_progress(
        spec,
        "RankTable",
        "Passed",
        "projected RankTable matches HCCL evidence",
        rankTableSha256=ranktable_sha256,
        nodeRanks=node_ranks,
    )

    publish_progress(spec, "RayWorkers", "Running", "connecting to Ray workers")
    try:
        ray.init(address="auto", log_to_driver=False)
    except Exception as error:
        raise CoordinatorError(f"could not connect to Ray: {error}") from error

    actors: list[Any] = []
    try:
        try:
            candidates = [
                node
                for node in ray.nodes()
                if node.get("Alive")
                and node.get("Resources", {}).get("NPU", 0) >= spec.devices_per_node
            ]
            if len(candidates) != spec.workers:
                raise CoordinatorError(
                    f"expected {spec.workers} Ray workers, found {len(candidates)}"
                )
            remote = ray.remote(
                num_cpus=0,
                max_restarts=0,
                max_task_retries=0,
                max_concurrency=2,
            )(StructuredWorker)
            for node in candidates:
                actors.append(
                    remote.options(
                        resources={"NPU": spec.devices_per_node},
                        scheduling_strategy=NodeAffinitySchedulingStrategy(
                            node["NodeID"], soft=False
                        ),
                    ).remote()
                )
            identities = list(
                ray.get([actor.identity.remote() for actor in actors], timeout=120)
            )
        except CoordinatorError:
            raise
        except Exception as error:
            raise CoordinatorError(f"Ray worker setup failed: {error}") from error

        by_node = {
            identity["NODE_NAME"]: (actor, identity)
            for actor, identity in zip(actors, identities)
        }
        if set(by_node) != set(spec.nodes) or len(by_node) != spec.workers:
            raise CoordinatorError("Ray worker identities differ from frozen topology")
        ordered = sorted(
            (
                node_ranks[node],
                node,
                by_node[node][0],
                by_node[node][1],
            )
            for node in spec.nodes
        )
        master_addr = ordered[0][3]["POD_IP"]
        publish_progress(
            spec,
            "RayWorkers",
            "Passed",
            "all Ray worker actors are bound to the selected nodes",
            nodes=[item[1] for item in ordered],
        )
        publish_progress(
            spec,
            "WorkerPreflight",
            "Running",
            "checking source, RankTable, and NPU devices on every worker",
        )
        try:
            preflights = list(
                ray.get(
                    [
                        actor.preflight.remote(
                            expected_node=node,
                            cwd=str(spec.working_directory),
                            ranktable=str(spec.ranktable_path),
                            ranktable_sha256=ranktable_sha256,
                            devices=spec.devices_per_node,
                        )
                        for _rank, node, actor, _identity in ordered
                    ],
                    timeout=300,
                )
            )
        except Exception as error:
            raise CoordinatorError(f"worker preflight failed: {error}") from error
        publish_progress(
            spec,
            "WorkerPreflight",
            "Passed",
            "worker preflight passed",
            passedWorkers=len(preflights),
        )

        resume_checkpoint: Mapping[str, Any] | None = None
        if spec.attempt > 0:
            before = list(
                ray.get(
                    [
                        actor.checkpoint.remote(str(spec.checkpoint_root))
                        for _rank, _node, actor, _identity in ordered
                    ],
                    timeout=300,
                )
            )
            resume_checkpoint = require_consistent(before, spec.workers)
        resume_from = (
            str(resume_checkpoint["selectedDir"])
            if resume_checkpoint is not None
            else None
        )
        retained_checkpoint_iteration = (
            int(resume_checkpoint["iteration"])
            if resume_checkpoint is not None
            else None
        )
        evaluation: EvaluationSupervisor | None = None
        if evaluation_config.enabled:
            evaluation_baseline = retained_checkpoint_iteration
            if evaluation_baseline is None:
                try:
                    baseline_views = list(
                        ray.get(
                            [
                                actor.checkpoint_iteration.remote(
                                    str(spec.checkpoint_root)
                                )
                                for _rank, _node, actor, _identity in ordered
                            ],
                            timeout=120,
                        )
                    )
                    evaluation_baseline = consistent_checkpoint_iteration(
                        baseline_views, spec.workers
                    )
                except Exception as error:
                    print(
                        "KCC_EVALUATION initial checkpoint observation warning: "
                        f"{type(error).__name__}: {error}",
                        flush=True,
                    )
            evaluation = EvaluationSupervisor(
                ray=ray,
                spec=spec,
                ordered_workers=ordered,
                config=evaluation_config,
                consistent_iteration=consistent_checkpoint_iteration,
                publish_progress=publish_progress,
                baseline_iteration=evaluation_baseline,
            )

        control_path = Path(os.environ.get("KCC_CONTROL_FILE", str(CONTROL_FILE)))
        try:
            control_poll_seconds = float(os.environ.get("KCC_CONTROL_POLL_SECONDS", "5"))
        except ValueError as error:
            raise CoordinatorError("KCC_CONTROL_POLL_SECONDS must be numeric") from error
        if control_poll_seconds <= 0:
            raise CoordinatorError("KCC_CONTROL_POLL_SECONDS must be positive")
        stop_request_generation: int | None = None
        stop_baseline_iteration = 0
        stop_issued = False
        immediate_stop_generation: int | None = None
        immediate_stop_issued = False

        pending: dict[Any, tuple[Any, Mapping[str, Any], int]] = {}
        for node_rank, _node, actor, identity in ordered:
            reference = actor.run.remote(
                node_rank=node_rank,
                workers=spec.workers,
                devices=spec.devices_per_node,
                master_addr=master_addr,
                master_port=29500 + spec.attempt,
                command=spec.command,
                cwd=str(spec.working_directory),
                environment=spec.environment,
                ranktable=str(spec.ranktable_path),
                log_root=str(log_root),
                checkpoint_root=str(spec.checkpoint_root),
                output_root=str(spec.output_root),
                attempt_root=str(attempt_root),
                attempt=spec.attempt,
                resume_from=resume_from,
                no_progress_seconds=spec.no_progress_seconds,
            )
            pending[reference] = (actor, identity, node_rank)

        outcomes: list[Mapping[str, Any]] = []
        failure = False
        training_started = time.monotonic()
        last_progress_report = 0.0
        training_log_offsets = {
            node_rank: {"stdout": 0, "stderr": 0}
            for node_rank, _node, _actor, _identity in ordered
        }

        def forward_training_output() -> None:
            for node_rank, node, _actor, _identity in ordered:
                for stream in ("stdout", "stderr"):
                    try:
                        text, next_offset = _training_output_chunk(
                            log_root / f"node-rank-{node_rank}" / f"{stream}.log",
                            training_log_offsets[node_rank][stream],
                            64 * 1024,
                        )
                    except OSError as error:
                        print(
                            f"KCC_TRAINING_OUTPUT read warning: {error}",
                            file=sys.stderr,
                            flush=True,
                        )
                        continue
                    training_log_offsets[node_rank][stream] = next_offset
                    if not text:
                        continue
                    print(
                        "KCC_TRAINING_OUTPUT "
                        + json.dumps(
                            {
                                "nodeRank": node_rank,
                                "nodeName": node,
                                "stream": stream,
                                "text": text,
                            },
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        flush=True,
                    )

        launch_details: dict[str, Any] = {
            "command": list(spec.command),
            "totalWorkers": spec.workers,
            "resumeFrom": resume_from,
        }
        if evaluation is not None:
            launch_details["evaluation"] = evaluation.summary()
        publish_progress(
            spec,
            "Training",
            "Running",
            "distributed training processes launched",
            **launch_details,
        )
        while pending:
            now = time.monotonic()
            if now - last_progress_report >= 10:
                last_progress_report = now
                checkpoint_iteration: int | None = None
                tracker = spec.checkpoint_root / TRACKER
                try:
                    if tracker.is_file():
                        checkpoint_iteration = int(
                            tracker.read_text(encoding="utf-8").strip()
                        )
                except (OSError, UnicodeError, ValueError):
                    checkpoint_iteration = None
                heartbeat_details: dict[str, Any] = {
                    "elapsedSeconds": round(now - training_started, 1),
                    "completedWorkers": len(outcomes),
                    "remainingWorkers": len(pending),
                    "checkpointIteration": checkpoint_iteration,
                }
                if evaluation is not None:
                    heartbeat_details["evaluation"] = evaluation.summary()
                publish_progress(
                    spec,
                    "Training",
                    "Running",
                    "distributed training is running",
                    **heartbeat_details,
                )
                forward_training_output()

            request = load_runtime_control(control_path, spec)
            if not stop_issued and not immediate_stop_issued and request is not None:
                if request["action"] == "Continue":
                    stop_request_generation = None
                    stop_baseline_iteration = 0
                    immediate_stop_generation = None
                elif request["action"] == "StopImmediate":
                    immediate_stop_generation = int(request["requestGeneration"])
                    immediate_stop_issued = True
                    publish_progress(
                        spec,
                        "CheckpointCleanup",
                        "Running",
                        "stopping workers before deleting unapproved checkpoints",
                        retainedIteration=retained_checkpoint_iteration,
                    )
                    ray.get(
                        [
                            actor.stop.remote(
                                f"immediate stop request {immediate_stop_generation}"
                            )
                            for _rank, _node, actor, _identity in ordered
                        ],
                        timeout=60,
                    )
                elif request["requestGeneration"] != stop_request_generation:
                    try:
                        baseline_views = list(
                            ray.get(
                                [
                                    actor.checkpoint_iteration.remote(
                                        str(spec.checkpoint_root)
                                    )
                                    for _rank, _node, actor, _identity in ordered
                                ],
                                timeout=120,
                            )
                        )
                        baseline = consistent_checkpoint_iteration(
                            baseline_views, spec.workers
                        )
                    except Exception:
                        baseline = None
                    else:
                        stop_request_generation = int(
                            request["requestGeneration"]
                        )
                        stop_baseline_iteration = baseline or 0

            if evaluation is not None:
                evaluation.tick(
                    allow_start=not immediate_stop_issued and not failure
                )

            if (
                stop_request_generation is not None
                and not stop_issued
                and not immediate_stop_issued
            ):
                try:
                    iteration_views = list(
                        ray.get(
                            [
                                actor.checkpoint_iteration.remote(
                                    str(spec.checkpoint_root)
                                )
                                for _rank, _node, actor, _identity in ordered
                            ],
                            timeout=120,
                        )
                    )
                    current_iteration = consistent_checkpoint_iteration(
                        iteration_views, spec.workers
                    )
                    if (
                        current_iteration is not None
                        and current_iteration > stop_baseline_iteration
                    ):
                        checkpoint_views = list(
                            ray.get(
                                [
                                    actor.checkpoint.remote(
                                        str(spec.checkpoint_root)
                                    )
                                    for _rank, _node, actor, _identity in ordered
                                ],
                                timeout=300,
                            )
                        )
                        candidate = require_consistent(
                            checkpoint_views, spec.workers
                        )
                        if (
                            candidate is None
                            or candidate.get("iteration") != current_iteration
                        ):
                            raise CheckpointError(
                                "checkpoint changed during graceful stop validation"
                            )
                        stop_issued = True
                        ray.get(
                            [
                                actor.stop.remote(
                                    "checkpoint stop request "
                                    f"{stop_request_generation}",
                                    cancel_evaluation=False,
                                )
                                for _rank, _node, actor, _identity in ordered
                            ],
                            timeout=60,
                        )
                except CheckpointError:
                    pass
            ready, _ = ray.wait(list(pending), num_returns=1, timeout=control_poll_seconds)
            if not ready:
                continue
            reference = ready[0]
            _actor, identity, node_rank = pending.pop(reference)
            try:
                raw_outcome = ray.get(reference)
                if not isinstance(raw_outcome, Mapping):
                    raise TypeError("worker result is not an object")
                outcome = dict(raw_outcome)
            except Exception as error:
                outcome = {
                    "status": "FAIL",
                    "failureScope": "infrastructure",
                    "failure": f"{type(error).__name__}: {error}",
                    "nodeName": identity["NODE_NAME"],
                    "nodeRank": node_rank,
                }
            outcomes.append(outcome)
            publish_progress(
                spec,
                "Training",
                "Running" if pending else "Passed",
                f"worker {identity['NODE_NAME']} finished with {outcome.get('status')}",
                completedWorkers=len(outcomes),
                remainingWorkers=len(pending),
                nodeName=identity["NODE_NAME"],
                nodeRank=node_rank,
                workerStatus=outcome.get("status"),
            )
            if outcome.get("status") not in {"PASS", "STOPPED"} and not failure:
                failure = True
                try:
                    ray.get(
                        [
                            actor.stop.remote("peer failure")
                            for _rank, _node, actor, _identity in ordered
                        ],
                        timeout=60,
                    )
                except Exception:
                    pass

        forward_training_output()
        if evaluation is not None:
            evaluation.settle(
                allow_new=not immediate_stop_issued and not failure,
                poll_seconds=control_poll_seconds,
            )

        checkpoint_cleanup: Mapping[str, Any] | None = None
        checkpoint_cleanup_error: str | None = None
        if immediate_stop_issued:
            try:
                checkpoint_cleanup = dict(
                    ray.get(
                        ordered[0][2].discard_uncommitted_checkpoints.remote(
                            str(spec.checkpoint_root),
                            retained_checkpoint_iteration,
                        ),
                        timeout=300,
                    )
                )
                checkpoint_cleanup = {**checkpoint_cleanup, "completed": True}
                publish_progress(
                    spec,
                    "CheckpointCleanup",
                    "Passed",
                    "unapproved checkpoints were deleted",
                    **checkpoint_cleanup,
                )
            except Exception as error:
                checkpoint_cleanup_error = f"{type(error).__name__}: {error}"
                checkpoint_cleanup = {
                    "completed": False,
                    "retainedIteration": retained_checkpoint_iteration,
                    "error": checkpoint_cleanup_error,
                }
                publish_progress(
                    spec,
                    "CheckpointCleanup",
                    "Failed",
                    "checkpoint cleanup failed",
                    **checkpoint_cleanup,
                )

        checkpoint: Mapping[str, Any] | None = None
        checkpoint_error: CheckpointError | None = None
        try:
            views = list(
                ray.get(
                    [
                        actor.checkpoint.remote(str(spec.checkpoint_root))
                        for _rank, _node, actor, _identity in ordered
                    ],
                    timeout=300,
                )
            )
            checkpoint = require_consistent(views, spec.workers)
        except Exception as error:
            checkpoint_error = (
                error
                if isinstance(error, CheckpointError)
                else CheckpointError(f"checkpoint inspection failed: {error}")
            )

        if stop_issued and (
            checkpoint is None
            or checkpoint.get("iteration", 0) <= stop_baseline_iteration
        ):
            checkpoint_error = CheckpointError(
                "graceful stop did not preserve a checkpoint newer than its baseline"
            )

        checkpoint_consistent = checkpoint_error is None
        checkpoint_available = checkpoint is not None
        outcomes.sort(key=lambda item: int(item.get("nodeRank", spec.workers)))
        if failure:
            status = "FAIL"
            failure_scope: str | None = _select_failure_scope(outcomes)
        elif checkpoint_error is not None:
            status = "FAIL"
            failure_scope = "checkpoint"
        elif evaluation is not None and evaluation.fatal_failure:
            status = "FAIL"
            failure_scope = "software"
        elif stop_issued or immediate_stop_issued:
            status = "STOPPED"
            failure_scope = None
        else:
            status = "PASS"
            failure_scope = None
        failed_nodes = sorted(
            {
                str(item["nodeName"])
                for item in outcomes
                if item.get("status") not in {"PASS", "STOPPED"}
                and isinstance(item.get("nodeName"), str)
            }
        )
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runName": spec.run_name,
            "namespace": spec.namespace,
            "runUid": spec.run_uid,
            "attempt": spec.attempt,
            "status": status,
            "checkpointConsistent": checkpoint_consistent,
            "checkpointAvailable": checkpoint_available,
            "checkpoint": checkpoint,
            "failureScope": failure_scope,
            "failedNodes": failed_nodes,
            "rankTableSha256": ranktable_sha256,
            "hccl": hccl_result,
            "preflights": preflights,
            "workers": outcomes,
        }
        if evaluation is not None:
            result["evaluation"] = dict(evaluation.summary())
            if (
                evaluation.fatal_failure
                and not failure
                and checkpoint_error is None
            ):
                result["failure"] = evaluation.failure_message
        if stop_issued:
            result.update(
                stopReason="AfterCheckpoint",
                stopRequestGeneration=stop_request_generation,
                stopBaselineIteration=stop_baseline_iteration,
            )
        elif immediate_stop_issued:
            result.update(
                stopReason="Immediate",
                stopRequestGeneration=immediate_stop_generation,
                checkpointCleanup=checkpoint_cleanup,
            )
        if checkpoint_error is not None:
            result["checkpointFailure"] = str(checkpoint_error)
        if checkpoint_cleanup_error is not None:
            result["checkpointCleanupFailure"] = checkpoint_cleanup_error
        completion_details: dict[str, Any] = {
            "completedWorkers": len(outcomes),
            "checkpointIteration": (
                checkpoint.get("iteration")
                if isinstance(checkpoint, Mapping)
                else None
            ),
        }
        if evaluation is not None:
            completion_details["evaluation"] = evaluation.summary()
        publish_progress(
            spec,
            "Training",
            "Passed" if status == "PASS" else status.title(),
            f"distributed training finished with {status}",
            **completion_details,
        )
        return result
    finally:
        for actor in actors:
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    spec: RuntimeSpec | None = None
    try:
        spec = RuntimeSpec.load(args.spec)
        result = execute(spec)
    except Exception as error:
        if spec is None:
            print(f"runtime refused invalid spec: {error}", file=sys.stderr)
            return 2
        result = failure_result(spec, error)
    try:
        _publish(result)
    except Exception as error:
        print(f"runtime result publication failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
