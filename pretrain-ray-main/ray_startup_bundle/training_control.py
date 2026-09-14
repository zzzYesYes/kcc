#!/usr/bin/env python3
"""Stop the owned Ray training now or after its next checkpoint commit."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

import cluster_config
import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_STATE_ROOT = PROJECT_ROOT / "log" / "training-jobs"
DEFAULT_ARTIFACT_ROOT = PROJECT_ROOT / "log" / "training-runs"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
RECOVERY_STATE_SCHEMA = "ray-training-recovery/v1"
STOP_REQUEST_SCHEMA = "ray-training-stop/v1"
STOP_REQUEST_FILENAME = "stop-request.json"
STOP_MODES = {"IMMEDIATE", "AFTER_CHECKPOINT"}
TRACKER_FILENAME = "latest_checkpointed_iteration.txt"
DEFAULT_JSON_MAX_BYTES = 64 * 1024
RECOVERY_STATE_MAX_BYTES = 1024 * 1024


class ControlError(RuntimeError):
    pass


def run(
    args: argparse.Namespace,
    *arguments: str,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    prefix = shlex.split(args.kubectl_command)
    if not prefix:
        raise ControlError("kubectl command is empty")
    command = [
        *prefix,
        "--kubeconfig",
        str(args.kubeconfig.resolve()),
        *arguments,
    ]
    try:
        result = subprocess.run(
            command,
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise ControlError(f"cannot run {shlex.join(command)}: {error}") from error
    if check and result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise ControlError(
            f"command failed ({result.returncode}): {shlex.join(command)}"
            + (f": {detail}" if detail else "")
        )
    return result


def read_json(
    path: Path,
    label: str,
    *,
    max_bytes: int = DEFAULT_JSON_MAX_BYTES,
) -> dict[str, Any]:
    if not path.is_file() or path.is_symlink():
        raise ControlError(f"{label} is not a regular file: {path}")
    try:
        if path.stat().st_size > max_bytes:
            raise ControlError(f"{label} is unexpectedly large: {path}")
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ControlError(f"cannot read {label} {path}: {error}") from error
    if not isinstance(value, dict):
        raise ControlError(f"{label} is not a JSON object: {path}")
    return value


def create_json(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically create a marker without replacing an existing request."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp"
    )
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError as error:
            raise ControlError(f"stop request already exists: {path}") from error
    finally:
        temporary.unlink(missing_ok=True)


def load_accepted_stop_request(
    path: Path,
    *,
    expected_job_id: str,
) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    request = read_json(path, "stop request")
    if (
        request.get("schemaVersion") != STOP_REQUEST_SCHEMA
        or request.get("jobId") != expected_job_id
        or request.get("status") != "ACCEPTED"
        or request.get("mode") not in STOP_MODES
    ):
        raise ControlError("stop request has invalid ownership or status")
    return request


def load_compatible_stop_request(
    path: Path,
    *,
    expected_job_id: str,
    expected_attempt: str,
    current_cluster_uid: str | None,
    expected_mode: str,
    expected_iteration: int | None,
    reuse_recorded_iteration: bool = False,
) -> dict[str, Any] | None:
    """Load an accepted request only when it is safe to reuse for cleanup."""

    request = load_accepted_stop_request(
        path,
        expected_job_id=expected_job_id,
    )
    if request is None:
        return None
    required = {"attemptRunId", "clusterUid", "committedIteration", "requestedAt"}
    recorded_uid = request.get("clusterUid")
    recorded_iteration = request.get("committedIteration")
    if (
        not required.issubset(request)
        or request.get("attemptRunId") != expected_attempt
        or request.get("mode") != expected_mode
        or not isinstance(request.get("requestedAt"), str)
        or not request["requestedAt"]
        or (
            recorded_uid is not None
            and (not isinstance(recorded_uid, str) or not recorded_uid)
        )
        or (
            recorded_uid is not None
            and current_cluster_uid is not None
            and recorded_uid != current_cluster_uid
        )
    ):
        raise ControlError("existing stop request does not match current training")
    if expected_mode == "IMMEDIATE":
        if recorded_iteration is not None:
            raise ControlError("existing stop request has an invalid checkpoint")
    elif (
        isinstance(recorded_iteration, bool)
        or not isinstance(recorded_iteration, int)
        or recorded_iteration <= 0
    ):
        raise ControlError("existing stop request has an invalid checkpoint")
    if not reuse_recorded_iteration and recorded_iteration != expected_iteration:
        raise ControlError("existing stop request does not match current checkpoint")
    return request


def cluster_identity(args: argparse.Namespace) -> tuple[str, str]:
    result = run(
        args,
        "get",
        "raycluster",
        args.cluster,
        "-n",
        args.namespace,
        "--ignore-not-found",
        "-o",
        "json",
    )
    if not result.stdout.strip():
        raise ControlError(f"no active RayCluster {args.namespace}/{args.cluster}")
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ControlError(f"cannot parse RayCluster ownership: {error}") from error
    metadata = document.get("metadata") if isinstance(document, Mapping) else None
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    uid = metadata.get("uid") if isinstance(metadata, Mapping) else None
    attempt = (
        annotations.get("trainctl.io/run-id")
        if isinstance(annotations, Mapping)
        else None
    )
    if (
        not isinstance(document, Mapping)
        or document.get("kind") != "RayCluster"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != args.cluster
        or metadata.get("namespace") != args.namespace
        or not isinstance(uid, str)
        or not uid
        or not isinstance(attempt, str)
        or RUN_ID_PATTERN.fullmatch(attempt) is None
    ):
        raise ControlError("RayCluster identity or run ownership is invalid")
    return uid, attempt


def owned_state_dir(args: argparse.Namespace, attempt: str) -> Path | None:
    if RUN_ID_PATTERN.fullmatch(args.run_id) is None:
        raise ControlError("run ID contains unsupported characters")
    if args.run_id == attempt:  # Single-pipeline --fresh/--all-nodes run.
        return args.training_artifact_root.resolve() / attempt
    state_dir = args.recovery_state_root.resolve() / args.run_id
    state = read_json(
        state_dir / "state.json",
        "recovery state",
        max_bytes=RECOVERY_STATE_MAX_BYTES,
    )
    attempts = state.get("attempts")
    if state.get("jobId") != args.run_id or not isinstance(attempts, list):
        raise ControlError("recovery state belongs to another logical job")
    if not any(
        isinstance(item, Mapping) and item.get("runId") == attempt
        for item in attempts
    ):
        raise ControlError("active attempt is not owned by the requested job")
    return state_dir


def fresh_artifact_dir(args: argparse.Namespace) -> Path | None:
    """Return an owned single-pipeline artifact directory when it is observable."""

    artifact_dir = args.training_artifact_root.resolve() / args.run_id
    manifest_path = artifact_dir / "raycluster.yaml"
    if not artifact_dir.is_dir() or artifact_dir.is_symlink():
        return None
    if not manifest_path.is_file() or manifest_path.is_symlink():
        return None
    try:
        if manifest_path.stat().st_size > RECOVERY_STATE_MAX_BYTES:
            raise ControlError(
                f"rendered RayCluster manifest is unexpectedly large: {manifest_path}"
            )
        documents = tuple(
            yaml.safe_load_all(manifest_path.read_text(encoding="utf-8"))
        )
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise ControlError(
            f"cannot read rendered RayCluster manifest {manifest_path}: {error}"
        ) from error
    for document in documents:
        if not isinstance(document, Mapping) or document.get("kind") != "RayCluster":
            continue
        metadata = document.get("metadata")
        annotations = (
            metadata.get("annotations") if isinstance(metadata, Mapping) else None
        )
        if (
            isinstance(metadata, Mapping)
            and metadata.get("name") == args.cluster
            and metadata.get("namespace") == args.namespace
            and isinstance(annotations, Mapping)
            and annotations.get("trainctl.io/run-id") == args.run_id
        ):
            return artifact_dir
    raise ControlError("rendered RayCluster manifest ownership is invalid")


def request_stop_without_cluster(args: argparse.Namespace) -> int:
    """Durably stop a run before its RayCluster is observable."""

    if RUN_ID_PATTERN.fullmatch(args.run_id) is None:
        raise ControlError("run ID contains unsupported characters")
    artifact_dir = fresh_artifact_dir(args)
    if artifact_dir is not None:
        mark_stop(args, artifact_dir, None, args.run_id, "IMMEDIATE", None)
        print(
            "PASS: stop request accepted before fresh Ray startup; "
            "checkpoints were untouched."
        )
        return 0
    state_dir = args.recovery_state_root.resolve() / args.run_id
    state = read_json(
        state_dir / "state.json",
        "recovery state",
        max_bytes=RECOVERY_STATE_MAX_BYTES,
    )
    if (
        state.get("schemaVersion") != RECOVERY_STATE_SCHEMA
        or state.get("jobId") != args.run_id
    ):
        raise ControlError("recovery state belongs to another logical job")
    status = state.get("status")
    if status == "STOPPED":
        request = load_accepted_stop_request(
            state_dir / STOP_REQUEST_FILENAME,
            expected_job_id=args.run_id,
        )
        if request is None:
            raise ControlError("stopped recovery state has no owned stop request")
        print("PASS: current training was already stopped; checkpoints were untouched.")
        return 0
    if status in {"PASS", "MANUAL_REQUIRED"}:
        raise ControlError(f"recovery job is already terminal: {status}")

    attempts = state.get("attempts")
    if not isinstance(attempts, list):
        raise ControlError("recovery state has an invalid attempt list")
    if attempts:
        latest = attempts[-1]
        attempt = latest.get("runId") if isinstance(latest, Mapping) else None
    else:
        attempt = f"{args.run_id}-a00"
    if not isinstance(attempt, str) or RUN_ID_PATTERN.fullmatch(attempt) is None:
        raise ControlError("recovery state has no valid current attempt")

    mark_stop(args, state_dir, None, attempt, "IMMEDIATE", None)
    print(
        "PASS: stop request accepted before Ray startup; checkpoints were untouched."
    )
    return 0


def worker_pods(args: argparse.Namespace) -> list[str]:
    result = run(
        args,
        "get",
        "pods",
        "-n",
        args.namespace,
        "-l",
        f"ray.io/cluster={args.cluster},ray.io/node-type=worker",
        "-o",
        "json",
    )
    try:
        items = json.loads(result.stdout).get("items", [])
    except (AttributeError, json.JSONDecodeError) as error:
        raise ControlError(f"cannot parse Ray workers: {error}") from error
    if not isinstance(items, list):
        raise ControlError("Ray worker response has invalid items")
    pods: list[str] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        metadata = item.get("metadata")
        status = item.get("status")
        if (
            isinstance(metadata, Mapping)
            and isinstance(status, Mapping)
            and status.get("phase") == "Running"
            and isinstance(metadata.get("name"), str)
        ):
            pods.append(str(metadata["name"]))
    return sorted(pods)


def checkpoint_config(args: argparse.Namespace, attempt: str) -> tuple[str, int, bool]:
    path = (
        args.training_artifact_root.resolve()
        / attempt
        / "injection"
        / "injection.json"
    )
    injection = read_json(path, "training injection")
    topology = injection.get("topology")
    checkpoint = injection.get("checkpointWrite")
    if (
        injection.get("runId") != attempt
        or not isinstance(topology, Mapping)
        or not isinstance(checkpoint, Mapping)
    ):
        raise ControlError("training injection ownership or policy is invalid")
    workers = topology.get("workers")
    save_dir = checkpoint.get("saveDir")
    if (
        checkpoint.get("enabled") is not True
        or not isinstance(save_dir, str)
        or not save_dir.startswith("/")
        or not isinstance(workers, int)
        or workers <= 0
    ):
        raise ControlError("training has no valid checkpoint writer")
    return save_dir, workers, injection.get("launchMode") == "fresh"


def committed_iteration(
    args: argparse.Namespace,
    pods: Sequence[str],
    checkpoint_dir: str,
) -> int | None:
    tracker = f"{checkpoint_dir.rstrip('/')}/{TRACKER_FILENAME}"
    values: list[int | None] = []
    for pod in pods:
        result = run(
            args,
            "exec",
            "-n",
            args.namespace,
            pod,
            "-c",
            "ray-worker",
            "--",
            "cat",
            tracker,
            check=False,
        )
        try:
            value = int(result.stdout.strip()) if result.returncode == 0 else 0
        except ValueError:
            value = 0
        values.append(value if value > 0 else None)
    if values and all(value is None for value in values):
        return None
    if not values or any(value is None for value in values):
        raise ControlError("checkpoint tracker is not readable on every worker")
    if len(set(values)) != 1:
        raise ControlError("workers see different committed checkpoint iterations")
    return values[0]


def mark_stop(
    args: argparse.Namespace,
    state_dir: Path | None,
    uid: str | None,
    attempt: str,
    mode: str,
    iteration: int | None,
) -> None:
    if state_dir is None:
        return
    path = state_dir / STOP_REQUEST_FILENAME
    compatible = {
        "expected_job_id": args.run_id,
        "expected_attempt": attempt,
        "current_cluster_uid": uid,
        "expected_mode": mode,
        "expected_iteration": iteration,
    }
    if load_compatible_stop_request(path, **compatible) is not None:
        return
    payload = {
        "schemaVersion": STOP_REQUEST_SCHEMA,
        "jobId": args.run_id,
        "attemptRunId": attempt,
        "clusterUid": uid,
        "mode": mode,
        "status": "ACCEPTED",
        "committedIteration": iteration,
        "requestedAt": datetime.now(timezone.utc).isoformat(),
    }
    try:
        create_json(path, payload)
    except ControlError:
        # A concurrent identical stop request is safe; create_json remains
        # create-only and every field is revalidated before reuse.
        if load_compatible_stop_request(path, **compatible) is None:
            raise


def delete_cluster(args: argparse.Namespace, uid: str, attempt: str) -> None:
    if cluster_identity(args) != (uid, attempt):
        raise ControlError("RayCluster ownership changed; nothing was deleted")
    run(
        args,
        "delete",
        "raycluster",
        args.cluster,
        "-n",
        args.namespace,
        "--wait=false",
    )
    deadline = time.monotonic() + args.cleanup_timeout_seconds
    while True:
        cluster = run(
            args,
            "get",
            "raycluster",
            args.cluster,
            "-n",
            args.namespace,
            "--ignore-not-found",
            "-o",
            "name",
        ).stdout.strip()
        pods = run(
            args,
            "get",
            "pods",
            "-n",
            args.namespace,
            "-l",
            f"ray.io/cluster={args.cluster}",
            "-o",
            "name",
        ).stdout.strip()
        if not cluster and not pods:
            return
        if cluster and cluster_identity(args) != (uid, attempt):
            raise ControlError("a different RayCluster appeared during cleanup")
        if time.monotonic() >= deadline:
            raise ControlError("timed out waiting for RayCluster and Pods to disappear")
        time.sleep(2)


def control(args: argparse.Namespace, mode: str) -> int:
    try:
        uid, attempt = cluster_identity(args)
    except ControlError as error:
        no_cluster = f"no active RayCluster {args.namespace}/{args.cluster}"
        if mode == "IMMEDIATE" and str(error) == no_cluster:
            return request_stop_without_cluster(args)
        raise

    state_dir = owned_state_dir(args, attempt)
    selected: int | None = None
    existing_request = None
    if state_dir is not None:
        existing_request = load_compatible_stop_request(
            state_dir / STOP_REQUEST_FILENAME,
            expected_job_id=args.run_id,
            expected_attempt=attempt,
            current_cluster_uid=uid,
            expected_mode=mode,
            expected_iteration=None,
            reuse_recorded_iteration=mode == "AFTER_CHECKPOINT",
        )
    if existing_request is not None:
        selected = existing_request["committedIteration"]

    if mode == "AFTER_CHECKPOINT" and existing_request is None:
        checkpoint_dir, expected_workers, fresh = checkpoint_config(args, attempt)
        pods = worker_pods(args)
        if len(pods) != expected_workers:
            raise ControlError(f"expected {expected_workers} workers, found {len(pods)}")
        baseline = committed_iteration(args, pods, checkpoint_dir)
        if baseline is None and not fresh:
            raise ControlError("resume training has no committed checkpoint")
        print(f"WAIT: next committed checkpoint after {baseline or 'fresh start'}")
        deadline = (
            None
            if args.timeout_seconds == 0
            else time.monotonic() + args.timeout_seconds
        )
        while selected is None:
            if cluster_identity(args) != (uid, attempt):
                raise ControlError("training changed before the next checkpoint")
            pods = worker_pods(args)
            if len(pods) == expected_workers:
                try:
                    current = committed_iteration(args, pods, checkpoint_dir)
                except ControlError:
                    current = None
                if current is not None and (baseline is None or current > baseline):
                    selected = current
                    break
            if deadline is not None and time.monotonic() >= deadline:
                raise ControlError("timed out; training was left running")
            time.sleep(args.poll_seconds)

    mark_stop(args, state_dir, uid, attempt, mode, selected)
    delete_cluster(args, uid, attempt)
    if selected is None:
        print("PASS: current training stopped; checkpoints were untouched.")
    else:
        print(f"PASS: checkpoint {selected} committed; training then stopped.")
    return 0


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--kubectl-command")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--namespace")
    parser.add_argument("--cluster")
    parser.add_argument("--recovery-state-root", type=Path, default=DEFAULT_STATE_ROOT)
    parser.add_argument(
        "--training-artifact-root",
        type=Path,
        default=DEFAULT_ARTIFACT_ROOT,
    )
    parser.add_argument("--cleanup-timeout-seconds", type=int)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kcc_ray")
    commands = parser.add_subparsers(dest="command", required=True)
    immediate = commands.add_parser("stop", help="stop current training now")
    add_common(immediate)
    after = commands.add_parser(
        "stop-after-checkpoint",
        help="wait for the next committed checkpoint, then stop",
    )
    add_common(after)
    after.add_argument("--poll-seconds", type=float, default=5.0)
    after.add_argument("--timeout-seconds", type=int, default=0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        defaults = cluster_config.apply_kubernetes_defaults(args)
        cluster_config.apply_cleanup_timeout_default(args, defaults)
        if args.cleanup_timeout_seconds <= 0:
            raise ControlError("cleanup timeout must be positive")
        if args.command == "stop-after-checkpoint" and (
            args.poll_seconds <= 0 or args.timeout_seconds < 0
        ):
            raise ControlError("poll interval must be positive and timeout non-negative")
        mode = "IMMEDIATE" if args.command == "stop" else "AFTER_CHECKPOINT"
        return control(args, mode)
    except (ControlError, cluster_config.ClusterConfigError) as error:
        print(f"STOP: training control failed: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("STOP: watcher cancelled; training was left running.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
