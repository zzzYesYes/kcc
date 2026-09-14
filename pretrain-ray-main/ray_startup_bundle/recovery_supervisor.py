#!/usr/bin/env python3
"""Recover a failed whole-world training run with a bounded spare-node pool."""

from __future__ import annotations

import argparse
import copy
from datetime import datetime, timezone
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import stat
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence

import cluster_config
import recovery_diagnostics
import ray_training_submit
import start_ray
import training_control


DEFAULT_STATE_ROOT = start_ray.DEFAULT_LOG_ROOT / "training-jobs"
RECOVERY_STATE_MAX_BYTES = training_control.RECOVERY_STATE_MAX_BYTES
RECOVERY_STATE_SCHEMA = "ray-training-recovery/v1"
RESUMABLE_STATE_STATUSES = frozenset(
    {"STARTING", "RUNNING", "WAITING_FOR_CLEANUP", "DIAGNOSING", "RETRYING"}
)
TERMINAL_STATE_STATUSES = frozenset({"PASS", "STOPPED", "MANUAL_REQUIRED"})
UNEXPECTED_SUPERVISOR_EXIT = 70
TRAINING_TEMPLATE_SNAPSHOT_SCHEMA = "training-template-snapshot/v1"
TRAINING_TEMPLATE_SNAPSHOT_FILENAME = "training-template.sh"
TRAINING_TEMPLATE_METADATA_FILENAME = "training-template.json"
TRAINING_TEMPLATE_MAX_BYTES = 1024 * 1024
SUPERVISOR_JOB_NAME_ENV = "KCC_RAY_SUPERVISOR_JOB_NAME"
MAX_RECOVERY_ATTEMPTS = 32
RECOVERY_POLICY_FIELDS = {
    "sameTopologyRetries": "same_topology_retries",
    "retryBackoffSeconds": "retry_backoff_seconds",
    "noProgressSeconds": "no_progress_seconds",
    "diagnosisWindowSeconds": "diagnosis_window_seconds",
    "diagnosisPollSeconds": "diagnosis_poll_seconds",
    "diagnosisStableSamples": "diagnosis_stable_samples",
}
NON_NODE_FAILURE_CLASSES = frozenset(
    {
        "CHECKPOINT_UNAVAILABLE",
        "DRIVER_INTERNAL_FAILURE",
        "DRIVER_PROTOCOL_FAILURE",
        "INTERRUPTED",
        "TIMEOUT",
        "TRAINING_NO_PROGRESS",
    }
)


def accepted_stop_request(
    path: Path,
    *,
    job_id: str,
) -> dict[str, Any] | None:
    try:
        return training_control.load_accepted_stop_request(
            path,
            expected_job_id=job_id,
        )
    except training_control.ControlError as error:
        raise RecoveryError(f"cannot trust stop request: {error}") from error


class RecoveryError(RuntimeError):
    pass


class RecoveryOwnershipError(RecoveryError):
    """A live Kubernetes object is proven to belong to another run."""

    pass


class RecoveryStopRequested(RuntimeError):
    """Interrupt a bounded wait after a durable user stop request appears."""

    pass


class SupervisorLock:
    """Hold an advisory lock for one logical run for this process lifetime."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._stream: Any | None = None

    def __enter__(self) -> "SupervisorLock":
        flags = os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0)
        try:
            descriptor = os.open(self.path, flags, 0o600)
            stream = os.fdopen(descriptor, "r+", encoding="utf-8")
        except OSError as error:
            raise RecoveryError(f"cannot open supervisor lock: {error}") from error
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            stream.close()
            raise RecoveryError(
                "another supervisor already owns this logical run"
            ) from error
        except OSError as error:
            stream.close()
            raise RecoveryError(f"cannot acquire supervisor lock: {error}") from error
        self._stream = stream
        return self

    def __exit__(self, _type: object, _value: object, _traceback: object) -> None:
        stream = self._stream
        self._stream = None
        if stream is not None:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            finally:
                stream.close()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def attempt_run_id(job_id: str, attempt: int) -> str:
    if attempt < 0:
        raise RecoveryError("attempt must be non-negative")
    candidate = f"{job_id}-a{attempt:02d}"
    if start_ray.RUN_ID_PATTERN.fullmatch(candidate) is None:
        raise RecoveryError(
            "job ID cannot form a valid attempt run ID; shorten the job ID"
        )
    return candidate


def replace_active_node(
    active_nodes: Sequence[str],
    spare_nodes: Sequence[str],
    *,
    bad_target: str,
    replacement_target: str,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    active = tuple(active_nodes)
    spares = tuple(spare_nodes)
    if active.count(bad_target) != 1:
        raise RecoveryError("diagnosed bad target is not exactly one active node")
    if replacement_target not in spares:
        raise RecoveryError("selected replacement is not in the spare pool")
    updated_active = tuple(
        replacement_target if target == bad_target else target for target in active
    )
    updated_spares = tuple(target for target in spares if target != replacement_target)
    if len(set(updated_active)) != len(updated_active):
        raise RecoveryError("replacement produced duplicate active nodes")
    return updated_active, updated_spares


def write_state(path: Path, payload: Mapping[str, Any]) -> None:
    try:
        serialized = (
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError, UnicodeError) as error:
        raise RecoveryError(f"cannot serialize recovery state: {error}") from error
    if len(serialized) > RECOVERY_STATE_MAX_BYTES:
        raise RecoveryError(
            "recovery state exceeds the 1 MiB safety limit; "
            "the previous state was left untouched"
        )

    temporary = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with temporary.open("xb") as stream:
            stream.write(serialized)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path.parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except OSError as error:
        raise RecoveryError(f"cannot persist recovery state: {error}") from error
    finally:
        temporary.unlink(missing_ok=True)


def record_stopped_state(
    *,
    state_path: Path,
    state: dict[str, Any],
    stop_request: Mapping[str, Any],
    attempt_state: dict[str, Any] | None = None,
    cleanup_failure: str | None = None,
) -> None:
    if attempt_state is not None:
        attempt_state["status"] = "STOPPED"
        attempt_state["stopRequest"] = dict(stop_request)
        if cleanup_failure is not None:
            attempt_state["cleanupFailure"] = cleanup_failure
    state["status"] = "STOPPED"
    state["stopRequest"] = dict(stop_request)
    if cleanup_failure is not None:
        state["cleanupFailure"] = cleanup_failure
    state["updatedAt"] = utc_now()
    write_state(state_path, state)


def read_bounded_regular_bytes(
    path: Path,
    *,
    label: str,
    max_bytes: int,
) -> bytes:
    """Read one stable regular file without following a symbolic link."""

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise RecoveryError(f"cannot open {label} {path}: {error}") from error
    try:
        before = os.fstat(descriptor)
        if not stat.S_ISREG(before.st_mode):
            raise RecoveryError(f"{label} is not a regular file: {path}")
        if before.st_size > max_bytes:
            raise RecoveryError(f"{label} exceeds the {max_bytes}-byte limit: {path}")
        chunks: list[bytes] = []
        total = 0
        while True:
            chunk = os.read(descriptor, min(65536, max_bytes + 1 - total))
            if not chunk:
                break
            chunks.append(chunk)
            total += len(chunk)
            if total > max_bytes:
                raise RecoveryError(
                    f"{label} exceeds the {max_bytes}-byte limit: {path}"
                )
        after = os.fstat(descriptor)
    except OSError as error:
        raise RecoveryError(f"cannot read {label} {path}: {error}") from error
    finally:
        os.close(descriptor)
    identity_before = (
        before.st_dev,
        before.st_ino,
        before.st_size,
        before.st_mtime_ns,
    )
    identity_after = (
        after.st_dev,
        after.st_ino,
        after.st_size,
        after.st_mtime_ns,
    )
    payload = b"".join(chunks)
    if identity_before != identity_after or len(payload) != after.st_size:
        raise RecoveryError(f"{label} changed while it was read: {path}")
    return payload


def create_regular_file_once(path: Path, payload: bytes, *, mode: int) -> bool:
    """Atomically create one durable file; never replace an existing path."""

    temporary = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
    try:
        with temporary.open("xb") as stream:
            os.chmod(temporary, mode)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        directory_flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(path.parent, directory_flags)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
        return True
    except OSError as error:
        raise RecoveryError(f"cannot create immutable template file {path}: {error}") from error
    finally:
        temporary.unlink(missing_ok=True)


def load_training_template_metadata(path: Path) -> dict[str, Any]:
    payload = read_bounded_regular_bytes(
        path,
        label="training template metadata",
        max_bytes=64 * 1024,
    )
    try:
        value = json.loads(payload.decode("utf-8"))
    except (UnicodeError, json.JSONDecodeError) as error:
        raise RecoveryError(f"cannot parse training template metadata: {error}") from error
    if not isinstance(value, dict):
        raise RecoveryError("training template metadata is not a JSON object")
    return value


def prepare_training_template_snapshot(
    *,
    state_dir: Path,
    job_id: str,
    selected_source: Path,
    existing_state: Mapping[str, Any] | None,
) -> tuple[Path, dict[str, Any]]:
    """Create or verify the immutable template used by every recovery attempt."""

    source_path = selected_source.expanduser().resolve()
    snapshot_path = state_dir / TRAINING_TEMPLATE_SNAPSHOT_FILENAME
    metadata_path = state_dir / TRAINING_TEMPLATE_METADATA_FILENAME
    metadata_present = metadata_path.exists() or metadata_path.is_symlink()
    snapshot_present = snapshot_path.exists() or snapshot_path.is_symlink()

    if not metadata_present:
        if existing_state is not None:
            raise RecoveryError(
                "recovery state has no immutable training template metadata"
            )
        if snapshot_present:
            raise RecoveryError(
                "training template snapshot exists without owned metadata"
            )
        source = read_bounded_regular_bytes(
            source_path,
            label="selected training template",
            max_bytes=TRAINING_TEMPLATE_MAX_BYTES,
        )
        metadata = {
            "schemaVersion": TRAINING_TEMPLATE_SNAPSHOT_SCHEMA,
            "jobId": job_id,
            "sourcePath": str(source_path),
            "snapshotPath": str(snapshot_path),
            "sha256": hashlib.sha256(source).hexdigest(),
            "sizeBytes": len(source),
        }
        encoded = (
            json.dumps(metadata, ensure_ascii=False, indent=2) + "\n"
        ).encode("utf-8")
        create_regular_file_once(metadata_path, encoded, mode=0o600)

    metadata = load_training_template_metadata(metadata_path)
    digest = metadata.get("sha256")
    size_bytes = metadata.get("sizeBytes")
    if (
        metadata.get("schemaVersion") != TRAINING_TEMPLATE_SNAPSHOT_SCHEMA
        or metadata.get("jobId") != job_id
        or metadata.get("sourcePath") != str(source_path)
        or metadata.get("snapshotPath") != str(snapshot_path)
        or not isinstance(digest, str)
        or len(digest) != 64
        or any(character not in "0123456789abcdef" for character in digest)
        or not isinstance(size_bytes, int)
        or isinstance(size_bytes, bool)
        or not 0 < size_bytes <= TRAINING_TEMPLATE_MAX_BYTES
    ):
        raise RecoveryError("training template snapshot metadata is invalid")

    if not snapshot_present:
        source = read_bounded_regular_bytes(
            source_path,
            label="selected training template",
            max_bytes=TRAINING_TEMPLATE_MAX_BYTES,
        )
        if len(source) != size_bytes or hashlib.sha256(source).hexdigest() != digest:
            raise RecoveryError(
                "selected training template changed before its snapshot completed"
            )
        create_regular_file_once(snapshot_path, source, mode=0o600)

    snapshot = read_bounded_regular_bytes(
        snapshot_path,
        label="training template snapshot",
        max_bytes=TRAINING_TEMPLATE_MAX_BYTES,
    )
    if len(snapshot) != size_bytes or hashlib.sha256(snapshot).hexdigest() != digest:
        raise RecoveryError("training template snapshot content differs from metadata")
    if existing_state is not None and existing_state.get("trainingTemplate") != metadata:
        raise RecoveryError("recovery state training template ownership is invalid")
    return snapshot_path, metadata


def load_training_result(
    path: Path,
    *,
    expected_run_id: str,
    expected_status: str,
) -> Mapping[str, Any]:
    if expected_status not in {"PASS", "FAIL"}:
        raise RecoveryError("expected training result status is invalid")
    if not path.is_file() or path.is_symlink():
        raise RecoveryError(
            "formal training did not export a regular execution-result.json; "
            "training state is uncertain"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecoveryError(f"cannot read formal training result: {error}") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != "ray-training-result/v1"
        or payload.get("status") != expected_status
        or payload.get("runId") != expected_run_id
    ):
        raise RecoveryError(
            "execution result is not a formal "
            f"{expected_status} for the current attempt"
        )
    return payload


def load_failed_training_result(
    path: Path,
    *,
    expected_run_id: str,
) -> Mapping[str, Any]:
    return load_training_result(
        path,
        expected_run_id=expected_run_id,
        expected_status="FAIL",
    )


def load_successful_training_result(
    path: Path,
    *,
    expected_run_id: str,
) -> Mapping[str, Any]:
    return load_training_result(
        path,
        expected_run_id=expected_run_id,
        expected_status="PASS",
    )


def existing_training_result_status(
    path: Path,
    *,
    expected_run_id: str,
) -> str | None:
    """Return an owned terminal result status without changing the file."""
    if not path.exists() and not path.is_symlink():
        return None
    if not path.is_file() or path.is_symlink():
        raise RecoveryError(
            "formal training result is not a regular file; training state is uncertain"
        )
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise RecoveryError(f"cannot read formal training result: {error}") from error
    status = payload.get("status") if isinstance(payload, Mapping) else None
    if (
        not isinstance(payload, Mapping)
        or payload.get("schemaVersion") != "ray-training-result/v1"
        or payload.get("runId") != expected_run_id
        or status not in {"PASS", "FAIL"}
    ):
        raise RecoveryError(
            "execution result is not an owned PASS/FAIL for the current attempt"
        )
    return str(status)


def load_recovery_state(path: Path, *, expected_job_id: str) -> dict[str, Any]:
    try:
        state = training_control.read_json(
            path,
            "recovery state",
            max_bytes=RECOVERY_STATE_MAX_BYTES,
        )
    except training_control.ControlError as error:
        raise RecoveryError(f"cannot resume recovery state: {error}") from error
    if (
        state.get("schemaVersion") != RECOVERY_STATE_SCHEMA
        or state.get("jobId") != expected_job_id
    ):
        raise RecoveryError("recovery state ownership or schema is invalid")
    return state


def compact_training_result_for_state(
    result: Mapping[str, Any],
) -> dict[str, Any]:
    """Keep recovery state small; full worker log tails remain in resultPath."""
    compact = copy.deepcopy(dict(result))
    nodes = compact.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if isinstance(node, dict):
                node.pop("stdoutTail", None)
                node.pop("stderrTail", None)
    return compact


def require_successful_checkpoint_resume(
    result: Mapping[str, Any],
    *,
    expected_workers: int,
) -> None:
    checkpoint = result.get("checkpoint")
    if not isinstance(checkpoint, Mapping) or checkpoint.get("status") != "PASS":
        raise RecoveryError(
            "successful recovery result lacks a passed checkpoint preflight"
        )
    policy = checkpoint.get("policy")
    if (
        not isinstance(policy, Mapping)
        or policy.get("requiredForRecovery") is not True
        or policy.get("selection") != "megatron-tracker"
    ):
        raise RecoveryError("successful recovery result has an invalid checkpoint policy")
    selected_iteration = checkpoint.get("selectedIteration")
    selected_dir = checkpoint.get("selectedDir")
    if (
        not isinstance(selected_iteration, int)
        or isinstance(selected_iteration, bool)
        or selected_iteration <= 0
        or not isinstance(selected_dir, str)
        or not selected_dir.startswith("/")
    ):
        raise RecoveryError("successful recovery result has no resumable checkpoint")
    views = checkpoint.get("nodes")
    if not isinstance(views, list) or len(views) != expected_workers:
        raise RecoveryError(
            "successful recovery result lacks checkpoint evidence from every worker"
        )
    for view in views:
        if (
            not isinstance(view, Mapping)
            or view.get("status") != "AVAILABLE_RESUME"
            or view.get("iteration") != selected_iteration
            or view.get("selectedDir") != selected_dir
        ):
            raise RecoveryError(
                "successful recovery result contains inconsistent checkpoint evidence"
            )


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise RecoveryError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def emit_manual_required_alert(
    args: argparse.Namespace,
    *,
    job_id: str,
    reason: str,
) -> None:
    """Emit one best-effort cluster-native warning after durable state is saved."""
    detail = " ".join(reason.split())[:500] or "manual recovery is required"
    print(f"ALERT: recovery job {job_id} requires manual action: {detail}", file=sys.stderr)
    supervisor_job = os.environ.get(SUPERVISOR_JOB_NAME_ENV)
    if not supervisor_job:
        return
    digest = hashlib.sha256(f"{job_id}\0{detail}".encode("utf-8")).hexdigest()[:12]
    event_name = f"kcc-ray-manual-{digest}"
    try:
        result = subprocess.run(
            [
                *kubectl_prefix(args.kubectl_command, args.kubeconfig.resolve()),
                "create",
                "event",
                event_name,
                "-n",
                args.namespace,
                f"--for=job/{supervisor_job}",
                "--type=Warning",
                "--reason=RecoveryManualRequired",
                f"--note=run {job_id}: {detail}",
            ],
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=10,
        )
        if result.returncode != 0 and "already exists" not in (
            result.stderr + result.stdout
        ).lower():
            message = (result.stderr.strip() or result.stdout.strip())[:300]
            print(f"ALERT: Kubernetes Warning Event could not be created: {message}", file=sys.stderr)
    except (OSError, RecoveryError, subprocess.TimeoutExpired) as error:
        print(f"ALERT: Kubernetes Warning Event could not be created: {error}", file=sys.stderr)


def run_cleanup_query(command: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RecoveryError(f"cannot query failed Ray resources: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip()
        raise RecoveryError(f"failed Ray resource query returned an error: {detail}")
    return result


def query_cluster_cleanup(
    *,
    kubectl: Sequence[str],
    namespace: str,
    cluster: str,
) -> tuple[list[str], tuple[str, ...]]:
    def get(*arguments: str) -> subprocess.CompletedProcess[str]:
        return run_cleanup_query([*kubectl, "get", *arguments, "-n", namespace])

    raycluster = get(
        "raycluster", cluster, "--ignore-not-found", "-o", "name"
    )
    pods_result = get(
        "pods", "-l", f"ray.io/cluster={cluster}", "-o", "json"
    )
    services_result = get(
        "service", "-l", f"ray.io/cluster={cluster}", "-o", "json"
    )
    podgroup = get(
        "podgroup", f"ray-{cluster}-pg", "--ignore-not-found", "-o", "name"
    )
    configmaps = get(
        "configmap",
        f"job-summary-{cluster}",
        f"hccl-sanitized-{cluster}",
        "--ignore-not-found",
        "-o",
        "name",
    )
    try:
        pods = json.loads(pods_result.stdout).get("items", [])
        services = json.loads(services_result.stdout).get("items", [])
    except (AttributeError, json.JSONDecodeError) as error:
        raise RecoveryError(
            f"cannot parse cleanup Pod/Service response: {error}"
        ) from error
    if not isinstance(pods, list) or not isinstance(services, list):
        raise RecoveryError("cleanup Pod/Service response has invalid items")
    remaining: list[str] = []
    if raycluster.stdout.strip():
        remaining.append("RayCluster")
    if pods:
        remaining.append(f"{len(pods)} Pod(s)")
    if services:
        remaining.append(f"{len(services)} Service(s)")
    if podgroup.stdout.strip():
        remaining.append("PodGroup")
    configmap_names = tuple(
        line.strip() for line in configmaps.stdout.splitlines() if line.strip()
    )
    return remaining, configmap_names


def query_owned_raycluster(
    *,
    kubectl: Sequence[str],
    namespace: str,
    cluster: str,
    run_id: str,
) -> Mapping[str, Any] | None:
    """Return the current owned RayCluster, or None when it is gone."""
    result = run_cleanup_query(
        [
            *kubectl,
            "get",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ]
    )
    if not result.stdout.strip():
        return None
    try:
        document = json.loads(result.stdout)
        metadata = document.get("metadata", {})
        annotations = metadata.get("annotations", {})
    except (AttributeError, json.JSONDecodeError) as error:
        raise RecoveryError(
            f"cannot parse RayCluster ownership: {error}"
        ) from error
    if (
        not isinstance(document, Mapping)
        or document.get("kind") != "RayCluster"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != cluster
        or metadata.get("namespace") != namespace
    ):
        raise RecoveryError("RayCluster ownership response is invalid")
    if not isinstance(annotations, Mapping):
        raise RecoveryError("RayCluster annotations are not an object")
    if annotations.get("trainctl.io/run-id") != run_id:
        raise RecoveryOwnershipError(
            "refusing to use a RayCluster whose run-id annotation does not "
            "match the current attempt"
        )
    return document


def get_cleanup_configmap(
    *,
    kubectl: Sequence[str],
    namespace: str,
    name: str,
) -> Mapping[str, object] | None:
    result = run_cleanup_query(
        [
            *kubectl,
            "get",
            "configmap",
            name,
            "-n",
            namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ]
    )
    if not result.stdout.strip():
        return None
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise RecoveryError(f"cannot parse stale ConfigMap {name}: {error}") from error
    if not isinstance(document, Mapping) or document.get("kind") != "ConfigMap":
        raise RecoveryError(f"stale object {name} is not a ConfigMap")
    metadata = document.get("metadata")
    if not isinstance(metadata, Mapping) or metadata.get("name") != name:
        raise RecoveryError(f"stale ConfigMap {name} has invalid metadata")
    return document


def delete_stale_ranktable_configmaps(
    *,
    kubectl: Sequence[str],
    namespace: str,
    cluster: str,
    poll_seconds: float,
) -> None:
    source_name = f"job-summary-{cluster}"
    target_name = f"hccl-sanitized-{cluster}"
    source = get_cleanup_configmap(
        kubectl=kubectl,
        namespace=namespace,
        name=source_name,
    )
    target = get_cleanup_configmap(
        kubectl=kubectl,
        namespace=namespace,
        name=target_name,
    )
    if source is None and target is None:
        return

    delete_names: list[str] = []
    source_uid: str | None = None
    if source is not None:
        source_metadata = source.get("metadata")
        if not isinstance(source_metadata, Mapping):
            raise RecoveryError("stale source RankTable has invalid metadata")
        source_uid_value = source_metadata.get("uid")
        source_data = source.get("data")
        if not isinstance(source_uid_value, str) or not source_uid_value:
            raise RecoveryError("stale source RankTable ConfigMap has no UID")
        if not isinstance(source_data, Mapping):
            raise RecoveryError("stale source RankTable ConfigMap has invalid data")
        if source_data.get("job_name") != cluster:
            raise RecoveryError(
                "refusing to delete a source RankTable for another job"
            )
        if source_data.get("operator") != "delete":
            raise RecoveryError(
                "ClusterD has not acknowledged the failed job deletion "
                "(job-summary operator is not delete)"
            )
        source_uid = source_uid_value
        delete_names.append(source_name)

    if target is not None:
        target_metadata = target.get("metadata")
        if not isinstance(target_metadata, Mapping):
            raise RecoveryError("stale sanitized RankTable has invalid metadata")
        labels = target_metadata.get("labels")
        annotations = target_metadata.get("annotations")
        if (
            not isinstance(labels, Mapping)
            or labels.get("app.kubernetes.io/managed-by")
            != "hccl-ranktable-sanitizer"
            or not isinstance(annotations, Mapping)
            or annotations.get("ranktable.hccl-check.local/source-configmap")
            != source_name
        ):
            raise RecoveryError(
                "refusing to delete a RankTable ConfigMap not owned by the sanitizer"
            )
        target_source_uid = annotations.get(
            "ranktable.hccl-check.local/source-uid"
        )
        if not isinstance(target_source_uid, str) or not target_source_uid:
            raise RecoveryError("stale sanitized RankTable has no source UID")
        if source_uid is not None and target_source_uid != source_uid:
            raise RecoveryError(
                "source and sanitized RankTable ConfigMaps have different UIDs"
            )
        delete_names.append(target_name)

    print(
        "RECOVERY: deleting stale RankTable ConfigMaps: "
        + ", ".join(delete_names),
        file=sys.stderr,
        flush=True,
    )
    run_cleanup_query(
        [
            *kubectl,
            "delete",
            "configmap",
            *delete_names,
            "-n",
            namespace,
            "--ignore-not-found=true",
            "--wait=false",
        ]
    )

    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        result = run_cleanup_query(
            [
                *kubectl,
                "get",
                "configmap",
                source_name,
                target_name,
                "-n",
                namespace,
                "--ignore-not-found",
                "-o",
                "name",
            ]
        )
        if not result.stdout.strip():
            return
        time.sleep(poll_seconds)
    raise RecoveryError(
        "RankTable ConfigMaps did not remain deleted after fallback cleanup"
    )


def wait_for_cluster_cleanup(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    run_id: str,
    timeout_seconds: int,
    poll_seconds: float = 2.0,
) -> None:
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    deadline = time.monotonic() + timeout_seconds
    delete_requested = False
    last_query_error: str | None = None
    # The Ray Jobs training submitter normally requests this deletion first. If
    # the object still exists, verify the run-id annotation written by the
    # existing renderer before repeating that same idempotent delete.  A
    # different annotation means another launcher owns the cluster name.
    try:
        current_cluster = query_owned_raycluster(
            kubectl=kubectl,
            namespace=namespace,
            cluster=cluster,
            run_id=run_id,
        )
        if current_cluster is not None:
            run_cleanup_query(
                [
                    *kubectl,
                    "delete",
                    "raycluster",
                    cluster,
                    "-n",
                    namespace,
                    "--ignore-not-found=true",
                    "--wait=false",
                ]
            )
        delete_requested = True
    except RecoveryOwnershipError:
        raise
    except RecoveryError as error:
        last_query_error = str(error)
        print(
            "WARNING: cleanup ownership query failed; retrying within the "
            f"{timeout_seconds}s cleanup deadline: {error}",
            file=sys.stderr,
            flush=True,
        )
    while time.monotonic() < deadline:
        try:
            if not delete_requested:
                current_cluster = query_owned_raycluster(
                    kubectl=kubectl,
                    namespace=namespace,
                    cluster=cluster,
                    run_id=run_id,
                )
                if current_cluster is not None:
                    # Deleting the RayCluster lets Kubernetes stop only that
                    # cluster's Pods; this never scans arbitrary host PIDs.
                    run_cleanup_query(
                        [
                            *kubectl,
                            "delete",
                            "raycluster",
                            cluster,
                            "-n",
                            namespace,
                            "--ignore-not-found=true",
                            "--wait=false",
                        ]
                    )
                delete_requested = True
            workload_remaining, configmaps = query_cluster_cleanup(
                kubectl=kubectl,
                namespace=namespace,
                cluster=cluster,
            )
        except RecoveryOwnershipError:
            raise
        except RecoveryError as error:
            last_query_error = str(error)
            print(
                "WARNING: cleanup status query failed; retrying within the "
                f"{timeout_seconds}s cleanup deadline: {error}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(poll_seconds)
            continue
        remaining = [*workload_remaining]
        if configmaps:
            remaining.append("RankTable ConfigMap(s)")
        if not remaining:
            return
        time.sleep(poll_seconds)

    try:
        workload_remaining, configmaps = query_cluster_cleanup(
            kubectl=kubectl,
            namespace=namespace,
            cluster=cluster,
        )
    except RecoveryError as error:
        detail = str(error) or last_query_error or "unknown query error"
        raise RecoveryError(
            f"timed out after {timeout_seconds}s while querying failed cluster "
            f"cleanup: {detail}"
        ) from error
    if workload_remaining:
        raise RecoveryError(
            f"timed out after {timeout_seconds}s waiting for failed cluster cleanup: "
            + ", ".join(workload_remaining)
            + " still exist"
        )
    if not configmaps:
        return
    print(
        f"RECOVERY: ClusterD did not remove stale RankTable ConfigMaps within "
        f"{timeout_seconds}s; checking exact-name fallback cleanup.",
        file=sys.stderr,
        flush=True,
    )
    delete_stale_ranktable_configmaps(
        kubectl=kubectl,
        namespace=namespace,
        cluster=cluster,
        poll_seconds=poll_seconds,
    )


def source_declared_npus(path: Path) -> int:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise RecoveryError(f"cannot read training script: {error}") from error
    import re

    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?NPUS_PER_NODE=([0-9]+)[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1 or int(matches[0]) <= 0:
        raise RecoveryError(
            "training script must have one positive literal NPUS_PER_NODE"
        )
    return int(matches[0])


def make_parser() -> argparse.ArgumentParser:
    parser = start_ray.make_parser()
    parser.description = (
        "Resume formal training with bounded automatic replacement from the "
        "configured spare-node pool."
    )
    parser.epilog = (
        "Launcher modes: add --all-nodes to use every configured node without "
        "spares; add --fresh to start from iteration zero. These mode flags are "
        "dispatched by kcc_ray before this supervisor starts."
    )
    for action in parser._actions:
        if action.dest == "run_id":
            action.help = (
                "logical recovery job ID; attempts use <run-id>-a00, -a01, ..."
            )
        elif action.dest == "failure_retention_seconds":
            action.help = (
                "accepted for common CLI compatibility; recovery forces failed "
                "attempt retention to 0 seconds before diagnosis"
            )
    parser.add_argument(
        "--spare-node",
        action="append",
        help=(
            "standby Kubernetes node name or InternalIP; repeat as needed "
            "(defaults to config/cluster.yaml spareNodes)"
        ),
    )
    parser.add_argument(
        "--max-recoveries",
        type=int,
        help=(
            "maximum number of spare machines that may be consumed; "
            "defaults to the initial spare count"
        ),
    )
    parser.add_argument(
        "--cleanup-timeout-seconds",
        type=int,
        help=(
            "wait for the failed RayCluster, Pods, Service, PodGroup, and "
            "RankTable objects to disappear; defaults to config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--same-topology-retries",
        type=int,
        help="same-node retry limit; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--retry-backoff-seconds",
        type=int,
        help="delay before a same-node retry; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--diagnosis-window-seconds",
        type=int,
        help="bounded diagnosis sampling window; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--diagnosis-poll-seconds",
        type=int,
        help="diagnosis sampling interval; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--diagnosis-stable-samples",
        type=int,
        help="matching samples required for replacement; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--recovery-state-root",
        type=Path,
        default=DEFAULT_STATE_ROOT,
        help="persistent root for logical-job recovery state",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help=(
            "create the logical job if absent, or safely reattach to its latest "
            "recorded Ray Job without resubmitting"
        ),
    )
    return parser


def apply_config_defaults(
    args: argparse.Namespace,
    config: cluster_config.ClusterConfig | None = None,
) -> cluster_config.ClusterConfig | None:
    """Freeze start and recovery defaults into one effective argument set."""
    needs_recovery_config = (
        getattr(args, "spare_node", None) is None
        or getattr(args, "cleanup_timeout_seconds", None) is None
        or any(
            hasattr(args, destination) and getattr(args, destination) is None
            for destination in (
                "same_topology_retries",
                "retry_backoff_seconds",
                "diagnosis_window_seconds",
                "diagnosis_poll_seconds",
                "diagnosis_stable_samples",
            )
        )
    )
    defaults = config
    if defaults is None and needs_recovery_config:
        defaults = cluster_config.load_cluster_config()
    loaded = start_ray.apply_config_defaults(args, defaults)
    defaults = defaults or loaded
    if needs_recovery_config and defaults is None:
        defaults = cluster_config.load_cluster_config()
    if getattr(args, "spare_node", None) is None:
        args.spare_node = list(defaults.spare_nodes)
    if getattr(args, "cleanup_timeout_seconds", None) is None:
        args.cleanup_timeout_seconds = defaults.timeouts.recovery_cleanup_seconds
    for destination in (
        "same_topology_retries",
        "retry_backoff_seconds",
        "diagnosis_window_seconds",
        "diagnosis_poll_seconds",
        "diagnosis_stable_samples",
    ):
        if hasattr(args, destination) and getattr(args, destination) is None:
            setattr(args, destination, getattr(defaults.recovery, destination))
    return defaults


def validate_node_pool(
    active_nodes: Sequence[str],
    spare_nodes: Sequence[str],
) -> None:
    combined = tuple(active_nodes) + tuple(spare_nodes)
    if not active_nodes:
        raise RecoveryError("at least one active node is required")
    if not spare_nodes:
        raise RecoveryError("at least one spare node is required")
    if len(set(combined)) != len(combined):
        raise RecoveryError("active and spare node targets must be unique")


def _state_string_tuple(
    value: object,
    *,
    label: str,
    allow_empty: bool,
) -> tuple[str, ...]:
    if (
        not isinstance(value, list)
        or any(not isinstance(item, str) or not item for item in value)
    ):
        raise RecoveryError(f"recovery state {label} is invalid")
    result = tuple(value)
    if not allow_empty and not result:
        raise RecoveryError(f"recovery state {label} must not be empty")
    if len(set(result)) != len(result):
        raise RecoveryError(f"recovery state {label} contains duplicates")
    return result


def _state_nonnegative_int(value: object, *, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RecoveryError(f"recovery state {label} is invalid")
    return value


def recovery_policy_from_args(args: argparse.Namespace) -> dict[str, int]:
    argument_names = tuple(RECOVERY_POLICY_FIELDS.values())
    present = tuple(hasattr(args, name) for name in argument_names)
    if not any(present):
        return legacy_recovery_policy()
    if not all(present):
        raise RecoveryError("recovery policy arguments are incomplete")
    policy: dict[str, int] = {}
    for state_name, argument_name in RECOVERY_POLICY_FIELDS.items():
        value = getattr(args, argument_name, None)
        if not isinstance(value, int) or isinstance(value, bool):
            raise RecoveryError(f"recovery policy {state_name} is invalid")
        policy[state_name] = value
    validate_recovery_policy(policy)
    return policy


def validate_recovery_policy(policy: Mapping[str, Any]) -> None:
    if set(policy) != set(RECOVERY_POLICY_FIELDS):
        raise RecoveryError("recovery policy fields are invalid")
    values = {
        key: _state_nonnegative_int(policy.get(key), label=f"policy {key}")
        for key in RECOVERY_POLICY_FIELDS
    }
    if values["diagnosisPollSeconds"] <= 0:
        raise RecoveryError("diagnosis poll interval must be positive")
    if values["diagnosisStableSamples"] <= 0:
        raise RecoveryError("diagnosis stable sample count must be positive")
    if values["sameTopologyRetries"] > 10:
        raise RecoveryError("same-topology retries must not exceed 10")
    if values["retryBackoffSeconds"] > 24 * 60 * 60:
        raise RecoveryError("retry backoff must not exceed one day")
    max_samples = diagnosis_sample_limit(
        values["diagnosisWindowSeconds"], values["diagnosisPollSeconds"]
    )
    if values["diagnosisStableSamples"] > max_samples:
        raise RecoveryError(
            "diagnosis stable sample count exceeds the configured sampling window"
        )


def legacy_recovery_policy() -> dict[str, int]:
    """Preserve the one-shot/no-watchdog behavior of pre-policy state files."""
    return {
        "sameTopologyRetries": 0,
        "retryBackoffSeconds": 0,
        "noProgressSeconds": 0,
        "diagnosisWindowSeconds": 0,
        "diagnosisPollSeconds": 1,
        "diagnosisStableSamples": 1,
    }


def apply_frozen_recovery_policy(
    args: argparse.Namespace,
    state: Mapping[str, Any] | None,
) -> dict[str, int]:
    raw_policy: Mapping[str, Any]
    if state is None:
        raw_policy = recovery_policy_from_args(args)
    else:
        stored = state.get("recoveryPolicy")
        raw_policy = legacy_recovery_policy() if stored is None else stored
        if not isinstance(raw_policy, Mapping):
            raise RecoveryError("recovery state policy is invalid")
        validate_recovery_policy(raw_policy)
    policy = {key: int(raw_policy[key]) for key in RECOVERY_POLICY_FIELDS}
    for state_name, argument_name in RECOVERY_POLICY_FIELDS.items():
        setattr(args, argument_name, policy[state_name])
    return policy


def diagnosis_sample_limit(window_seconds: int, poll_seconds: int) -> int:
    if window_seconds <= 0:
        return 1
    return 1 + window_seconds // poll_seconds


def maximum_attempt_count(max_replacements: int, same_topology_retries: int) -> int:
    count = (max_replacements + 1) * (same_topology_retries + 1)
    if count > MAX_RECOVERY_ATTEMPTS:
        raise RecoveryError(
            f"recovery policy permits {count} attempts; limit is {MAX_RECOVERY_ATTEMPTS}"
        )
    return count


def same_topology_retries_used(attempts: Sequence[Mapping[str, Any]]) -> int:
    used = 0
    for attempt in reversed(attempts):
        if attempt.get("status") == "REPLACED":
            break
        if (
            attempt.get("status") == "RETRY_SAME_TOPOLOGY"
            and attempt.get("recoveryAction") == "same-topology"
        ):
            used += 1
    return used


def diagnosed_failed_nodes(diagnosis: object) -> tuple[str, ...]:
    if not isinstance(diagnosis, Mapping):
        return ()
    raw = diagnosis.get("failedActiveNodes")
    if not isinstance(raw, list):
        replacements = diagnosis.get("replacements")
        raw = (
            [
                item.get("failedNode")
                for item in replacements
                if isinstance(item, Mapping)
            ]
            if isinstance(replacements, list)
            else []
        )
    return tuple(sorted({item for item in raw if isinstance(item, str) and item}))


def validate_resumable_state(
    state: Mapping[str, Any],
    *,
    job_id: str,
    initial_active_nodes: Sequence[str],
    initial_spare_nodes: Sequence[str],
    max_replacements: int,
    same_topology_retries: int,
    training_artifact_root: Path,
) -> tuple[tuple[str, ...], tuple[str, ...], list[str], int, int, bool]:
    """Validate durable state and return the exact continuation point."""
    status = state.get("status")
    if status not in RESUMABLE_STATE_STATUSES:
        raise RecoveryError(f"recovery state cannot be resumed from status {status!r}")

    active_nodes = _state_string_tuple(
        state.get("activeNodes"), label="activeNodes", allow_empty=False
    )
    spare_nodes = _state_string_tuple(
        state.get("spareNodes"), label="spareNodes", allow_empty=True
    )
    quarantined = list(
        _state_string_tuple(
            state.get("quarantinedNodes"),
            label="quarantinedNodes",
            allow_empty=True,
        )
    )
    initial_active = tuple(initial_active_nodes)
    initial_spares = tuple(initial_spare_nodes)
    initial_spare_count = _state_nonnegative_int(
        state.get("initialSpareCount"), label="initialSpareCount"
    )
    remaining_spare_count = _state_nonnegative_int(
        state.get("remainingSpareCount"), label="remainingSpareCount"
    )
    recorded_max = _state_nonnegative_int(
        state.get("maxReplacementCount"), label="maxReplacementCount"
    )
    replacements_used = _state_nonnegative_int(
        state.get("replacementCount"), label="replacementCount"
    )
    complete_pool = (*active_nodes, *spare_nodes, *quarantined)
    initial_pool = (*initial_active, *initial_spares)
    if (
        initial_spare_count != len(initial_spares)
        or recorded_max != max_replacements
        or remaining_spare_count != len(spare_nodes)
        or replacements_used != len(quarantined)
        or replacements_used != len(initial_spares) - len(spare_nodes)
        or len(active_nodes) != len(initial_active)
        or set(complete_pool) != set(initial_pool)
        or len(complete_pool) != len(set(complete_pool))
    ):
        raise RecoveryError("recovery state topology or replacement counts differ")

    attempts = state.get("attempts")
    max_attempts = maximum_attempt_count(max_replacements, same_topology_retries)
    if not isinstance(attempts, list) or len(attempts) > max_attempts:
        raise RecoveryError("recovery state attempts are invalid")
    artifact_root = training_artifact_root.resolve()
    for index, item in enumerate(attempts):
        run_id = attempt_run_id(job_id, index)
        expected_result = artifact_root / run_id / "execution-result.json"
        if (
            not isinstance(item, Mapping)
            or not isinstance(item.get("attempt"), int)
            or isinstance(item.get("attempt"), bool)
            or item.get("attempt") != index
            or item.get("runId") != run_id
            or item.get("resultPath") != str(expected_result)
            or item.get("status")
            not in {
                "RUNNING",
                "FAIL",
                "PASS",
                "REPLACED",
                "RETRY_SAME_TOPOLOGY",
                "MANUAL_REQUIRED",
                "STOPPED",
            }
        ):
            raise RecoveryError(f"recovery state attempt {index} is invalid")
        attempt_active = _state_string_tuple(
            item.get("activeNodes"),
            label=f"attempt {index} activeNodes",
            allow_empty=False,
        )
        attempt_spares = _state_string_tuple(
            item.get("spareNodes"),
            label=f"attempt {index} spareNodes",
            allow_empty=True,
        )
        available_spares = item.get("availableSpareCount")
        if (
            not isinstance(available_spares, int)
            or isinstance(available_spares, bool)
            or available_spares != len(attempt_spares)
        ):
            raise RecoveryError(f"recovery state attempt {index} spare count differs")
        if index == 0 and (
            attempt_active != initial_active or attempt_spares != initial_spares
        ):
            raise RecoveryError("recovery state initial topology differs from arguments")

    if status == "STARTING":
        if attempts or active_nodes != initial_active or spare_nodes != initial_spares:
            raise RecoveryError("STARTING recovery state already contains attempt data")
        return active_nodes, spare_nodes, quarantined, replacements_used, 0, False
    if not attempts:
        raise RecoveryError(f"{status} recovery state has no attempt")
    latest = attempts[-1]
    if status == "RETRYING":
        if latest.get("status") not in {"REPLACED", "RETRY_SAME_TOPOLOGY"}:
            raise RecoveryError("RETRYING recovery state lacks a recovery action")
        if latest.get("status") == "RETRY_SAME_TOPOLOGY" and latest.get(
            "recoveryAction"
        ) != "same-topology":
            raise RecoveryError("same-topology retry record is invalid")
        if same_topology_retries_used(attempts) > same_topology_retries:
            raise RecoveryError("same-topology retry budget differs from state")
        return (
            active_nodes,
            spare_nodes,
            quarantined,
            replacements_used,
            len(attempts),
            False,
        )
    expected_latest_status = "RUNNING" if status == "RUNNING" else "FAIL"
    if (
        latest.get("status") != expected_latest_status
        or tuple(latest.get("activeNodes", ())) != active_nodes
        or tuple(latest.get("spareNodes", ())) != spare_nodes
    ):
        raise RecoveryError("latest recovery attempt does not match durable state")
    return (
        active_nodes,
        spare_nodes,
        quarantined,
        replacements_used,
        len(attempts) - 1,
        True,
    )


def run_supervisor(args: argparse.Namespace) -> int:
    apply_config_defaults(args)
    locked_args = copy.copy(args)
    job_id = args.run_id or start_ray.new_run_id()
    if start_ray.RUN_ID_PATTERN.fullmatch(job_id) is None:
        raise RecoveryError("logical run ID contains unsupported characters")
    locked_args.run_id = job_id
    state_root = args.recovery_state_root.resolve()
    lock_dir = state_root / ".supervisor-locks"
    try:
        lock_dir.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        raise RecoveryError(f"cannot create supervisor lock directory: {error}") from error
    if not lock_dir.is_dir() or lock_dir.is_symlink():
        raise RecoveryError("supervisor lock directory is not a regular directory")
    with SupervisorLock(lock_dir / f"{job_id}.lock"):
        return _run_supervisor_locked(locked_args)


def _run_supervisor_locked(args: argparse.Namespace) -> int:
    if bool(getattr(args, "fresh", False)):
        raise RecoveryError(
            "--fresh is supported by 'kcc_ray start' only; recovery must "
            "begin from an existing checkpoint"
        )
    if bool(getattr(args, "all_nodes", False)):
        raise RecoveryError(
            "--all-nodes is supported by 'kcc_ray start' only; recovery "
            "requires a separate spare-node pool"
        )
    job_id = args.run_id
    defaults = None
    if args.node is None or args.spare_node is None:
        defaults = cluster_config.load_cluster_config()
    initial_active_nodes = (
        tuple(args.node) if args.node is not None else defaults.active_nodes
    )
    initial_spare_nodes = (
        tuple(args.spare_node)
        if args.spare_node is not None
        else defaults.spare_nodes
    )
    validate_node_pool(initial_active_nodes, initial_spare_nodes)
    max_replacements = (
        len(initial_spare_nodes)
        if args.max_recoveries is None
        else args.max_recoveries
    )
    if not 0 <= max_replacements <= len(initial_spare_nodes):
        raise RecoveryError(
            "max recoveries must be between zero and the initial spare count"
        )
    if args.cleanup_timeout_seconds <= 0:
        raise RecoveryError("cleanup timeout must be positive")
    state_dir = args.recovery_state_root.resolve() / job_id
    state_path = state_dir / "state.json"
    stop_request_path = state_dir / training_control.STOP_REQUEST_FILENAME
    resume_requested = bool(getattr(args, "resume", False))
    if state_dir.is_symlink() or (state_dir.exists() and not state_dir.is_dir()):
        raise RecoveryError("recovery job state path is not a regular directory")
    state_file_present = state_path.exists() or state_path.is_symlink()
    if state_file_present and not resume_requested:
        raise RecoveryError(
            f"recovery job state already exists; use --resume to reattach: {state_dir}"
        )
    state: dict[str, Any] | None = None
    if state_file_present:
        state = load_recovery_state(state_path, expected_job_id=job_id)
    recovery_policy = apply_frozen_recovery_policy(args, state)
    if state is not None and state.get("status") in TERMINAL_STATE_STATUSES:
        terminal_status = state["status"]
        print(
            f"Recovery job {job_id} is already terminal: {terminal_status}",
            flush=True,
        )
        return 0 if terminal_status in {"PASS", "STOPPED"} else 1
    max_attempts = maximum_attempt_count(
        max_replacements, recovery_policy["sameTopologyRetries"]
    )
    attempt_run_id(job_id, max_attempts - 1)
    if state_dir.exists() and not state_file_present:
        try:
            entries = {entry.name for entry in state_dir.iterdir()}
        except OSError as error:
            raise RecoveryError(f"cannot inspect recovery state directory: {error}") from error
        allowed_initialization_entries = {
            TRAINING_TEMPLATE_METADATA_FILENAME,
            TRAINING_TEMPLATE_SNAPSHOT_FILENAME,
        }
        if entries - allowed_initialization_entries or not resume_requested:
            raise RecoveryError(
                f"recovery job state already exists; refusing a duplicate supervisor: {state_dir}"
            )
    elif not state_dir.exists():
        try:
            state_dir.mkdir(parents=True, exist_ok=False)
        except OSError as error:
            raise RecoveryError(
                f"cannot initialize recovery state directory: {error}"
            ) from error

    snapshot_path, template_metadata = prepare_training_template_snapshot(
        state_dir=state_dir,
        job_id=job_id,
        selected_source=args.train_script,
        existing_state=state,
    )
    args.train_script = snapshot_path
    # Validate the recipe declaration, but use the frozen accelerator profile
    # for node health.  A recipe's initial NPUS_PER_NODE is later rewritten
    # from the generated HCCL topology and is not a scheduling contract.
    source_declared_npus(snapshot_path)
    expected_npus = args.devices_per_node

    initial_args = copy.copy(args)
    initial_args.node = list(initial_active_nodes)
    initial_args.failure_retention_seconds = 0
    initial_args.require_resumable_checkpoint = True
    start_ray.validate_args(initial_args, attempt_run_id(job_id, 0))

    if state is not None:
        (
            active_nodes,
            spare_nodes,
            quarantined,
            replacements_used,
            attempt,
            resume_current_attempt,
        ) = validate_resumable_state(
            state,
            job_id=job_id,
            initial_active_nodes=initial_active_nodes,
            initial_spare_nodes=initial_spare_nodes,
            max_replacements=max_replacements,
            same_topology_retries=recovery_policy["sameTopologyRetries"],
            training_artifact_root=args.training_artifact_root,
        )
        print(f"Recovery job ID: {job_id} (reattaching)", flush=True)
        print(f"Recovery state: {state_path}", flush=True)
    else:
        for possible_attempt in range(max_attempts):
            possible_dir = (
                args.training_artifact_root.resolve()
                / attempt_run_id(job_id, possible_attempt)
            )
            for artifact in (
                possible_dir / "execution-result.json",
                possible_dir / ray_training_submit.SUBMISSION_RECORD_FILENAME,
            ):
                if artifact.exists() or artifact.is_symlink():
                    raise RecoveryError(
                        "an artifact already exists for a possible attempt; "
                        f"choose a new logical run ID: {artifact}"
                    )
        state = {
            "schemaVersion": RECOVERY_STATE_SCHEMA,
            "jobId": job_id,
            "trainingTemplate": template_metadata,
            "status": "STARTING",
            "activeNodes": list(initial_active_nodes),
            "spareNodes": list(initial_spare_nodes),
            "initialSpareCount": len(initial_spare_nodes),
            "remainingSpareCount": len(initial_spare_nodes),
            "maxReplacementCount": max_replacements,
            "replacementCount": 0,
            "recoveryPolicy": recovery_policy,
            "quarantinedNodes": [],
            "attempts": [],
            "createdAt": utc_now(),
            "updatedAt": utc_now(),
        }
        write_state(state_path, state)
        print(f"Recovery job ID: {job_id}", flush=True)
        print(f"Recovery state: {state_path}", flush=True)
        active_nodes = initial_active_nodes
        spare_nodes = initial_spare_nodes
        quarantined = []
        replacements_used = 0
        attempt = 0
        resume_current_attempt = False

    if args.failure_retention_seconds != 0:
        print(
            "RECOVERY MODE: failed-resource retention is forced to 0 seconds; "
            "diagnosis starts only after cleanup completes.",
            file=sys.stderr,
        )
    while True:
        run_id = attempt_run_id(job_id, attempt)
        stop_request = accepted_stop_request(
            stop_request_path,
            job_id=job_id,
        )
        if stop_request is not None:
            accepted_attempts = {run_id}
            if state.get("status") == "RETRYING" and state.get("attempts"):
                latest_run_id = state["attempts"][-1].get("runId")
                if isinstance(latest_run_id, str):
                    accepted_attempts.add(latest_run_id)
            if stop_request.get("attemptRunId") not in accepted_attempts:
                raise RecoveryError("stop request belongs to another attempt")
            record_stopped_state(
                state_path=state_path,
                state=state,
                stop_request=stop_request,
            )
            print(
                "STOP: accepted user stop request; no new recovery attempt "
                "will be started.",
                file=sys.stderr,
            )
            return 0
        attempt_args = copy.copy(args)
        attempt_args.node = list(active_nodes)
        attempt_args.failure_retention_seconds = 0
        attempt_args.require_resumable_checkpoint = True
        if attempt != 0:
            start_ray.validate_args(attempt_args, run_id)
        result_path = (
            attempt_args.training_artifact_root.resolve()
            / run_id
            / "execution-result.json"
        )
        submission_path = result_path.with_name(
            ray_training_submit.SUBMISSION_RECORD_FILENAME
        )
        failed_stage: dict[str, Any] = {}

        def stop_for_manual_without_cleanup(reason: str) -> int:
            attempt_state["status"] = "MANUAL_REQUIRED"
            attempt_state["connectionState"] = "UNCERTAIN"
            attempt_state["recoveryFailure"] = reason[:1000]
            attempt_state["finishedAt"] = utc_now()
            attempt_state["returncode"] = 1
            state["status"] = "MANUAL_REQUIRED"
            state["activeNodes"] = list(active_nodes)
            state["spareNodes"] = list(spare_nodes)
            state["remainingSpareCount"] = len(spare_nodes)
            state["replacementCount"] = replacements_used
            state["quarantinedNodes"] = list(quarantined)
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            emit_manual_required_alert(
                attempt_args,
                job_id=job_id,
                reason=reason,
            )
            print(
                "STOP: Ray Job state is uncertain; automatic cleanup was not "
                "started and existing resources were retained: "
                f"{reason}",
                file=sys.stderr,
            )
            return 1

        def handle_stage_failure(index: int, name: str) -> None:
            failed_stage["index"] = index
            failed_stage["name"] = name
            submission_uncertain = (
                name == "formal Ray training"
                and not (result_path.exists() or result_path.is_symlink())
            )
            if submission_uncertain:
                attempt_state["connectionState"] = "UNCERTAIN"
                state["status"] = "RUNNING"
                state["updatedAt"] = utc_now()
                write_state(state_path, state)
                return
            attempt_state["status"] = "FAIL"
            attempt_state["failedStageIndex"] = index
            attempt_state["failedStageName"] = name
            attempt_state.setdefault("returncode", 1)
            state["status"] = "WAITING_FOR_CLEANUP"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            if name == "formal training parameter injection":
                start_ray.retain_then_delete_cluster(attempt_args)

        if resume_current_attempt:
            attempts = state.get("attempts")
            if not isinstance(attempts, list) or not isinstance(attempts[-1], dict):
                raise RecoveryError("latest recovery attempt is no longer trustworthy")
            attempt_state = attempts[-1]
            resume_phase = state.get("status")
            if resume_phase in {"WAITING_FOR_CLEANUP", "DIAGNOSING"}:
                returncode = attempt_state.get("returncode", 1)
                if not isinstance(returncode, int) or returncode == 0:
                    raise RecoveryError("recorded failed attempt has an invalid return code")
                failed_stage["index"] = attempt_state.get("failedStageIndex")
                failed_stage["name"] = attempt_state.get("failedStageName")
            else:
                terminal_status = existing_training_result_status(
                    result_path,
                    expected_run_id=run_id,
                )
                current_stage_index = attempt_state.get("currentStageIndex")
                submission_present = submission_path.exists() or submission_path.is_symlink()
                formal_stage_started = (
                    terminal_status is not None
                    or submission_present
                    or current_stage_index == 6
                )
                if terminal_status is None and not formal_stage_started:
                    failed_stage["index"] = current_stage_index
                    failed_stage["name"] = attempt_state.get(
                        "currentStageName", "supervisor startup"
                    )
                    returncode = 1
                    attempt_state["status"] = "FAIL"
                    attempt_state["failedStageIndex"] = failed_stage["index"]
                    attempt_state["failedStageName"] = failed_stage["name"]
                    state["status"] = "WAITING_FOR_CLEANUP"
                    state["updatedAt"] = utc_now()
                    write_state(state_path, state)
                elif terminal_status is None:
                    if resume_phase != "RUNNING":
                        raise RecoveryError(
                            "recovery phase requires a recorded FAIL result"
                        )
                    try:
                        ray_training_submit.load_submission_record(
                            submission_path,
                            expected_run_id=run_id,
                            expected_namespace=attempt_args.namespace,
                            expected_cluster=attempt_args.cluster,
                        )
                    except ray_training_submit.SubmitError as error:
                        try:
                            cluster_present = query_owned_raycluster(
                                kubectl=kubectl_prefix(
                                    attempt_args.kubectl_command,
                                    attempt_args.kubeconfig.resolve(),
                                ),
                                namespace=attempt_args.namespace,
                                cluster=attempt_args.cluster,
                                run_id=run_id,
                            ) is not None
                        except RecoveryError as query_error:
                            return stop_for_manual_without_cleanup(
                                "submission record is invalid and RayCluster "
                                f"ownership cannot be proved: {error}; {query_error}"
                            )
                        if cluster_present:
                            return stop_for_manual_without_cleanup(
                                "submission record is missing or invalid while "
                                f"the owned RayCluster still exists: {error}"
                            )
                        returncode = 1
                        failed_stage["index"] = 6
                        failed_stage["name"] = "formal Ray training"
                    else:
                        while terminal_status is None:
                            stop_request = accepted_stop_request(
                                stop_request_path,
                                job_id=job_id,
                            )
                            if stop_request is not None:
                                if stop_request.get("attemptRunId") != run_id:
                                    raise RecoveryError(
                                        "stop request belongs to another attempt"
                                    )
                                returncode = 1
                                failed_stage["index"] = 6
                                failed_stage["name"] = "formal Ray training"
                                break
                            try:
                                ray_training_submit.resume_existing_submission(
                                    expected_run_id=run_id,
                                    kubectl_command=attempt_args.kubectl_command,
                                    kubeconfig=attempt_args.kubeconfig.resolve(),
                                    namespace=attempt_args.namespace,
                                    cluster=attempt_args.cluster,
                                    result_path=result_path,
                                    failure_retention_seconds=0,
                                    poll_seconds=(
                                        ray_training_submit.DEFAULT_POLL_SECONDS
                                    ),
                                    keep_success_resources=bool(
                                        getattr(
                                            attempt_args,
                                            "keep_success_resources",
                                            False,
                                        )
                                    ),
                                )
                                reattach_error: Exception | None = None
                            except ray_training_submit.SubmitError as error:
                                reattach_error = error
                            terminal_status = existing_training_result_status(
                                result_path,
                                expected_run_id=run_id,
                            )
                            if terminal_status is not None:
                                break
                            try:
                                cluster_present = query_owned_raycluster(
                                    kubectl=kubectl_prefix(
                                        attempt_args.kubectl_command,
                                        attempt_args.kubeconfig.resolve(),
                                    ),
                                    namespace=attempt_args.namespace,
                                    cluster=attempt_args.cluster,
                                    run_id=run_id,
                                ) is not None
                            except RecoveryOwnershipError as error:
                                return stop_for_manual_without_cleanup(str(error))
                            except RecoveryError as error:
                                cluster_present = True
                                reattach_error = reattach_error or error
                            if not cluster_present:
                                returncode = 1
                                failed_stage["index"] = 6
                                failed_stage["name"] = "formal Ray training"
                                break
                            detail = str(
                                reattach_error
                                or "reattach returned without a terminal result"
                            )
                            attempt_state["connectionState"] = "UNCERTAIN"
                            attempt_state["lastConnectionFailure"] = detail[:1000]
                            state["status"] = "RUNNING"
                            state["updatedAt"] = utc_now()
                            write_state(state_path, state)
                            print(
                                "WARNING: Ray Job connection remains uncertain; "
                                "the owned RayCluster is still present and will "
                                f"not be cleaned up: {detail}",
                                file=sys.stderr,
                                flush=True,
                            )
                            retry_deadline = time.monotonic() + 30
                            while time.monotonic() < retry_deadline:
                                if accepted_stop_request(
                                    stop_request_path,
                                    job_id=job_id,
                                ) is not None:
                                    break
                                time.sleep(
                                    min(
                                        1.0,
                                        max(
                                            0.0,
                                            retry_deadline - time.monotonic(),
                                        ),
                                    )
                                )
                if terminal_status is not None:
                    returncode = 0 if terminal_status == "PASS" else 1
                    attempt_state.pop("connectionState", None)
                    attempt_state.pop("lastConnectionFailure", None)
                    if returncode != 0:
                        failed_stage["index"] = 6
                        failed_stage["name"] = "formal Ray training"
            attempt_state.setdefault("finishedAt", utc_now())
            attempt_state["returncode"] = returncode
            resume_current_attempt = False
        else:
            for artifact in (result_path, submission_path):
                if artifact.exists() or artifact.is_symlink():
                    raise RecoveryError(
                        "refusing to start an unrecorded attempt with existing artifact: "
                        f"{artifact}"
                    )
            stages = start_ray.build_stage_commands(attempt_args, run_id=run_id)
            attempt_state = {
                "attempt": attempt,
                "runId": run_id,
                "status": "RUNNING",
                "activeNodes": list(active_nodes),
                "spareNodes": list(spare_nodes),
                "availableSpareCount": len(spare_nodes),
                "startedAt": utc_now(),
                "resultPath": str(result_path),
            }
            state["status"] = "RUNNING"
            state["activeNodes"] = list(active_nodes)
            state["spareNodes"] = list(spare_nodes)
            state["remainingSpareCount"] = len(spare_nodes)
            state["replacementCount"] = replacements_used
            state["quarantinedNodes"] = list(quarantined)
            state["attempts"].append(attempt_state)
            state["updatedAt"] = utc_now()
            write_state(state_path, state)

            def before_stage(_index: int, _name: str) -> bool:
                request = accepted_stop_request(
                    stop_request_path,
                    job_id=job_id,
                )
                if request is not None:
                    if request.get("attemptRunId") != run_id:
                        raise RecoveryError("stop request belongs to another attempt")
                    return False
                attempt_state["currentStageIndex"] = _index
                attempt_state["currentStageName"] = _name
                state["updatedAt"] = utc_now()
                write_state(state_path, state)
                return True

            returncode = start_ray.execute_pipeline(
                stages,
                failure_handler=handle_stage_failure,
                before_stage=before_stage,
            )
            attempt_state["finishedAt"] = utc_now()
            attempt_state["returncode"] = returncode
            if (
                returncode != 0
                and attempt_state.get("connectionState") == "UNCERTAIN"
                and not (result_path.exists() or result_path.is_symlink())
            ):
                stop_request = accepted_stop_request(
                    stop_request_path,
                    job_id=job_id,
                )
                if stop_request is None:
                    attempt_state.pop("finishedAt", None)
                    attempt_state.pop("returncode", None)
                    resume_current_attempt = True
                    print(
                        "RECOVERY: Ray Job connection is uncertain; retaining "
                        "the running cluster and reattaching without resubmission.",
                        file=sys.stderr,
                        flush=True,
                    )
                    continue
        stop_request = accepted_stop_request(
            stop_request_path,
            job_id=job_id,
        )
        if returncode != 0 and stop_request is not None:
            if stop_request.get("attemptRunId") != run_id:
                raise RecoveryError("stop request belongs to another attempt")
            cleanup_failure: str | None = None
            try:
                wait_for_cluster_cleanup(
                    kubectl_command=attempt_args.kubectl_command,
                    kubeconfig=attempt_args.kubeconfig.resolve(),
                    namespace=attempt_args.namespace,
                    cluster=attempt_args.cluster,
                    run_id=run_id,
                    timeout_seconds=args.cleanup_timeout_seconds,
                )
            except RecoveryError as error:
                cleanup_failure = str(error)
            record_stopped_state(
                state_path=state_path,
                state=state,
                stop_request=stop_request,
                attempt_state=attempt_state,
                cleanup_failure=cleanup_failure,
            )
            print(
                "STOP: training ended in response to the accepted user stop "
                "request; automatic recovery is disabled.",
                file=sys.stderr,
            )
            return 0
        if returncode == 0:
            try:
                success_result = load_successful_training_result(
                    result_path,
                    expected_run_id=run_id,
                )
                require_successful_checkpoint_resume(
                    success_result,
                    expected_workers=len(active_nodes),
                )
            except RecoveryError as error:
                attempt_state["status"] = "MANUAL_REQUIRED"
                attempt_state["resultValidationFailure"] = str(error)
                state["status"] = "MANUAL_REQUIRED"
                state["activeNodes"] = list(active_nodes)
                state["spareNodes"] = list(spare_nodes)
                state["remainingSpareCount"] = len(spare_nodes)
                state["replacementCount"] = replacements_used
                state["quarantinedNodes"] = list(quarantined)
                state["updatedAt"] = utc_now()
                write_state(state_path, state)
                emit_manual_required_alert(
                    attempt_args,
                    job_id=job_id,
                    reason=f"successful result validation failed: {error}",
                )
                print(
                    "STOP: successful pipeline result could not prove checkpoint "
                    f"recovery: {error}",
                    file=sys.stderr,
                )
                return 1
            attempt_state["trainingResult"] = compact_training_result_for_state(
                success_result
            )
            attempt_state["status"] = "PASS"
            state["status"] = "PASS"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            return 0

        attempt_state["status"] = "FAIL"
        attempt_state["failedStageIndex"] = failed_stage.get("index")
        attempt_state["failedStageName"] = failed_stage.get("name")
        try:
            state["status"] = "WAITING_FOR_CLEANUP"
            state["updatedAt"] = utc_now()
            write_state(state_path, state)

            failure_result: Mapping[str, Any] | None = None
            failure_class: str | None = None
            if failed_stage.get("name") == "formal Ray training":
                try:
                    failure_result = load_failed_training_result(
                        result_path,
                        expected_run_id=run_id,
                    )
                except RecoveryError as error:
                    attempt_state["trainingResultFailure"] = str(error)
                    if result_path.exists() or result_path.is_symlink():
                        return stop_for_manual_without_cleanup(
                            "formal training result exists but is not a trusted "
                            f"FAIL result: {error}"
                        )
                    try:
                        cluster_present = query_owned_raycluster(
                            kubectl=kubectl_prefix(
                                attempt_args.kubectl_command,
                                attempt_args.kubeconfig.resolve(),
                            ),
                            namespace=attempt_args.namespace,
                            cluster=attempt_args.cluster,
                            run_id=run_id,
                        ) is not None
                    except RecoveryError as query_error:
                        return stop_for_manual_without_cleanup(
                            "formal training produced no trusted result and "
                            "RayCluster ownership cannot be proved: "
                            f"{error}; {query_error}"
                        )
                    if cluster_present:
                        return stop_for_manual_without_cleanup(
                            "formal training produced no trusted result while "
                            "the owned RayCluster still exists"
                        )
                else:
                    attempt_state["trainingResult"] = (
                        compact_training_result_for_state(failure_result)
                    )
                    raw_failure_class = failure_result.get("failureClass")
                    if isinstance(raw_failure_class, str):
                        failure_class = raw_failure_class
                    if failure_class == "CHECKPOINT_UNAVAILABLE":
                        attempt_state["checkpointFailure"] = failure_result.get(
                            "checkpoint"
                        )

            wait_for_cluster_cleanup(
                kubectl_command=attempt_args.kubectl_command,
                kubeconfig=attempt_args.kubeconfig.resolve(),
                namespace=attempt_args.namespace,
                cluster=attempt_args.cluster,
                run_id=run_id,
                timeout_seconds=args.cleanup_timeout_seconds,
            )

            stop_request = accepted_stop_request(
                stop_request_path,
                job_id=job_id,
            )
            if stop_request is not None:
                if stop_request.get("attemptRunId") != run_id:
                    raise RecoveryError("stop request belongs to another attempt")
                record_stopped_state(
                    state_path=state_path,
                    state=state,
                    stop_request=stop_request,
                    attempt_state=attempt_state,
                )
                print(
                    "STOP: accepted user stop request during recovery; no new "
                    "attempt will be started.",
                    file=sys.stderr,
                )
                return 0

            diagnosis_summary: Mapping[str, Any] | None = None
            if failure_class not in NON_NODE_FAILURE_CLASSES:
                state["status"] = "DIAGNOSING"
                state["updatedAt"] = utc_now()
                write_state(state_path, state)

                def diagnose_once() -> Mapping[str, Any]:
                    return recovery_diagnostics.diagnose_replacement(
                        kubectl_command=attempt_args.kubectl_command,
                        kubeconfig=attempt_args.kubeconfig.resolve(),
                        active_nodes=active_nodes,
                        spare_nodes=spare_nodes,
                        expected_npus=expected_npus,
                        npu_resource=attempt_args.npu_resource,
                        exporter_app=attempt_args.npu_exporter_app,
                        exporter_port=attempt_args.npu_exporter_port,
                    )

                def diagnosis_sleep(seconds: float) -> None:
                    deadline = time.monotonic() + seconds
                    while time.monotonic() < deadline:
                        if accepted_stop_request(
                            stop_request_path,
                            job_id=job_id,
                        ) is not None:
                            raise RecoveryStopRequested
                        time.sleep(
                            min(1.0, max(0.0, deadline - time.monotonic()))
                        )

                try:
                    diagnosis_summary = (
                        recovery_diagnostics.diagnose_stable_replacement(
                            diagnose_once,
                            stable_samples=recovery_policy[
                                "diagnosisStableSamples"
                            ],
                            max_samples=diagnosis_sample_limit(
                                recovery_policy["diagnosisWindowSeconds"],
                                recovery_policy["diagnosisPollSeconds"],
                            ),
                            poll_seconds=recovery_policy[
                                "diagnosisPollSeconds"
                            ],
                            sleep_fn=diagnosis_sleep,
                        )
                    )
                except RecoveryStopRequested:
                    stop_request = accepted_stop_request(
                        stop_request_path,
                        job_id=job_id,
                    )
                    if (
                        stop_request is None
                        or stop_request.get("attemptRunId") != run_id
                    ):
                        raise RecoveryError(
                            "diagnosis stop request belongs to another attempt"
                        )
                    record_stopped_state(
                        state_path=state_path,
                        state=state,
                        stop_request=stop_request,
                        attempt_state=attempt_state,
                    )
                    print(
                        "STOP: accepted user stop request during diagnosis; "
                        "no new attempt will be started.",
                        file=sys.stderr,
                    )
                    return 0
                latest_diagnosis = diagnosis_summary.get("latestDiagnosis")
                if isinstance(latest_diagnosis, Mapping):
                    attempt_state["diagnosis"] = dict(latest_diagnosis)
                attempt_state["diagnosisSummary"] = {
                    "stable": diagnosis_summary.get("stable") is True,
                    "sampleCount": diagnosis_summary.get("sampleCount"),
                    "confirmedFailedNodes": diagnosis_summary.get(
                        "confirmedFailedNodes", []
                    ),
                    "reason": diagnosis_summary.get("reason"),
                }

            if diagnosis_summary is not None and diagnosis_summary.get("stable") is True:
                diagnosis = diagnosis_summary.get("latestDiagnosis")
                if not isinstance(diagnosis, Mapping):
                    raise RecoveryError("stable diagnosis has no latest snapshot")
                replacements = diagnosis.get("replacements")
                if not isinstance(replacements, list) or not replacements:
                    raise RecoveryError(
                        "diagnosis allowed replacement without a replacement list"
                    )
                if diagnosis.get("replacementCount") not in (None, len(replacements)):
                    raise RecoveryError("diagnosis replacement count is inconsistent")
                if replacements_used + len(replacements) > max_replacements:
                    raise RecoveryError(
                        "diagnosed failures exceed the configured spare replacement budget"
                    )
                if len(replacements) > len(spare_nodes):
                    raise RecoveryError(
                        "diagnosed failures exceed the currently recorded spare count"
                    )

                next_active = active_nodes
                next_spares = spare_nodes
                replacement_records: list[dict[str, str]] = []
                for replacement in replacements:
                    if not isinstance(replacement, Mapping):
                        raise RecoveryError(
                            "diagnosis returned an invalid replacement entry"
                        )
                    bad_value = replacement.get("failedNode")
                    spare_value = replacement.get("spareNode")
                    if not isinstance(bad_value, str) or not bad_value:
                        raise RecoveryError("diagnosis omitted a failed active target")
                    if not isinstance(spare_value, str) or not spare_value:
                        raise RecoveryError("diagnosis omitted a spare target")
                    next_active, next_spares = replace_active_node(
                        next_active,
                        next_spares,
                        bad_target=bad_value,
                        replacement_target=spare_value,
                    )
                    replacement_records.append(
                        {"badTarget": bad_value, "replacementTarget": spare_value}
                    )

                active_nodes = next_active
                spare_nodes = next_spares
                replacements_used += len(replacement_records)
                quarantined.extend(
                    item["badTarget"] for item in replacement_records
                )
                attempt_state["status"] = "REPLACED"
                attempt_state["recoveryAction"] = "replace"
                attempt_state["replacements"] = replacement_records
                attempt_state["replacementCount"] = len(replacement_records)
                for replacement in replacement_records:
                    print(
                        "RECOVERY: replacing failed active node "
                        f"{replacement['badTarget']} with spare "
                        f"{replacement['replacementTarget']}.",
                        flush=True,
                    )
            else:
                latest_diagnosis = (
                    diagnosis_summary.get("latestDiagnosis")
                    if diagnosis_summary is not None
                    else None
                )
                unsafe_nodes = diagnosed_failed_nodes(latest_diagnosis)
                if unsafe_nodes:
                    raise RecoveryError(
                        "node health remained unsafe but did not meet the stable, "
                        "restart-ready replacement rule: " + ", ".join(unsafe_nodes)
                    )
                retries_used = same_topology_retries_used(state["attempts"])
                retry_limit = recovery_policy["sameTopologyRetries"]
                if retries_used >= retry_limit:
                    detail = (
                        f"{failure_class or failed_stage.get('name') or 'unknown failure'}; "
                        f"same-topology retry budget {retry_limit} exhausted"
                    )
                    raise RecoveryError(detail)
                retry_number = retries_used + 1
                attempt_state["status"] = "RETRY_SAME_TOPOLOGY"
                attempt_state["recoveryAction"] = "same-topology"
                attempt_state["sameTopologyRetry"] = retry_number
                print(
                    "RECOVERY: no stable hardware failure was confirmed; "
                    f"retrying the same topology ({retry_number}/{retry_limit}).",
                    flush=True,
                )
        except RecoveryError as error:
            attempt_state["status"] = "MANUAL_REQUIRED"
            attempt_state["recoveryFailure"] = str(error)
            state["status"] = "MANUAL_REQUIRED"
            state["activeNodes"] = list(active_nodes)
            state["spareNodes"] = list(spare_nodes)
            state["remainingSpareCount"] = len(spare_nodes)
            state["replacementCount"] = replacements_used
            state["quarantinedNodes"] = list(quarantined)
            state["updatedAt"] = utc_now()
            write_state(state_path, state)
            emit_manual_required_alert(
                attempt_args,
                job_id=job_id,
                reason=str(error),
            )
            print(f"STOP: automatic recovery refused: {error}", file=sys.stderr)
            return returncode or 1

        state["status"] = "RETRYING"
        state["activeNodes"] = list(active_nodes)
        state["spareNodes"] = list(spare_nodes)
        state["remainingSpareCount"] = len(spare_nodes)
        state["replacementCount"] = replacements_used
        state["quarantinedNodes"] = list(quarantined)
        state["updatedAt"] = utc_now()
        write_state(state_path, state)
        if attempt_state.get("recoveryAction") == "same-topology":
            deadline = time.monotonic() + recovery_policy["retryBackoffSeconds"]
            while time.monotonic() < deadline:
                stop_request = accepted_stop_request(
                    stop_request_path,
                    job_id=job_id,
                )
                if stop_request is not None:
                    if stop_request.get("attemptRunId") != run_id:
                        raise RecoveryError("stop request belongs to another attempt")
                    record_stopped_state(
                        state_path=state_path,
                        state=state,
                        stop_request=stop_request,
                        attempt_state=attempt_state,
                    )
                    print(
                        "STOP: accepted user stop request during retry backoff; "
                        "no new attempt will be started.",
                        file=sys.stderr,
                    )
                    return 0
                time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
        attempt += 1


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        return run_supervisor(args)
    except (RecoveryError, ValueError) as error:
        print(f"STOP: invalid recovery workflow: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print(
            "STOP: recovery supervisor interrupted; existing resources and "
            "checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130
    except Exception as error:
        print(
            "STOP: unexpected recovery supervisor crash; the outer Job may "
            f"retry safely: {error}",
            file=sys.stderr,
        )
        return UNEXPECTED_SUPERVISOR_EXIT


if __name__ == "__main__":
    raise SystemExit(main())
