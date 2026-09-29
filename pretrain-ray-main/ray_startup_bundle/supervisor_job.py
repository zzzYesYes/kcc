#!/usr/bin/env python3
"""Create and inspect the singleton Kubernetes Job for a recovery supervisor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
from typing import Any, Mapping, Sequence

import cluster_config
import recovery_supervisor
import start_ray


BUNDLE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BUNDLE_DIR.parent
DEFAULT_RBAC_MANIFEST = BUNDLE_DIR / "supervisor-rbac.yaml"
CONTAINER_PYTHON = "/home/ray/anaconda3/bin/python"
INTERNAL_KUBECTL_COMMAND = "/usr/local/bin/kubectl"
INTERNAL_KUBECONFIG = Path("/etc/trainctl/kubeconfig")
SUPERVISOR_SCRIPT = BUNDLE_DIR / "recovery_supervisor.py"
MANAGED_BY = "kcc-ray-supervisor"
MANAGED_BY_ANNOTATION = "trainctl.io/managed-by"
RUN_ID_ANNOTATION = "trainctl.io/run-id"
ARGS_SHA256_ANNOTATION = "trainctl.io/args-sha256"
CLUSTER_ANNOTATION = "trainctl.io/cluster"
RBAC_CLUSTER_TOKEN = "__KCC_RAY_CLUSTER__"
RBAC_NAMESPACE_TOKEN = "__KCC_RAY_NAMESPACE__"
RBAC_EXPORTER_NAMESPACE_TOKEN = "__KCC_RAY_EXPORTER_NAMESPACE__"
RBAC_SERVICE_ACCOUNT_TOKEN = "__KCC_RAY_SUPERVISOR_SERVICE_ACCOUNT__"
LOCAL_PATH_DESTINATIONS = frozenset(
    {
        "manifest",
        "runtime_source_dir",
        "hccl_evidence_root",
        "train_script",
        "training_artifact_root",
        "recovery_state_root",
    }
)
LEGACY_RECOVERY_ARGUMENT_DESTINATIONS = frozenset(
    {
        "no_progress_seconds",
        "same_topology_retries",
        "retry_backoff_seconds",
        "diagnosis_window_seconds",
        "diagnosis_poll_seconds",
        "diagnosis_stable_samples",
    }
)
LEGACY_HARDWARE_ARGUMENT_DESTINATIONS = frozenset(
    {
        "devices_per_node",
        "runtime_class_name",
        "head_selector",
        "worker_selector",
    }
)
_SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class SupervisorJobError(RuntimeError):
    """A Kubernetes supervisor Job operation could not be proven safe."""


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    try:
        command = shlex.split(command_text)
    except ValueError as error:
        raise SupervisorJobError(f"invalid kubectl command: {error}") from error
    if not command:
        raise SupervisorJobError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def _run(
    command: Sequence[str],
    *,
    input_text: str | None = None,
    timeout: int = 60,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            input=input_text,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SupervisorJobError(f"cannot execute kubectl: {error}") from error
    if result.returncode != 0:
        detail = result.stderr.strip() or result.stdout.strip() or "unknown error"
        raise SupervisorJobError(f"kubectl command failed: {detail}")
    return result


def _json_document(text: str, *, label: str) -> dict[str, Any]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise SupervisorJobError(f"kubectl returned invalid {label} JSON: {error}") from error
    if not isinstance(value, dict):
        raise SupervisorJobError(f"kubectl returned a non-object {label}")
    return value


def supervisor_job_name(cluster: str) -> str:
    """Return one stable DNS-safe Job name for a RayCluster identity."""
    if not cluster:
        raise SupervisorJobError("cluster name is empty")
    normalized = re.sub(r"[^a-z0-9-]+", "-", cluster.lower()).strip("-")
    normalized = re.sub(r"-+", "-", normalized) or "cluster"
    digest = hashlib.sha256(cluster.encode("utf-8")).hexdigest()[:10]
    prefix = "kcc-ray-supervisor-"
    room = 63 - len(prefix) - len(digest) - 1
    stem = normalized[:room].rstrip("-") or "cluster"
    return f"{prefix}{stem}-{digest}"


def arguments_sha256(arguments: Sequence[str]) -> str:
    encoded = json.dumps(
        list(arguments), ensure_ascii=False, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _inside_project(path: Path) -> Path:
    try:
        resolved = path.expanduser().resolve()
    except (OSError, RuntimeError) as error:
        raise SupervisorJobError(f"cannot resolve local path {path}: {error}") from error
    try:
        resolved.relative_to(PROJECT_ROOT)
    except ValueError as error:
        raise SupervisorJobError(
            f"custom local path must stay inside {PROJECT_ROOT}: {path}"
        ) from error
    return resolved


def _normalize_local_paths(args: argparse.Namespace) -> None:
    for destination in LOCAL_PATH_DESTINATIONS:
        value = getattr(args, destination, None)
        if value is not None:
            setattr(args, destination, _inside_project(Path(value)))


def parse_start_arguments(argv: Sequence[str]) -> tuple[argparse.ArgumentParser, argparse.Namespace]:
    """Parse the existing recovery CLI and add only launcher-owned defaults."""
    parser = recovery_supervisor.make_parser()
    args = parser.parse_args(list(argv))
    if args.fresh or args.all_nodes:
        raise SupervisorJobError(
            "the supervisor Job launcher accepts only the default recovery mode"
        )
    if args.run_id is None:
        args.run_id = start_ray.new_run_id()
    if start_ray.RUN_ID_PATTERN.fullmatch(args.run_id) is None:
        raise SupervisorJobError("logical run ID contains unsupported characters")
    active_from_config = args.node is None
    try:
        recovery_supervisor.apply_config_defaults(args)
    except cluster_config.ClusterConfigError as error:
        raise SupervisorJobError(str(error)) from error
    if active_from_config:
        args.allow_topology_change = True
    _normalize_local_paths(args)
    try:
        recovery_supervisor.validate_node_pool(args.node, args.spare_node)
        max_recoveries = (
            len(args.spare_node)
            if args.max_recoveries is None
            else args.max_recoveries
        )
        if not 0 <= max_recoveries <= len(args.spare_node):
            raise ValueError(
                "max recoveries must be between zero and the initial spare count"
            )
        if args.cleanup_timeout_seconds <= 0:
            raise ValueError("cleanup timeout must be positive")
        recovery_policy = recovery_supervisor.recovery_policy_from_args(args)
        recovery_supervisor.maximum_attempt_count(
            max_recoveries,
            recovery_policy["sameTopologyRetries"],
        )
        first_attempt = recovery_supervisor.attempt_run_id(args.run_id, 0)
        start_ray.validate_args(args, first_attempt)
    except (ValueError, recovery_supervisor.RecoveryError) as error:
        raise SupervisorJobError(str(error)) from error
    state_dir = args.recovery_state_root / args.run_id
    state_path = state_dir / "state.json"
    if state_dir.is_symlink() or state_path.is_symlink():
        raise SupervisorJobError("recovery state must not use symbolic links")
    state_present = state_path.exists() or state_path.is_symlink()
    if state_dir.exists() and not state_dir.is_dir():
        state_present = True
    elif state_dir.is_dir():
        try:
            state_present = state_present or any(state_dir.iterdir())
        except OSError as error:
            raise SupervisorJobError(
                f"cannot inspect recovery state directory: {error}"
            ) from error
    if not args.resume and state_present:
        raise SupervisorJobError(
            "recovery state already exists; pass --resume to reattach safely"
        )
    return parser, args


def _action_option(action: argparse.Action) -> str:
    long_options = [item for item in action.option_strings if item.startswith("--")]
    if not long_options:
        raise SupervisorJobError(f"unsupported positional recovery argument: {action.dest}")
    return long_options[0]


def build_supervisor_arguments(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
    *,
    omit_destinations: frozenset[str] = frozenset(),
    boolean_overrides: Mapping[str, bool] | None = None,
) -> list[str]:
    """Build deterministic in-Pod argv without inheriting local auth settings."""
    result: list[str] = []
    overrides = boolean_overrides or {}
    for action in parser._actions:
        destination = action.dest
        if destination in {"help", "resume"} or destination in omit_destinations:
            continue
        option = _action_option(action)
        if destination in overrides:
            value: Any = overrides[destination]
        elif destination == "kubectl_command":
            value = INTERNAL_KUBECTL_COMMAND
        elif destination == "kubeconfig":
            value = INTERNAL_KUBECONFIG
        else:
            value = getattr(args, destination)
        if isinstance(action, argparse._StoreTrueAction):
            if value:
                result.append(option)
        elif isinstance(action, argparse._StoreFalseAction):
            if not value:
                result.append(option)
        elif isinstance(action, argparse._AppendAction):
            for item in value or ():
                result.extend((option, str(item)))
        elif value is not None:
            result.extend((option, str(value)))
    result.append("--resume")
    if result.count("--resume") != 1:
        raise SupervisorJobError("internal resume argument construction failed")
    return result


def compatible_resume_argument_digests(
    parser: argparse.ArgumentParser,
    args: argparse.Namespace,
) -> frozenset[str]:
    """Exact argv hashes for current and legacy launcher formats."""
    digests: set[str] = set()
    for omit_recovery in (False, True):
        for omit_hardware in (False, True):
            omitted = frozenset().union(
                LEGACY_RECOVERY_ARGUMENT_DESTINATIONS if omit_recovery else (),
                LEGACY_HARDWARE_ARGUMENT_DESTINATIONS if omit_hardware else (),
            )
            for legacy_confirm in (False, True):
                arguments = build_supervisor_arguments(
                    parser,
                    args,
                    omit_destinations=omitted,
                    boolean_overrides={
                        "confirm_checkpoint_exclusive": legacy_confirm,
                    },
                )
                digests.add(arguments_sha256(arguments))
    return frozenset(digests)


def build_job_manifest(
    *,
    args: argparse.Namespace,
    supervisor_arguments: Sequence[str],
    args_sha256: str,
) -> dict[str, Any]:
    cluster = args.cluster
    namespace = args.namespace
    name = supervisor_job_name(cluster)
    project = str(PROJECT_ROOT)
    log_root = str(PROJECT_ROOT / "log")
    annotations = {
        MANAGED_BY_ANNOTATION: MANAGED_BY,
        RUN_ID_ANNOTATION: args.run_id,
        ARGS_SHA256_ANNOTATION: args_sha256,
        CLUSTER_ANNOTATION: cluster,
    }
    labels = {
        "app.kubernetes.io/name": "kcc-ray-supervisor",
        "app.kubernetes.io/component": "recovery-supervisor",
        "app.kubernetes.io/managed-by": "kcc-ray",
    }
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": labels,
            "annotations": annotations,
        },
        "spec": {
            "backoffLimit": args.supervisor_backoff_limit,
            "ttlSecondsAfterFinished": args.supervisor_finished_ttl_seconds,
            "podFailurePolicy": {
                "rules": [
                    {
                        "action": "Ignore",
                        "onPodConditions": [{"type": "DisruptionTarget"}],
                    },
                    {
                        "action": "FailJob",
                        "onExitCodes": {
                            "containerName": "supervisor",
                            "operator": "In",
                            "values": [1, 2, 130],
                        },
                    },
                ]
            },
            "template": {
                "metadata": {"labels": labels, "annotations": annotations},
                "spec": {
                    "restartPolicy": "Never",
                    "serviceAccountName": args.supervisor_service_account,
                    "automountServiceAccountToken": True,
                    "nodeSelector": {
                        "kubernetes.io/hostname": args.supervisor_node
                    },
                    "securityContext": {
                        "runAsUser": 1001,
                        "runAsGroup": 1001,
                        "runAsNonRoot": True,
                        "fsGroup": 1001,
                        "fsGroupChangePolicy": "OnRootMismatch",
                        "seccompProfile": {"type": "RuntimeDefault"},
                    },
                    "containers": [
                        {
                            "name": "supervisor",
                            "image": args.supervisor_image,
                            "imagePullPolicy": args.supervisor_image_pull_policy,
                            "securityContext": {
                                "allowPrivilegeEscalation": False,
                                "readOnlyRootFilesystem": True,
                                "capabilities": {"drop": ["ALL"]},
                            },
                            "command": [CONTAINER_PYTHON],
                            "args": [str(SUPERVISOR_SCRIPT), *supervisor_arguments],
                            "workingDir": str(BUNDLE_DIR),
                            "env": [
                                {"name": "HOME", "value": "/tmp"},
                                {"name": "KUBECONFIG", "value": str(INTERNAL_KUBECONFIG)},
                                {"name": "PYTHONPATH", "value": str(BUNDLE_DIR)},
                                {"name": "PYTHONUNBUFFERED", "value": "1"},
                                {"name": "PYTHONDONTWRITEBYTECODE", "value": "1"},
                                {
                                    "name": "KCC_RAY_SUPERVISOR_JOB_NAME",
                                    "value": name,
                                },
                            ],
                            "volumeMounts": [
                                {
                                    "name": "project",
                                    "mountPath": project,
                                    "readOnly": True,
                                },
                                {"name": "log", "mountPath": log_root},
                                {
                                    "name": "kubectl",
                                    "mountPath": INTERNAL_KUBECTL_COMMAND,
                                    "readOnly": True,
                                },
                                {
                                    "name": "incluster-kubeconfig",
                                    "mountPath": str(INTERNAL_KUBECONFIG),
                                    "subPath": "kubeconfig",
                                    "readOnly": True,
                                },
                                {"name": "tmp", "mountPath": "/tmp"},
                            ],
                        }
                    ],
                    "volumes": [
                        {
                            "name": "project",
                            "hostPath": {"path": project, "type": "Directory"},
                        },
                        {
                            "name": "log",
                            "hostPath": {"path": log_root, "type": "Directory"},
                        },
                        {
                            "name": "kubectl",
                            "hostPath": {
                                "path": str(args.supervisor_kubectl_host_path),
                                "type": "File",
                            },
                        },
                        {
                            "name": "incluster-kubeconfig",
                            "configMap": {
                                "name": f"{cluster}-incluster-kubeconfig",
                                "defaultMode": 0o444,
                            },
                        },
                        {"name": "tmp", "emptyDir": {}},
                    ],
                },
            },
        },
    }


def render_supervisor_rbac(args: argparse.Namespace) -> str:
    """Render the packaged RBAC template for the selected installation identity."""
    if not DEFAULT_RBAC_MANIFEST.is_file() or DEFAULT_RBAC_MANIFEST.is_symlink():
        raise SupervisorJobError(
            f"Supervisor RBAC template is unavailable: {DEFAULT_RBAC_MANIFEST}"
        )
    try:
        source = DEFAULT_RBAC_MANIFEST.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise SupervisorJobError(f"cannot read Supervisor RBAC template: {error}") from error
    replacements = {
        RBAC_CLUSTER_TOKEN: args.cluster,
        RBAC_NAMESPACE_TOKEN: args.namespace,
        RBAC_EXPORTER_NAMESPACE_TOKEN: args.npu_exporter_namespace,
        RBAC_SERVICE_ACCOUNT_TOKEN: args.supervisor_service_account,
    }
    rendered = source
    for token, value in replacements.items():
        rendered = rendered.replace(token, value)
    if any(token in rendered for token in replacements):
        raise SupervisorJobError("Supervisor RBAC template contains unresolved tokens")
    return rendered


def get_supervisor_job(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
) -> dict[str, Any] | None:
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    result = _run(
        [
            *kubectl,
            "get",
            "job",
            supervisor_job_name(cluster),
            "-n",
            namespace,
            "--ignore-not-found",
            "-o",
            "json",
        ]
    )
    if not result.stdout.strip():
        return None
    return _json_document(result.stdout, label="Job")


def validate_owned_job(
    job: Mapping[str, Any],
    *,
    namespace: str,
    cluster: str,
    run_id: str | None = None,
    args_sha256: str | None = None,
) -> None:
    metadata = job.get("metadata")
    if (
        job.get("apiVersion") != "batch/v1"
        or job.get("kind") != "Job"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != supervisor_job_name(cluster)
        or metadata.get("namespace") != namespace
    ):
        raise SupervisorJobError("refusing a foreign or malformed supervisor Job")
    annotations = metadata.get("annotations")
    if not isinstance(annotations, Mapping):
        raise SupervisorJobError("supervisor Job ownership annotations are missing")
    recorded_run_id = annotations.get(RUN_ID_ANNOTATION)
    recorded_digest = annotations.get(ARGS_SHA256_ANNOTATION)
    if (
        annotations.get(MANAGED_BY_ANNOTATION) != MANAGED_BY
        or annotations.get(CLUSTER_ANNOTATION) != cluster
        or not isinstance(recorded_run_id, str)
        or start_ray.RUN_ID_PATTERN.fullmatch(recorded_run_id) is None
        or not isinstance(recorded_digest, str)
        or _SHA256_PATTERN.fullmatch(recorded_digest) is None
    ):
        raise SupervisorJobError("supervisor Job ownership annotations are invalid")
    if run_id is not None and recorded_run_id != run_id:
        raise SupervisorJobError("an active supervisor Job belongs to another run ID")
    if args_sha256 is not None and recorded_digest != args_sha256:
        raise SupervisorJobError("supervisor Job arguments differ from this request")


def validate_resume_job_arguments(
    job: Mapping[str, Any],
    *,
    namespace: str,
    cluster: str,
    run_id: str,
    accepted_digests: frozenset[str],
) -> None:
    validate_owned_job(
        job,
        namespace=namespace,
        cluster=cluster,
        run_id=run_id,
    )
    annotations = job["metadata"]["annotations"]
    if annotations[ARGS_SHA256_ANNOTATION] not in accepted_digests:
        raise SupervisorJobError(
            "supervisor Job arguments differ from this request"
        )


def _job_phase(job: Mapping[str, Any]) -> str:
    status = job.get("status")
    if not isinstance(status, Mapping):
        return "Pending"
    conditions = status.get("conditions")
    if isinstance(conditions, list):
        for condition in conditions:
            if not isinstance(condition, Mapping) or condition.get("status") != "True":
                continue
            if condition.get("type") == "Complete":
                return "Complete"
            if condition.get("type") == "Failed":
                return "Failed"
    if status.get("active", 0):
        return "Running"
    if status.get("succeeded", 0):
        return "Completing"
    if status.get("failed", 0):
        return "Retrying"
    return "Pending"


def _job_summary(job: Mapping[str, Any], *, reused: bool = False) -> dict[str, Any]:
    metadata = job["metadata"]
    annotations = metadata["annotations"]
    status = job.get("status") if isinstance(job.get("status"), Mapping) else {}
    return {
        "name": metadata["name"],
        "namespace": metadata["namespace"],
        "cluster": annotations[CLUSTER_ANNOTATION],
        "runId": annotations[RUN_ID_ANNOTATION],
        "argsSha256": annotations[ARGS_SHA256_ANNOTATION],
        "phase": _job_phase(job),
        "active": int(status.get("active", 0) or 0),
        "succeeded": int(status.get("succeeded", 0) or 0),
        "failed": int(status.get("failed", 0) or 0),
        "reused": reused,
    }


def supervisor_job_status(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    run_id: str | None = None,
) -> dict[str, Any]:
    job = get_supervisor_job(
        kubectl_command=kubectl_command,
        kubeconfig=kubeconfig,
        namespace=namespace,
        cluster=cluster,
    )
    if job is None:
        return {
            "name": supervisor_job_name(cluster),
            "namespace": namespace,
            "cluster": cluster,
            "runId": run_id,
            "phase": "NotFound",
        }
    validate_owned_job(job, namespace=namespace, cluster=cluster, run_id=run_id)
    return _job_summary(job)


def supervisor_job_logs(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    run_id: str | None = None,
    tail: int | None = None,
) -> str:
    job = get_supervisor_job(
        kubectl_command=kubectl_command,
        kubeconfig=kubeconfig,
        namespace=namespace,
        cluster=cluster,
    )
    if job is None:
        raise SupervisorJobError("supervisor Job does not exist")
    validate_owned_job(job, namespace=namespace, cluster=cluster, run_id=run_id)
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    command = [
        *kubectl,
        "logs",
        f"job/{supervisor_job_name(cluster)}",
        "-n",
        namespace,
        "--all-containers=true",
    ]
    if tail is not None:
        if tail < 0:
            raise SupervisorJobError("log tail must be non-negative")
        command.append(f"--tail={tail}")
    return _run(command, timeout=300).stdout


def _raycluster_exists(
    *, kubectl: Sequence[str], namespace: str, cluster: str
) -> bool:
    result = _run(
        [
            *kubectl,
            "get",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "--ignore-not-found",
            "-o",
            "name",
        ]
    )
    return bool(result.stdout.strip())


def _namespace_exists(*, kubectl: Sequence[str], namespace: str) -> bool:
    result = _run(
        [
            *kubectl,
            "get",
            "namespace",
            namespace,
            "--ignore-not-found",
            "-o",
            "name",
        ]
    )
    return bool(result.stdout.strip())


def _get_raycluster(
    *, kubectl: Sequence[str], namespace: str, cluster: str
) -> dict[str, Any] | None:
    result = _run(
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
    return _json_document(result.stdout, label="RayCluster")


def _validate_orphan_raycluster_resume(
    args: argparse.Namespace,
    raycluster: Mapping[str, Any],
) -> None:
    """Prove that an orphan RayCluster is the current recorded attempt."""
    if not args.resume:
        raise SupervisorJobError(
            "a RayCluster already exists without an owned resumable Supervisor Job"
        )

    state_path = args.recovery_state_root / args.run_id / "state.json"
    try:
        state = recovery_supervisor.load_recovery_state(
            state_path,
            expected_job_id=args.run_id,
        )
        if state.get("status") != "RUNNING":
            raise recovery_supervisor.RecoveryError(
                "an orphan RayCluster requires RUNNING recovery state"
            )
        initial_active_nodes = tuple(args.node)
        initial_spare_nodes = tuple(args.spare_node)
        recovery_supervisor.validate_node_pool(
            initial_active_nodes,
            initial_spare_nodes,
        )
        max_replacements = (
            len(initial_spare_nodes)
            if args.max_recoveries is None
            else args.max_recoveries
        )
        if not 0 <= max_replacements <= len(initial_spare_nodes):
            raise recovery_supervisor.RecoveryError(
                "max recoveries differ from the resumable topology"
            )
        recovery_policy = recovery_supervisor.apply_frozen_recovery_policy(
            args, state
        )
        _, _, _, _, attempt, resume_current_attempt = (
            recovery_supervisor.validate_resumable_state(
                state,
                job_id=args.run_id,
                initial_active_nodes=initial_active_nodes,
                initial_spare_nodes=initial_spare_nodes,
                max_replacements=max_replacements,
                same_topology_retries=recovery_policy[
                    "sameTopologyRetries"
                ],
                training_artifact_root=args.training_artifact_root,
            )
        )
    except recovery_supervisor.RecoveryError as error:
        raise SupervisorJobError(
            f"cannot prove orphan RayCluster recovery ownership: {error}"
        ) from error

    attempts = state["attempts"]
    latest = attempts[-1]
    expected_attempt = recovery_supervisor.attempt_run_id(args.run_id, attempt)
    if (
        not resume_current_attempt
        or latest.get("status") != "RUNNING"
        or latest.get("runId") != expected_attempt
    ):
        raise SupervisorJobError(
            "orphan RayCluster is not the current RUNNING recovery attempt"
        )

    metadata = raycluster.get("metadata")
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    uid = metadata.get("uid") if isinstance(metadata, Mapping) else None
    if (
        raycluster.get("apiVersion") != "ray.io/v1"
        or raycluster.get("kind") != "RayCluster"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != args.cluster
        or metadata.get("namespace") != args.namespace
        or not isinstance(uid, str)
        or not uid
        or not isinstance(annotations, Mapping)
        or annotations.get("trainctl.io/run-id") != expected_attempt
        or annotations.get("trainctl.io/allowed-nodes")
        != ",".join(latest["activeNodes"])
    ):
        raise SupervisorJobError(
            "orphan RayCluster ownership differs from the current recovery attempt"
        )


def _delete_owned_job(
    job: Mapping[str, Any],
    *,
    kubectl: Sequence[str],
    namespace: str,
    cluster: str,
) -> None:
    validate_owned_job(job, namespace=namespace, cluster=cluster)
    metadata = job["metadata"]
    uid = metadata.get("uid")
    current_result = _run(
        [
            *kubectl,
            "get",
            "job",
            supervisor_job_name(cluster),
            "-n",
            namespace,
            "-o",
            "json",
        ]
    )
    current = _json_document(current_result.stdout, label="Job")
    validate_owned_job(current, namespace=namespace, cluster=cluster)
    if uid is not None and current.get("metadata", {}).get("uid") != uid:
        raise SupervisorJobError("supervisor Job changed before deletion; refusing")
    _run(
        [
            *kubectl,
            "delete",
            "job",
            supervisor_job_name(cluster),
            "-n",
            namespace,
            "--wait=true",
        ],
        timeout=180,
    )


def start(argv: Sequence[str]) -> dict[str, Any]:
    parser, args = parse_start_arguments(argv)
    supervisor_arguments = build_supervisor_arguments(parser, args)
    digest = arguments_sha256(supervisor_arguments)
    resume_digests = (
        compatible_resume_argument_digests(parser, args)
        if args.resume
        else frozenset()
    )
    manifest = build_job_manifest(
        args=args,
        supervisor_arguments=supervisor_arguments,
        args_sha256=digest,
    )
    kubectl = kubectl_prefix(args.kubectl_command, args.kubeconfig)

    namespace_exists = _namespace_exists(
        kubectl=kubectl, namespace=args.namespace
    )
    existing = (
        get_supervisor_job(
            kubectl_command=args.kubectl_command,
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            cluster=args.cluster,
        )
        if namespace_exists
        else None
    )
    if existing is not None:
        validate_owned_job(existing, namespace=args.namespace, cluster=args.cluster)
        phase = _job_phase(existing)
        annotations = existing["metadata"]["annotations"]
        same_run = annotations[RUN_ID_ANNOTATION] == args.run_id
        if phase not in {"Complete", "Failed"}:
            if not args.resume:
                raise SupervisorJobError(
                    "a supervisor Job is already active; use --resume to reattach"
                )
            validate_resume_job_arguments(
                existing,
                namespace=args.namespace,
                cluster=args.cluster,
                run_id=args.run_id,
                accepted_digests=resume_digests,
            )
            return _job_summary(existing, reused=True)

        if same_run:
            if not args.resume:
                raise SupervisorJobError(
                    "the terminal supervisor Job belongs to this run; use --resume"
                )
            validate_resume_job_arguments(
                existing,
                namespace=args.namespace,
                cluster=args.cluster,
                run_id=args.run_id,
                accepted_digests=resume_digests,
            )
        elif _raycluster_exists(
            kubectl=kubectl, namespace=args.namespace, cluster=args.cluster
        ):
            raise SupervisorJobError(
                "the previous supervisor Job is terminal but its RayCluster still exists"
            )
        _delete_owned_job(
            existing,
            kubectl=kubectl,
            namespace=args.namespace,
            cluster=args.cluster,
        )
    elif namespace_exists:
        orphan_raycluster = _get_raycluster(
            kubectl=kubectl,
            namespace=args.namespace,
            cluster=args.cluster,
        )
        if orphan_raycluster is not None:
            _validate_orphan_raycluster_resume(args, orphan_raycluster)

    _run(
        [*kubectl, "apply", "-f", "-"],
        input_text=render_supervisor_rbac(args),
        timeout=120,
    )
    result = _run(
        [*kubectl, "create", "-f", "-", "-o", "json"],
        input_text=json.dumps(manifest, ensure_ascii=False, separators=(",", ":")),
        timeout=120,
    )
    created = _json_document(result.stdout, label="Job")
    validate_owned_job(
        created,
        namespace=args.namespace,
        cluster=args.cluster,
        run_id=args.run_id,
        args_sha256=digest,
    )
    return _job_summary(created)


def main(argv: Sequence[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    if not arguments or arguments[0] != "start":
        print("usage: supervisor_job.py start [START_OPTIONS...]", file=sys.stderr)
        return 2
    try:
        summary = start(arguments[1:])
    except (SupervisorJobError, ValueError) as error:
        print(f"STOP: supervisor Job launch refused: {error}", file=sys.stderr)
        return 2
    action = "reused" if summary.get("reused") else "created"
    print(
        f"PASS: Supervisor Job {summary['namespace']}/{summary['name']} "
        f"{action} for run {summary['runId']}."
    )
    print(f"Status: kcc_ray status --run-id {summary['runId']}")
    print(
        "Supervisor logs: "
        f"kcc_ray logs --run-id {summary['runId']} --supervisor"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
