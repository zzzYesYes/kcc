#!/usr/bin/env python3
"""Run environment, Ray, HCCL, parameter injection, and formal training."""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time
from typing import Callable, Sequence

import cluster_config
import training_control


BUNDLE_DIR = Path(__file__).resolve().parent
DEFAULT_MANIFEST = BUNDLE_DIR / "raycluster.yaml"
DEFAULT_RUNTIME_SOURCE = BUNDLE_DIR / "hccl_runtime"
DEFAULT_LOG_ROOT = BUNDLE_DIR.parent / "log"
DEFAULT_EVIDENCE_ROOT = DEFAULT_LOG_ROOT / "hccl-startup"
DEFAULT_TRAINING_ARTIFACT_ROOT = (
    DEFAULT_LOG_ROOT / "training-runs"
)
DEFAULT_WORKER_ARCHIVE_ROOT = "/mnt/models/pretrain-ray-platform/archive"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
Stage = tuple[str, Sequence[str]]


def new_run_id() -> str:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"train-{timestamp}-{secrets.token_hex(4)}"


def selector_argument(value: str) -> str:
    key, separator, label_value = value.partition("=")
    if (
        not separator
        or not key
        or not label_value
        or any(character.isspace() for character in key + label_value)
    ):
        raise argparse.ArgumentTypeError("selector must be KEY=VALUE without whitespace")
    return value


def selected_worker_nodes(args: argparse.Namespace) -> tuple[str, ...]:
    explicit = tuple(args.node or ())
    all_nodes = bool(getattr(args, "all_nodes", False))
    if all_nodes and explicit and not getattr(args, "_nodes_from_config", False):
        raise ValueError("--all-nodes cannot be combined with explicit --node values")
    if explicit:
        return explicit
    defaults = cluster_config.load_cluster_config()
    return defaults.all_nodes if all_nodes else defaults.active_nodes


def apply_config_defaults(
    args: argparse.Namespace,
    config: cluster_config.ClusterConfig | None = None,
) -> cluster_config.ClusterConfig | None:
    """Materialize YAML defaults once; explicit CLI values always win."""

    configurable = (
        "kubectl_command",
        "kubeconfig",
        "namespace",
        "cluster",
        "runtime_configmap",
        "head_node",
        "timeout_seconds",
        "hccl_timeout_seconds",
        "failure_retention_seconds",
        "train_script",
        "training_cwd",
        "workspace_host_path",
        "ray_head_image",
        "ray_worker_image",
        "ray_image_pull_policy",
        "training_timeout_seconds",
        "no_progress_seconds",
        "npu_resource",
        "devices_per_node",
        "runtime_class_name",
        "head_selector",
        "worker_selector",
        "npu_exporter_namespace",
        "npu_exporter_app",
        "npu_exporter_port",
        "supervisor_node",
        "supervisor_service_account",
        "supervisor_image",
        "supervisor_image_pull_policy",
        "supervisor_kubectl_host_path",
        "supervisor_backoff_limit",
        "supervisor_finished_ttl_seconds",
    )
    needs_config = getattr(args, "node", None) is None or any(
        getattr(args, destination, None) is None for destination in configurable
    )
    if not needs_config:
        return config
    defaults = config or cluster_config.load_cluster_config()

    def fill(destination: str, value: object) -> None:
        if getattr(args, destination, None) is None:
            setattr(args, destination, value)

    fill("kubectl_command", defaults.kubernetes.kubectl_command)
    fill("kubeconfig", defaults.kubernetes.kubeconfig)
    fill("namespace", defaults.kubernetes.namespace)
    fill("cluster", defaults.kubernetes.cluster_name)
    fill("head_node", defaults.topology.head_node)
    fill("timeout_seconds", defaults.timeouts.ray_startup_seconds)
    fill("hccl_timeout_seconds", defaults.timeouts.hccl_gate_seconds)
    fill(
        "failure_retention_seconds",
        defaults.timeouts.failed_resource_retention_seconds,
    )
    fill("train_script", defaults.training.template)
    fill("training_cwd", defaults.training.working_directory)
    fill("workspace_host_path", defaults.training.workspace_host_path)
    fill("ray_head_image", defaults.images.ray_head)
    fill("ray_worker_image", defaults.images.ray_worker)
    fill("ray_image_pull_policy", defaults.images.pull_policy)
    fill("training_timeout_seconds", defaults.timeouts.training_seconds)
    fill("no_progress_seconds", defaults.recovery.no_progress_seconds)
    fill("npu_resource", defaults.accelerator.resource_name)
    fill("devices_per_node", defaults.accelerator.devices_per_node)
    fill("runtime_class_name", defaults.accelerator.runtime_class_name)
    fill(
        "head_selector",
        [f"{key}={value}" for key, value in defaults.topology.head_selector.items()],
    )
    fill(
        "worker_selector",
        [f"{key}={value}" for key, value in defaults.topology.worker_selector.items()],
    )
    fill("npu_exporter_namespace", defaults.npu_check.exporter_namespace)
    fill("npu_exporter_app", defaults.npu_check.exporter_app)
    fill("npu_exporter_port", defaults.npu_check.exporter_port)
    fill("supervisor_node", defaults.supervisor.node)
    fill("supervisor_service_account", defaults.supervisor.service_account)
    fill("supervisor_image", defaults.supervisor.image)
    fill("supervisor_image_pull_policy", defaults.supervisor.image_pull_policy)
    fill("supervisor_kubectl_host_path", defaults.supervisor.kubectl_host_path)
    fill("supervisor_backoff_limit", defaults.supervisor.backoff_limit)
    fill(
        "supervisor_finished_ttl_seconds",
        defaults.supervisor.finished_ttl_seconds,
    )
    if getattr(args, "node", None) is None:
        args.node = list(
            defaults.all_nodes
            if bool(getattr(args, "all_nodes", False))
            else defaults.active_nodes
        )
        args._nodes_from_config = True
        args.allow_topology_change = True
    if getattr(args, "runtime_configmap", None) is None:
        args.runtime_configmap = f"{args.cluster}-hccl-runtime"
    return defaults


def execute_pipeline(
    stages: Sequence[Stage],
    *,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
    failure_handler: Callable[[int, str], None] | None = None,
    before_stage: Callable[[int, str], bool] | None = None,
) -> int:
    for index, (name, command) in enumerate(stages, start=1):
        if before_stage is not None and not before_stage(index, name):
            print(
                f"STOP: stage {index} was not started because a stop request was accepted.",
                file=sys.stderr,
                flush=True,
            )
            return 130
        print(f"=== Stage {index}/{len(stages)}: {name} ===", flush=True)
        print("$ " + shlex.join(command), flush=True)
        result = runner(list(command), check=False, shell=False)
        if result.returncode != 0:
            if failure_handler is not None:
                try:
                    failure_handler(index, name)
                except KeyboardInterrupt:
                    raise
                except Exception as error:
                    print(
                        f"WARNING: failure cleanup did not complete: {error}",
                        file=sys.stderr,
                    )
            print(
                f"STOP: stage {index} failed ({name}); later stages were not invoked.",
                file=sys.stderr,
            )
            return result.returncode or 1
    print(
        "PASS: formal training workflow completed successfully.",
        flush=True,
    )
    return 0


def source_declared_workers(path: Path) -> int:
    if not path.is_file() or path.is_symlink():
        raise ValueError(f"training script is not a regular file: {path}")
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read training script: {error}") from error
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?NNODES=([0-9]+)[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1 or int(matches[0]) <= 0:
        raise ValueError("training script must have one positive literal NNODES")
    return int(matches[0])


def source_checkpoint_save_dir(path: Path) -> str | None:
    try:
        source = path.read_text(encoding="utf-8")
    except (OSError, UnicodeError) as error:
        raise ValueError(f"cannot read training script: {error}") from error
    if re.search(
        r"^[ \t]*--save(?:[ \t=]|$)",
        source,
        flags=re.MULTILINE,
    ) is None:
        return None
    matches = re.findall(
        r"^[ \t]*(?:export[ \t]+)?CKPT_SAVE_DIR="
        r'(?:\"([^\"]+)\"|\'([^\']+)\'|([^ \t#]+))'
        r"[ \t]*(?:#.*)?$",
        source,
        flags=re.MULTILINE,
    )
    if len(matches) != 1:
        raise ValueError(
            "active --save requires one literal CKPT_SAVE_DIR assignment"
        )
    save_dir = next(part for part in matches[0] if part)
    if not save_dir.startswith("/"):
        raise ValueError("CKPT_SAVE_DIR must be an absolute path")
    return save_dir


def retain_then_delete_cluster(
    args: argparse.Namespace,
) -> None:
    retention_seconds = args.failure_retention_seconds
    if retention_seconds < 0:
        print("FAILED RESOURCE RETENTION: RayCluster is retained indefinitely.")
        return
    print(
        "FAILED RESOURCE RETENTION: parameter injection failed; "
        f"keeping RayCluster for {retention_seconds} seconds."
    )
    deadline = time.monotonic() + retention_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(60.0, remaining))
    kubectl = shlex.split(args.kubectl_command)
    if not kubectl:
        raise RuntimeError("kubectl command is empty")
    kubectl.extend(("--kubeconfig", str(args.kubeconfig)))
    command = [
        *kubectl,
        "delete",
        "raycluster",
        args.cluster,
        "-n",
        args.namespace,
        "--ignore-not-found=true",
        "--wait=false",
    ]
    print("$ " + shlex.join(command), flush=True)
    result = subprocess.run(
        command,
        check=False,
        shell=False,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError("RayCluster cleanup command failed")


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="kcc_ray start",
        description=__doc__,
    )
    parser.add_argument(
        "--node",
        action="append",
        help=(
            "target node name or InternalIP; repeat as needed "
            "(defaults to config/cluster.yaml activeNodes)"
        ),
    )
    parser.add_argument(
        "--all-nodes",
        action="store_true",
        help="use activeNodes and spareNodes together without a standby pool",
    )
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument(
        "--runtime-source-dir",
        type=Path,
        default=DEFAULT_RUNTIME_SOURCE,
    )
    parser.add_argument(
        "--runtime-configmap",
        help="runtime ConfigMap name; defaults to <cluster>-hccl-runtime",
    )
    parser.add_argument(
        "--kubectl-command",
        help="kubectl command prefix; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--kubeconfig",
        type=Path,
        help="Kubernetes kubeconfig; defaults to config/cluster.yaml",
    )
    parser.add_argument("--namespace", help="defaults to config/cluster.yaml")
    parser.add_argument("--cluster", help="defaults to config/cluster.yaml")
    parser.add_argument(
        "--head-node",
        help="Ray head Kubernetes node; defaults to config/cluster.yaml",
    )
    parser.add_argument(
        "--expected-workers",
        type=int,
        help="optional assertion; normally derived from the repeated --node values",
    )
    parser.add_argument("--timeout-seconds", type=int)
    parser.add_argument("--hccl-timeout-seconds", type=int)
    parser.add_argument(
        "--expected-world-size",
        type=int,
        help="optional strict rank count; omitted uses HCCL topology discovery",
    )
    parser.add_argument(
        "--hccl-evidence-root",
        type=Path,
        default=DEFAULT_EVIDENCE_ROOT,
    )
    parser.add_argument(
        "--failure-retention-seconds",
        type=int,
        help=(
            "failed Ray/HCCL startup retention; -1 keeps resources indefinitely; "
            "defaults to config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--run-id",
        help="caller-owned run ID; omitted generates a unique ID",
    )
    parser.add_argument(
        "--train-script",
        type=Path,
        help=(
            "select a formal training template; defaults to the template selected "
            "by config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--training-cwd",
        help=(
            "absolute training source directory inside every Ray worker; "
            "defaults to config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--workspace-host-path",
        type=Path,
        help=(
            "worker-node host directory mounted at /mnt/models; "
            "defaults to training.workspaceHostPath"
        ),
    )
    parser.add_argument(
        "--ray-head-image",
        help="single-run override for images.rayHead",
    )
    parser.add_argument(
        "--ray-worker-image",
        help="single-run override for images.rayWorker",
    )
    parser.add_argument(
        "--ray-image-pull-policy",
        choices=("Always", "IfNotPresent", "Never"),
        help="single-run override for images.pullPolicy",
    )
    parser.add_argument(
        "--training-artifact-root",
        type=Path,
        default=DEFAULT_TRAINING_ARTIFACT_ROOT,
    )
    parser.add_argument(
        "--master-port",
        type=int,
        help="optional override; omitted preserves the source script value",
    )
    parser.add_argument(
        "--allow-topology-change",
        action="store_true",
        help="explicitly approve discovered NNODES/NPUS differing from the source",
    )
    parser.add_argument(
        "--confirm-checkpoint-exclusive",
        action="store_true",
        help=argparse.SUPPRESS,
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help=(
            "start from iteration zero and create a new worker archive for "
            "checkpoints and logs"
        ),
    )
    parser.add_argument(
        "--training-timeout-seconds",
        type=int,
        help=(
            "per-worker formal training timeout; 0 means no timeout; "
            "defaults to config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--no-progress-seconds",
        type=int,
        help=(
            "rank-zero training/checkpoint inactivity timeout; 0 disables it; "
            "defaults to config/cluster.yaml"
        ),
    )
    parser.add_argument(
        "--npu-resource", help="single-run override for accelerator.resourceName"
    )
    parser.add_argument(
        "--devices-per-node",
        type=int,
        help="single-run override for accelerator.devicesPerNode",
    )
    parser.add_argument(
        "--runtime-class-name",
        help="single-run override for accelerator.runtimeClassName",
    )
    parser.add_argument(
        "--head-selector",
        action="append",
        type=selector_argument,
        help="replace config head selectors with KEY=VALUE; repeat as needed",
    )
    parser.add_argument(
        "--worker-selector",
        action="append",
        type=selector_argument,
        help="replace config worker selectors with KEY=VALUE; repeat as needed",
    )
    parser.add_argument(
        "--npu-exporter-namespace",
        help="single-run override for npuCheck.exporterNamespace",
    )
    parser.add_argument(
        "--npu-exporter-app", help="single-run override for npuCheck.exporterApp"
    )
    parser.add_argument(
        "--npu-exporter-port",
        type=int,
        help="single-run override for npuCheck.exporterPort",
    )
    parser.add_argument(
        "--supervisor-node", help="single-run override for supervisor.node"
    )
    parser.add_argument(
        "--supervisor-service-account",
        help="single-run override for supervisor.serviceAccount",
    )
    parser.add_argument(
        "--supervisor-image", help="single-run digest-pinned Supervisor image"
    )
    parser.add_argument(
        "--supervisor-image-pull-policy",
        choices=("Always", "IfNotPresent", "Never"),
        help="single-run override for supervisor.imagePullPolicy",
    )
    parser.add_argument(
        "--supervisor-kubectl-host-path",
        type=Path,
        help="single-run override for supervisor.kubectlHostPath",
    )
    parser.add_argument(
        "--supervisor-backoff-limit",
        type=int,
        help="single-run override for supervisor.backoffLimit",
    )
    parser.add_argument(
        "--supervisor-finished-ttl-seconds",
        type=int,
        help="single-run override for supervisor.finishedTtlSeconds",
    )
    parser.add_argument(
        "--keep-success-resources",
        action="store_true",
        help="leave the RayCluster allocated after successful training",
    )
    return parser


def build_stage_commands(
    args: argparse.Namespace,
    *,
    run_id: str,
) -> tuple[Stage, ...]:
    apply_config_defaults(args)
    fresh_start = bool(getattr(args, "fresh", False))
    nodes = selected_worker_nodes(args)
    worker_count = len(nodes)
    script_dir = Path(__file__).resolve().parent
    hccl_run_dir = args.hccl_evidence_root.resolve() / run_id
    training_run_dir = args.training_artifact_root.resolve() / run_id
    injection_dir = training_run_dir / "injection"
    result_path = training_run_dir / "execution-result.json"
    rendered_manifest = training_run_dir / "raycluster.yaml"

    preflight_command = [
        sys.executable,
        str(script_dir / "environment_check.py"),
    ]
    for node in nodes:
        preflight_command.extend(("--node", node))
    preflight_command.extend(
        (
            "--kubectl-command",
            args.kubectl_command,
            "--kubeconfig",
            str(args.kubeconfig),
            "--npu-resource",
            args.npu_resource,
            "--expected-devices-per-node",
            str(args.devices_per_node),
            "--npu-exporter-app",
            args.npu_exporter_app,
            "--npu-exporter-port",
            str(args.npu_exporter_port),
        )
    )

    render_command = [
        sys.executable,
        str(script_dir / "render_raycluster.py"),
        "--base-manifest",
        str(args.manifest.resolve()),
        "--output-manifest",
        str(rendered_manifest),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--head-node",
        args.head_node,
        "--npu-resource",
        args.npu_resource,
        "--devices-per-node",
        str(args.devices_per_node),
        "--workspace-host-path",
        str(args.workspace_host_path),
        "--ray-head-image",
        args.ray_head_image,
        "--ray-worker-image",
        args.ray_worker_image,
        "--ray-image-pull-policy",
        args.ray_image_pull_policy,
        "--runtime-configmap",
        args.runtime_configmap,
        "--run-id",
        run_id,
    ]
    if args.runtime_class_name is not None:
        render_command.extend(("--runtime-class-name", args.runtime_class_name))
    for selector in args.head_selector:
        render_command.extend(("--head-selector", selector))
    for selector in args.worker_selector:
        render_command.extend(("--worker-selector", selector))
    for node in nodes:
        render_command.extend(("--node", node))

    launch_command = [
        sys.executable,
        str(script_dir / "ray_cluster_start.py"),
        "--manifest",
        str(rendered_manifest),
        "--runtime-source-dir",
        str(args.runtime_source_dir.resolve()),
        "--runtime-configmap",
        args.runtime_configmap,
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--expected-workers",
        str(worker_count),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
        "--run-id",
        run_id,
        "--stop-request",
        str(training_run_dir / training_control.STOP_REQUEST_FILENAME),
    ]
    hccl_command = [
        sys.executable,
        str(script_dir / "hccl_gate.py"),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--expected-workers",
        str(worker_count),
        "--timeout-seconds",
        str(args.hccl_timeout_seconds),
        "--evidence-root",
        str(args.hccl_evidence_root.resolve()),
        "--run-id",
        run_id,
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
    ]
    if args.expected_world_size is not None:
        hccl_command.extend(
            ("--expected-world-size", str(args.expected_world_size))
        )

    injection_command = [
        sys.executable,
        str(script_dir / "inject_training_params.py"),
        "--source",
        str(args.train_script.resolve()),
        "--hccl-evidence",
        str(hccl_run_dir / "03-hccl.json"),
        "--hccl-ping-evidence",
        str(hccl_run_dir / "01-ping.json"),
        "--output-dir",
        str(injection_dir),
        "--run-id",
        run_id,
        "--training-cwd",
        args.training_cwd,
    ]
    if args.master_port is not None:
        injection_command.extend(("--master-port", str(args.master_port)))
    if args.allow_topology_change:
        injection_command.append("--allow-topology-change")
    if getattr(args, "require_resumable_checkpoint", False):
        injection_command.append("--require-resumable-checkpoint")
    if fresh_start:
        injection_command.append("--fresh")

    training_command = [
        sys.executable,
        str(script_dir / "ray_training_submit.py"),
        "--injection-dir",
        str(injection_dir),
        "--kubectl-command",
        args.kubectl_command,
        "--kubeconfig",
        str(args.kubeconfig),
        "--namespace",
        args.namespace,
        "--cluster",
        args.cluster,
        "--result",
        str(result_path),
        "--timeout-seconds",
        str(args.training_timeout_seconds),
        "--no-progress-seconds",
        str(args.no_progress_seconds),
        "--failure-retention-seconds",
        str(args.failure_retention_seconds),
    ]
    if args.keep_success_resources:
        training_command.append("--keep-success-resources")
    return (
        ("environment check", preflight_command),
        ("per-run RayCluster rendering", render_command),
        ("Ray cluster startup", launch_command),
        ("topology, RankTable, and HCCL gate", hccl_command),
        ("formal training parameter injection", injection_command),
        ("formal Ray training", training_command),
    )


def validate_args(args: argparse.Namespace, run_id: str) -> None:
    apply_config_defaults(args)
    fresh_start = bool(getattr(args, "fresh", False))
    if RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise ValueError("run ID contains unsupported characters")
    cluster_config.validate_dns_label(args.namespace, "namespace")
    cluster_config.validate_dns_label(args.cluster, "cluster")
    cluster_config.validate_dns_label(args.runtime_configmap, "runtime ConfigMap")
    cluster_config.validate_dns_label(
        args.npu_exporter_namespace, "NPU exporter namespace"
    )
    cluster_config.validate_dns_label(
        args.supervisor_service_account, "supervisor service account"
    )
    cluster_config.validate_pinned_image(args.supervisor_image, "supervisor image")
    cluster_config.validate_profile_image(args.ray_head_image, "Ray head image")
    cluster_config.validate_profile_image(
        args.ray_worker_image,
        "Ray worker image",
    )
    cluster_config.validate_image_pull_policy(
        args.ray_image_pull_policy,
        "Ray image pull policy",
    )
    for label, value in (
        ("head node", args.head_node),
        ("supervisor node", args.supervisor_node),
        ("NPU resource", args.npu_resource),
        ("NPU exporter app", args.npu_exporter_app),
    ):
        if not value or any(character.isspace() for character in value):
            raise ValueError(f"{label} must be non-empty and whitespace-free")
    if not 1 <= args.devices_per_node <= 64:
        raise ValueError("devices per node must be within 1..64")
    if args.runtime_class_name is not None and (
        not args.runtime_class_name
        or any(character.isspace() for character in args.runtime_class_name)
    ):
        raise ValueError("runtime class name must be non-empty and whitespace-free")
    for label, selectors in (
        ("head selectors", args.head_selector),
        ("worker selectors", args.worker_selector),
    ):
        keys = [selector.partition("=")[0] for selector in selectors]
        if len(keys) != len(set(keys)):
            raise ValueError(f"{label} contain duplicate keys")
    if (
        args.timeout_seconds <= 0
        or args.hccl_timeout_seconds <= 0
    ):
        raise ValueError("workers and startup/HCCL timeouts must be positive")
    nodes = selected_worker_nodes(args)
    if len(set(nodes)) != len(nodes):
        raise ValueError("worker node targets must be unique")
    if args.expected_workers is not None and args.expected_workers != len(nodes):
        raise ValueError(
            "expected workers differs from the number of selected --node targets"
        )
    source_workers = source_declared_workers(args.train_script.resolve())
    if source_workers != len(nodes) and not args.allow_topology_change:
        raise ValueError(
            "selected worker count differs from source NNODES "
            f"({source_workers} -> {len(nodes)}); "
            "--allow-topology-change is required before any cluster is started"
        )
    checkpoint_save_dir = source_checkpoint_save_dir(
        args.train_script.resolve()
    )
    if fresh_start and checkpoint_save_dir is None:
        raise ValueError(
            "fresh start requires the training script to enable --save"
        )
    if fresh_start and getattr(args, "require_resumable_checkpoint", False):
        raise ValueError(
            "fresh start cannot require an existing resumable checkpoint"
        )
    if (
        args.expected_world_size is not None
        and args.expected_world_size <= 0
    ):
        raise ValueError("expected world size must be positive")
    if args.failure_retention_seconds < -1:
        raise ValueError("failure retention must be -1 or non-negative")
    if args.training_timeout_seconds < 0:
        raise ValueError("training timeout must be zero or positive")
    if args.no_progress_seconds < 0:
        raise ValueError("no-progress timeout must be zero or positive")
    if not 1 <= args.npu_exporter_port <= 65535:
        raise ValueError("NPU exporter port must be within 1..65535")
    if args.supervisor_backoff_limit < 0:
        raise ValueError("Supervisor backoff limit must be non-negative")
    if args.supervisor_finished_ttl_seconds <= 0:
        raise ValueError("Supervisor finished TTL must be positive")
    if not args.supervisor_kubectl_host_path.is_absolute():
        raise ValueError("Supervisor kubectl host path must be absolute")
    if args.master_port is not None and not 1024 <= args.master_port <= 65535:
        raise ValueError("master port must be within 1024..65535")
    if not args.training_cwd.startswith("/"):
        raise ValueError("training cwd must be an absolute worker path")
    if not args.workspace_host_path.is_absolute():
        raise ValueError("workspace host path must be absolute")


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    run_id = args.run_id or new_run_id()
    try:
        apply_config_defaults(args)
        validate_args(args, run_id)
        stages = build_stage_commands(args, run_id=run_id)
    except ValueError as error:
        print(f"STOP: invalid workflow arguments: {error}", file=sys.stderr)
        return 2
    print(f"Run ID: {run_id}", flush=True)
    if bool(getattr(args, "fresh", False)):
        print(
            "Fresh archive: "
            f"{DEFAULT_WORKER_ARCHIVE_ROOT}/{run_id}",
            flush=True,
        )
    print(
        "Training result: "
        f"{args.training_artifact_root.resolve() / run_id / 'execution-result.json'}",
        flush=True,
    )
    try:
        return execute_pipeline(
            stages,
            failure_handler=(
                lambda _index, name: retain_then_delete_cluster(args)
                if name == "formal training parameter injection"
                else None
            ),
        )
    except KeyboardInterrupt:
        print(
            "STOP: interrupted; existing Ray resources and checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
