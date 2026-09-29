#!/usr/bin/env python3
"""Load the distribution-wide defaults used by the public ``kcc_ray`` CLI."""

from __future__ import annotations

from dataclasses import dataclass
import os
from pathlib import Path
import re
import shlex
import stat
from typing import Any, Mapping

import yaml


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "config" / "cluster.yaml"
CONFIG_ENVIRONMENT_VARIABLE = "KCC_RAY_CONFIG"
CONFIG_SCHEMA = "kcc-ray-config/v2"
PREVIOUS_CONFIG_SCHEMA = "kcc-ray-config/v1"
LEGACY_CONFIG_SCHEMA = "kcc-ray-cluster/v1"
CONFIG_MAX_BYTES = 64 * 1024
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_PINNED_IMAGE = re.compile(r"^\S+@sha256:[0-9a-f]{64}$")
_IMAGE_REFERENCE_CHARACTERS = re.compile(r"^[A-Za-z0-9._:/@-]+$")
_IMAGE_TAG = re.compile(r"^[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}$")

DEFAULT_RAY_HEAD_IMAGE = (
    "110.120.0.3:8889/pretrain/ray-head@sha256:"
    "121fff1a4b0f991121ba7dc85cbb7a77643d28c79cc355ba3f1536abb51c865b"
)
DEFAULT_RAY_WORKER_IMAGE = (
    "110.120.0.3:8889/ascendhub/verl_pt27_25rc3@sha256:"
    "0c263b4d1989bf41a38f0fe560c60b97b0f4e670f1a4378319dc98117ac4113c"
)
DEFAULT_RAY_IMAGE_PULL_POLICY = "IfNotPresent"


class ClusterConfigError(ValueError):
    """The selected operator configuration is missing, malformed, or unsafe."""


@dataclass(frozen=True)
class KubernetesDefaults:
    kubectl_command: str
    kubeconfig: Path
    namespace: str
    cluster_name: str


@dataclass(frozen=True)
class TopologyDefaults:
    head_node: str
    active_nodes: tuple[str, ...]
    spare_nodes: tuple[str, ...]
    head_selector: Mapping[str, str]
    worker_selector: Mapping[str, str]

    @property
    def all_nodes(self) -> tuple[str, ...]:
        return (*self.active_nodes, *self.spare_nodes)


@dataclass(frozen=True)
class NpuCheckDefaults:
    exporter_namespace: str
    exporter_app: str
    exporter_port: int


@dataclass(frozen=True)
class AcceleratorDefaults:
    resource_name: str
    devices_per_node: int
    runtime_class_name: str | None


@dataclass(frozen=True)
class TrainingDefaults:
    template: Path
    working_directory: str
    workspace_host_path: Path


@dataclass(frozen=True)
class ImageDefaults:
    ray_head: str
    ray_worker: str
    pull_policy: str


@dataclass(frozen=True)
class SupervisorDefaults:
    node: str
    service_account: str
    image: str
    image_pull_policy: str
    kubectl_host_path: Path
    backoff_limit: int
    finished_ttl_seconds: int


@dataclass(frozen=True)
class TimeoutDefaults:
    ray_startup_seconds: int
    hccl_gate_seconds: int
    training_seconds: int
    failed_resource_retention_seconds: int
    recovery_cleanup_seconds: int


@dataclass(frozen=True)
class RecoveryDefaults:
    same_topology_retries: int
    retry_backoff_seconds: int
    no_progress_seconds: int
    diagnosis_window_seconds: int
    diagnosis_poll_seconds: int
    diagnosis_stable_samples: int


@dataclass(frozen=True)
class ClusterConfig:
    path: Path
    kubernetes: KubernetesDefaults
    topology: TopologyDefaults
    accelerator: AcceleratorDefaults
    npu_check: NpuCheckDefaults
    training: TrainingDefaults
    images: ImageDefaults
    supervisor: SupervisorDefaults
    timeouts: TimeoutDefaults
    recovery: RecoveryDefaults

    # Compatibility accessors for the first, topology-only configuration.
    @property
    def active_nodes(self) -> tuple[str, ...]:
        return self.topology.active_nodes

    @property
    def spare_nodes(self) -> tuple[str, ...]:
        return self.topology.spare_nodes

    @property
    def all_nodes(self) -> tuple[str, ...]:
        return self.topology.all_nodes


def selected_config_path() -> Path:
    configured = os.environ.get(CONFIG_ENVIRONMENT_VARIABLE)
    return Path(configured).expanduser() if configured else DEFAULT_CONFIG_PATH


def _fail_unknown(
    document: Mapping[str, Any],
    expected: set[str],
    label: str,
    *,
    optional: set[str] | None = None,
) -> None:
    unknown = set(document) - expected - (optional or set())
    missing = expected - set(document)
    if unknown:
        raise ClusterConfigError(
            f"{label} contains unsupported keys: "
            + ", ".join(sorted(str(key) for key in unknown))
        )
    if missing:
        raise ClusterConfigError(
            f"{label} is missing required keys: "
            + ", ".join(sorted(str(key) for key in missing))
        )


def _default_recovery() -> RecoveryDefaults:
    return RecoveryDefaults(
        same_topology_retries=2,
        retry_backoff_seconds=60,
        no_progress_seconds=3600,
        diagnosis_window_seconds=300,
        diagnosis_poll_seconds=30,
        diagnosis_stable_samples=2,
    )


def _default_images() -> ImageDefaults:
    return ImageDefaults(
        ray_head=DEFAULT_RAY_HEAD_IMAGE,
        ray_worker=DEFAULT_RAY_WORKER_IMAGE,
        pull_policy=DEFAULT_RAY_IMAGE_PULL_POLICY,
    )


def _mapping(
    document: Mapping[str, Any], key: str, expected: set[str]
) -> Mapping[str, Any]:
    value = document.get(key)
    if not isinstance(value, Mapping):
        raise ClusterConfigError(f"{key} must be a YAML mapping")
    _fail_unknown(value, expected, key)
    return value


def _text(value: Any, label: str, *, whitespace_free: bool = False) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ClusterConfigError(f"{label} must be a non-empty string")
    if whitespace_free and any(character.isspace() for character in value):
        raise ClusterConfigError(f"{label} must not contain whitespace")
    return value


def _dns_label(value: Any, label: str) -> str:
    result = _text(value, label, whitespace_free=True)
    if len(result) > 63 or _DNS_LABEL.fullmatch(result) is None:
        raise ClusterConfigError(f"{label} must be a Kubernetes DNS label")
    return result


def validate_dns_label(value: Any, label: str) -> str:
    """Validate an effective CLI override with the same rule as YAML values."""
    return _dns_label(value, label)


def validate_pinned_image(value: Any, label: str) -> str:
    result = _text(value, label, whitespace_free=True)
    if _PINNED_IMAGE.fullmatch(result) is None:
        raise ClusterConfigError(f"{label} must be pinned with @sha256:<digest>")
    return result


def validate_profile_image(value: Any, label: str) -> str:
    """Require an explicit digest or a non-latest tag for a Ray runtime image."""

    result = _text(value, label, whitespace_free=True)
    if (
        _IMAGE_REFERENCE_CHARACTERS.fullmatch(result) is None
        or result.startswith(("/", "."))
        or result.endswith(("/", ":", "@"))
        or "//" in result
    ):
        raise ClusterConfigError(
            f"{label} must be a valid image reference with an explicit tag or digest"
        )
    if "@" in result:
        if result.count("@") != 1 or _PINNED_IMAGE.fullmatch(result) is None:
            raise ClusterConfigError(
                f"{label} digest must use @sha256:<64 lowercase hex characters>"
            )
        return result
    leaf = result.rsplit("/", 1)[-1]
    name, separator, tag = leaf.rpartition(":")
    if (
        not separator
        or not name
        or _IMAGE_TAG.fullmatch(tag) is None
        or tag.lower() == "latest"
    ):
        raise ClusterConfigError(
            f"{label} must use an explicit non-latest tag or sha256 digest"
        )
    return result


def validate_image_pull_policy(value: Any, label: str) -> str:
    result = _text(value, label, whitespace_free=True)
    if result not in {"Always", "IfNotPresent", "Never"}:
        raise ClusterConfigError(f"{label} must be Always, IfNotPresent, or Never")
    return result


def _integer(
    value: Any,
    label: str,
    *,
    minimum: int,
    maximum: int | None = None,
) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
        raise ClusterConfigError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ClusterConfigError(f"{label} must be an integer <= {maximum}")
    return value


def _path(value: Any, label: str, *, base: Path) -> Path:
    text = _text(value, label)
    candidate = Path(text).expanduser()
    if not candidate.is_absolute():
        candidate = base / candidate
    try:
        return candidate.resolve(strict=False)
    except (OSError, RuntimeError) as error:
        raise ClusterConfigError(f"cannot resolve {label}: {error}") from error


def _node_list(
    document: Mapping[str, Any], key: str, *, allow_empty: bool = False
) -> tuple[str, ...]:
    value = document.get(key)
    if not isinstance(value, list) or (not value and not allow_empty):
        qualifier = "a YAML list" if allow_empty else "a non-empty YAML list"
        raise ClusterConfigError(f"topology.{key} must be {qualifier}")
    nodes = tuple(
        _text(node, f"topology.{key} item", whitespace_free=True) for node in value
    )
    if len(nodes) != len(set(nodes)):
        raise ClusterConfigError(f"topology.{key} contains duplicate node targets")
    return nodes


def _string_map(value: Any, label: str) -> dict[str, str]:
    if not isinstance(value, Mapping):
        raise ClusterConfigError(f"{label} must be a YAML mapping")
    result: dict[str, str] = {}
    for key, item in value.items():
        parsed_key = _text(key, f"{label} key", whitespace_free=True)
        parsed_value = _text(item, f"{label}.{parsed_key}", whitespace_free=True)
        result[parsed_key] = parsed_value
    return result


def _builtin_config(path: Path, active: tuple[str, ...], spare: tuple[str, ...]) -> ClusterConfig:
    """Supply non-topology values only for the short-lived legacy v1 format."""
    return ClusterConfig(
        path=path,
        kubernetes=KubernetesDefaults(
            kubectl_command="/usr/local/bin/k3s kubectl",
            kubeconfig=Path("/home/ywj/.kube/k3s-learning.yaml"),
            namespace="pretrain-ray",
            cluster_name="pretrain-gpu00-gpu01",
        ),
        topology=TopologyDefaults(
            head_node="server-00",
            active_nodes=active,
            spare_nodes=spare,
            head_selector={"kubernetes.io/arch": "amd64"},
            worker_selector={
                "kubernetes.io/arch": "arm64",
                "node.kubernetes.io/npu.chip.name": "910B3",
            },
        ),
        accelerator=AcceleratorDefaults(
            resource_name="huawei.com/Ascend910",
            devices_per_node=8,
            runtime_class_name="ascend",
        ),
        npu_check=NpuCheckDefaults(
            exporter_namespace="npu-exporter",
            exporter_app="npu-exporter",
            exporter_port=8082,
        ),
        training=TrainingDefaults(
            template=PROJECT_ROOT
            / "ray_startup_bundle"
            / "training_templates"
            / "pretrain_150M.sh",
            working_directory="/mnt/models/CODE/MindSpeed-LLM-v2.3.0",
            workspace_host_path=Path("/mnt/models"),
        ),
        images=_default_images(),
        supervisor=SupervisorDefaults(
            node="server-00",
            service_account="pretrain-ray-supervisor",
            image=(
                "110.120.0.3:8889/pretrain/ray-head@sha256:"
                "121fff1a4b0f991121ba7dc85cbb7a77643d28c79cc355ba3f1536abb51c865b"
            ),
            image_pull_policy="IfNotPresent",
            kubectl_host_path=Path("/usr/local/bin/k3s"),
            backoff_limit=3,
            finished_ttl_seconds=7 * 24 * 60 * 60,
        ),
        timeouts=TimeoutDefaults(
            ray_startup_seconds=1800,
            hccl_gate_seconds=3600,
            training_seconds=0,
            failed_resource_retention_seconds=1800,
            recovery_cleanup_seconds=300,
        ),
        recovery=_default_recovery(),
    )


def load_cluster_config(path: Path | None = None) -> ClusterConfig:
    source = (path or selected_config_path()).expanduser()
    try:
        resolved = source.resolve(strict=True)
        metadata = resolved.stat()
        if not stat.S_ISREG(metadata.st_mode):
            raise ClusterConfigError(f"configuration is not a regular file: {source}")
        if metadata.st_size > CONFIG_MAX_BYTES:
            raise ClusterConfigError(
                f"configuration exceeds {CONFIG_MAX_BYTES} bytes: {source}"
            )
        document = yaml.safe_load(resolved.read_text(encoding="utf-8"))
    except ClusterConfigError:
        raise
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise ClusterConfigError(f"cannot load configuration {source}: {error}") from error

    if not isinstance(document, Mapping):
        raise ClusterConfigError("configuration must be a YAML mapping")
    schema = document.get("schemaVersion")
    if schema == LEGACY_CONFIG_SCHEMA:
        _fail_unknown(
            document, {"schemaVersion", "activeNodes", "spareNodes"}, "configuration"
        )
        active = _node_list(document, "activeNodes")
        spare = _node_list(document, "spareNodes")
        overlap = sorted(set(active) & set(spare))
        if overlap:
            raise ClusterConfigError(
                "activeNodes and spareNodes overlap: " + ", ".join(overlap)
            )
        return _builtin_config(resolved, active, spare)
    if schema not in {CONFIG_SCHEMA, PREVIOUS_CONFIG_SCHEMA}:
        raise ClusterConfigError(
            "configuration schemaVersion must be "
            f"{CONFIG_SCHEMA} (or the migration-only {PREVIOUS_CONFIG_SCHEMA})"
        )
    current_schema = schema == CONFIG_SCHEMA

    required_sections = {
        "schemaVersion",
        "kubernetes",
        "topology",
        "npuCheck",
        "training",
        "supervisor",
        "timeouts",
    }
    if current_schema:
        required_sections.add("accelerator")
        required_sections.add("images")
    _fail_unknown(
        document,
        required_sections,
        "configuration",
        optional={"recovery"},
    )
    kubernetes = _mapping(
        document,
        "kubernetes",
        {"kubectlCommand", "kubeconfig", "namespace", "clusterName"},
    )
    topology_keys = {"headNode", "activeNodes", "spareNodes"}
    if current_schema:
        topology_keys.update({"headSelector", "workerSelector"})
    topology = _mapping(document, "topology", topology_keys)
    accelerator = None
    if current_schema:
        accelerator = _mapping(
            document,
            "accelerator",
            {"resourceName", "devicesPerNode", "runtimeClassName"},
        )
    npu_check_keys = {"exporterNamespace", "exporterApp", "exporterPort"}
    if not current_schema:
        npu_check_keys.add("resourceName")
    npu_check = _mapping(
        document,
        "npuCheck",
        npu_check_keys,
    )
    training_keys = {"defaultTemplate", "workingDirectory"}
    if current_schema:
        training_keys.add("workspaceHostPath")
    training = _mapping(document, "training", training_keys)
    images = None
    if current_schema:
        images = _mapping(
            document,
            "images",
            {"rayHead", "rayWorker", "pullPolicy"},
        )

    supervisor = _mapping(
        document,
        "supervisor",
        {
            "node",
            "serviceAccount",
            "image",
            "imagePullPolicy",
            "kubectlHostPath",
            "backoffLimit",
            "finishedTtlSeconds",
        },
    )
    timeouts = _mapping(
        document,
        "timeouts",
        {
            "rayStartupSeconds",
            "hcclGateSeconds",
            "trainingSeconds",
            "failedResourceRetentionSeconds",
            "recoveryCleanupSeconds",
        },
    )
    recovery = None
    if "recovery" in document:
        recovery = _mapping(
            document,
            "recovery",
            {
                "sameTopologyRetries",
                "retryBackoffSeconds",
                "noProgressSeconds",
                "diagnosisWindowSeconds",
                "diagnosisPollSeconds",
                "diagnosisStableSamples",
            },
        )

    kubectl_command = _text(kubernetes["kubectlCommand"], "kubernetes.kubectlCommand")
    try:
        if not shlex.split(kubectl_command):
            raise ValueError("empty command")
    except ValueError as error:
        raise ClusterConfigError(
            f"kubernetes.kubectlCommand is invalid: {error}"
        ) from error
    namespace = _dns_label(kubernetes["namespace"], "kubernetes.namespace")
    cluster_name = _dns_label(kubernetes["clusterName"], "kubernetes.clusterName")
    active_nodes = _node_list(topology, "activeNodes")
    spare_nodes = _node_list(topology, "spareNodes", allow_empty=current_schema)
    if current_schema:
        head_selector = _string_map(
            topology["headSelector"], "topology.headSelector"
        )
        worker_selector = _string_map(
            topology["workerSelector"], "topology.workerSelector"
        )
    else:
        # The v1 shape described the original 8-card 910B3 installation.
        # Keep that behavior only while old configs are migrated to v2.
        head_selector = {"kubernetes.io/arch": "amd64"}
        worker_selector = {
            "kubernetes.io/arch": "arm64",
            "node.kubernetes.io/npu.chip.name": "910B3",
        }
        accelerator = {
            "resourceName": npu_check["resourceName"],
            "devicesPerNode": 8,
            "runtimeClassName": "ascend",
        }
    overlap = sorted(set(active_nodes) & set(spare_nodes))
    if overlap:
        raise ClusterConfigError(
            "topology.activeNodes and topology.spareNodes overlap: "
            + ", ".join(overlap)
        )

    worker_cwd = _text(training["workingDirectory"], "training.workingDirectory")
    if not worker_cwd.startswith("/"):
        raise ClusterConfigError("training.workingDirectory must be absolute")
    if current_schema:
        workspace_host_path = Path(
            _text(training["workspaceHostPath"], "training.workspaceHostPath")
        )
        if not workspace_host_path.is_absolute():
            raise ClusterConfigError("training.workspaceHostPath must be absolute")
        image_defaults = ImageDefaults(
            ray_head=validate_profile_image(images["rayHead"], "images.rayHead"),
            ray_worker=validate_profile_image(
                images["rayWorker"], "images.rayWorker"
            ),
            pull_policy=validate_image_pull_policy(
                images["pullPolicy"], "images.pullPolicy"
            ),
        )
    else:
        workspace_host_path = Path("/mnt/models")
        image_defaults = _default_images()
    image = validate_pinned_image(supervisor["image"], "supervisor.image")
    pull_policy = _text(
        supervisor["imagePullPolicy"], "supervisor.imagePullPolicy"
    )
    if pull_policy not in {"Always", "IfNotPresent", "Never"}:
        raise ClusterConfigError(
            "supervisor.imagePullPolicy must be Always, IfNotPresent, or Never"
        )

    recovery_defaults = _default_recovery()
    if recovery is not None:
        recovery_defaults = RecoveryDefaults(
            same_topology_retries=_integer(
                recovery["sameTopologyRetries"],
                "recovery.sameTopologyRetries",
                minimum=0,
            ),
            retry_backoff_seconds=_integer(
                recovery["retryBackoffSeconds"],
                "recovery.retryBackoffSeconds",
                minimum=0,
            ),
            no_progress_seconds=_integer(
                recovery["noProgressSeconds"],
                "recovery.noProgressSeconds",
                minimum=0,
            ),
            diagnosis_window_seconds=_integer(
                recovery["diagnosisWindowSeconds"],
                "recovery.diagnosisWindowSeconds",
                minimum=0,
            ),
            diagnosis_poll_seconds=_integer(
                recovery["diagnosisPollSeconds"],
                "recovery.diagnosisPollSeconds",
                minimum=1,
            ),
            diagnosis_stable_samples=_integer(
                recovery["diagnosisStableSamples"],
                "recovery.diagnosisStableSamples",
                minimum=1,
            ),
        )

    return ClusterConfig(
        path=resolved,
        kubernetes=KubernetesDefaults(
            kubectl_command=kubectl_command,
            kubeconfig=_path(
                kubernetes["kubeconfig"],
                "kubernetes.kubeconfig",
                base=resolved.parent,
            ),
            namespace=namespace,
            cluster_name=cluster_name,
        ),
        topology=TopologyDefaults(
            head_node=_text(
                topology["headNode"], "topology.headNode", whitespace_free=True
            ),
            active_nodes=active_nodes,
            spare_nodes=spare_nodes,
            head_selector=head_selector,
            worker_selector=worker_selector,
        ),
        accelerator=AcceleratorDefaults(
            resource_name=_text(
                accelerator["resourceName"],
                "accelerator.resourceName",
                whitespace_free=True,
            ),
            devices_per_node=_integer(
                accelerator["devicesPerNode"],
                "accelerator.devicesPerNode",
                minimum=1,
                maximum=64,
            ),
            runtime_class_name=(
                _dns_label(
                    accelerator["runtimeClassName"],
                    "accelerator.runtimeClassName",
                )
                if accelerator["runtimeClassName"] is not None
                else None
            ),
        ),
        npu_check=NpuCheckDefaults(
            exporter_namespace=_dns_label(
                npu_check["exporterNamespace"], "npuCheck.exporterNamespace"
            ),
            exporter_app=_text(
                npu_check["exporterApp"],
                "npuCheck.exporterApp",
                whitespace_free=True,
            ),
            exporter_port=_integer(
                npu_check["exporterPort"],
                "npuCheck.exporterPort",
                minimum=1,
                maximum=65535,
            ),
        ),
        training=TrainingDefaults(
            template=_path(
                training["defaultTemplate"],
                "training.defaultTemplate",
                base=resolved.parent,
            ),
            working_directory=worker_cwd,
            workspace_host_path=workspace_host_path,
        ),
        images=image_defaults,
        supervisor=SupervisorDefaults(
            node=_text(
                supervisor["node"], "supervisor.node", whitespace_free=True
            ),
            service_account=_dns_label(
                supervisor["serviceAccount"], "supervisor.serviceAccount"
            ),
            image=image,
            image_pull_policy=pull_policy,
            kubectl_host_path=_path(
                supervisor["kubectlHostPath"],
                "supervisor.kubectlHostPath",
                base=resolved.parent,
            ),
            backoff_limit=_integer(
                supervisor["backoffLimit"],
                "supervisor.backoffLimit",
                minimum=0,
                maximum=100,
            ),
            finished_ttl_seconds=_integer(
                supervisor["finishedTtlSeconds"],
                "supervisor.finishedTtlSeconds",
                minimum=1,
            ),
        ),
        timeouts=TimeoutDefaults(
            ray_startup_seconds=_integer(
                timeouts["rayStartupSeconds"],
                "timeouts.rayStartupSeconds",
                minimum=1,
            ),
            hccl_gate_seconds=_integer(
                timeouts["hcclGateSeconds"],
                "timeouts.hcclGateSeconds",
                minimum=1,
            ),
            training_seconds=_integer(
                timeouts["trainingSeconds"],
                "timeouts.trainingSeconds",
                minimum=0,
            ),
            failed_resource_retention_seconds=_integer(
                timeouts["failedResourceRetentionSeconds"],
                "timeouts.failedResourceRetentionSeconds",
                minimum=-1,
            ),
            recovery_cleanup_seconds=_integer(
                timeouts["recoveryCleanupSeconds"],
                "timeouts.recoveryCleanupSeconds",
                minimum=1,
            ),
        ),
        recovery=recovery_defaults,
    )


# A clearer name for new callers; retain load_cluster_config for compatibility.
load_config = load_cluster_config


def apply_kubernetes_defaults(
    args: Any,
    config: ClusterConfig | None = None,
) -> ClusterConfig | None:
    """Fill common CLI destinations without overwriting explicit arguments."""
    destinations = ("kubectl_command", "kubeconfig", "namespace", "cluster")
    missing = any(
        hasattr(args, destination) and getattr(args, destination) is None
        for destination in destinations
    )
    if not missing:
        return config
    defaults = config or load_cluster_config()
    values = {
        "kubectl_command": defaults.kubernetes.kubectl_command,
        "kubeconfig": defaults.kubernetes.kubeconfig,
        "namespace": defaults.kubernetes.namespace,
        "cluster": defaults.kubernetes.cluster_name,
    }
    for destination, value in values.items():
        if hasattr(args, destination) and getattr(args, destination) is None:
            setattr(args, destination, value)
    return defaults


def apply_cleanup_timeout_default(
    args: Any,
    config: ClusterConfig | None = None,
) -> ClusterConfig | None:
    if not hasattr(args, "cleanup_timeout_seconds"):
        return config
    if args.cleanup_timeout_seconds is not None:
        return config
    defaults = config or load_cluster_config()
    args.cleanup_timeout_seconds = defaults.timeouts.recovery_cleanup_seconds
    return defaults
