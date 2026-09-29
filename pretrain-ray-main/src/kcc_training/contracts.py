"""Strict v1beta1 manifests shared with platform integrations."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Any, Mapping

import yaml


API_VERSION = "training.kcc.io/v1beta1"
_DNS_LABEL = re.compile(r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$")
_DNS_SUBDOMAIN = re.compile(
    r"^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?(?:\.[a-z0-9](?:[-a-z0-9]*[a-z0-9])?)*$"
)
_DIGEST_IMAGE = re.compile(r"^[A-Za-z0-9._:/-]+@sha256:[0-9a-f]{64}$")
_ENVIRONMENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_ARTIFACT_URI = re.compile(r"^artifact://[^/?#\s]+/[^/?#\s]+/[^/?#\s]+$")
_BLOCKED_ENVIRONMENT = {
    "RANK_TABLE_FILE",
    "MASTER_ADDR",
    "MASTER_PORT",
    "NODE_RANK",
    "WORLD_SIZE",
    "KUBECONFIG",
    "KCC_SOURCE_DIR",
    "KCC_MODEL_DIR",
    "KCC_DATA_DIR",
    "KCC_OUTPUT_ROOT",
    "KCC_CHECKPOINT_ROOT",
    "KCC_RUN_NAME",
}


class ContractError(ValueError):
    """A public API document is malformed or unsupported."""


def _mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ContractError(f"{label} must be a mapping")
    return value


def _exact_keys(
    value: Mapping[str, Any],
    required: set[str],
    label: str,
    optional: set[str] | None = None,
) -> None:
    missing = required - set(value)
    unknown = set(value) - required - (optional or set())
    if missing:
        raise ContractError(f"{label} is missing: {', '.join(sorted(missing))}")
    if unknown:
        raise ContractError(f"{label} has unsupported keys: {', '.join(sorted(unknown))}")


def _text(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or value != value.strip():
        raise ContractError(f"{label} must be a non-empty trimmed string")
    return value


def _name(value: Any, label: str) -> str:
    result = _text(value, label)
    if len(result) > 63 or _DNS_LABEL.fullmatch(result) is None:
        raise ContractError(f"{label} must be a Kubernetes DNS label")
    return result


def _subdomain(value: Any, label: str) -> str:
    result = _text(value, label)
    if len(result) > 253 or _DNS_SUBDOMAIN.fullmatch(result) is None:
        raise ContractError(f"{label} must be a Kubernetes DNS subdomain")
    return result


def _integer(
    value: Any,
    label: str,
    *,
    minimum: int = 1,
    maximum: int | None = None,
) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise ContractError(f"{label} must be an integer >= {minimum}")
    if maximum is not None and value > maximum:
        raise ContractError(f"{label} must be <= {maximum}")
    return value


def _document(path: Path) -> Mapping[str, Any]:
    try:
        value = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise ContractError(f"cannot load {path}: {error}") from error
    return _mapping(value, "document")


def _header(document: Mapping[str, Any], kind: str) -> tuple[str, str, Mapping[str, Any]]:
    _exact_keys(document, {"apiVersion", "kind", "metadata", "spec"}, "document")
    if document["apiVersion"] != API_VERSION:
        raise ContractError(f"apiVersion must be {API_VERSION}")
    if document["kind"] != kind:
        raise ContractError(f"kind must be {kind}")
    metadata = _mapping(document["metadata"], "metadata")
    _exact_keys(metadata, {"name", "namespace"}, "metadata")
    return (
        _name(metadata["name"], "metadata.name"),
        _name(metadata["namespace"], "metadata.namespace"),
        _mapping(document["spec"], "spec"),
    )


def _string_map(value: Any, label: str) -> dict[str, str]:
    source = _mapping(value, label)
    result: dict[str, str] = {}
    for key, item in source.items():
        if not isinstance(key, str) or not key or not isinstance(item, str):
            raise ContractError(f"{label} must contain string keys and values")
        result[key] = item
    return result


def _string_list(value: Any, label: str, *, allow_empty: bool = True) -> tuple[str, ...]:
    if not isinstance(value, list) or (not value and not allow_empty):
        requirement = "a list" if allow_empty else "a non-empty list"
        raise ContractError(f"{label} must be {requirement}")
    result = tuple(_text(item, f"{label} item") for item in value)
    if len(result) != len(set(result)):
        raise ContractError(f"{label} must not contain duplicates")
    return result


def _image(value: Any, label: str) -> str:
    result = _text(value, label)
    if _DIGEST_IMAGE.fullmatch(result) is None:
        raise ContractError(f"{label} must use an @sha256 digest")
    return result


def _artifact_uri(value: Any, label: str) -> str:
    result = _text(value, label)
    if _ARTIFACT_URI.fullmatch(result) is None:
        raise ContractError(f"{label} must be artifact://namespace/name/version")
    return result


def _quantity_map(value: Any, label: str) -> dict[str, str | int]:
    source = _mapping(value, label)
    result: dict[str, str | int] = {}
    for key, item in source.items():
        if not isinstance(key, str) or not key:
            raise ContractError(f"{label} has an invalid resource name")
        if isinstance(item, bool) or not isinstance(item, (str, int)) or item == "":
            raise ContractError(f"{label}.{key} must be a string or integer quantity")
        result[key] = item
    return result


def _pod_template(value: Any, label: str) -> Mapping[str, Any]:
    pod = _mapping(value, label)
    _exact_keys(
        pod,
        set(),
        label,
        optional={"resources", "tolerations", "priorityClassName", "rayCpus"},
    )
    if "resources" in pod:
        resources = _mapping(pod["resources"], f"{label}.resources")
        _exact_keys(resources, {"requests", "limits"}, f"{label}.resources")
        _quantity_map(resources["requests"], f"{label}.resources.requests")
        _quantity_map(resources["limits"], f"{label}.resources.limits")
    if "tolerations" in pod:
        tolerations = pod["tolerations"]
        if not isinstance(tolerations, list) or not all(
            isinstance(item, Mapping) for item in tolerations
        ):
            raise ContractError(f"{label}.tolerations must be a list of objects")
    if "priorityClassName" in pod:
        _text(pod["priorityClassName"], f"{label}.priorityClassName")
    if "rayCpus" in pod:
        _integer(pod["rayCpus"], f"{label}.rayCpus")
    return dict(pod)


@dataclass(frozen=True)
class TrainingRuntimeProfile:
    name: str
    namespace: str
    head_image: str
    worker_image: str
    ray_version: str
    accelerator_resource: str
    devices_per_node: int
    rank_table_provider: str
    health_provider: str
    artifact_provider: str
    workspace_claim: str
    workspace_mount_path: str
    active_nodes: tuple[str, ...]
    spare_nodes: tuple[str, ...]
    physical_device_ids: tuple[int, ...] = ()

    @classmethod
    def load(cls, path: Path) -> "TrainingRuntimeProfile":
        name, namespace, spec = _header(_document(path), "TrainingRuntimeProfile")
        _exact_keys(
            spec,
            {"images", "rayVersion", "accelerator", "integrations", "workspace", "scheduling"},
            "spec",
            optional={"podTemplate"},
        )
        images = _mapping(spec["images"], "spec.images")
        _exact_keys(images, {"head", "worker"}, "spec.images", optional={"pullSecrets"})
        pull_secrets = _string_list(images.get("pullSecrets", []), "spec.images.pullSecrets")
        for index, secret in enumerate(pull_secrets):
            _subdomain(secret, f"spec.images.pullSecrets[{index}]")
        accelerator = _mapping(spec["accelerator"], "spec.accelerator")
        _exact_keys(
            accelerator,
            {"resourceName", "devicesPerNode"},
            "spec.accelerator",
            optional={"runtimeClassName", "physicalDeviceIDs"},
        )
        if accelerator.get("runtimeClassName") is not None:
            _name(accelerator["runtimeClassName"], "spec.accelerator.runtimeClassName")
        devices_per_node = _integer(
            accelerator["devicesPerNode"],
            "spec.accelerator.devicesPerNode",
            maximum=64,
        )
        raw_physical_device_ids = accelerator.get("physicalDeviceIDs", [])
        if not isinstance(raw_physical_device_ids, list) or any(
            isinstance(item, bool) or not isinstance(item, int) or not 0 <= item <= 63
            for item in raw_physical_device_ids
        ):
            raise ContractError("physicalDeviceIDs must contain integers from 0 to 63")
        physical_device_ids = tuple(raw_physical_device_ids)
        if len(physical_device_ids) != len(set(physical_device_ids)):
            raise ContractError("physicalDeviceIDs must be unique")
        if physical_device_ids and len(physical_device_ids) != devices_per_node:
            raise ContractError("physicalDeviceIDs must match devicesPerNode")
        accelerator_resource = _text(
            accelerator["resourceName"], "spec.accelerator.resourceName"
        )
        if physical_device_ids and accelerator_resource != "huawei.com/Ascend910":
            raise ContractError(
                "physicalDeviceIDs currently requires resourceName huawei.com/Ascend910"
            )
        integrations = _mapping(spec["integrations"], "spec.integrations")
        _exact_keys(
            integrations,
            {"rankTableProvider", "healthProvider"},
            "spec.integrations",
            optional={"artifactProvider"},
        )
        rank_provider = _text(integrations["rankTableProvider"], "rankTableProvider")
        if rank_provider != "clusterd":
            raise ContractError("rankTableProvider must be clusterd")
        health_provider = _text(integrations["healthProvider"], "healthProvider")
        if health_provider not in {"kubernetes", "npu-exporter"}:
            raise ContractError("healthProvider must be kubernetes or npu-exporter")
        artifact_provider = _text(
            integrations.get("artifactProvider", "gateway"), "artifactProvider"
        )
        if artifact_provider not in {"gateway", "workspace"}:
            raise ContractError("artifactProvider must be gateway or workspace")

        workspace = _mapping(spec["workspace"], "spec.workspace")
        _exact_keys(workspace, {"claimName", "mountPath"}, "spec.workspace")
        mount_path = _text(workspace["mountPath"], "spec.workspace.mountPath")
        if not mount_path.startswith("/"):
            raise ContractError("spec.workspace.mountPath must be absolute")
        scheduling = _mapping(spec["scheduling"], "spec.scheduling")
        _exact_keys(
            scheduling,
            {"activeNodes", "spareNodes", "headSelector", "workerSelector"},
            "spec.scheduling",
        )
        active = _string_list(
            scheduling["activeNodes"], "spec.scheduling.activeNodes", allow_empty=False
        )
        spare = _string_list(scheduling["spareNodes"], "spec.scheduling.spareNodes")
        if set(active) & set(spare):
            raise ContractError("activeNodes and spareNodes must be disjoint")
        _string_map(scheduling["headSelector"], "spec.scheduling.headSelector")
        _string_map(scheduling["workerSelector"], "spec.scheduling.workerSelector")
        pod_template = _mapping(spec.get("podTemplate", {}), "spec.podTemplate")
        _exact_keys(pod_template, set(), "spec.podTemplate", optional={"head", "worker"})
        for role in ("head", "worker"):
            _pod_template(pod_template.get(role, {}), f"spec.podTemplate.{role}")
        return cls(
            name=name,
            namespace=namespace,
            head_image=_image(images["head"], "spec.images.head"),
            worker_image=_image(images["worker"], "spec.images.worker"),
            ray_version=_text(spec["rayVersion"], "spec.rayVersion"),
            accelerator_resource=accelerator_resource,
            devices_per_node=devices_per_node,
            rank_table_provider=rank_provider,
            health_provider=health_provider,
            workspace_claim=_name(workspace["claimName"], "spec.workspace.claimName"),
            artifact_provider=artifact_provider,
            workspace_mount_path=mount_path,
            active_nodes=active,
            spare_nodes=spare,
            physical_device_ids=physical_device_ids,
        )


@dataclass(frozen=True)
class TrainingRecipe:
    name: str
    namespace: str
    framework: str
    command: tuple[str, ...]
    source_uri: str
    model_uri: str
    data_uri: str
    output_subpath: str

    @property
    def entrypoint(self) -> tuple[str, ...]:
        """Compatibility alias for callers of the pre-v1beta1 validator."""
        return self.command

    @classmethod
    def load(cls, path: Path) -> "TrainingRecipe":
        name, namespace, spec = _header(_document(path), "TrainingRecipe")
        _exact_keys(
            spec,
            {"framework", "command", "workingDirectory", "environment", "artifacts"},
            "spec",
        )
        command = _string_list(spec["command"], "spec.command", allow_empty=False)
        working_directory = _text(spec["workingDirectory"], "spec.workingDirectory")
        if Path(working_directory).is_absolute() or ".." in Path(working_directory).parts:
            raise ContractError("workingDirectory must be a safe relative source path")
        environment = _string_map(spec["environment"], "spec.environment")
        if any(_ENVIRONMENT.fullmatch(key) is None for key in environment):
            raise ContractError("spec.environment contains an invalid variable name")
        if _BLOCKED_ENVIRONMENT & set(environment):
            raise ContractError("spec.environment overrides controller-owned variables")
        artifacts = _mapping(spec["artifacts"], "spec.artifacts")
        _exact_keys(artifacts, {"source", "model", "data", "outputSubpath"}, "spec.artifacts")
        output = _text(artifacts["outputSubpath"], "spec.artifacts.outputSubpath")
        if output.startswith("/") or ".." in Path(output).parts:
            raise ContractError("outputSubpath must be a safe relative path")
        return cls(
            name=name,
            namespace=namespace,
            framework=_text(spec["framework"], "spec.framework"),
            command=command,
            source_uri=_artifact_uri(artifacts["source"], "spec.artifacts.source"),
            model_uri=_artifact_uri(artifacts["model"], "spec.artifacts.model"),
            data_uri=_artifact_uri(artifacts["data"], "spec.artifacts.data"),
            output_subpath=output,
        )


@dataclass(frozen=True)
class TrainingRun:
    name: str
    namespace: str
    runtime_profile: str
    recipe: str
    workers: int
    same_topology_retries: int
    max_replacements: int
    no_progress_seconds: int
    suspended: bool
    suspend_mode: str
    depends_on: str | None = None

    @classmethod
    def load(cls, path: Path) -> "TrainingRun":
        name, namespace, spec = _header(_document(path), "TrainingRun")
        _exact_keys(
            spec,
            {"runtimeProfile", "recipe", "workers", "recovery"},
            "spec",
            optional={"suspend", "suspendMode", "dependsOn"},
        )
        recovery = _mapping(spec["recovery"], "spec.recovery")
        _exact_keys(
            recovery,
            {"sameTopologyRetries", "maxReplacements", "noProgressSeconds"},
            "spec.recovery",
        )
        suspended = spec.get("suspend", False)
        if not isinstance(suspended, bool):
            raise ContractError("spec.suspend must be boolean")
        suspend_mode = spec.get("suspendMode", "Immediate")
        if suspend_mode not in {"Immediate", "AfterCheckpoint"}:
            raise ContractError("spec.suspendMode must be Immediate or AfterCheckpoint")
        depends_on = (
            _name(spec["dependsOn"], "spec.dependsOn")
            if "dependsOn" in spec
            else None
        )
        if depends_on == name:
            raise ContractError("spec.dependsOn cannot reference the same TrainingRun")
        same_topology_retries = _integer(
            recovery["sameTopologyRetries"],
            "spec.recovery.sameTopologyRetries",
            minimum=0,
            maximum=10,
        )
        max_replacements = _integer(
            recovery["maxReplacements"],
            "spec.recovery.maxReplacements",
            minimum=0,
            maximum=100,
        )
        if (max_replacements + 1) * (same_topology_retries + 1) > 100:
            raise ContractError(
                "recovery attempt budget must satisfy "
                "(maxReplacements + 1) * (sameTopologyRetries + 1) <= 100"
            )
        return cls(
            name=name,
            namespace=namespace,
            runtime_profile=_name(spec["runtimeProfile"], "spec.runtimeProfile"),
            recipe=_name(spec["recipe"], "spec.recipe"),
            workers=_integer(spec["workers"], "spec.workers", maximum=1024),
            same_topology_retries=same_topology_retries,
            max_replacements=max_replacements,
            no_progress_seconds=_integer(
                recovery["noProgressSeconds"],
                "spec.recovery.noProgressSeconds",
                minimum=0,
            ),
            suspended=suspended,
            suspend_mode=suspend_mode,
            depends_on=depends_on,
        )

LOADERS = {
    "TrainingRuntimeProfile": TrainingRuntimeProfile.load,
    "TrainingRecipe": TrainingRecipe.load,
    "TrainingRun": TrainingRun.load,
}


def load_contract(path: Path) -> object:
    document = _document(path)
    kind = document.get("kind")
    loader = LOADERS.get(kind)
    if loader is None:
        raise ContractError(f"unsupported kind: {kind!r}")
    return loader(path)
