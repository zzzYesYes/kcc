"""Pure KubeRay manifest rendering for one immutable training attempt."""

from __future__ import annotations

import hashlib
import json
from pathlib import PurePosixPath
from typing import Any, Mapping, Sequence

from .api_v1beta1 import Recipe, Run, RuntimeProfile

KUBERAY_HEAD_SERVICE_SUFFIX = "-head-svc"
MAX_RAYCLUSTER_NAME_LENGTH = 63 - len(KUBERAY_HEAD_SERVICE_SUFFIX)
CONTROL_SCHEMA = "kcc-runtime-control/v1"
CONTROL_ACTIONS = {"Continue", "StopAfterCheckpoint", "StopImmediate"}
CONTROL_MOUNT_PATH = "/etc/kcc/control"


def attempt_name(run_name: str, attempt: int) -> str:
    suffix = f"-a{attempt:02d}"
    candidate = f"{run_name}{suffix}"
    if len(candidate) <= MAX_RAYCLUSTER_NAME_LENGTH:
        return candidate
    digest = hashlib.sha256(run_name.encode()).hexdigest()[:8]
    return f"{run_name[: MAX_RAYCLUSTER_NAME_LENGTH - len(suffix) - 9]}-{digest}{suffix}"



def _artifact_target(mount_path: str, kind: str, uri: str) -> str:
    digest = hashlib.sha256(uri.encode("utf-8")).hexdigest()[:24]
    return str(PurePosixPath(mount_path) / ".kcc" / "artifacts" / kind / digest)


def _source_working_directory(mount_path: str, source_target: str, configured: str) -> str:
    if configured.startswith("/"):
        legacy_root = PurePosixPath(mount_path) / "source"
        try:
            relative = PurePosixPath(configured).relative_to(legacy_root)
        except ValueError as error:
            raise ValueError(
                "absolute workingDirectory must be the legacy <workspace>/source path; "
                "use a relative path for new recipes"
            ) from error
    else:
        relative = PurePosixPath(configured)
    return str(PurePosixPath(source_target) / relative)


def _worker_resources(profile: RuntimeProfile) -> dict[str, dict[str, str | int]]:
    resources = {name: dict(values) for name, values in profile.worker_resources.items()}
    resources["requests"][profile.resource_name] = profile.devices_per_node
    resources["limits"][profile.resource_name] = profile.devices_per_node

    return resources
def owner_reference(run: Run) -> list[dict[str, Any]]:
    return [
        {
            "apiVersion": "training.kcc.io/v1beta1",
            "kind": "TrainingRun",
            "name": run.identity.name,
            "uid": run.identity.uid,
            "controller": True,
        }
    ]

def render_control(
    run: Run,
    *,
    attempt: int,
    action: str,
    request_generation: int,
) -> dict[str, Any]:
    if action not in CONTROL_ACTIONS:
        raise ValueError("unsupported runtime control action")
    if request_generation < 1:
        raise ValueError("runtime control generation must be positive")
    name = attempt_name(run.identity.name, attempt)
    labels = {
        "app.kubernetes.io/name": "kcc-training",
        "app.kubernetes.io/component": "runtime-control",
        "training.kcc.io/run": run.identity.name,
        "training.kcc.io/attempt": str(attempt),
    }
    payload = {
        "schemaVersion": CONTROL_SCHEMA,
        "runName": run.identity.name,
        "runUid": run.identity.uid,
        "attempt": attempt,
        "action": action,
        "requestGeneration": request_generation,
    }
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{name}-control",
            "namespace": run.identity.namespace,
            "labels": labels,
            "annotations": {
                "training.kcc.io/run-uid": run.identity.uid,
                "training.kcc.io/attempt": str(attempt),
            },
            "ownerReferences": owner_reference(run),
        },
        "data": {"control.json": json.dumps(payload, ensure_ascii=False, sort_keys=True)},
    }


def runtime_spec(
    run: Run,
    profile: RuntimeProfile,
    recipe: Recipe,
    *,
    attempt: int,
    active_nodes: Sequence[str],
) -> dict[str, Any]:
    source_uri = run.source_uri or recipe.source_uri
    model_uri = run.model_uri or recipe.model_uri
    data_uri = run.data_uri or recipe.data_uri
    output_subpath = run.output_subpath or recipe.output_subpath
    source_target = _artifact_target(profile.workspace_mount_path, "source", source_uri)
    model_target = _artifact_target(profile.workspace_mount_path, "model", model_uri)
    data_target = _artifact_target(profile.workspace_mount_path, "data", data_uri)
    working_directory = _source_working_directory(
        profile.workspace_mount_path,
        source_target,
        recipe.working_directory,
    )
    run_identity = hashlib.sha256(run.identity.uid.encode("utf-8")).hexdigest()[:12]
    output_root = str(
        PurePosixPath(profile.workspace_mount_path)
        / output_subpath
        / f"{run.identity.name}-{run_identity}"
    )
    checkpoint_root = str(PurePosixPath(output_root) / "checkpoints")
    environment = {
        **dict(recipe.environment),
        **dict(run.environment),
        "KCC_SOURCE_DIR": source_target,
        "KCC_MODEL_DIR": model_target,
        "KCC_DATA_DIR": data_target,
        "KCC_OUTPUT_ROOT": output_root,
        "KCC_CHECKPOINT_ROOT": checkpoint_root,
        "KCC_RUN_NAME": run.identity.name,
    }
    return {
        "schemaVersion": "kcc-runtime/v1",
        "run": {
            "name": run.identity.name,
            "namespace": run.identity.namespace,
            "uid": run.identity.uid,
            "attempt": attempt,
        },
        "topology": {
            "workers": len(active_nodes),
            "nodes": list(active_nodes),
            "devicesPerNode": profile.devices_per_node,
            "resourceName": profile.resource_name,
            "rankTablePath": "/etc/kcc/ranktable/hccl.json",
        },
        "training": {
            "framework": recipe.framework,
            "command": [*(run.command or recipe.command), *run.command_arguments],
            "workingDirectory": working_directory,
            "environment": environment,
            "noProgressSeconds": run.no_progress_seconds,
        },
        "artifacts": {
            "provider": profile.artifact_provider,
            "source": {"uri": source_uri, "target": source_target},
            "model": {"uri": model_uri, "target": model_target},
            "data": {"uri": data_uri, "target": data_target},
            "outputRoot": output_root,
            "checkpointRoot": checkpoint_root,
        },
    }

def render_attempt(
    run: Run,
    profile: RuntimeProfile,
    recipe: Recipe,
    *,
    attempt: int,
    active_nodes: Sequence[str],
    runtime_service_account: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if len(active_nodes) != run.workers or len(set(active_nodes)) != len(active_nodes):
        raise ValueError("active node count/uniqueness differs from TrainingRun")
    if not set(active_nodes) <= set((*profile.active_nodes, *profile.spare_nodes)):
        raise ValueError("active nodes are outside the RuntimeProfile pool")
    name = attempt_name(run.identity.name, attempt)
    owners = owner_reference(run)
    labels = {
        "app.kubernetes.io/name": "kcc-training",
        "app.kubernetes.io/component": "runtime",
        "training.kcc.io/run": run.identity.name,
        "training.kcc.io/attempt": str(attempt),
    }
    head_labels = {**labels, "training.kcc.io/role": "head"}
    worker_labels = {**labels, "training.kcc.io/role": "worker"}
    head_metadata: dict[str, Any] = {"labels": head_labels}
    if profile.resource_name == "huawei.com/Ascend910":
        # KubeRay puts the CPU-only head and NPU workers in one Volcano
        # PodGroup. Ascend-for-Volcano explicitly requires this annotation on
        # a zero-NPU task so the worker allocation can be validated normally.
        head_metadata["annotations"] = {
            "huawei.com/skip-ascend-plugin": "enabled"
        }
    worker_metadata: dict[str, Any] = {"labels": worker_labels}
    worker_environment: list[dict[str, Any]] = [
        {"name": "NODE_NAME", "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}}},
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "POD_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.podIP"}}},
        {"name": "HOST_IP", "valueFrom": {"fieldRef": {"fieldPath": "status.hostIP"}}},
    ]
    if profile.physical_device_ids:
        physical = ",".join(str(item) for item in profile.physical_device_ids)
        logical = ",".join(str(item) for item in range(profile.devices_per_node))
        worker_metadata["annotations"] = {
            profile.resource_name: ",".join(
                f"Ascend910-{item}" for item in profile.physical_device_ids
            )
        }
        worker_environment.extend(
            [
                {"name": "ASCEND_VISIBLE_DEVICES", "value": physical},
                {"name": "ASCEND_RT_VISIBLE_DEVICES", "value": logical},
                {"name": "RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES", "value": "1"},
            ]
        )
    elif profile.resource_name == "huawei.com/Ascend910":
        logical = ",".join(str(item) for item in range(profile.devices_per_node))
        worker_environment.extend(
            [
                {"name": "ASCEND_VISIBLE_DEVICES", "value": logical},
                {"name": "ASCEND_RT_VISIBLE_DEVICES", "value": logical},
                {"name": "RAY_EXPERIMENTAL_NOSET_ASCEND_RT_VISIBLE_DEVICES", "value": "1"},
            ]
        )
    spec = runtime_spec(run, profile, recipe, attempt=attempt, active_nodes=active_nodes)
    if (
        (run.wandb_secret_ref is not None or recipe.framework == "mindspeed-llm")
        and spec["training"]["environment"].get("WANDB_MODE", "disabled").lower()
        not in {"disabled", "offline"}
    ):
        worker_environment.append(
            {
                "name": "WANDB_API_KEY",
                "valueFrom": {
                    "secretKeyRef": {
                        "name": run.wandb_secret_ref or "kcc-wandb",
                        "key": "WANDB_API_KEY",
                    }
                },
            }
        )
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{name}-spec",
            "namespace": run.identity.namespace,
            "labels": labels,
            "ownerReferences": owners,
        },
        "data": {"run.json": json.dumps(spec, ensure_ascii=False, sort_keys=True)},
    }
    if recipe.script_name is not None and recipe.script_content is not None:
        configmap["data"][recipe.script_name] = recipe.script_content
    shared_volumes = [
        {"name": "run-spec", "configMap": {"name": f"{name}-spec"}},
        {"name": "control", "configMap": {"name": f"{name}-control"}},
        {
            "name": "ranktable",
            "configMap": {"name": f"hccl-sanitized-{name}", "optional": True},
        },
        {
            "name": "workspace",
            "persistentVolumeClaim": {"claimName": profile.workspace_claim},
        },
    ]
    mounts = [
        {"name": "run-spec", "mountPath": "/etc/kcc/run", "readOnly": True},
        {"name": "control", "mountPath": CONTROL_MOUNT_PATH, "readOnly": True},
        {"name": "ranktable", "mountPath": "/etc/kcc/ranktable", "readOnly": True},
        {"name": "workspace", "mountPath": profile.workspace_mount_path},
    ]
    worker_volumes = list(shared_volumes)
    worker_mounts = list(mounts)
    if recipe.script_name is not None:
        worker_mounts.append(
            {
                "name": "run-spec",
                "mountPath": str(
                    PurePosixPath(spec["training"]["workingDirectory"])
                    / recipe.script_name
                ),
                "subPath": recipe.script_name,
                "readOnly": True,
            }
        )
    if profile.resource_name == "huawei.com/Ascend910":
        # The Ascend device plugin allocates devices, but this target cluster
        # does not inject the host driver tools into the container. HCCL
        # preflight and torch-npu both need the matching host driver tree.
        worker_volumes.append(
            {
                "name": "ascend-driver",
                "hostPath": {
                    "path": "/usr/local/Ascend/driver",
                    "type": "Directory",
                },
            }
        )
        worker_mounts.append(
            {
                "name": "ascend-driver",
                "mountPath": "/usr/local/Ascend/driver",
                "readOnly": True,
            }
        )
    if recipe.framework == "mindspeed-llm":
        # The existing MindSpeed training estate is an NFS filesystem mounted
        # by every GPU worker at /mnt/models.  Keep KCC's portable workspace
        # PVC for source, checkpoints, logs, and output, and expose the large
        # legacy model/data tree only to the processes that consume it.  The
        # Ray head runs on the control node, which cannot route to that NFS
        # backend and therefore must not mount it.
        worker_volumes.append(
            {
                "name": "mindspeed-models",
                "hostPath": {"path": "/mnt/models", "type": "Directory"},
            }
        )
        worker_mounts.append(
            {
                "name": "mindspeed-models",
                "mountPath": "/mnt/models",
            }
        )
    cluster = {
        "apiVersion": "ray.io/v1",
        "kind": "RayCluster",
        "metadata": {
            "name": name,
            "namespace": run.identity.namespace,
            "labels": labels,
            "annotations": {"training.kcc.io/run-uid": run.identity.uid},
            "ownerReferences": owners,
        },
        "spec": {
            "rayVersion": profile.ray_version,
            "enableInTreeAutoscaling": False,
            "headGroupSpec": {
                "serviceType": "ClusterIP",
                "rayStartParams": {"dashboard-host": "0.0.0.0", "num-cpus": str(profile.head_ray_cpus)},
                "template": {
                    "metadata": head_metadata,
                    "spec": {
                        "serviceAccountName": runtime_service_account,
                        "nodeSelector": dict(profile.head_selector),
                        "containers": [
                            {
                                "name": "ray-head",
                                "image": profile.head_image,
                                "imagePullPolicy": "IfNotPresent",
                                "resources": {name: dict(values) for name, values in profile.head_resources.items()},
                                "volumeMounts": mounts,
                            }
                        ],
                        "volumes": shared_volumes,
                    },
                },
            },
            "workerGroupSpecs": [
                {
                    "groupName": "npu-workers",
                    "replicas": len(active_nodes),
                    "minReplicas": len(active_nodes),
                    "maxReplicas": len(active_nodes),
                    "rayStartParams": {
                        "num-cpus": str(profile.worker_ray_cpus),
                    },
                    "template": {
                        "metadata": worker_metadata,
                        "spec": {
                            "automountServiceAccountToken": False,
                            "runtimeClassName": profile.runtime_class_name,
                            "nodeSelector": dict(profile.worker_selector),
                            "affinity": {
                                "nodeAffinity": {
                                    "requiredDuringSchedulingIgnoredDuringExecution": {
                                        "nodeSelectorTerms": [
                                            {
                                                "matchExpressions": [
                                                    {
                                                        "key": "kubernetes.io/hostname",
                                                        "operator": "In",
                                                        "values": list(active_nodes),
                                                    }
                                                ]
                                            }
                                        ]
                                    }
                                },
                                "podAntiAffinity": {
                                    "requiredDuringSchedulingIgnoredDuringExecution": [
                                        {
                                            "labelSelector": {"matchLabels": worker_labels},
                                            "topologyKey": "kubernetes.io/hostname",
                                        }
                                    ]
                                },
                            },
                            "containers": [
                                {
                                    "name": "ray-worker",
                                    "image": profile.worker_image,
                                    "imagePullPolicy": "IfNotPresent",
                                    "resources": _worker_resources(profile),
                                    "env": worker_environment,
                                    "volumeMounts": worker_mounts,
                                }
                            ],
                            "volumes": worker_volumes,
                        },
                    },
                }
            ],
        },
    }
    head_pod = cluster["spec"]["headGroupSpec"]["template"]["spec"]
    worker_pod = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]
    if profile.resource_name == "huawei.com/Ascend910":
        worker_pod["containers"][0]["securityContext"] = {"privileged": True}
    for pod in (head_pod, worker_pod):
        pod["securityContext"] = {"fsGroup": 1000, "fsGroupChangePolicy": "OnRootMismatch"}
        if profile.image_pull_secrets:
            pod["imagePullSecrets"] = [{"name": name} for name in profile.image_pull_secrets]
    if profile.head_tolerations:
        head_pod["tolerations"] = [dict(item) for item in profile.head_tolerations]
    if profile.worker_tolerations:
        worker_pod["tolerations"] = [dict(item) for item in profile.worker_tolerations]
    if profile.head_priority_class_name:
        head_pod["priorityClassName"] = profile.head_priority_class_name
    if profile.worker_priority_class_name:
        worker_pod["priorityClassName"] = profile.worker_priority_class_name
    if profile.runtime_class_name is None:
        worker_pod.pop("runtimeClassName", None)
    return configmap, cluster
