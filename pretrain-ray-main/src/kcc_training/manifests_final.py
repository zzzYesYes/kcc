"""Production attempt renderer with artifact materialization and no host paths."""

from __future__ import annotations

import os
from typing import Any, Sequence

from .api_v1beta1 import Recipe, Run, RuntimeProfile
from .raycluster import render_attempt as render_base


def render_attempt(
    run: Run,
    profile: RuntimeProfile,
    recipe: Recipe,
    *,
    attempt: int,
    active_nodes: Sequence[str],
    runtime_service_account: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    configmap, cluster = render_base(
        run,
        profile,
        recipe,
        attempt=attempt,
        active_nodes=active_nodes,
        runtime_service_account=runtime_service_account,
    )
    head_spec = cluster["spec"]["headGroupSpec"]["template"]["spec"]
    worker_spec = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]
    for pod_spec in (head_spec, worker_spec):
        ranktable_volume = next(
            volume
            for volume in pod_spec["volumes"]
            if volume.get("name") == "ranktable"
        )
        ranktable_volume["configMap"]["optional"] = True
    if profile.artifact_provider == "workspace":
        return configmap, cluster
    gateway = os.environ.get("KCC_ARTIFACT_GATEWAY")
    if not gateway:
        raise ValueError("KCC_ARTIFACT_GATEWAY is required for gateway artifactProvider")
    token_secret = os.environ.get("KCC_ARTIFACT_TOKEN_SECRET")
    materializer_mounts = [
        {"name": "run-spec", "mountPath": "/etc/kcc/run", "readOnly": True},
        {"name": "workspace", "mountPath": profile.workspace_mount_path},
    ]
    environment = [{"name": "KCC_ARTIFACT_GATEWAY", "value": gateway}]
    if token_secret:
        head_spec["volumes"].append(
            {"name": "artifact-token", "secret": {"secretName": token_secret, "defaultMode": 0o440}}
        )
        materializer_mounts.append(
            {"name": "artifact-token", "mountPath": "/var/run/secrets/kcc-artifact", "readOnly": True}
        )
        environment.append(
            {"name": "KCC_ARTIFACT_TOKEN_FILE", "value": "/var/run/secrets/kcc-artifact/token"}
        )
    head_spec["initContainers"] = [
        {
            "name": "materialize-artifacts",
            "image": profile.head_image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["python", "-m", "kcc_training.runtime.materializer"],
            "args": ["--spec", "/etc/kcc/run/run.json"],
            "env": environment,
            "volumeMounts": materializer_mounts,
            "securityContext": {
                "allowPrivilegeEscalation": False,
                "capabilities": {"drop": ["ALL"]},
            },
        }
    ]
    return configmap, cluster

