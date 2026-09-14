"""Release manifest renderer, including Artifact Gateway output credentials."""

from __future__ import annotations

import os
from typing import Any, Sequence

from .api_v1beta1 import Recipe, Run, RuntimeProfile
from .manifests_final import render_attempt as render_materialized


def render_attempt(
    run: Run,
    profile: RuntimeProfile,
    recipe: Recipe,
    *,
    attempt: int,
    active_nodes: Sequence[str],
    runtime_service_account: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    configmap, cluster = render_materialized(
        run,
        profile,
        recipe,
        attempt=attempt,
        active_nodes=active_nodes,
        runtime_service_account=runtime_service_account,
    )
    head_spec = cluster["spec"]["headGroupSpec"]["template"]["spec"]
    head = head_spec["containers"][0]
    if profile.artifact_provider == "workspace":
        return configmap, cluster
    head.setdefault("env", []).append(
        {"name": "KCC_ARTIFACT_GATEWAY", "value": os.environ["KCC_ARTIFACT_GATEWAY"]}
    )
    token_secret = os.environ.get("KCC_ARTIFACT_TOKEN_SECRET")
    if token_secret:
        head.setdefault("volumeMounts", []).append(
            {"name": "artifact-token", "mountPath": "/var/run/secrets/kcc-artifact", "readOnly": True}
        )
        head["env"].append(
            {"name": "KCC_ARTIFACT_TOKEN_FILE", "value": "/var/run/secrets/kcc-artifact/token"}
        )
    return configmap, cluster
