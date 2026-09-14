"""Hardened production composition root used by the release Helm chart."""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from . import controller as engine
from .api_v1beta1 import Recipe, Run, RuntimeProfile
from .artifacts import ArtifactError, ArtifactRef
from .controller_entrypoint import main as health_wrapped_main
from .manifests_release import render_attempt
from .npu_health import NpuExporterHealthProvider
from .ray_jobs_release import ReleaseRayJobsRest


_BaseReconciler = engine.Reconciler
_base_trusted_result = engine._trusted_result


def trusted_result(document: Mapping[str, Any] | None, run: Run, attempt: int) -> Mapping[str, Any] | None:
    result = _base_trusted_result(document, run, attempt)
    if result is None or result.get("status") != "PASS":
        return result
    if result.get("checkpointConsistent") is not True:
        raise engine.ControllerError("PASS result lacks a consistent checkpoint")
    uri = result.get("outputArtifact")
    try:
        ref = ArtifactRef.parse(uri) if isinstance(uri, str) else None
    except ArtifactError as error:
        raise engine.ControllerError(f"PASS result output artifact is invalid: {error}") from error
    if ref is None or ref.namespace != run.identity.namespace or ref.name != f"{run.identity.name}-output":
        raise engine.ControllerError("PASS result output artifact ownership differs")
    if not ref.version.startswith(f"attempt-{attempt:02d}-"):
        raise engine.ControllerError("PASS result output artifact attempt differs")
    return result


class ReleaseReconciler(_BaseReconciler):
    def __init__(self, api, jobs, *, runtime_service_account, health=None, stable_diagnosis_samples=2, **controller_options):
        super().__init__(
            api,
            jobs,
            runtime_service_account=runtime_service_account,
            health=health,
            stable_diagnosis_samples=stable_diagnosis_samples,
            **controller_options,
        )

    def _reconcile_valid(
        self,
        resource: Mapping[str, Any],
        run: Run,
        profile: RuntimeProfile,
        recipe: Recipe,
    ) -> str:
        if profile.health_provider != "npu-exporter":
            raise engine.ControllerError(
                "release controller currently requires integrations.healthProvider=npu-exporter"
            )
        self.health = NpuExporterHealthProvider(
            self.api,
            exporter_namespace=os.environ.get("KCC_NPU_EXPORTER_NAMESPACE", "npu-exporter"),
            exporter_app=os.environ.get("KCC_NPU_EXPORTER_APP", "npu-exporter"),
            exporter_port=int(os.environ.get("KCC_NPU_EXPORTER_PORT", "8082")),
            resource_name=profile.resource_name,
            expected_devices=profile.devices_per_node,
        )
        return super()._reconcile_valid(resource, run, profile, recipe)


def main(argv: Sequence[str] | None = None) -> int:
    engine.render_attempt = render_attempt
    engine._trusted_result = trusted_result
    engine.RayJobsRest = ReleaseRayJobsRest
    engine.Reconciler = ReleaseReconciler
    return health_wrapped_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())
