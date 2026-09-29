"""Balanced production controller: strict at boundaries, permissive for valid jobs."""

from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from . import controller as engine
from .api_v1beta1 import Recipe, Run, RuntimeProfile
from .artifacts import ArtifactError, ArtifactRef
from .controller_entrypoint import main as health_wrapped_main
from .manifests_release import render_attempt
from .npu_health import NpuExporterHealthProvider
from .ray_jobs_stable import StableRayJobsRest


_BaseReconciler = engine.Reconciler
_base_trusted_result = engine._trusted_result


StableRuntimeProfile = RuntimeProfile


def trusted_result(
    document: Mapping[str, Any] | None,
    run: Run,
    attempt: int,
) -> Mapping[str, Any] | None:
    result = _base_trusted_result(document, run, attempt)
    if result is None or result.get("status") != "PASS":
        return result
    checkpoint_available = result.get("checkpointAvailable")
    if not isinstance(checkpoint_available, bool):
        raise engine.ControllerError("PASS result lacks checkpoint availability evidence")
    if result.get("checkpointConsistent") is not True:
        raise engine.ControllerError("PASS result lacks checkpoint consistency evidence")
    checkpoint = result.get("checkpoint")
    if checkpoint_available and (not isinstance(checkpoint, Mapping) or not checkpoint):
        raise engine.ControllerError("PASS result declares an available checkpoint without metadata")
    if not checkpoint_available and checkpoint is not None:
        raise engine.ControllerError("PASS result includes checkpoint metadata marked unavailable")
    uri = result.get("outputArtifact")
    try:
        ref = ArtifactRef.parse(uri) if isinstance(uri, str) else None
    except ArtifactError as error:
        raise engine.ControllerError(f"PASS result output artifact is invalid: {error}") from error
    if ref is None or ref.namespace != run.identity.namespace or ref.name != f"{run.identity.name}-output":
        raise engine.ControllerError("PASS result output artifact ownership differs")
    if not ref.version.startswith(f"attempt-{attempt:02d}-"):
        raise engine.ControllerError("PASS result output artifact attempt differs")
    output_provider = result.get("outputProvider", "gateway")
    if output_provider not in {"gateway", "workspace"}:
        raise engine.ControllerError("PASS result output provider is invalid")
    if output_provider == "workspace":
        output_path = result.get("outputPath")
        if not isinstance(output_path, str) or not output_path.startswith("/"):
            raise engine.ControllerError("PASS workspace result lacks an absolute output path")
    return result


class StableReconciler(_BaseReconciler):
    def _reconcile_valid(
        self,
        resource: Mapping[str, Any],
        run: Run,
        profile: RuntimeProfile,
        recipe: Recipe,
    ) -> str:
        self.health = None
        if run.max_replacements > 0 and profile.health_provider != "npu-exporter":
            raise engine.ControllerError(
                "automatic node replacement requires integrations.healthProvider=npu-exporter"
            )
        if profile.health_provider == "npu-exporter":
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
    def run_controller(controller_argv: Sequence[str] | None) -> int:
        return engine.main(
            controller_argv,
            reconciler_factory=StableReconciler,
            jobs_factory=StableRayJobsRest,
            profile_loader=RuntimeProfile.from_resource,
            recipe_loader=Recipe.from_resource,
            renderer=render_attempt,
            result_validator=trusted_result,
        )

    return health_wrapped_main(argv, controller=run_controller)


if __name__ == "__main__":
    raise SystemExit(main())
