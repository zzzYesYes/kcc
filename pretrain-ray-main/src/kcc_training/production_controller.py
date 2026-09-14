"""Production composition root for controller ports and providers."""

from __future__ import annotations

import os
from typing import Sequence

from . import controller as engine
from .controller_entrypoint import main as health_wrapped_main
from .manifests_final import render_attempt
from .npu_health import NpuExporterHealthProvider


_BaseReconciler = engine.Reconciler


class ProductionReconciler(_BaseReconciler):
    def __init__(self, api, jobs, *, runtime_service_account, health=None, stable_diagnosis_samples=2, **controller_options):
        del health
        exporter = NpuExporterHealthProvider(
            api,
            exporter_namespace=os.environ.get("KCC_NPU_EXPORTER_NAMESPACE", "npu-exporter"),
            exporter_app=os.environ.get("KCC_NPU_EXPORTER_APP", "npu-exporter"),
            exporter_port=int(os.environ.get("KCC_NPU_EXPORTER_PORT", "8082")),
            resource_name=os.environ.get("KCC_NPU_RESOURCE", "huawei.com/Ascend910"),
            expected_devices=int(os.environ.get("KCC_DEVICES_PER_NODE", "8")),
        )
        super().__init__(
            api,
            jobs,
            runtime_service_account=runtime_service_account,
            health=exporter,
            stable_diagnosis_samples=stable_diagnosis_samples,
            **controller_options,
        )


def main(argv: Sequence[str] | None = None) -> int:
    # The reusable engine exposes its renderer and reconciler as composition
    # points.  Bind production providers once before the polling loop starts.
    engine.render_attempt = render_attempt
    engine.Reconciler = ProductionReconciler
    return health_wrapped_main(argv)


if __name__ == "__main__":
    raise SystemExit(main())

