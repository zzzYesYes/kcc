import unittest
from unittest.mock import patch

from kcc_training import controller as engine
from kcc_training.manifests_final import render_attempt
from kcc_training.production_controller import ProductionReconciler


class ProductionCompositionTests(unittest.TestCase):
    def test_reconciler_wires_exporter_provider(self):
        class Api:
            pass
        with patch("kcc_training.production_controller.NpuExporterHealthProvider") as provider:
            reconciler = ProductionReconciler(Api(), object(), runtime_service_account="runtime")
        provider.assert_called_once()
        self.assertIs(reconciler.health, provider.return_value)

    def test_final_renderer_is_explicit_composition_port(self):
        self.assertTrue(callable(render_attempt))
        self.assertTrue(hasattr(engine, "render_attempt"))


if __name__ == "__main__":
    unittest.main()
