import unittest

from kcc_training.npu_health import NpuExporterHealthProvider, parse_metrics


METRICS = """machine_npu_nums 2
npu_chip_info_health_status{id="0"} 1
npu_chip_info_health_status{id="1"} 1
npu_chip_info_process_info_num{id="0"} 0
npu_chip_info_process_info_num{id="1"} 0
"""


class Api:
    def list(self, path):
        if path == "/api/v1/nodes":
            return {"items": [{
                "metadata": {"name": "node-a"},
                "status": {
                    "addresses": [{"type": "InternalIP", "address": "10.0.0.1"}],
                    "conditions": [{"type": "Ready", "status": "True"}],
                    "allocatable": {"huawei.com/Ascend910": "2"},
                },
            }]}
        return {"items": [{
            "metadata": {"name": "npu-exporter-1", "labels": {"app": "npu-exporter"}},
            "spec": {"nodeName": "node-a"}, "status": {"phase": "Running"},
        }]}


class NpuHealthTests(unittest.TestCase):
    def test_metrics_are_complete_and_idle(self):
        self.assertEqual(parse_metrics(METRICS)["processCount"], 0)
        provider = NpuExporterHealthProvider(
            Api(), exporter_namespace="npu-exporter", expected_devices=2, raw_get=lambda path: METRICS
        )
        result = provider.observe(("node-a",))
        self.assertTrue(result["complete"])
        self.assertTrue(result["nodes"]["node-a"]["hardwareHealthy"])
        self.assertTrue(result["nodes"]["node-a"]["idle"])

    def test_missing_health_sample_fails_closed(self):
        provider = NpuExporterHealthProvider(
            Api(), exporter_namespace="npu-exporter", expected_devices=2,
            raw_get=lambda path: METRICS.replace('npu_chip_info_health_status{id="1"} 1\n', ''),
        )
        result = provider.observe(("node-a",))
        self.assertFalse(result["complete"])
        self.assertIsNone(result["nodes"]["node-a"]["hardwareHealthy"])

    def test_duplicate_device_ids_do_not_count_as_complete_evidence(self):
        duplicate_ids = (
            METRICS
            .replace('npu_chip_info_health_status{id="1"} 1', 'npu_chip_info_health_status{id="0"} 1')
            .replace('npu_chip_info_process_info_num{id="1"} 0', 'npu_chip_info_process_info_num{id="0"} 0')
        )
        parsed = parse_metrics(duplicate_ids)
        self.assertEqual(parsed["healthDeviceCount"], 1)
        self.assertEqual(parsed["processDeviceCount"], 1)
        self.assertEqual(parsed["duplicateHealthIds"], ["0"])
        self.assertEqual(parsed["duplicateProcessIds"], ["0"])
        provider = NpuExporterHealthProvider(
            Api(), exporter_namespace="npu-exporter", expected_devices=2,
            raw_get=lambda path: duplicate_ids,
        )
        result = provider.observe(("node-a",))
        self.assertFalse(result["complete"])
        self.assertFalse(result["nodes"]["node-a"]["complete"])
        self.assertIsNone(result["nodes"]["node-a"]["hardwareHealthy"])

    def test_missing_process_device_sample_does_not_prove_idle(self):
        missing_process = METRICS.replace('npu_chip_info_process_info_num{id="1"} 0\n', '')
        provider = NpuExporterHealthProvider(
            Api(), exporter_namespace="npu-exporter", expected_devices=2,
            raw_get=lambda path: missing_process,
        )
        result = provider.observe(("node-a",))
        self.assertFalse(result["complete"])
        self.assertFalse(result["nodes"]["node-a"]["complete"])
        self.assertFalse(result["nodes"]["node-a"]["idle"])

if __name__ == "__main__":
    unittest.main()
