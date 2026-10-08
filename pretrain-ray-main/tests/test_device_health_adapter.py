import unittest

from kcc_training.adapters.device_health import KubernetesDeviceHealthAdapter


class FakeReader:
    def __init__(self, nodes, pods):
        self.nodes = nodes
        self.pods = pods

    def list_json(self, kind, **kwargs):
        del kwargs
        return self.nodes if kind == "nodes" else self.pods


class DeviceHealthTests(unittest.TestCase):
    def test_reports_capacity_and_non_terminal_owner(self) -> None:
        nodes = {
            "items": [
                {
                    "metadata": {"name": "node-a"},
                    "status": {
                        "conditions": [{"type": "Ready", "status": "True"}],
                        "allocatable": {"huawei.com/Ascend910": "8"},
                    },
                }
            ]
        }
        pods = {
            "items": [
                {
                    "metadata": {"namespace": "jobs", "name": "other"},
                    "spec": {
                        "nodeName": "node-a",
                        "containers": [
                            {
                                "resources": {
                                    "limits": {"huawei.com/Ascend910": "8"}
                                }
                            }
                        ],
                    },
                    "status": {"phase": "Running"},
                }
            ]
        }
        adapter = KubernetesDeviceHealthAdapter(
            FakeReader(nodes, pods), "huawei.com/Ascend910"
        )
        result = adapter.observe(("node-a",))
        self.assertTrue(result["healthy"])
        self.assertFalse(result["idle"])
        self.assertEqual(result["nodes"]["node-a"]["owners"][0]["npu"], 8)

    def test_missing_node_fails_closed(self) -> None:
        result = KubernetesDeviceHealthAdapter(
            FakeReader({"items": []}, {"items": []}), "huawei.com/Ascend910"
        ).observe(("missing",))
        self.assertFalse(result["healthy"])


if __name__ == "__main__":
    unittest.main()

