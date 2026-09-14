from pathlib import Path
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))

import cluster_config  # noqa: E402


class LegacyConfigTests(unittest.TestCase):
    def test_current_cluster_config_still_loads(self) -> None:
        value = cluster_config.load_config(ROOT / "config/cluster.yaml")
        self.assertEqual(value.kubernetes.namespace, "pretrain-ray")
        self.assertEqual(value.recovery.same_topology_retries, 2)
        self.assertTrue(value.topology.active_nodes)
        self.assertEqual(value.accelerator.resource_name, "huawei.com/Ascend910")
        self.assertEqual(value.accelerator.devices_per_node, 8)
        self.assertEqual(value.accelerator.runtime_class_name, "ascend")
        self.assertEqual(dict(value.topology.head_selector), {})
        self.assertEqual(dict(value.topology.worker_selector), {})
        self.assertEqual(value.training.workspace_host_path, Path("/mnt/models"))
        self.assertEqual(
            value.images.ray_head,
            cluster_config.DEFAULT_RAY_HEAD_IMAGE,
        )
        self.assertEqual(
            value.images.ray_worker,
            cluster_config.DEFAULT_RAY_WORKER_IMAGE,
        )
        self.assertEqual(value.images.pull_policy, "IfNotPresent")

    def test_a3_profile_has_explicit_hardware_contract_and_no_spares(self) -> None:
        value = cluster_config.load_config(ROOT / "config/cluster.a3.yaml")
        self.assertEqual(value.topology.active_nodes, ("a3-server-00",))
        self.assertEqual(value.topology.spare_nodes, ())
        self.assertEqual(value.accelerator.devices_per_node, 8)
        self.assertIsNone(value.accelerator.runtime_class_name)
        self.assertEqual(value.training.workspace_host_path, Path("/home"))
        self.assertEqual(
            value.training.working_directory,
            "/mnt/models/zc/CODE/MindSpeed-LLM-v2.3.0",
        )
        self.assertEqual(
            value.images.ray_head,
            cluster_config.DEFAULT_RAY_HEAD_IMAGE,
        )
        self.assertEqual(
            value.images.ray_worker,
            (
                "swr.cn-south-1.myhuaweicloud.com/ascendhub/verl_pt27_25rc3@sha256:"
                "2d25563176ab5313bbef71fea36c64a926391a404213165133f5fcba3c80768a"
            ),
        )
        self.assertEqual(value.images.pull_policy, "IfNotPresent")
        self.assertEqual(
            value.topology.worker_selector["node.kubernetes.io/npu.chip.name"],
            "Ascend910",
        )

    def test_profile_image_validation_is_strict(self) -> None:
        valid_tag = "registry.example/ascend/worker:a3-arm"
        self.assertEqual(
            cluster_config.validate_profile_image(valid_tag, "image"),
            valid_tag,
        )
        for invalid in (
            "repo/image",
            "repo/image:latest",
            "repo/image@sha256:abc",
            "repo/image tag:v1",
        ):
            with self.subTest(image=invalid):
                with self.assertRaises(cluster_config.ClusterConfigError):
                    cluster_config.validate_profile_image(invalid, "image")
        with self.assertRaises(cluster_config.ClusterConfigError):
            cluster_config.validate_image_pull_policy("Sometimes", "policy")

    def test_v1_config_keeps_legacy_image_defaults(self) -> None:
        document = yaml.safe_load(
            (ROOT / "config/cluster.yaml").read_text(encoding="utf-8")
        )
        document["schemaVersion"] = cluster_config.PREVIOUS_CONFIG_SCHEMA
        document.pop("accelerator")
        document.pop("images")
        document["topology"].pop("headSelector")
        document["topology"].pop("workerSelector")
        document["npuCheck"]["resourceName"] = "huawei.com/Ascend910"
        document["training"].pop("workspaceHostPath")

        with tempfile.TemporaryDirectory() as temporary_directory:
            path = Path(temporary_directory) / "cluster.v1.yaml"
            path.write_text(
                yaml.safe_dump(document),
                encoding="utf-8",
            )
            value = cluster_config.load_config(path)

        self.assertEqual(
            value.images.ray_head,
            cluster_config.DEFAULT_RAY_HEAD_IMAGE,
        )
        self.assertEqual(
            value.images.ray_worker,
            cluster_config.DEFAULT_RAY_WORKER_IMAGE,
        )
        self.assertEqual(value.images.pull_policy, "IfNotPresent")


if __name__ == "__main__":
    unittest.main()
