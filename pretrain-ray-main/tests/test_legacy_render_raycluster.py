from pathlib import Path
import json
import sys
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))

import render_raycluster  # noqa: E402


class LegacyRayClusterRenderTests(unittest.TestCase):
    def test_a3_profile_replaces_all_template_hardware_values(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            output = Path(temporary_directory) / "raycluster.yaml"
            render_raycluster.render_manifest(
                base_manifest=ROOT / "ray_startup_bundle/raycluster.yaml",
                output_manifest=output,
                node_names=("a3-server-00",),
                namespace="pretrain-ray",
                cluster="pretrain-a3-smoke",
                head_node="server-00",
                npu_resource="huawei.com/Ascend910",
                devices_per_node=16,
                workspace_host_path=Path("/home"),
                ray_head_image="registry.example/ray/head:test",
                ray_worker_image=(
                    "swr.cn-south-1.myhuaweicloud.com/ascendhub/verl_pt27_25rc3@sha256:"
                    "2d25563176ab5313bbef71fea36c64a926391a404213165133f5fcba3c80768a"
                ),
                ray_image_pull_policy="IfNotPresent",
                runtime_class_name=None,
                head_selector={"kubernetes.io/arch": "amd64"},
                worker_selector={
                    "kubernetes.io/arch": "arm64",
                    "node.kubernetes.io/npu.chip.name": "Ascend910",
                },
                runtime_configmap="pretrain-a3-smoke-hccl-runtime",
                run_id="a3-smoke",
            )

            rendered_text = output.read_text(encoding="utf-8")
            self.assertNotIn("910B3", rendered_text)
            self.assertNotIn("__KCC_RAY_", rendered_text)
            self.assertNotIn("0c263b4d", rendered_text)
            documents = tuple(yaml.safe_load_all(rendered_text))
            cluster = next(item for item in documents if item.get("kind") == "RayCluster")
            spec = cluster["spec"]
            worker_group = spec["workerGroupSpecs"][0]
            worker_spec = worker_group["template"]["spec"]
            self.assertEqual(worker_group["replicas"], 1)
            self.assertNotIn("runtimeClassName", worker_spec)
            self.assertEqual(
                worker_spec["nodeSelector"],
                {
                    "kubernetes.io/arch": "arm64",
                    "node.kubernetes.io/npu.chip.name": "Ascend910",
                },
            )
            ray_resources = json.loads(
                json.loads(worker_group["rayStartParams"]["resources"])
            )
            self.assertEqual(ray_resources, {"NPU": 16, "trainctl_worker": 1})
            container = next(
                item for item in worker_spec["containers"] if item["name"] == "ray-worker"
            )
            self.assertEqual(
                container["image"],
                (
                    "swr.cn-south-1.myhuaweicloud.com/ascendhub/verl_pt27_25rc3@sha256:"
                    "2d25563176ab5313bbef71fea36c64a926391a404213165133f5fcba3c80768a"
                ),
            )
            self.assertEqual(container["imagePullPolicy"], "IfNotPresent")
            self.assertEqual(
                container["resources"]["requests"]["huawei.com/Ascend910"],
                "16",
            )
            self.assertEqual(
                container["resources"]["limits"]["huawei.com/Ascend910"],
                "16",
            )
            models_mount = next(
                item for item in container["volumeMounts"] if item["name"] == "models"
            )
            self.assertEqual(models_mount["mountPath"], "/mnt/models")
            models_volume = next(
                item for item in worker_spec["volumes"] if item["name"] == "models"
            )
            self.assertEqual(models_volume["hostPath"]["path"], "/home")
            head_spec = spec["headGroupSpec"]["template"]["spec"]
            head_container = next(
                item for item in head_spec["containers"] if item["name"] == "ray-head"
            )
            self.assertEqual(
                head_container["image"],
                "registry.example/ray/head:test",
            )
            self.assertEqual(head_container["imagePullPolicy"], "IfNotPresent")
            self.assertEqual(
                spec["headGroupSpec"]["template"]["spec"]["nodeSelector"],
                {
                    "kubernetes.io/arch": "amd64",
                    "kubernetes.io/hostname": "server-00",
                },
            )


if __name__ == "__main__":
    unittest.main()
