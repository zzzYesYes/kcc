import json
from pathlib import Path
import re
import tomllib
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]
CHART = ROOT / "deploy/helm/kcc-training-stable"


def crd(name: str) -> dict:
    return yaml.safe_load((CHART / "crds" / name).read_text(encoding="utf-8"))


class DistributionTests(unittest.TestCase):
    def test_release_versions_are_identical(self) -> None:
        project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
        version = project["project"]["version"]
        chart = yaml.safe_load((CHART / "Chart.yaml").read_text(encoding="utf-8"))
        module = (ROOT / "src/kcc_training/__init__.py").read_text(encoding="utf-8")
        self.assertEqual(chart["version"], version)
        self.assertEqual(chart["appVersion"], version)
        self.assertRegex(module, rf'__version__\s*=\s*"{re.escape(version)}"')

    def test_only_stable_chart_is_publishable(self) -> None:
        self.assertFalse(
            yaml.safe_load((CHART / "Chart.yaml").read_text(encoding="utf-8")).get(
                "deprecated", False
            )
        )
        legacy = (
            "kcc-training",
            "kcc-training-final",
            "kcc-training-release",
            "kcc-training-v1",
            "kcc-training-v2",
        )
        for name in legacy:
            with self.subTest(chart=name):
                metadata = yaml.safe_load(
                    (ROOT / "deploy/helm" / name / "Chart.yaml").read_text(
                        encoding="utf-8"
                    )
                )
                self.assertTrue(metadata.get("deprecated"))
                self.assertIn("DEPRECATED", metadata["description"])

    def test_runtime_profile_crd_matches_supported_portability_fields(self) -> None:
        profile_crd = crd("trainingruntimeprofiles.yaml")
        schema = profile_crd["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]["properties"]["spec"]["properties"]
        self.assertEqual(
            schema["integrations"]["properties"]["rankTableProvider"]["enum"],
            ["clusterd"],
        )
        artifact_provider = schema["integrations"]["properties"]["artifactProvider"]
        self.assertEqual(
            artifact_provider["enum"], ["gateway", "workspace"]
        )
        self.assertEqual(artifact_provider["default"], "gateway")
        accelerator = schema["accelerator"]
        physical_device_ids = accelerator["properties"]["physicalDeviceIDs"]
        self.assertEqual(physical_device_ids["x-kubernetes-list-type"], "set")
        self.assertEqual(physical_device_ids["maxItems"], 64)
        self.assertEqual(physical_device_ids["items"]["maximum"], 63)
        self.assertTrue(
            any(
                "physicalDeviceIDs" in validation["rule"]
                and "devicesPerNode" in validation["rule"]
                for validation in accelerator["x-kubernetes-validations"]
            )
        )
        pull_secret = schema["images"]["properties"]["pullSecrets"]["items"]
        self.assertLessEqual(len("registry.credentials"), pull_secret["maxLength"])
        self.assertIsNotNone(re.fullmatch(pull_secret["pattern"], "registry.credentials"))
        for role in ("head", "worker"):
            quantities = schema["podTemplate"]["properties"][role]["properties"][
                "resources"
            ]["properties"]
            for category in ("requests", "limits"):
                quantity = quantities[category]["additionalProperties"]
                self.assertEqual(
                    {item["type"] for item in quantity["anyOf"]},
                    {"integer", "string"},
                )
                self.assertTrue(quantity["x-kubernetes-int-or-string"])
        profile_spec = profile_crd["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]["properties"]["spec"]
        validations = profile_spec["x-kubernetes-validations"]
        self.assertFalse(
            any(item.get("rule") == "self == oldSelf" for item in validations)
        )
        self.assertTrue(
            any("activeNodes" in item.get("rule", "") for item in validations)
        )

    def test_training_run_crd_caps_combined_recovery_attempts(self) -> None:
        validations = crd("trainingruns.yaml")["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]["properties"]["spec"]["x-kubernetes-validations"]
        self.assertTrue(
            any(
                "maxReplacements" in validation["rule"]
                and "sameTopologyRetries" in validation["rule"]
                and "<= 100" in validation["rule"]
                for validation in validations
            )
        )

    def test_training_run_contract_exposes_checkpoint_stop_control(self) -> None:
        schema = crd("trainingruns.yaml")["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]
        suspend_mode = schema["properties"]["spec"]["properties"][
            "suspendMode"
        ]
        self.assertEqual(
            suspend_mode["enum"],
            ["Immediate", "AfterCheckpoint"],
        )
        status = schema["properties"]["status"]["properties"]
        self.assertIn("Stopping", status["phase"]["enum"])
        self.assertIn("stopRequestGeneration", status)
        self.assertIn("stopBaselineIteration", status)

    def test_training_run_contract_exposes_serial_dependency(self) -> None:
        schema = crd("trainingruns.yaml")["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]
        spec = schema["properties"]["spec"]
        self.assertIn("dependsOn", spec["properties"])
        self.assertTrue(
            any(
                "dependsOn" in validation["rule"]
                for validation in spec["x-kubernetes-validations"]
            )
        )
        self.assertIn("Queued", schema["properties"]["status"]["properties"]["phase"]["enum"])
        contract = json.loads(
            (ROOT / "contracts/training-run.schema.json").read_text(encoding="utf-8")
        )
        self.assertIn("dependsOn", contract["properties"]["spec"]["properties"])
        self.assertIn("Queued", contract["$defs"]["status"]["properties"]["phase"]["enum"])

    def test_training_run_status_covers_runtime_checkpoint_and_diagnosis(self) -> None:
        status = crd("trainingruns.yaml")["spec"]["versions"][0]["schema"][
            "openAPIV3Schema"
        ]["properties"]["status"]
        self.assertNotIn("x-kubernetes-preserve-unknown-fields", status)
        checkpoint = status["properties"]["checkpoint"]["properties"]
        for name in (
            "available",
            "iteration",
            "snapshotSha256",
            "hashMode",
            "sampleBytesPerFile",
        ):
            self.assertIn(name, checkpoint)
        diagnosis = status["properties"]["diagnosis"]["properties"]
        self.assertIn("reportedFailedNodes", diagnosis)

    def test_training_run_json_contract_exposes_same_checkpoint_fields(self) -> None:
        contract = json.loads(
            (ROOT / "contracts/training-run.schema.json").read_text(encoding="utf-8")
        )
        checkpoint = contract["$defs"]["checkpoint"]["properties"]
        self.assertEqual(checkpoint["hashMode"]["const"], "sampled-v1")
        self.assertIn("sampleBytesPerFile", checkpoint)
        self.assertIn(
            "reportedFailedNodes",
            contract["$defs"]["diagnosis"]["properties"],
        )

    def test_crd_schemas_use_kubernetes_structural_collections(self) -> None:
        def walk(value: object, path: str = "$"):
            if isinstance(value, dict):
                yield path, value
                for key, child in value.items():
                    yield from walk(child, f"{path}.{key}")
            elif isinstance(value, list):
                for index, child in enumerate(value):
                    yield from walk(child, f"{path}[{index}]")

        for name in (
            "trainingrecipes.yaml",
            "trainingruns.yaml",
            "trainingruntimeprofiles.yaml",
        ):
            schema = crd(name)["spec"]["versions"][0]["schema"]["openAPIV3Schema"]
            for path, node in walk(schema):
                self.assertFalse(
                    "properties" in node and "additionalProperties" in node,
                    f"{name}:{path} mixes properties and additionalProperties",
                )
                self.assertNotEqual(
                    node.get("uniqueItems"),
                    True,
                    f"{name}:{path} uses quadratic uniqueItems",
                )

        profile = crd("trainingruntimeprofiles.yaml")["spec"]["versions"][0][
            "schema"
        ]["openAPIV3Schema"]["properties"]["spec"]["properties"]
        collections = {
            "pullSecrets": profile["images"]["properties"]["pullSecrets"],
            "activeNodes": profile["scheduling"]["properties"]["activeNodes"],
            "spareNodes": profile["scheduling"]["properties"]["spareNodes"],
        }
        for name, collection in collections.items():
            self.assertEqual(
                collection.get("x-kubernetes-list-type"),
                "set",
                name,
            )
        self.assertEqual(collections["activeNodes"]["maxItems"], 1024)
        self.assertEqual(collections["spareNodes"]["maxItems"], 100)
        for collection in collections.values():
            self.assertEqual(collection["items"]["maxLength"], 253)

    def test_chart_defaults_use_release_scoped_runtime_sa_and_pdb(self) -> None:
        values = yaml.safe_load((CHART / "values.yaml").read_text(encoding="utf-8"))
        self.assertEqual(values["runtimeServiceAccount"]["name"], "")
        self.assertTrue(values["controller"]["podDisruptionBudget"]["enabled"])
        self.assertEqual(
            values["controller"]["podDisruptionBudget"]["minAvailable"],
            1,
        )

        template = (CHART / "templates/poddisruptionbudget.yaml").read_text(
            encoding="utf-8"
        )
        self.assertIn(
            "minAvailable: "
            "{{ .Values.controller.podDisruptionBudget.minAvailable | toJson }}",
            template,
        )

    def test_preflight_checks_token_key_and_supports_bundle_chart(self) -> None:
        script = (ROOT / "scripts/stable-preflight.sh").read_text(encoding="utf-8")
        self.assertIn('print(f"tokenSecret={token}")', script)
        self.assertIn('"token" in (json.load(sys.stdin).get("data") or {})', script)
        self.assertIn("KCC_PREFLIGHT_RENDER_ONLY", script)
        self.assertIn("kcc-training-*.tgz", script)

    def test_image_dockerfiles_can_build_from_non_root_bases(self) -> None:
        for name in ("controller", "head", "worker"):
            dockerfile = (ROOT / "docker" / f"Dockerfile.{name}").read_text(encoding="utf-8")
            self.assertIn("USER root", dockerfile, name)
        controller = (ROOT / "docker" / "Dockerfile.controller").read_text(encoding="utf-8")
        self.assertIn("USER 65532:65532", controller)

    def test_image_workflows_enable_arm64_emulation(self) -> None:
        workflows = (
            ROOT / ".github/workflows/ci.yml",
            ROOT.parent / ".github/workflows/kcc-training.yml",
        )
        for workflow in workflows:
            with self.subTest(workflow=workflow):
                content = workflow.read_text(encoding="utf-8")
                self.assertIn("docker/setup-qemu-action@v3", content)
                self.assertIn("worker_platform", content)

if __name__ == "__main__":
    unittest.main()
