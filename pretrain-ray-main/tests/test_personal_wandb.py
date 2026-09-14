import json
import unittest
from dataclasses import replace

from kcc_training.api_v1beta1 import ApiValidationError, Run
from kcc_training.raycluster import render_attempt
from tests.test_api_v1beta1 import resource
from tests.test_raycluster import objects


class PersonalWandbTests(unittest.TestCase):
    def document(self, name, secret):
        return resource("TrainingRun", name, {
            "runtimeProfile": "a3", "recipe": "recipe", "workers": 1,
            "recovery": {"sameTopologyRetries": 0, "maxReplacements": 0, "noProgressSeconds": 600},
            "training": {"wandbSecretRef": secret},
        })

    def test_own_secret_including_long_run_name(self):
        for name in ("personal-run", "a" * 63):
            run = Run.from_resource(self.document(name, name + "-wandb"))
            self.assertEqual(run.wandb_secret_ref, name + "-wandb")

    def test_rejects_another_runs_secret(self):
        with self.assertRaisesRegex(ApiValidationError, "must belong"):
            Run.from_resource(self.document("personal-run", "other-wandb"))

    def test_pod_uses_personal_key_without_configmap_credentials(self):
        run, profile, recipe = objects()
        run = replace(run, wandb_secret_ref="run-1-wandb", environment={"WANDB_MODE": "online"})
        configmap, cluster = render_attempt(
            run, profile, recipe, attempt=0,
            active_nodes=("node-a", "node-b"), runtime_service_account="runtime",
        )
        env = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["env"]
        key = next(item for item in env if item["name"] == "WANDB_API_KEY")
        self.assertEqual(key["valueFrom"]["secretKeyRef"], {
            "name": "run-1-wandb", "key": "WANDB_API_KEY",
        })
        self.assertNotIn("WANDB_API_KEY", json.loads(configmap["data"]["run.json"])["training"]["environment"])

    def test_disabled_wandb_does_not_mount_secret(self):
        run, profile, recipe = objects()
        run = replace(run, wandb_secret_ref="run-1-wandb", environment={"WANDB_MODE": "disabled"})
        _, cluster = render_attempt(
            run, profile, recipe, attempt=0,
            active_nodes=("node-a", "node-b"), runtime_service_account="runtime",
        )
        env = cluster["spec"]["workerGroupSpecs"][0]["template"]["spec"]["containers"][0]["env"]
        self.assertNotIn("WANDB_API_KEY", [item["name"] for item in env])
