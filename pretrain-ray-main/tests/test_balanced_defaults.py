import json
import unittest
from unittest.mock import patch

from kcc_training.controller import ControllerError
from kcc_training.controller_stable import StableReconciler, StableRuntimeProfile, main as stable_main, trusted_result
from tests.test_api_v1beta1 import IMAGE, resource


class BalancedDefaultTests(unittest.TestCase):
    def test_profile_allows_no_spares_for_retry_only_policy(self):
        profile = StableRuntimeProfile.from_resource(resource("TrainingRuntimeProfile", "p", {
            "images": {"head": IMAGE, "worker": IMAGE}, "rayVersion": "2.49.0",
            "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8, "runtimeClassName": "ascend"},
            "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
            "scheduling": {"activeNodes": ["node-a"], "spareNodes": [], "headSelector": {}, "workerSelector": {}},
            "integrations": {"rankTableProvider": "clusterd", "healthProvider": "kubernetes"},
        }))
        self.assertEqual(profile.spare_nodes, ())

    def test_kubernetes_health_is_retry_only(self):
        from kcc_training.api_v1beta1 import Recipe, Run

        profile_doc = resource("TrainingRuntimeProfile", "p", {
            "images": {"head": IMAGE, "worker": IMAGE}, "rayVersion": "2.49.0",
            "accelerator": {"resourceName": "huawei.com/Ascend910", "devicesPerNode": 8, "runtimeClassName": "ascend"},
            "workspace": {"claimName": "workspace", "mountPath": "/workspace"},
            "scheduling": {"activeNodes": ["node-a"], "spareNodes": ["node-b"], "headSelector": {}, "workerSelector": {}},
            "integrations": {"rankTableProvider": "clusterd", "healthProvider": "kubernetes"},
        })
        recipe_doc = resource("TrainingRecipe", "r", {
            "framework": "mindspeed", "command": ["python", "train.py"],
            "workingDirectory": ".", "environment": {},
            "artifacts": {"source": "artifact://training/source/v1", "model": "artifact://training/model/v1", "data": "artifact://training/data/v1", "outputSubpath": "runs/x"},
        })
        run_doc = resource("TrainingRun", "run-1", {
            "runtimeProfile": "p", "recipe": "r", "workers": 1,
            "recovery": {"sameTopologyRetries": 1, "maxReplacements": 1, "noProgressSeconds": 60},
        })
        reconciler = StableReconciler(None, None, runtime_service_account="runtime")
        with self.assertRaisesRegex(ControllerError, "npu-exporter"):
            reconciler._reconcile_valid(
                run_doc,
                Run.from_resource(run_doc),
                StableRuntimeProfile.from_resource(profile_doc),
                Recipe.from_resource(recipe_doc),
            )

    def test_stable_composition_uses_explicit_dependencies(self):
        from kcc_training import controller as engine
        from kcc_training.ray_jobs_stable import StableRayJobsRest

        original_reconciler = engine.Reconciler
        original_jobs = engine.RayJobsRest
        with patch("kcc_training.controller_stable.engine.main", return_value=0) as controller_main:
            with patch("kcc_training.controller_stable.health_wrapped_main") as wrapped:
                wrapped.side_effect = lambda argv, *, controller: controller(argv)
                self.assertEqual(stable_main(["--namespace", "training"]), 0)
        kwargs = controller_main.call_args.kwargs
        self.assertIs(kwargs["reconciler_factory"], StableReconciler)
        self.assertIs(kwargs["jobs_factory"], StableRayJobsRest)
        self.assertIs(engine.Reconciler, original_reconciler)
        self.assertIs(engine.RayJobsRest, original_jobs)

    def test_success_does_not_require_a_checkpoint(self):
        from kcc_training.api_v1beta1 import Run
        run = Run.from_resource(resource("TrainingRun", "run-1", {
            "runtimeProfile": "p", "recipe": "r", "workers": 1,
            "recovery": {"sameTopologyRetries": 0, "maxReplacements": 0, "noProgressSeconds": 0},
        }))
        result = {
            "schemaVersion": "kcc-runtime-result/v1", "runUid": "uid-1", "attempt": 0,
            "status": "PASS", "checkpointAvailable": False, "checkpointConsistent": True, "checkpoint": None,
            "outputArtifact": "artifact://training/run-1-output/attempt-00-0123456789abcdef",
        }
        document = {
            "metadata": {"annotations": {"training.kcc.io/run-uid": "uid-1", "training.kcc.io/attempt": "0"}},
            "data": {"result.json": json.dumps(result)},
        }
        self.assertIsNotNone(trusted_result(document, run, 0))
        invalid = dict(result, checkpointConsistent=False)
        document["data"]["result.json"] = json.dumps(invalid)
        with self.assertRaisesRegex(ControllerError, "consistency"):
            trusted_result(document, run, 0)
        invalid = dict(result, checkpointAvailable=True, checkpoint=None)
        document["data"]["result.json"] = json.dumps(invalid)
        with self.assertRaisesRegex(ControllerError, "without metadata"):
            trusted_result(document, run, 0)
        result["checkpointAvailable"] = False
        result["checkpointConsistent"] = True
        del result["outputArtifact"]
        document["data"]["result.json"] = json.dumps(result)
        with self.assertRaises(ControllerError):
            trusted_result(document, run, 0)


if __name__ == "__main__":
    unittest.main()
