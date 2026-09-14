from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import Mock, patch

from kcc_training.runtime.coordinator_stable import execute
from kcc_training.runtime import coordinator as coordinator_module
from kcc_training.runtime import coordinator_release as release_module
from kcc_training.artifacts import ArtifactError
from kcc_training.runtime.coordinator_release import run as run_release


class StableRuntimeTests(unittest.TestCase):
    def test_completed_training_without_checkpoint_is_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            spec = SimpleNamespace(output_root=Path(temporary), workers=2)
            raw = {
                "status": "FAIL",
                "failureScope": "checkpoint",
                "checkpointConsistent": False,
                "checkpoint": None,
                "workers": [{"status": "PASS"}, {"status": "PASS"}],
            }
            with patch("kcc_training.runtime.coordinator_stable.core.execute", return_value=raw):
                result = execute(spec)
            self.assertEqual(result["status"], "PASS")
            self.assertFalse(result["checkpointAvailable"])

    def test_checkpoint_disagreement_is_not_hidden(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkpoint = root / "checkpoints"
            checkpoint.mkdir()
            (checkpoint / "latest_checkpointed_iteration.txt").write_text("3\n")
            spec = SimpleNamespace(output_root=root, workers=2)
            raw = {
                "status": "FAIL",
                "failureScope": "checkpoint",
                "checkpointConsistent": False,
                "workers": [{"status": "PASS"}, {"status": "PASS"}],
            }
            with patch("kcc_training.runtime.coordinator_stable.core.execute", return_value=raw):
                result = execute(spec)
            self.assertEqual(result["status"], "FAIL")

    def test_artifact_failure_preserves_successful_training_evidence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = SimpleNamespace(
                output_root=root,
                checkpoint_root=root / "checkpoints",
                workers=2,
                devices_per_node=8,
                command=("python", "train.py"),
                working_directory=root,
                environment={},
                run_name="run-1",
                namespace="training",
                run_uid="uid-1",
                attempt=0,
            )
            checkpoint = {"snapshotSha256": "abc", "iteration": 2}
            trained = {
                "schemaVersion": "kcc-runtime-result/v1",
                "runName": "run-1",
                "namespace": "training",
                "runUid": "uid-1",
                "attempt": 0,
                "status": "PASS",
                "checkpointConsistent": True,
                "checkpointAvailable": True,
                "checkpoint": checkpoint,
                "failureScope": None,
                "failedNodes": [],
                "workers": [{"status": "PASS"}, {"status": "PASS"}],
            }
            publish_calls = []

            def fail_publish(*args):
                publish_calls.append(args)
                raise ArtifactError("gateway unavailable")

            result = run_release(
                spec,
                SimpleNamespace(),
                execute_fn=lambda _spec: trained,
                publish_fn=fail_publish,
                sleep_fn=lambda _seconds: None,
            )
        self.assertEqual(result["status"], "FAIL")
        self.assertEqual(result["trainingStatus"], "PASS")
        self.assertEqual(result["failureScope"], "artifact")
        self.assertTrue(result["publicationRetryable"])
        self.assertEqual(result["publicationAttempts"], 3)
        self.assertEqual(len(publish_calls), 3)
        self.assertTrue(result["checkpointConsistent"])
        self.assertEqual(result["checkpoint"], checkpoint)

    def test_next_attempt_reuses_completion_receipt_without_training(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            checkpoint_root = root / "checkpoints"
            selected = checkpoint_root / "iter_0000001"
            selected.mkdir(parents=True)
            (checkpoint_root / "latest_checkpointed_iteration.txt").write_text(
                "1\n", encoding="utf-8"
            )
            (selected / "model.pt").write_bytes(b"checkpoint")
            checkpoint = release_module.snapshot(checkpoint_root)
            common = {
                "output_root": root,
                "checkpoint_root": checkpoint_root,
                "workers": 2,
                "devices_per_node": 8,
                "command": ("python", "train.py"),
                "working_directory": root,
                "environment": {},
                "run_name": "run-1",
                "namespace": "training",
                "run_uid": "uid-1",
            }
            first_spec = SimpleNamespace(**common, attempt=0)
            trained = {
                "schemaVersion": "kcc-runtime-result/v1",
                "runName": "run-1",
                "namespace": "training",
                "runUid": "uid-1",
                "attempt": 0,
                "status": "PASS",
                "checkpointConsistent": True,
                "checkpointAvailable": True,
                "checkpoint": checkpoint,
                "failureScope": None,
                "failedNodes": [],
            }
            first_execute = Mock(return_value=trained)
            run_release(
                first_spec,
                SimpleNamespace(),
                execute_fn=first_execute,
                publish_fn=Mock(side_effect=ArtifactError("temporary")),
                sleep_fn=lambda _seconds: None,
            )
            second_execute = Mock(side_effect=AssertionError("training reran"))
            publish_attempts = []

            def publish_ok(_gateway, _namespace, _name, _root, attempt):
                publish_attempts.append(attempt)
                return "artifact://training/run-1-output/attempt-01-deadbeef"

            result = run_release(
                SimpleNamespace(**common, attempt=1),
                SimpleNamespace(),
                execute_fn=second_execute,
                publish_fn=publish_ok,
                sleep_fn=lambda _seconds: None,
            )
        first_execute.assert_called_once()
        second_execute.assert_not_called()
        self.assertTrue(result["reusedTrainingReceipt"])
        self.assertEqual(result["attempt"], 1)
        self.assertEqual(result["trainingAttempt"], 0)
        self.assertEqual(publish_attempts, [1])
        self.assertEqual(
            result["outputArtifact"],
            "artifact://training/run-1-output/attempt-01-deadbeef",
        )
        replayed_checkpoint = coordinator_module._compact_checkpoint(
            result["checkpoint"]
        )
        self.assertEqual(replayed_checkpoint["fileCount"], 1)
        self.assertEqual(replayed_checkpoint["totalBytes"], len(b"checkpoint"))
        self.assertEqual(replayed_checkpoint["hashMode"], "sampled-v1")
        self.assertEqual(replayed_checkpoint["sampleBytesPerFile"], 192 * 1024)
        self.assertEqual(result["status"], "PASS")

    def test_workspace_result_does_not_publish_to_gateway(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = SimpleNamespace(
                output_root=root,
                checkpoint_root=root / "checkpoints",
                workers=3,
                devices_per_node=2,
                command=("python", "train.py"),
                working_directory=root,
                environment={},
                run_name="run-1",
                namespace="training",
                run_uid="uid-1",
                attempt=0,
                artifact_provider="workspace",
            )
            trained = {
                "schemaVersion": "kcc-runtime-result/v1",
                "runName": "run-1",
                "namespace": "training",
                "runUid": "uid-1",
                "attempt": 0,
                "status": "PASS",
                "checkpointConsistent": True,
                "checkpointAvailable": False,
                "checkpoint": None,
                "failureScope": None,
                "failedNodes": [],
            }
            execute_fn = Mock(return_value=trained)
            result = release_module.run_workspace(spec, execute_fn=execute_fn)
        execute_fn.assert_called_once_with(spec)
        self.assertEqual(result["status"], "PASS")
        self.assertEqual(result["outputProvider"], "workspace")
        self.assertEqual(result["outputPath"], str(root))
        self.assertIn("/attempt-00-workspace-", result["outputArtifact"])
        self.assertEqual(result["publicationAttempts"], 0)

    def test_missing_gateway_publishes_structured_artifact_failure(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            spec = SimpleNamespace(
                output_root=root,
                checkpoint_root=root / "checkpoints",
                run_name="run-1",
                namespace="training",
                run_uid="uid-1",
                attempt=0,
            )
            with patch.object(
                release_module.RuntimeSpec,
                "load",
                return_value=spec,
            ), patch.object(release_module, "_publish") as publish, patch(
                "builtins.print"
            ):
                returncode = release_module.main(
                    ["--spec", "ignored.json", "--gateway", ""]
                )
        self.assertEqual(returncode, 1)
        result = publish.call_args.args[0]
        self.assertEqual(result["failureScope"], "artifact")
        self.assertTrue(result["checkpointConsistent"])
        self.assertFalse(result["checkpointAvailable"])


if __name__ == "__main__":
    unittest.main()
