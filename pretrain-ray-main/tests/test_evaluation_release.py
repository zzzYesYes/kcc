from __future__ import annotations

import json
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from kcc_training.runtime import coordinator_release as release_module
from kcc_training.runtime.coordinator import _result_payload
from kcc_training.runtime.coordinator_release import _receipt_result


class EvaluationReleaseTests(unittest.TestCase):
    def test_completion_receipt_preserves_evaluation_summary(self) -> None:
        evaluation = {
            "status": "PASS",
            "iteration": 7,
            "summary": {"task": "piqa", "accuracy": 0.75},
        }
        receipt = _receipt_result(
            {
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
                "evaluation": evaluation,
            }
        )

        self.assertEqual(receipt["evaluation"], evaluation)

    def test_completed_progress_carries_evaluation_summary(self) -> None:
        evaluation = {
            "status": "PASS",
            "iteration": 7,
            "summary": {"accuracy": 0.75},
        }
        spec = SimpleNamespace(artifact_provider="workspace")
        result = {
            "status": "PASS",
            "checkpoint": {"iteration": 7},
            "outputArtifact": "artifact://training/run-output/v1",
            "evaluation": evaluation,
        }
        with patch.object(
            release_module.RuntimeSpec, "load", return_value=spec
        ), patch.object(
            release_module, "run_workspace", return_value=result
        ), patch.object(
            release_module, "publish_progress"
        ) as progress, patch.object(
            release_module, "_publish"
        ), patch(
            "builtins.print"
        ):
            returncode = release_module.main(["--spec", "ignored.json"])

        self.assertEqual(returncode, 0)
        self.assertEqual(progress.call_args.kwargs["evaluation"], evaluation)

    def test_bounded_terminal_result_keeps_evaluation(self) -> None:
        evaluation = {"status": "FAIL", "iteration": 7, "returncode": 9}
        payload, truncated, _digest = _result_payload(
            {
                "schemaVersion": "kcc-runtime-result/v1",
                "runName": "run-1",
                "namespace": "training",
                "runUid": "uid-1",
                "attempt": 0,
                "status": "FAIL",
                "checkpointConsistent": True,
                "checkpointAvailable": False,
                "checkpoint": None,
                "failureScope": "software",
                "failedNodes": [],
                "failure": "x" * (2 * 1024 * 1024),
                "evaluation": evaluation,
            }
        )

        self.assertTrue(truncated)
        self.assertEqual(json.loads(payload)["evaluation"], evaluation)


if __name__ == "__main__":
    unittest.main()
