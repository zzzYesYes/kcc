import unittest

from kcc_training.legacy_bridge import recovery_evidence_from_legacy
from kcc_training.recovery import (
    FailureScope,
    RecoveryAction,
    RecoveryPolicy,
    decide_recovery,
)


class LegacyBridgeTests(unittest.TestCase):
    def test_stable_restart_ready_diagnosis_maps_to_hardware(self) -> None:
        summary = {
            "stable": True,
            "latestDiagnosis": {
                "failedActiveNodes": ["node-b", "node-a"],
                "ambiguousActiveNodes": [],
                "nonIdleActiveNodes": [],
                "availableSpareCount": 2,
            },
        }
        evidence = recovery_evidence_from_legacy(
            failure_class=None,
            diagnosis_summary=summary,
            checkpoint_consistent=True,
        )
        self.assertEqual(evidence.scope, FailureScope.HARDWARE)
        self.assertEqual(evidence.failed_nodes, ("node-a", "node-b"))
        decision = decide_recovery(
            RecoveryPolicy(2, 2), evidence, retries_used=0, replacements_used=0
        )
        self.assertEqual(decision.action, RecoveryAction.REPLACE_NODES)

    def test_unstable_unknown_diagnosis_preserves_legacy_same_topology_retry(self) -> None:
        evidence = recovery_evidence_from_legacy(
            failure_class=None,
            diagnosis_summary={"stable": False},
            checkpoint_consistent=True,
        )
        decision = decide_recovery(
            RecoveryPolicy(2, 2), evidence, retries_used=0, replacements_used=0
        )
        self.assertEqual(decision.action, RecoveryAction.RETRY_SAME_TOPOLOGY)

    def test_checkpoint_disagreement_still_fails_closed(self) -> None:
        evidence = recovery_evidence_from_legacy(
            failure_class="TIMEOUT",
            diagnosis_summary=None,
            checkpoint_consistent=False,
        )
        decision = decide_recovery(
            RecoveryPolicy(2, 2), evidence, retries_used=0, replacements_used=0
        )
        self.assertEqual(decision.action, RecoveryAction.MANUAL_REQUIRED)


if __name__ == "__main__":
    unittest.main()

