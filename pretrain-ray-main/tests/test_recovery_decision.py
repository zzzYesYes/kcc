import unittest

from kcc_training.recovery import (
    FailureScope,
    RecoveryAction,
    RecoveryEvidence,
    RecoveryPolicy,
    RecoveryPolicyError,
    decide_recovery,
    diagnosis_sample_limit,
    maximum_attempt_count,
)


class RecoveryDecisionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.policy = RecoveryPolicy(same_topology_retries=2, max_replacements=2)

    def test_software_failure_retries_without_replacement(self) -> None:
        decision = decide_recovery(
            self.policy,
            RecoveryEvidence(scope=FailureScope.SOFTWARE, checkpoint_consistent=True),
            retries_used=0,
            replacements_used=0,
        )
        self.assertEqual(decision.action, RecoveryAction.RETRY_SAME_TOPOLOGY)

    def test_checkpoint_disagreement_always_requires_human(self) -> None:
        decision = decide_recovery(
            self.policy,
            RecoveryEvidence(
                scope=FailureScope.HARDWARE,
                failed_nodes=("node-a",),
                diagnosis_stable=True,
                checkpoint_consistent=False,
                survivors_healthy_and_idle=True,
                spares_healthy_and_idle=2,
            ),
            retries_used=0,
            replacements_used=0,
        )
        self.assertEqual(decision.action, RecoveryAction.MANUAL_REQUIRED)

    def test_hardware_replacement_requires_stable_complete_evidence(self) -> None:
        evidence = RecoveryEvidence(
            scope=FailureScope.HARDWARE,
            failed_nodes=("node-a", "node-b", "node-a"),
            diagnosis_stable=True,
            checkpoint_consistent=True,
            survivors_healthy_and_idle=True,
            spares_healthy_and_idle=2,
        )
        decision = decide_recovery(
            self.policy, evidence, retries_used=2, replacements_used=0
        )
        self.assertEqual(decision.action, RecoveryAction.REPLACE_NODES)
        self.assertEqual(decision.replacement_count, 2)

    def test_replacement_budget_is_n_for_n(self) -> None:
        evidence = RecoveryEvidence(
            scope=FailureScope.HARDWARE,
            failed_nodes=("node-a", "node-b"),
            diagnosis_stable=True,
            checkpoint_consistent=True,
            survivors_healthy_and_idle=True,
            spares_healthy_and_idle=2,
        )
        decision = decide_recovery(
            self.policy, evidence, retries_used=2, replacements_used=1
        )
        self.assertEqual(decision.action, RecoveryAction.MANUAL_REQUIRED)

    def test_artifact_and_infrastructure_failures_are_retryable(self) -> None:
        for scope in (FailureScope.ARTIFACT, FailureScope.INFRASTRUCTURE):
            with self.subTest(scope=scope):
                decision = decide_recovery(
                    self.policy,
                    RecoveryEvidence(scope=scope, checkpoint_consistent=True),
                    retries_used=0,
                    replacements_used=0,
                )
                self.assertEqual(decision.action, RecoveryAction.RETRY_SAME_TOPOLOGY)

    def test_unknown_scope_is_not_silently_reclassified(self) -> None:
        decision = decide_recovery(
            self.policy,
            RecoveryEvidence(scope=FailureScope.UNKNOWN, checkpoint_consistent=True),
            retries_used=0,
            replacements_used=0,
        )
        self.assertEqual(decision.action, RecoveryAction.MANUAL_REQUIRED)

    def test_attempt_and_sample_bounds_match_legacy_semantics(self) -> None:
        self.assertEqual(maximum_attempt_count(2, 2), 9)
        self.assertEqual(diagnosis_sample_limit(300, 30), 11)
        with self.assertRaises(RecoveryPolicyError):
            maximum_attempt_count(100, 10)


if __name__ == "__main__":
    unittest.main()
