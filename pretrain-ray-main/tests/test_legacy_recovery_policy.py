from pathlib import Path
import sys
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))

import recovery_supervisor as recovery  # noqa: E402


class LegacyRecoveryPolicyTests(unittest.TestCase):
    def test_attempt_budget_includes_each_topology(self) -> None:
        self.assertEqual(recovery.maximum_attempt_count(2, 2), 9)

    def test_sampling_window_includes_initial_sample(self) -> None:
        self.assertEqual(recovery.diagnosis_sample_limit(300, 30), 11)
        self.assertEqual(recovery.diagnosis_sample_limit(0, 30), 1)

    def test_retry_count_resets_after_replacement(self) -> None:
        attempts = [
            {"status": "RETRY_SAME_TOPOLOGY", "recoveryAction": "same-topology"},
            {"status": "REPLACED"},
            {"status": "RETRY_SAME_TOPOLOGY", "recoveryAction": "same-topology"},
            {"status": "RETRY_SAME_TOPOLOGY", "recoveryAction": "same-topology"},
        ]
        self.assertEqual(recovery.same_topology_retries_used(attempts), 2)

    def test_failed_nodes_are_sorted_and_deduplicated(self) -> None:
        diagnosis = {"failedActiveNodes": ["node-b", "node-a", "node-b", None]}
        self.assertEqual(recovery.diagnosed_failed_nodes(diagnosis), ("node-a", "node-b"))


if __name__ == "__main__":
    unittest.main()

