import unittest

from kcc_training.state import (
    InvalidTransition,
    Phase,
    RecoveryBudget,
    require_transition,
)


class StateTests(unittest.TestCase):
    def test_happy_path(self) -> None:
        path = [
            Phase.PENDING,
            Phase.PREFLIGHT,
            Phase.STARTING,
            Phase.HCCL_GATE,
            Phase.RUNNING,
            Phase.SUCCEEDED,
        ]
        for current, target in zip(path, path[1:]):
            require_transition(current, target)

    def test_terminal_phase_cannot_restart(self) -> None:
        with self.assertRaises(InvalidTransition):
            require_transition(Phase.SUCCEEDED, Phase.STARTING)

    def test_recovery_budget_is_bounded(self) -> None:
        budget = RecoveryBudget(same_topology_retries=2, max_replacements=1)
        self.assertEqual(
            budget.decide(retries_used=0, replacements_used=0, hardware_fault=False),
            "retry-same-topology",
        )
        self.assertEqual(
            budget.decide(retries_used=2, replacements_used=0, hardware_fault=True),
            "replace-nodes",
        )
        self.assertEqual(
            budget.decide(retries_used=2, replacements_used=1, hardware_fault=True),
            "manual-required",
        )


if __name__ == "__main__":
    unittest.main()

