from pathlib import Path
import sys
import unittest

from kcc_training.recovery import diagnosis_sample_limit, maximum_attempt_count


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))
import recovery_supervisor as legacy  # noqa: E402


class RecoveryParityTests(unittest.TestCase):
    def test_attempt_count_matches_supported_legacy_budgets(self) -> None:
        for replacements in range(5):
            for retries in range(4):
                self.assertEqual(
                    maximum_attempt_count(replacements, retries),
                    legacy.maximum_attempt_count(replacements, retries),
                )

    def test_diagnosis_sample_limit_matches_legacy(self) -> None:
        for window in (0, 1, 30, 300):
            for poll in (1, 10, 30):
                self.assertEqual(
                    diagnosis_sample_limit(window, poll),
                    legacy.diagnosis_sample_limit(window, poll),
                )


if __name__ == "__main__":
    unittest.main()
