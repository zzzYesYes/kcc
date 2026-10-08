from pathlib import Path
import tempfile
import unittest

from kcc_training.contracts import (
    ContractError,
    TrainingRecipe,
    TrainingRun,
    TrainingRuntimeProfile,
    load_contract,
)


ROOT = Path(__file__).resolve().parents[1]


class ContractTests(unittest.TestCase):
    def test_shipped_examples_are_valid(self) -> None:
        self.assertIsInstance(
            load_contract(ROOT / "examples/runtime-profile.yaml"),
            TrainingRuntimeProfile,
        )
        self.assertIsInstance(load_contract(ROOT / "examples/recipe.yaml"), TrainingRecipe)
        self.assertIsInstance(load_contract(ROOT / "examples/training-run.yaml"), TrainingRun)

    def test_unknown_fields_fail_closed(self) -> None:
        payload = (ROOT / "examples/training-run.yaml").read_text(encoding="utf-8")
        payload += "  arbitraryNode: gpu-server-00\n"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.yaml"
            path.write_text(payload, encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "unsupported keys"):
                TrainingRun.load(path)

    def test_recipe_rejects_output_traversal(self) -> None:
        payload = (ROOT / "examples/recipe.yaml").read_text(encoding="utf-8")
        payload = payload.replace("runs/qwen3-canary", "../outside")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "recipe.yaml"
            path.write_text(payload, encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "safe relative path"):
                TrainingRecipe.load(path)

    def test_profile_accepts_dns_subdomain_pull_secret(self) -> None:
        payload = (ROOT / "examples/runtime-profile.yaml").read_text(encoding="utf-8")
        payload = payload.replace("pullSecrets: []", "pullSecrets: [registry.credentials]")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "profile.yaml"
            path.write_text(payload, encoding="utf-8")
            self.assertIsInstance(TrainingRuntimeProfile.load(path), TrainingRuntimeProfile)

    def test_recipe_rejects_artifact_query_or_fragment(self) -> None:
        payload = (ROOT / "examples/recipe.yaml").read_text(encoding="utf-8")
        for suffix in ("?download=1", "#mutable"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as directory:
                path = Path(directory) / "recipe.yaml"
                path.write_text(
                    payload.replace("qwen3-source/v1", f"qwen3-source/v1{suffix}"),
                    encoding="utf-8",
                )
                with self.assertRaisesRegex(ContractError, "artifact://"):
                    TrainingRecipe.load(path)

    def test_recipe_rejects_absolute_working_directory(self) -> None:
        payload = (ROOT / "examples/recipe.yaml").read_text(encoding="utf-8")
        payload = payload.replace("workingDirectory: .", "workingDirectory: /tmp/source")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "recipe.yaml"
            path.write_text(payload, encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "safe relative"):
                TrainingRecipe.load(path)

    def test_run_rejects_recovery_attempt_budget_above_100(self) -> None:
        payload = (ROOT / "examples/training-run.yaml").read_text(encoding="utf-8")
        payload = payload.replace("sameTopologyRetries: 1", "sameTopologyRetries: 10")
        payload = payload.replace("maxReplacements: 0", "maxReplacements: 9")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.yaml"
            path.write_text(payload, encoding="utf-8")
            with self.assertRaisesRegex(ContractError, "attempt budget"):
                TrainingRun.load(path)

if __name__ == "__main__":
    unittest.main()

