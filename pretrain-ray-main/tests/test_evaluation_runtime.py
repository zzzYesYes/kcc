from __future__ import annotations

import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from kcc_training.runtime.evaluation import EvaluationConfig
from kcc_training.runtime.worker import StructuredWorker, WorkerError


class _Process:
    """Small controllable Popen stand-in for the evaluation lifecycle tests."""

    pid = 12345

    def __init__(self, returncode: int | None = None) -> None:
        self.returncode = returncode
        self.wait_called = False

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_called = True
        if self.returncode is None:
            raise AssertionError("start_evaluation must not wait for the subprocess")
        return self.returncode

    def finish(self, returncode: int) -> None:
        self.returncode = returncode


def _enabled_environment(**changes: str) -> dict[str, str]:
    environment = {
        "EVALUATION_ENABLED": "true",
        "EVALUATION_COMMAND_JSON": json.dumps(
            ["python3", "-m", "lighteval", "--tasks", "piqa"]
        ),
        "EVALUATION_DEVICE_ID": "1",
        "EVALUATION_EVERY_CHECKPOINTS": "2",
        "EVALUATION_FAILURE_POLICY": "Continue",
    }
    environment.update(changes)
    return environment


class EvaluationConfigTests(unittest.TestCase):
    def test_disabled_config_does_not_parse_unrelated_invalid_values(self) -> None:
        config = EvaluationConfig.from_environment(
            {
                "EVALUATION_ENABLED": "false",
                "EVALUATION_COMMAND_JSON": "not-json",
                "EVALUATION_DEVICE_ID": "not-an-integer",
                "EVALUATION_EVERY_CHECKPOINTS": "zero",
                "EVALUATION_FAILURE_POLICY": "unsupported",
            },
            devices_per_node=2,
        )

        self.assertFalse(config.enabled)

    def test_enabled_config_parses_json_argv_and_policy(self) -> None:
        config = EvaluationConfig.from_environment(
            _enabled_environment(), devices_per_node=4
        )

        self.assertTrue(config.enabled)
        self.assertEqual(
            tuple(config.command),
            ("python3", "-m", "lighteval", "--tasks", "piqa"),
        )
        self.assertEqual(config.device_id, 1)
        self.assertEqual(config.every_checkpoints, 2)
        self.assertEqual(config.failure_policy, "Continue")

    def test_enabled_config_rejects_invalid_values(self) -> None:
        invalid = (
            ("command JSON", {"EVALUATION_COMMAND_JSON": "not-json"}),
            ("command argv", {"EVALUATION_COMMAND_JSON": json.dumps([])}),
            (
                "command item",
                {"EVALUATION_COMMAND_JSON": json.dumps(["python3", 1])},
            ),
            ("device", {"EVALUATION_DEVICE_ID": "4"}),
            ("frequency", {"EVALUATION_EVERY_CHECKPOINTS": "0"}),
            ("policy", {"EVALUATION_FAILURE_POLICY": "StopEverything"}),
        )

        for label, changes in invalid:
            with self.subTest(label=label), self.assertRaises(ValueError):
                EvaluationConfig.from_environment(
                    _enabled_environment(**changes), devices_per_node=4
                )


class StructuredWorkerEvaluationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }

    def _start(
        self,
        worker: StructuredWorker,
        root: Path,
        *,
        iteration: int = 7,
        environment: dict[str, str] | None = None,
    ):
        checkpoint_root = root / "checkpoints"
        checkpoint_path = checkpoint_root / f"iter_{iteration:07d}"
        checkpoint_path.mkdir(parents=True, exist_ok=True)
        return worker.start_evaluation(
            iteration=iteration,
            checkpoint_path=str(checkpoint_path),
            checkpoint_root=str(checkpoint_root),
            output_root=str(root),
            command=("python3", "evaluate.py", "--task", "piqa"),
            cwd=str(root),
            environment=environment or {},
            device_id=1,
        )

    def test_start_is_nonblocking_and_injects_paths_device_and_logs(self) -> None:
        process = _Process()
        captured: dict[str, object] = {}

        def popen(argv, **kwargs):
            captured["argv"] = argv
            captured.update(kwargs)
            return process

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", side_effect=popen
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ):
            root = Path(directory)
            worker = StructuredWorker()
            started = self._start(
                worker,
                root,
                environment={
                    "EVALUATION_TASKS": "piqa",
                    "EVALUATION_MAX_SAMPLES": "8",
                    "RANK": "9",
                    "WORLD_SIZE": "16",
                },
            )
            status = worker.evaluation_status()

            self.assertFalse(process.wait_called)
            self.assertEqual(started["status"], "RUNNING")
            self.assertEqual(status["status"], "RUNNING")
            self.assertEqual(list(captured["argv"]), [
                "python3",
                "evaluate.py",
                "--task",
                "piqa",
            ])
            self.assertFalse(captured["shell"])
            self.assertEqual(captured["cwd"], str(root))

            child_environment = captured["env"]
            self.assertEqual(
                child_environment["EVALUATION_CHECKPOINT_PATH"],
                str(root / "checkpoints" / "iter_0000007"),
            )
            self.assertEqual(
                child_environment["EVALUATION_CHECKPOINT_ROOT"],
                str(root / "checkpoints"),
            )
            self.assertEqual(child_environment["ASCEND_RT_VISIBLE_DEVICES"], "1")
            self.assertEqual(child_environment["EVALUATION_TASKS"], "piqa")
            self.assertEqual(child_environment["EVALUATION_MAX_SAMPLES"], "8")
            self.assertNotIn("RANK", child_environment)
            self.assertNotIn("WORLD_SIZE", child_environment)
            self.assertEqual(
                child_environment["PYTHONPATH"].split(os.pathsep)[0],
                "/opt/kcc/lighteval-python",
            )

            output_dir = Path(child_environment["EVALUATION_OUTPUT_DIR"])
            self.assertEqual(
                output_dir,
                root / "evaluation" / "results" / "iter_0000007",
            )
            self.assertEqual(
                Path(captured["stdout"].name),
                root / "evaluation" / "logs" / "iter_0000007.stdout.log",
            )
            self.assertEqual(
                Path(captured["stderr"].name),
                root / "evaluation" / "logs" / "iter_0000007.stderr.log",
            )

    def test_success_reads_bounded_summary_from_iteration_output(self) -> None:
        process = _Process()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ):
            worker = StructuredWorker()
            started = self._start(worker, Path(directory))
            Path(started["outputDir"]).joinpath("summary.json").write_text(
                json.dumps({"task": "piqa", "accuracy": 0.75}),
                encoding="utf-8",
            )
            process.finish(0)
            status = worker.evaluation_status()

        self.assertEqual(status["status"], "PASS")
        self.assertEqual(status["summary"]["accuracy"], 0.75)

    def test_success_reads_last_record_from_lighteval_array_summary(self) -> None:
        process = _Process()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ):
            worker = StructuredWorker()
            started = self._start(worker, Path(directory))
            output_dir = Path(started["outputDir"])
            result_path = output_dir / "artifacts" / "results.json"
            result_path.parent.mkdir()
            extra_tasks = {
                f"task_{task}": {
                    f"metric_{metric}": metric for metric in range(34)
                }
                for task in range(40)
            }
            result_path.write_text(
                json.dumps(
                    {
                        "results": {
                            "piqa_local|0": {
                                "acc": 0.75,
                                "acc_stderr": 0.1,
                                "acc_norm": 0.8,
                                "acc_norm_stderr": 0.09,
                                "label": "quick",
                                "complete": True,
                                "optional": None,
                                "nested": {"must": "be skipped"},
                            },
                            "all": {"acc": 1.0},
                            **extra_tasks,
                        }
                    }
                ),
                encoding="utf-8",
            )
            Path(started["outputDir"]).joinpath("summary.json").write_text(
                json.dumps(
                    [
                        {
                            "id": "iter_0000003",
                            "iteration": 3,
                            "status": "success",
                            "exit_code": 0,
                            "result_path": None,
                        },
                        {
                            "id": "iter_0000007",
                            "iteration": 7,
                            "status": "success",
                            "exit_code": 0,
                            "result_path": str(result_path),
                        },
                    ]
                ),
                encoding="utf-8",
            )
            process.finish(0)
            status = worker.evaluation_status()

        self.assertEqual(status["status"], "PASS")
        self.assertEqual(status["summary"]["id"], "iter_0000007")
        self.assertEqual(status["summary"]["iteration"], 7)
        results = status["summary"]["results"]
        self.assertEqual(
            results["piqa_local|0"],
            {
                "acc": 0.75,
                "acc_stderr": 0.1,
                "acc_norm": 0.8,
                "acc_norm_stderr": 0.09,
                "label": "quick",
                "complete": True,
                "optional": None,
            },
        )
        self.assertNotIn("all", results)
        self.assertEqual(len(results), 32)
        self.assertEqual(len(results["task_0"]), 32)

    def test_invalid_lighteval_array_summary_reports_clear_error(self) -> None:
        invalid_summaries = (
            ([], "evaluation summary array is empty"),
            (
                [{"id": "iter_0000007"}, "not-an-object"],
                "evaluation summary array entries must be objects",
            ),
        )

        for value, expected_error in invalid_summaries:
            with self.subTest(value=value):
                with tempfile.TemporaryDirectory() as directory:
                    output_dir = Path(directory)
                    output_dir.joinpath("summary.json").write_text(
                        json.dumps(value), encoding="utf-8"
                    )
                    worker = StructuredWorker()
                    summary, error = worker._read_evaluation_summary(output_dir)

                    self.assertIsNone(summary)
                    self.assertIn(expected_error, error or "")

    def test_lighteval_result_read_errors_are_nonfatal(self) -> None:
        cases = (
            ("outside", "escapes output directory"),
            ("symlink", "uses a symbolic link"),
            ("oversized", "evaluation result is too large"),
            ("invalid-json", "JSONDecodeError"),
        )

        for case, expected_error in cases:
            with self.subTest(case=case):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    output_dir = root / "output"
                    output_dir.mkdir()
                    if case == "outside":
                        result_path = root / "outside.json"
                        result_path.write_text(
                            json.dumps({"results": {"secret": {"value": 1}}}),
                            encoding="utf-8",
                        )
                    elif case == "symlink":
                        target = output_dir / "target.json"
                        target.write_text(
                            json.dumps({"results": {"piqa_local|0": {"acc": 1}}}),
                            encoding="utf-8",
                        )
                        result_path = output_dir / "result.json"
                        result_path.symlink_to(target)
                    elif case == "oversized":
                        result_path = output_dir / "result.json"
                        result_path.write_bytes(b"x" * (256 * 1024 + 1))
                    else:
                        result_path = output_dir / "result.json"
                        result_path.write_text("{not-json", encoding="utf-8")
                    output_dir.joinpath("summary.json").write_text(
                        json.dumps(
                            [
                                {
                                    "id": "iter_0000007",
                                    "iteration": 7,
                                    "status": "success",
                                    "exit_code": 0,
                                    "result_path": str(result_path),
                                }
                            ]
                        ),
                        encoding="utf-8",
                    )
                    worker = StructuredWorker()
                    summary, summary_error = worker._read_evaluation_summary(
                        output_dir
                    )

                    self.assertIsNone(summary_error)
                    self.assertEqual(summary["status"], "success")
                    self.assertNotIn("results", summary)
                    self.assertIn(expected_error, summary["resultsError"])

    def test_continue_policy_process_failure_is_reported_as_fail(self) -> None:
        process = _Process(returncode=9)
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ):
            worker = StructuredWorker()
            self._start(
                worker,
                Path(directory),
                environment={"EVALUATION_FAILURE_POLICY": "Continue"},
            )
            status = worker.evaluation_status()

        self.assertEqual(status["status"], "FAIL")
        self.assertEqual(status["returncode"], 9)

    def test_checkpoint_stop_leaves_running_evaluation_alive(self) -> None:
        process = _Process()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ), patch.object(StructuredWorker, "_terminate") as terminate:
            worker = StructuredWorker()
            self._start(worker, Path(directory))
            worker.stop(
                "checkpoint stop request 2",
                cancel_evaluation=False,
            )
            status = worker.evaluation_status()

        terminate.assert_not_called()
        self.assertEqual(status["status"], "RUNNING")

    def test_stop_cancels_a_running_evaluation_by_default(self) -> None:
        process = _Process()

        def terminate(target, grace=15):
            del grace
            target.finish(-15)

        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ), patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ), patch.object(
            StructuredWorker, "_terminate", side_effect=terminate
        ):
            worker = StructuredWorker()
            self._start(worker, Path(directory))
            worker.stop("operator stop", cancel_evaluation=True)
            status = worker.evaluation_status()

        self.assertEqual(status["status"], "CANCELLED")
        self.assertEqual(status["iteration"], 7)

    def test_same_iteration_is_idempotent_and_only_one_evaluation_runs(self) -> None:
        process = _Process()
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, self.identity
        ), patch(
            "kcc_training.runtime.worker.subprocess.Popen", return_value=process
        ) as popen, patch(
            "kcc_training.runtime.worker.sourced_ascend_environment", return_value={}
        ):
            root = Path(directory)
            worker = StructuredWorker()
            first = self._start(worker, root, iteration=7)
            repeated = self._start(worker, root, iteration=7)

            self.assertEqual(repeated, first)
            self.assertEqual(popen.call_count, 1)
            with self.assertRaises(WorkerError):
                self._start(worker, root, iteration=8)
            self.assertEqual(popen.call_count, 1)


if __name__ == "__main__":
    unittest.main()
