"""Single-process checkpoint evaluation mixed into a structured Ray worker."""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import threading
import time
from typing import Any, Mapping, Sequence


MAX_EVALUATION_SUMMARY_BYTES = 64 * 1024
MAX_EVALUATION_RESULTS_BYTES = 256 * 1024
MAX_EVALUATION_RESULT_TASKS = 32
MAX_EVALUATION_RESULT_METRICS = 32


class EvaluationProcessMixin:
    """Manage an evaluator independently from the worker's training process."""

    def _init_evaluation(self) -> None:
        self.evaluation_process: subprocess.Popen[str] | None = None
        self._evaluation_started: float | None = None
        self._evaluation_metadata: dict[str, Any] | None = None
        self._evaluation_cancel_reason: str | None = None
        self._evaluation_results: dict[int, Mapping[str, Any]] = {}
        self._evaluation_lock = threading.Lock()

    def _evaluation_error(self, message: str) -> Exception:
        # StructuredWorker supplies its public error type without creating an
        # import cycle between this mixin and worker.py.
        return getattr(self, "evaluation_error_type", RuntimeError)(message)

    def _read_evaluation_results(
        self, output_dir: Path, result_path: object
    ) -> tuple[Mapping[str, Any] | None, str | None]:
        try:
            if not isinstance(result_path, str) or not result_path:
                raise ValueError("evaluation result path must be a non-empty string")
            if output_dir.is_symlink():
                raise ValueError("evaluation output directory is a symbolic link")

            output_path = Path(os.path.abspath(output_dir))
            candidate = Path(result_path)
            if not candidate.is_absolute():
                candidate = output_path / candidate
            candidate_path = Path(os.path.abspath(candidate))
            try:
                relative_path = candidate_path.relative_to(output_path)
            except ValueError as error:
                raise ValueError(
                    "evaluation result path escapes output directory"
                ) from error

            current_path = output_path
            for part in relative_path.parts:
                current_path = current_path / part
                if current_path.is_symlink():
                    raise ValueError("evaluation result path uses a symbolic link")
            if not current_path.is_file():
                raise ValueError("evaluation result is not a regular file")
            if current_path.stat().st_size > MAX_EVALUATION_RESULTS_BYTES:
                raise ValueError("evaluation result is too large")
            with current_path.open("rb") as stream:
                payload = stream.read(MAX_EVALUATION_RESULTS_BYTES + 1)
            if len(payload) > MAX_EVALUATION_RESULTS_BYTES:
                raise ValueError("evaluation result is too large")

            document = json.loads(payload)
            if not isinstance(document, Mapping):
                raise ValueError("evaluation result JSON is not an object")
            raw_results = document.get("results")
            if not isinstance(raw_results, Mapping):
                raise ValueError("evaluation result JSON has no results object")

            compact_results: dict[str, Mapping[str, Any]] = {}
            for task_name, raw_metrics in raw_results.items():
                if (
                    task_name == "all"
                    or not isinstance(task_name, str)
                    or not isinstance(raw_metrics, Mapping)
                ):
                    continue
                metrics: dict[str, Any] = {}
                for metric_name, metric_value in raw_metrics.items():
                    if len(metrics) >= MAX_EVALUATION_RESULT_METRICS:
                        break
                    if not isinstance(metric_name, str):
                        continue
                    if metric_value is None or isinstance(
                        metric_value, (bool, int, float, str)
                    ):
                        metrics[metric_name] = metric_value
                if metrics:
                    compact_results[task_name] = metrics
                    if len(compact_results) >= MAX_EVALUATION_RESULT_TASKS:
                        break
            return compact_results, None
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
            return None, f"{type(error).__name__}: {error}"

    def _read_evaluation_summary(
        self, output_dir: Path
    ) -> tuple[Mapping[str, Any] | None, str | None]:
        path = output_dir / "summary.json"
        if not path.exists():
            return None, None
        try:
            if path.is_symlink() or not path.is_file():
                raise ValueError("evaluation summary is not a regular file")
            if path.stat().st_size > MAX_EVALUATION_SUMMARY_BYTES:
                raise ValueError("evaluation summary is too large")
            value = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(value, Mapping):
                return dict(value), None
            if not isinstance(value, list):
                raise ValueError(
                    "evaluation summary must be an object or array of objects"
                )
            if not value:
                raise ValueError("evaluation summary array is empty")
            if not all(isinstance(record, Mapping) for record in value):
                raise ValueError("evaluation summary array entries must be objects")
            # LightEval appends checkpoint records in evaluation order. KCC invokes
            # it for one checkpoint, so the last record is the current result while
            # remaining compatible with a batch-produced summary.
            record = dict(value[-1])
            record.pop("results", None)
            record.pop("resultsError", None)
            result_path = record.get("result_path")
            if result_path is not None:
                results, results_error = self._read_evaluation_results(
                    output_dir, result_path
                )
                if results is not None:
                    record["results"] = results
                if results_error is not None:
                    record["resultsError"] = results_error
            return record, None
        except (OSError, UnicodeError, json.JSONDecodeError, ValueError) as error:
            return None, f"{type(error).__name__}: {error}"

    def _evaluation_status_locked(self) -> Mapping[str, Any]:
        process = self.evaluation_process
        metadata = self._evaluation_metadata
        if process is None or metadata is None:
            if self._evaluation_results:
                return dict(self._evaluation_results[max(self._evaluation_results)])
            return {"status": "NOT_STARTED"}

        started = self._evaluation_started
        duration = round(time.monotonic() - started, 3) if started is not None else 0.0
        returncode = process.poll()
        if returncode is None:
            return {**metadata, "status": "RUNNING", "durationSeconds": duration}

        if self._evaluation_cancel_reason is not None:
            status = "CANCELLED"
        elif returncode == 0:
            status = "PASS"
        else:
            status = "FAIL"
        result: dict[str, Any] = {
            **metadata,
            "status": status,
            "returncode": returncode,
            "durationSeconds": duration,
        }
        if self._evaluation_cancel_reason is not None:
            result["cancelReason"] = self._evaluation_cancel_reason
        elif returncode != 0:
            result["failure"] = f"evaluation command exited with code {returncode}"
        summary, summary_error = self._read_evaluation_summary(
            Path(str(metadata["outputDir"]))
        )
        if summary is not None:
            result["summary"] = summary
        if summary_error is not None:
            result["summaryError"] = summary_error
        iteration = int(metadata["iteration"])
        self._evaluation_results[iteration] = result
        self.evaluation_process = None
        self._evaluation_started = None
        self._evaluation_metadata = None
        self._evaluation_cancel_reason = None
        return dict(result)

    def start_evaluation(
        self,
        *,
        iteration: int,
        checkpoint_path: str,
        checkpoint_root: str,
        output_root: str,
        command: Sequence[str],
        cwd: str,
        environment: Mapping[str, str],
        device_id: int,
        checkpoint_snapshot_sha256: str | None = None,
    ) -> Mapping[str, Any]:
        """Start one evaluator and return without waiting for its subprocess."""
        if isinstance(iteration, bool) or iteration <= 0:
            raise self._evaluation_error("evaluation iteration must be positive")
        if (
            isinstance(command, (str, bytes))
            or not command
            or not all(isinstance(item, str) and item for item in command)
        ):
            raise self._evaluation_error("evaluation command must be a non-empty argv")
        if isinstance(device_id, bool) or device_id < 0:
            raise self._evaluation_error("evaluation device ID must be non-negative")

        with self._evaluation_lock:
            self._evaluation_status_locked()
            previous = self._evaluation_results.get(iteration)
            if previous is not None:
                return dict(previous)
            if self.evaluation_process is not None:
                current_iteration = (self._evaluation_metadata or {}).get("iteration")
                if current_iteration == iteration:
                    return self._evaluation_status_locked()
                raise self._evaluation_error(
                    "another checkpoint evaluation is already running "
                    f"for iteration {current_iteration}"
                )
            stop_reason = getattr(self, "stop_reason", None)
            if stop_reason is not None and not str(stop_reason).startswith(
                "checkpoint stop request "
            ):
                raise self._evaluation_error(
                    "worker is stopping; evaluation was not started"
                )

            working_directory = Path(cwd)
            selected_checkpoint = Path(checkpoint_path)
            root = Path(checkpoint_root)
            if not working_directory.is_dir() or working_directory.is_symlink():
                raise self._evaluation_error(
                    "evaluation working directory is missing or unsafe"
                )
            if not selected_checkpoint.is_dir() or selected_checkpoint.is_symlink():
                raise self._evaluation_error(
                    "evaluation checkpoint is missing or unsafe"
                )
            if not root.is_dir() or root.is_symlink():
                raise self._evaluation_error(
                    "evaluation checkpoint root is missing or unsafe"
                )

            evaluation_root = Path(output_root) / "evaluation"
            output_dir = evaluation_root / "results" / f"iter_{iteration:07d}"
            log_root = evaluation_root / "logs"
            output_dir.mkdir(parents=True, exist_ok=True)
            log_root.mkdir(parents=True, exist_ok=True)
            stdout_path = log_root / f"iter_{iteration:07d}.stdout.log"
            stderr_path = log_root / f"iter_{iteration:07d}.stderr.log"

            # Import lazily so worker.py can own Ascend environment discovery
            # and tests can patch the existing public helper.
            from .worker import sourced_ascend_environment

            child_env = dict(os.environ)
            child_env.update(sourced_ascend_environment())
            child_env.update(environment)
            evaluation_pythonpath = environment.get(
                "EVALUATION_PYTHONPATH", "/opt/kcc/lighteval-python"
            )
            inherited_pythonpath = child_env.get("PYTHONPATH")
            child_env["PYTHONPATH"] = (
                evaluation_pythonpath
                if not inherited_pythonpath
                else evaluation_pythonpath + os.pathsep + inherited_pythonpath
            )
            for key in (
                "RANK",
                "WORLD_SIZE",
                "MASTER_ADDR",
                "MASTER_PORT",
                "LOCAL_RANK",
                "GROUP_RANK",
                "ROLE_RANK",
            ):
                child_env.pop(key, None)
            child_env.update(
                {
                    "EVALUATION_CHECKPOINT_PATH": str(selected_checkpoint),
                    "EVALUATION_CHECKPOINT_ROOT": str(root),
                    "EVALUATION_OUTPUT_DIR": str(output_dir),
                    "ASCEND_RT_VISIBLE_DEVICES": str(device_id),
                    "PYTHONUNBUFFERED": "1",
                }
            )
            metadata: dict[str, Any] = {
                "iteration": iteration,
                "checkpointPath": str(selected_checkpoint),
                "checkpointRoot": str(root),
                "outputDir": str(output_dir),
                "stdout": str(stdout_path),
                "stderr": str(stderr_path),
                "deviceId": device_id,
                "nodeName": self.identity()["NODE_NAME"],
            }
            if checkpoint_snapshot_sha256 is not None:
                metadata["checkpointSnapshotSha256"] = checkpoint_snapshot_sha256

            try:
                with stdout_path.open("a", encoding="utf-8") as stdout, (
                    stderr_path.open("a", encoding="utf-8")
                ) as stderr:
                    process = subprocess.Popen(
                        list(command),
                        cwd=cwd,
                        env=child_env,
                        stdin=subprocess.DEVNULL,
                        stdout=stdout,
                        stderr=stderr,
                        text=True,
                        shell=False,
                        start_new_session=True,
                    )
            except OSError as error:
                raise self._evaluation_error(
                    f"cannot start checkpoint evaluation: {error}"
                ) from error
            self.evaluation_process = process
            self._evaluation_started = time.monotonic()
            self._evaluation_metadata = metadata
            self._evaluation_cancel_reason = None
            return {**metadata, "status": "RUNNING", "durationSeconds": 0.0}

    def evaluation_status(self) -> Mapping[str, Any]:
        with self._evaluation_lock:
            return self._evaluation_status_locked()

    def cancel_evaluation(self, reason: str) -> None:
        process: subprocess.Popen[str] | None = None
        with self._evaluation_lock:
            if self.evaluation_process is not None:
                self._evaluation_cancel_reason = reason
                process = self.evaluation_process
        if process is not None:
            self._terminate(process)
