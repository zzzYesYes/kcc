"""Coordinator-side checkpoint observation for optional in-worker evaluation."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
import time
from typing import Any

from .checkpoints import CheckpointError, require_consistent
from .evaluation import EvaluationConfig


class EvaluationSupervisor:
    """Observe committed checkpoints and serialize evaluation on rank zero."""

    def __init__(
        self,
        *,
        ray: Any,
        spec: Any,
        ordered_workers: Sequence[tuple[int, str, Any, Mapping[str, Any]]],
        config: EvaluationConfig,
        consistent_iteration: Callable[[Sequence[Mapping[str, Any]], int], int | None],
        publish_progress: Callable[..., None],
        baseline_iteration: int | None,
    ) -> None:
        self.ray = ray
        self.spec = spec
        self.ordered_workers = tuple(ordered_workers)
        self.config = config
        self.consistent_iteration = consistent_iteration
        self.publish_progress = publish_progress
        self.worker = self.ordered_workers[0][2]
        self.observed_iteration = baseline_iteration
        self.observed_checkpoints = 0
        self.pending_iteration: int | None = None
        self.active = False
        self.scheduled_iterations: set[int] = set()
        self.latest: dict[str, Any] | None = None
        self.fatal_failure = False
        self.failure_message: str | None = None

    def summary(self) -> Mapping[str, Any]:
        if self.latest is not None:
            return dict(self.latest)
        return {
            "status": "NOT_RUN",
            "everyCheckpoints": self.config.every_checkpoints,
            "failurePolicy": self.config.failure_policy,
        }

    def _record(self, value: Mapping[str, Any], *, publish: bool) -> None:
        state = dict(value)
        state.setdefault("everyCheckpoints", self.config.every_checkpoints)
        state.setdefault("failurePolicy", self.config.failure_policy)
        self.latest = state
        status = str(state.get("status", "FAIL"))
        if status == "FAIL" and self.config.failure_policy == "Fail":
            self.fatal_failure = True
            self.failure_message = str(
                state.get("failure") or "checkpoint evaluation failed"
            )
        if not publish:
            return
        progress_status = {
            "RUNNING": "Running",
            "PASS": "Passed",
            "FAIL": "Failed",
            "CANCELLED": "Cancelled",
        }.get(status, status.title())
        iteration = state.get("iteration")
        message = {
            "RUNNING": f"evaluating checkpoint iteration {iteration}",
            "PASS": f"checkpoint iteration {iteration} evaluation passed",
            "FAIL": f"checkpoint iteration {iteration} evaluation failed",
            "CANCELLED": f"checkpoint iteration {iteration} evaluation cancelled",
        }.get(status, "checkpoint evaluation state changed")
        self.publish_progress(
            self.spec,
            "Evaluation",
            progress_status,
            message,
            evaluation=state,
        )

    def poll(self) -> None:
        if not self.active:
            return
        try:
            raw = self.ray.get(self.worker.evaluation_status.remote(), timeout=60)
            if not isinstance(raw, Mapping):
                raise TypeError("evaluation status is not an object")
            state = dict(raw)
        except Exception as error:
            state = {
                "status": "FAIL",
                "iteration": (
                    self.latest.get("iteration") if self.latest is not None else None
                ),
                "failure": f"evaluation status failed: {type(error).__name__}: {error}",
            }
        status = state.get("status")
        if status == "RUNNING":
            self._record(state, publish=False)
            return
        if status not in {"PASS", "FAIL", "CANCELLED"}:
            state = {
                **state,
                "status": "FAIL",
                "failure": f"evaluation returned invalid status {status!r}",
            }
        self.active = False
        self._record(state, publish=True)

    def observe_checkpoint(self) -> None:
        try:
            views = list(
                self.ray.get(
                    [
                        actor.checkpoint_iteration.remote(
                            str(self.spec.checkpoint_root)
                        )
                        for _rank, _node, actor, _identity in self.ordered_workers
                    ],
                    timeout=120,
                )
            )
            iteration = self.consistent_iteration(views, self.spec.workers)
        except Exception as error:
            print(
                "KCC_EVALUATION checkpoint observation warning: "
                f"{type(error).__name__}: {error}",
                flush=True,
            )
            return
        if iteration is None or iteration == self.observed_iteration:
            return
        if (
            self.observed_iteration is not None
            and iteration < self.observed_iteration
        ):
            print(
                "KCC_EVALUATION ignored regressed checkpoint tracker "
                f"{iteration} < {self.observed_iteration}",
                flush=True,
            )
            return
        self.observed_iteration = iteration
        self.observed_checkpoints += 1
        if self.observed_checkpoints % self.config.every_checkpoints == 0:
            if iteration not in self.scheduled_iterations:
                self.pending_iteration = iteration

    def start_pending(self) -> bool:
        iteration = self.pending_iteration
        if self.active or iteration is None or iteration in self.scheduled_iterations:
            return False
        try:
            views = list(
                self.ray.get(
                    [
                        actor.checkpoint.remote(str(self.spec.checkpoint_root))
                        for _rank, _node, actor, _identity in self.ordered_workers
                    ],
                    timeout=300,
                )
            )
            checkpoint = require_consistent(views, self.spec.workers)
            if checkpoint is None or checkpoint.get("iteration") != iteration:
                raise CheckpointError(
                    "committed checkpoint changed before evaluation launch"
                )
            raw = self.ray.get(
                self.worker.start_evaluation.remote(
                    iteration=iteration,
                    checkpoint_path=str(checkpoint["selectedDir"]),
                    checkpoint_root=str(self.spec.checkpoint_root),
                    output_root=str(self.spec.output_root),
                    command=self.config.command,
                    cwd=str(self.spec.working_directory),
                    environment=self.spec.environment,
                    device_id=self.config.device_id,
                    checkpoint_snapshot_sha256=str(checkpoint["snapshotSha256"]),
                ),
                timeout=60,
            )
            if not isinstance(raw, Mapping):
                raise TypeError("evaluation start result is not an object")
            state = dict(raw)
        except CheckpointError as error:
            print(
                "KCC_EVALUATION checkpoint snapshot warning: "
                f"{error}",
                flush=True,
            )
            return False
        except Exception as error:
            self.pending_iteration = None
            self.scheduled_iterations.add(iteration)
            self._record(
                {
                    "status": "FAIL",
                    "iteration": iteration,
                    "failure": (
                        "evaluation launch failed: "
                        f"{type(error).__name__}: {error}"
                    ),
                },
                publish=True,
            )
            return False
        self.pending_iteration = None
        self.scheduled_iterations.add(iteration)
        status = state.get("status")
        self.active = status == "RUNNING"
        self._record(state, publish=True)
        return self.active

    def tick(self, *, allow_start: bool = True) -> None:
        self.poll()
        if not allow_start or self.fatal_failure:
            return
        self.observe_checkpoint()
        if not self.active:
            self.start_pending()

    def settle(self, *, allow_new: bool, poll_seconds: float) -> None:
        """Capture the final commit and wait for work already safe to run."""
        if allow_new and not self.fatal_failure:
            self.observe_checkpoint()
            if not self.active:
                self.start_pending()
        while self.active:
            self.poll()
            if self.active:
                time.sleep(min(max(poll_seconds, 0.1), 5.0))
        if (
            allow_new
            and not self.fatal_failure
            and self.pending_iteration is not None
            and self.start_pending()
        ):
            while self.active:
                self.poll()
                if self.active:
                    time.sleep(min(max(poll_seconds, 0.1), 5.0))
