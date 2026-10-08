from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
import unittest

from kcc_training.runtime.coordinator import consistent_checkpoint_iteration
from kcc_training.runtime.evaluation import EvaluationConfig
from kcc_training.runtime.evaluation_coordinator import EvaluationSupervisor


class _RemoteMethod:
    def __init__(self, function):
        self.function = function
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def remote(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.function(*args, **kwargs)

    @property
    def call_count(self) -> int:
        return len(self.calls)


class _FakeRay:
    def __init__(self) -> None:
        self.get_calls: list[tuple[object, object]] = []

    def get(self, value, timeout=None):
        self.get_calls.append((value, timeout))
        return value


class _FakeActor:
    def __init__(self, node: str) -> None:
        self.node = node
        self.iteration: int | None = None
        self.snapshot_digest = "a" * 64
        self.evaluation_state: dict[str, object] = {"status": "RUNNING"}
        self.checkpoint_iteration = _RemoteMethod(self._checkpoint_iteration)
        self.checkpoint = _RemoteMethod(self._checkpoint)
        self.start_evaluation = _RemoteMethod(self._start_evaluation)
        self.evaluation_status = _RemoteMethod(self._evaluation_status)

    def _checkpoint_iteration(self, _checkpoint_root: str):
        if self.iteration is None:
            return {"available": False, "nodeName": self.node}
        return {
            "available": True,
            "iteration": self.iteration,
            "nodeName": self.node,
        }

    def _checkpoint(self, checkpoint_root: str):
        if self.iteration is None:
            return {"available": False, "nodeName": self.node}
        return {
            "available": True,
            "iteration": self.iteration,
            "snapshotSha256": self.snapshot_digest,
            "selectedDir": str(
                Path(checkpoint_root) / f"iter_{self.iteration:07d}"
            ),
            "nodeName": self.node,
        }

    def _start_evaluation(self, **kwargs):
        return {
            "status": "RUNNING",
            "iteration": kwargs["iteration"],
            "checkpointPath": kwargs["checkpoint_path"],
        }

    def _evaluation_status(self):
        return dict(self.evaluation_state)


class EvaluationSupervisorTests(unittest.TestCase):
    def _supervisor(
        self,
        *,
        baseline: int | None,
        every_checkpoints: int = 1,
        failure_policy: str = "Continue",
    ):
        ray = _FakeRay()
        actors = (_FakeActor("node-a"), _FakeActor("node-b"))
        spec = SimpleNamespace(
            workers=2,
            checkpoint_root=Path("/workspace/run/checkpoints"),
            output_root=Path("/workspace/run"),
            working_directory=Path("/workspace/source"),
            environment={"EVALUATION_TASKS": "piqa"},
        )
        progress: list[tuple[tuple[object, ...], dict[str, object]]] = []

        def publish_progress(*args, **kwargs):
            progress.append((args, kwargs))

        config = EvaluationConfig(
            enabled=True,
            command=("python3", "evaluate.py"),
            device_id=0,
            every_checkpoints=every_checkpoints,
            failure_policy=failure_policy,
        )
        supervisor = EvaluationSupervisor(
            ray=ray,
            spec=spec,
            ordered_workers=(
                (0, "node-a", actors[0], {"NODE_NAME": "node-a"}),
                (1, "node-b", actors[1], {"NODE_NAME": "node-b"}),
            ),
            config=config,
            consistent_iteration=consistent_checkpoint_iteration,
            publish_progress=publish_progress,
            baseline_iteration=baseline,
        )
        return supervisor, actors, progress

    @staticmethod
    def _commit(actors: tuple[_FakeActor, _FakeActor], iteration: int) -> None:
        for actor in actors:
            actor.iteration = iteration

    def test_baseline_iteration_does_not_trigger_evaluation(self) -> None:
        supervisor, actors, _progress = self._supervisor(baseline=7)
        self._commit(actors, 7)

        supervisor.tick()

        self.assertFalse(supervisor.active)
        self.assertIsNone(supervisor.pending_iteration)
        self.assertEqual(actors[0].checkpoint.call_count, 0)
        self.assertEqual(actors[0].start_evaluation.call_count, 0)

    def test_changed_tracker_with_inconsistent_snapshot_does_not_start(self) -> None:
        supervisor, actors, _progress = self._supervisor(baseline=7)
        self._commit(actors, 8)
        actors[1].snapshot_digest = "b" * 64

        supervisor.tick()

        self.assertFalse(supervisor.active)
        self.assertEqual(supervisor.pending_iteration, 8)
        self.assertEqual(actors[0].checkpoint.call_count, 1)
        self.assertEqual(actors[1].checkpoint.call_count, 1)
        self.assertEqual(actors[0].start_evaluation.call_count, 0)
        self.assertEqual(actors[1].start_evaluation.call_count, 0)

    def test_consistent_snapshot_starts_rank_zero_once_and_is_idempotent(self) -> None:
        supervisor, actors, _progress = self._supervisor(baseline=7)
        self._commit(actors, 8)

        supervisor.tick()
        supervisor.tick()

        self.assertTrue(supervisor.active)
        self.assertEqual(supervisor.scheduled_iterations, {8})
        self.assertEqual(actors[0].start_evaluation.call_count, 1)
        self.assertEqual(actors[1].start_evaluation.call_count, 0)
        _args, kwargs = actors[0].start_evaluation.calls[0]
        self.assertEqual(kwargs["iteration"], 8)
        self.assertEqual(kwargs["checkpoint_snapshot_sha256"], "a" * 64)

    def test_every_two_checkpoints_starts_only_on_second_change(self) -> None:
        supervisor, actors, _progress = self._supervisor(
            baseline=None, every_checkpoints=2
        )
        self._commit(actors, 1)

        supervisor.tick()
        self.assertEqual(actors[0].start_evaluation.call_count, 0)

        self._commit(actors, 2)
        supervisor.tick()

        self.assertTrue(supervisor.active)
        self.assertEqual(supervisor.observed_checkpoints, 2)
        self.assertEqual(actors[0].start_evaluation.call_count, 1)
        _args, kwargs = actors[0].start_evaluation.calls[0]
        self.assertEqual(kwargs["iteration"], 2)

    def test_immediate_stop_gate_does_not_observe_or_start_new_evaluation(self) -> None:
        supervisor, actors, _progress = self._supervisor(baseline=7)
        self._commit(actors, 8)

        supervisor.tick(allow_start=False)

        self.assertEqual(supervisor.observed_iteration, 7)
        self.assertEqual(actors[0].start_evaluation.call_count, 0)

    def test_settle_waits_for_current_evaluation_before_release(self) -> None:
        supervisor, actors, _progress = self._supervisor(baseline=7)
        self._commit(actors, 8)
        supervisor.tick()
        actors[0].evaluation_state = {
            "status": "PASS",
            "iteration": 8,
            "summary": {"accuracy": 0.75},
        }

        supervisor.settle(allow_new=True, poll_seconds=0.1)

        self.assertFalse(supervisor.active)
        self.assertEqual(supervisor.summary()["status"], "PASS")
        self.assertEqual(actors[0].start_evaluation.call_count, 1)

    def test_evaluation_failure_only_becomes_fatal_for_fail_policy(self) -> None:
        for policy, expected_fatal in (("Continue", False), ("Fail", True)):
            with self.subTest(policy=policy):
                supervisor, actors, _progress = self._supervisor(
                    baseline=0, failure_policy=policy
                )
                self._commit(actors, 1)
                supervisor.tick()
                actors[0].evaluation_state = {
                    "status": "FAIL",
                    "iteration": 1,
                    "failure": "accuracy command failed",
                }

                supervisor.tick()

                self.assertEqual(supervisor.summary()["status"], "FAIL")
                self.assertEqual(supervisor.fatal_failure, expected_fatal)
                if expected_fatal:
                    self.assertEqual(
                        supervisor.failure_message, "accuracy command failed"
                    )


if __name__ == "__main__":
    unittest.main()
