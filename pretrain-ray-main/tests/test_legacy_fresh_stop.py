from argparse import Namespace
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "ray_startup_bundle"))

import ray_cluster_start  # noqa: E402
import start_ray  # noqa: E402
import training_control  # noqa: E402


class LegacyFreshStopTests(unittest.TestCase):
    def test_fresh_stop_before_cluster_apply_uses_rendered_run_ownership(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            artifact_dir = root / "training-runs" / "fresh-early"
            artifact_dir.mkdir(parents=True)
            (artifact_dir / "raycluster.yaml").write_text(
                """apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: pretrain-ray
  namespace: pretrain-ray
  annotations:
    trainctl.io/run-id: fresh-early
""",
                encoding="utf-8",
            )
            args = Namespace(
                run_id="fresh-early",
                training_artifact_root=root / "training-runs",
                recovery_state_root=root / "training-jobs",
                namespace="pretrain-ray",
                cluster="pretrain-ray",
            )

            self.assertEqual(training_control.request_stop_without_cluster(args), 0)
            request = training_control.load_compatible_stop_request(
                artifact_dir / "stop-request.json",
                expected_job_id="fresh-early",
                expected_attempt="fresh-early",
                current_cluster_uid=None,
                expected_mode="IMMEDIATE",
                expected_iteration=None,
            )
            self.assertIsNotNone(request)

    def test_fresh_stop_writes_run_artifact_marker_before_cleanup(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            artifact_root = Path(temporary_directory) / "training-runs"
            marker = artifact_root / "fresh-test" / "stop-request.json"
            args = Namespace(
                run_id="fresh-test",
                training_artifact_root=artifact_root,
                recovery_state_root=Path(temporary_directory) / "training-jobs",
            )

            def verify_marker_before_delete(*_arguments: object) -> None:
                request = training_control.load_compatible_stop_request(
                    marker,
                    expected_job_id="fresh-test",
                    expected_attempt="fresh-test",
                    current_cluster_uid="cluster-uid",
                    expected_mode="IMMEDIATE",
                    expected_iteration=None,
                )
                self.assertIsNotNone(request)

            with patch.object(
                training_control,
                "cluster_identity",
                return_value=("cluster-uid", "fresh-test"),
            ), patch.object(
                training_control,
                "delete_cluster",
                side_effect=verify_marker_before_delete,
            ):
                self.assertEqual(training_control.control(args, "IMMEDIATE"), 0)

    def test_stage_three_observes_fresh_stop_before_next_kubectl_poll(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            marker_dir = Path(temporary_directory) / "fresh-test"
            args = Namespace(run_id="fresh-test")
            training_control.mark_stop(
                args,
                marker_dir,
                "cluster-uid",
                "fresh-test",
                "IMMEDIATE",
                None,
            )
            marker = marker_dir / training_control.STOP_REQUEST_FILENAME

            with patch.object(ray_cluster_start, "run_command") as run_command:
                with self.assertRaises(ray_cluster_start.StopRequested):
                    ray_cluster_start.wait_for_ray_pods(
                        ("kubectl",),
                        namespace="pretrain-ray",
                        cluster="pretrain-ray",
                        expected_workers=1,
                        timeout_seconds=1800,
                        stop_request=marker,
                        run_id="fresh-test",
                    )
            run_command.assert_not_called()

    def test_stop_during_stage_three_skips_failure_retention(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            manifest = root / "raycluster.yaml"
            manifest.write_text("kind: RayCluster\n", encoding="utf-8")
            completed = subprocess.CompletedProcess([], 0, "", "")

            with patch.object(
                ray_cluster_start,
                "prepare_runtime_configmap",
            ), patch.object(
                ray_cluster_start,
                "run_command",
                return_value=completed,
            ), patch.object(
                ray_cluster_start,
                "wait_for_ray_pods",
                side_effect=ray_cluster_start.StopRequested("stop accepted"),
            ), patch.object(
                ray_cluster_start,
                "retain_then_delete_failed_cluster",
            ) as retain, patch.object(
                ray_cluster_start,
                "delete_stopped_cluster",
            ) as delete:
                with self.assertRaises(ray_cluster_start.StopRequested):
                    ray_cluster_start.start_ray_cluster(
                        manifest=manifest,
                        runtime_source_dir=root,
                        runtime_configmap="runtime",
                        kubectl_command="kubectl",
                        kubeconfig=None,
                        namespace="pretrain-ray",
                        cluster="pretrain-ray",
                        expected_workers=1,
                        timeout_seconds=1800,
                        failure_retention_seconds=1800,
                    )

            retain.assert_not_called()
            delete.assert_called_once()

    def test_stage_three_command_receives_run_specific_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            artifact_root = Path(temporary_directory) / "training-runs"
            args = start_ray.make_parser().parse_args(
                [
                    "--fresh",
                    "--node",
                    "worker-a",
                    "--run-id",
                    "fresh-wire",
                    "--training-artifact-root",
                    str(artifact_root),
                ]
            )
            stages = start_ray.build_stage_commands(args, run_id="fresh-wire")
            launch_command = list(stages[2][1])

            marker_index = launch_command.index("--stop-request")
            self.assertEqual(
                launch_command[marker_index + 1],
                str(artifact_root / "fresh-wire" / "stop-request.json"),
            )
            run_index = launch_command.index("--run-id")
            self.assertEqual(launch_command[run_index + 1], "fresh-wire")


if __name__ == "__main__":
    unittest.main()
