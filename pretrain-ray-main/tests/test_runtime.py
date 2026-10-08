from pathlib import Path
from types import SimpleNamespace
import json
import os
import tempfile
import unittest
from unittest.mock import patch

from kcc_training.runtime import coordinator as coordinator_module
from kcc_training.runtime.checkpoints import (
    CheckpointError,
    CheckpointUnavailable,
    discard_uncommitted,
    require_consistent,
    snapshot,
)
from kcc_training.runtime.coordinator import (
    CoordinatorError,
    consistent_checkpoint_iteration,
    load_runtime_control,
    _node_ranks_from_hccl,
    failure_result,
    run_hccl_gate,
    wait_ranktable,
)
from kcc_training.runtime.spec import RuntimeSpec, RuntimeSpecError
from kcc_training.runtime.worker import (
    StructuredWorker,
    shared_training_progress_signature,
    sourced_ascend_environment,
)
from ray_startup_bundle.hccl_runtime.hccl_check import _ping as ping_module
from ray_startup_bundle.hccl_runtime.hccl_check import _hccl as hccl_module


def spec_document():
    return {
        "schemaVersion": "kcc-runtime/v1",
        "run": {"name": "run-1", "namespace": "training", "uid": "uid-1", "attempt": 0},
        "topology": {
            "workers": 2, "nodes": ["node-a", "node-b"], "devicesPerNode": 8,
            "resourceName": "huawei.com/Ascend910", "rankTablePath": "/etc/kcc/ranktable/hccl.json",
        },
        "training": {
            "framework": "mindspeed", "command": ["python", "pretrain.py"],
            "workingDirectory": "/workspace/source", "environment": {}, "noProgressSeconds": 3600,
        },
        "artifacts": {"provider": "workspace", "source": "artifact://s", "model": "artifact://m", "data": "artifact://d", "outputRoot": "/workspace/runs/run-1"},
    }


class RuntimeTests(unittest.TestCase):
    def test_worker_sources_available_ascend_runtime_environment(self):
        with tempfile.TemporaryDirectory() as directory:
            toolkit = Path(directory) / "toolkit.sh"
            atb = Path(directory) / "atb.sh"
            toolkit.write_text(
                "export ASCEND_HOME_PATH=/opt/ascend\n"
                "export LD_LIBRARY_PATH=/opt/ascend/lib\n",
                encoding="utf-8",
            )
            atb.write_text(
                "export ATB_HOME_PATH=/opt/atb\n"
                "export LD_LIBRARY_PATH=/opt/atb/lib:$LD_LIBRARY_PATH\n"
                "export UNRELATED_VALUE=ignored\n",
                encoding="utf-8",
            )
            environment = sourced_ascend_environment((str(toolkit), str(atb)))
        self.assertEqual(environment["ASCEND_HOME_PATH"], "/opt/ascend")
        self.assertEqual(environment["ATB_HOME_PATH"], "/opt/atb")
        self.assertEqual(
            environment["LD_LIBRARY_PATH"],
            "/opt/atb/lib:/opt/ascend/lib",
        )
        self.assertNotIn("UNRELATED_VALUE", environment)

    def test_single_rank_hccl_records_no_collective_evidence(self):
        preparation = {
            "ray_node_id": "ray-node-a",
            "server_id": "server-a",
            "rank_start": 0,
            "rank_ids": [0],
            "device_ids": [0],
            "device_ips": ["192.0.2.10"],
            "ranktable_sha256": "a" * 64,
            "probe_binary_sha256": "b" * 64,
        }
        results = hccl_module.single_rank_no_collective_results(
            [preparation], {"world_size": 1}
        )
        self.assertIsNotNone(results)
        self.assertEqual(results[0]["status"], "PASS")
        self.assertTrue(results[0]["collective_skipped"])
        self.assertTrue(results[0]["ranks"][0]["collective_skipped"])
        self.assertIsNone(
            hccl_module.single_rank_no_collective_results(
                [preparation], {"world_size": 2}
            )
        )

    def test_spec_loads_structured_command(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(spec_document()), encoding="utf-8")
            spec = RuntimeSpec.load(path)
            self.assertEqual(spec.command, ("python", "pretrain.py"))
            self.assertEqual(spec.artifact_provider, "workspace")

    def test_spec_rejects_duplicate_topology(self):
        document = spec_document()
        document["topology"]["nodes"] = ["node-a", "node-a"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(RuntimeSpecError, "unique"):
                RuntimeSpec.load(path)

    def test_checkpoint_uses_tracker_not_largest_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text("7\n", encoding="utf-8")
            committed = root / "iter_0000007" / "mp_rank_00"
            committed.mkdir(parents=True)
            (committed / "model.pt").write_bytes(b"valid")
            incomplete = root / "iter_0000008"
            incomplete.mkdir()
            report = snapshot(root)
            self.assertEqual(report["iteration"], 7)
            self.assertNotIn("iter_0000008", str(report["files"]))

    def test_checkpoint_views_must_match(self):
        with self.assertRaisesRegex(CheckpointError, "different"):
            require_consistent([{"snapshotSha256": "a"}, {"snapshotSha256": "b"}], 2)

    def test_immediate_stop_discards_new_checkpoints_and_retains_baseline(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "checkpoints"
            for iteration in (7, 8, 9):
                target = root / f"iter_{iteration:07d}"
                target.mkdir(parents=True)
                (target / "model.pt").write_bytes(str(iteration).encode())
            (root / "partial-upload").mkdir()
            (root / "latest_checkpointed_iteration.txt").write_text(
                "9\n", encoding="utf-8"
            )
            result = discard_uncommitted(root, 7)
            self.assertEqual(result["retainedIteration"], 7)
            self.assertTrue((root / "iter_0000007").is_dir())
            self.assertFalse((root / "iter_0000008").exists())
            self.assertFalse((root / "iter_0000009").exists())
            self.assertFalse((root / "partial-upload").exists())
            self.assertEqual(
                (root / "latest_checkpointed_iteration.txt").read_text().strip(),
                "7",
            )

    def test_immediate_stop_without_baseline_removes_checkpoint_root(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "checkpoints"
            (root / "iter_0000001").mkdir(parents=True)
            (root / "iter_0000001" / "model.pt").write_bytes(b"incomplete")
            result = discard_uncommitted(root, None)
            self.assertIsNone(result["retainedIteration"])
            self.assertFalse(root.exists())

    def test_runtime_control_is_bound_to_run_attempt_and_generation(self):
        spec = SimpleNamespace(
            run_name="run-1",
            run_uid="uid-1",
            attempt=0,
        )
        payload = {
            "schemaVersion": "kcc-runtime-control/v1",
            "runName": "run-1",
            "runUid": "uid-1",
            "attempt": 0,
            "action": "StopAfterCheckpoint",
            "requestGeneration": 2,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "control.json"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(
                load_runtime_control(path, spec)["requestGeneration"],
                2,
            )
            payload["action"] = "StopImmediate"
            path.write_text(json.dumps(payload), encoding="utf-8")
            self.assertEqual(load_runtime_control(path, spec)["action"], "StopImmediate")
            payload["runUid"] = "other"
            path.write_text(json.dumps(payload), encoding="utf-8")
            with self.assertRaisesRegex(CoordinatorError, "identity"):
                load_runtime_control(path, spec)

    def test_checkpoint_tracker_views_must_advance_consistently(self):
        self.assertEqual(
            consistent_checkpoint_iteration(
                [
                    {"available": True, "iteration": 8},
                    {"available": True, "iteration": 8},
                ],
                2,
            ),
            8,
        )
        self.assertIsNone(
            consistent_checkpoint_iteration(
                [{"available": False}, {"available": False}],
                2,
            )
        )
        with self.assertRaisesRegex(CheckpointError, "different"):
            consistent_checkpoint_iteration(
                [
                    {"available": True, "iteration": 8},
                    {"available": True, "iteration": 9},
                ],
                2,
            )

    def test_worker_reads_committed_checkpoint_iteration(self):
        identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }
        with tempfile.TemporaryDirectory() as directory, patch.dict(
            os.environ, identity
        ):
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text(
                "7\n", encoding="utf-8"
            )
            self.assertEqual(
                StructuredWorker().checkpoint_iteration(str(root))["iteration"],
                7,
            )

    def test_ranktable_hashes_exact_projected_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "hccl.json"
            path.write_text('{"server_list":[]}', encoding="utf-8")
            self.assertEqual(len(wait_ranktable(path, 1)), 64)

    def test_missing_checkpoint_is_available_state_not_corruption(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaises(CheckpointUnavailable):
                snapshot(Path(directory))
        self.assertIsNone(
            require_consistent(
                [{"available": False}, {"available": False}],
                2,
            )
        )

    def test_checkpoint_digest_includes_file_content(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "latest_checkpointed_iteration.txt").write_text("1\n")
            shard = root / "iter_0000001" / "model.pt"
            shard.parent.mkdir()
            payload = bytearray(512 * 1024)
            shard.write_bytes(payload)
            first_report = snapshot(root)
            self.assertEqual(first_report["hashMode"], "sampled-v1")
            self.assertEqual(first_report["sampleBytesPerFile"], 192 * 1024)
            payload[len(payload) // 2] = 1
            shard.write_bytes(payload)
            second_report = snapshot(root)
            self.assertNotEqual(
                first_report["snapshotSha256"],
                second_report["snapshotSha256"],
            )

    def test_spec_accepts_explicit_checkpoint_root(self):
        document = spec_document()
        document["artifacts"]["checkpointRoot"] = "/workspace/checkpoints/run-1"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            self.assertEqual(
                RuntimeSpec.load(path).checkpoint_root,
                Path("/workspace/checkpoints/run-1"),
            )

    def test_training_node_rank_comes_from_hccl_rank_start(self):
        result = {
            "stages": [
                {
                    "name": "hccl",
                    "result": {
                        "preflight": {
                            "workers": [
                                {"node_name": "node-b", "rank_start": 0},
                                {"node_name": "node-a", "rank_start": 8},
                            ]
                        }
                    },
                }
            ]
        }
        spec = SimpleNamespace(
            nodes=("node-a", "node-b"),
            workers=2,
            devices_per_node=8,
        )
        self.assertEqual(
            _node_ranks_from_hccl(result, spec),
            {"node-a": 1, "node-b": 0},
        )

    def test_hccl_gate_runs_before_ranktable_exists_and_passes_probe(self):
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=0,
            stdout=json.dumps({"status": "PASS"}),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ) as invoked:
                self.assertEqual(
                    run_hccl_gate(spec, Path(directory))["status"],
                    "PASS",
                )
        command = invoked.call_args.args[0]
        self.assertEqual(command[1:3], ["-m", "hccl_check"])
        probe_index = command.index("--probe-binary") + 1
        self.assertEqual(
            command[probe_index],
            "/opt/kcc-hccl/bin/ranktable_allreduce_probe",
        )

        device_ids_index = command.index("--device-ids") + 1
        self.assertEqual(command[device_ids_index], "0,1,2,3,4,5,6,7")
    def test_failure_before_first_checkpoint_is_retryable_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            spec = SimpleNamespace(
                checkpoint_root=Path(directory) / "checkpoints",
                run_name="run-1",
                namespace="training",
                run_uid="uid-1",
                attempt=0,
            )
            result = failure_result(
                spec,
                CoordinatorError("Ray unavailable", scope="infrastructure"),
            )
        self.assertTrue(result["checkpointConsistent"])
        self.assertFalse(result["checkpointAvailable"])
        self.assertEqual(result["failureScope"], "infrastructure")


    def test_result_configmap_payload_is_bounded(self):
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runName": "run-1",
            "namespace": "training",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "FAIL",
            "checkpointConsistent": True,
            "failureScope": "software",
            "failedNodes": [],
            "failure": "x" * (2 * 1024 * 1024),
            "hccl": {"status": "FAIL", "failure": "y" * (2 * 1024 * 1024)},
        }
        payload, truncated, digest = coordinator_module._result_payload(result)
        self.assertLessEqual(
            len(payload.encode("utf-8")),
            coordinator_module.MAX_RESULT_CONFIGMAP_BYTES,
        )
        self.assertTrue(truncated)
        self.assertEqual(len(digest), 64)

    def test_result_configmap_has_nonblocking_trainingrun_owner(self):
        result = {
            "schemaVersion": "kcc-runtime-result/v1",
            "runName": "run-1",
            "namespace": "training",
            "runUid": "uid-1",
            "attempt": 0,
            "status": "FAIL",
            "checkpointConsistent": True,
            "failureScope": "software",
            "failedNodes": [],
        }
        with patch.object(coordinator_module, "KubernetesApi") as api_type:
            coordinator_module._publish(result)
        document = api_type.return_value.upsert.call_args.args[2]
        self.assertEqual(
            document["metadata"]["ownerReferences"],
            [
                {
                    "apiVersion": "training.kcc.io/v1beta1",
                    "kind": "TrainingRun",
                    "name": "run-1",
                    "uid": "uid-1",
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        )

    def test_progress_is_printed_and_published_as_owned_configmap(self):
        spec = SimpleNamespace(
            run_name="run-1",
            namespace="training",
            run_uid="uid-1",
            attempt=2,
        )
        with patch.object(coordinator_module, "KubernetesApi") as api_type, patch(
            "builtins.print"
        ) as output:
            coordinator_module.publish_progress(
                spec,
                "Training",
                "Running",
                "distributed training is running",
                checkpointIteration=12,
            )
        document = api_type.return_value.upsert.call_args.args[2]
        progress = json.loads(document["data"]["progress.json"])
        self.assertEqual(document["metadata"]["name"], "run-1-a02-progress")
        self.assertEqual(progress["stage"], "Training")
        self.assertEqual(progress["details"]["checkpointIteration"], 12)
        history = json.loads(document["data"]["history.json"])
        self.assertEqual(history, [progress])
        self.assertTrue(output.call_args.args[0].startswith("KCC_PROGRESS "))

    def test_progress_history_keeps_milestones_and_coalesces_heartbeats(self):
        existing = [
            {"stage": "HcclTest", "status": "Passed", "message": "passed"},
            {
                "stage": "Training",
                "status": "Running",
                "message": "distributed training is running",
                "details": {"checkpointIteration": 10},
            },
        ]
        current = {"data": {"history.json": json.dumps(existing)}}
        progress = {
            "stage": "Training",
            "status": "Running",
            "message": "distributed training is running",
            "details": {"checkpointIteration": 20},
        }
        history = coordinator_module._progress_history(current, progress)
        self.assertEqual(len(history), 2)
        self.assertEqual(history[0]["stage"], "HcclTest")
        self.assertEqual(history[-1]["details"]["checkpointIteration"], 20)

    def test_training_output_chunk_reads_only_new_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "stdout.log"
            path.write_text("first\nsecond\n", encoding="utf-8")
            first, offset = coordinator_module._training_output_chunk(path, 0, 6)
            second, _offset = coordinator_module._training_output_chunk(path, offset, 64)
        self.assertEqual(first, "first\n")
        self.assertEqual(second, "second\n")

    def test_worker_progress_follows_output_from_any_node_rank(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            log_root = root / "logs"
            checkpoint_root = root / "checkpoints"
            (log_root / "node-rank-0").mkdir(parents=True)
            (log_root / "node-rank-1").mkdir(parents=True)
            for node_rank in (0, 1):
                for stream in ("stdout", "stderr"):
                    (log_root / f"node-rank-{node_rank}" / f"{stream}.log").touch()
            before = shared_training_progress_signature(
                log_root, 2, checkpoint_root
            )
            (log_root / "node-rank-1" / "stdout.log").write_text(
                "iteration 10/100\n", encoding="utf-8"
            )
            after = shared_training_progress_signature(
                log_root, 2, checkpoint_root
            )
        self.assertNotEqual(before, after)

    def test_runtime_exception_after_spec_load_is_published(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "run.json"
            path.write_text(json.dumps(spec_document()), encoding="utf-8")
            spec = RuntimeSpec.load(path)
            with patch.object(
                coordinator_module.RuntimeSpec,
                "load",
                return_value=spec,
            ), patch.object(
                coordinator_module,
                "execute",
                side_effect=ValueError("ray setup exploded"),
            ), patch.object(coordinator_module, "_publish") as publish, patch(
                "builtins.print"
            ):
                returncode = coordinator_module.main(["--spec", str(path)])
        self.assertEqual(returncode, 1)
        published = publish.call_args.args[0]
        self.assertEqual(published["status"], "FAIL")
        self.assertTrue(published["checkpointConsistent"])
        self.assertFalse(published["checkpointAvailable"])
        self.assertIn("ray setup exploded", published["failure"])

    def test_worker_injects_standard_runtime_paths(self):
        captured = {}

        class CompletedProcess:
            pid = 12345

            def poll(self):
                return 0

            def wait(self):
                return 0

        def start_process(argv, **kwargs):
            captured["argv"] = argv
            captured["env"] = kwargs["env"]
            return CompletedProcess()

        identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, identity), patch(
                "kcc_training.runtime.worker.subprocess.Popen",
                side_effect=start_process,
            ), patch.object(StructuredWorker, "_terminate"):
                outcome = StructuredWorker().run(
                    node_rank=1,
                    workers=2,
                    devices=8,
                    master_addr="10.0.0.3",
                    master_port=29501,
                    command=("python", "train.py"),
                    cwd=str(root),
                    environment={"KCC_RESUME_FROM": "stale"},
                    ranktable="/etc/kcc/ranktable/hccl.json",
                    log_root=str(root / "logs"),
                    checkpoint_root="/workspace/checkpoints/run-1",
                    output_root="/workspace/runs/run-1",
                    attempt_root="/workspace/runs/run-1/attempt-01",
                    attempt=1,
                    resume_from="/workspace/checkpoints/run-1/iter_0000007",
                    no_progress_seconds=0,
                )
        self.assertEqual(outcome["status"], "PASS")
        self.assertEqual(captured["env"]["KCC_OUTPUT_ROOT"], "/workspace/runs/run-1")
        self.assertEqual(
            captured["env"]["KCC_CHECKPOINT_ROOT"],
            "/workspace/checkpoints/run-1",
        )
        self.assertEqual(
            captured["env"]["KCC_RESUME_FROM"],
            "/workspace/checkpoints/run-1/iter_0000007",
        )
        self.assertEqual(captured["env"]["KCC_ATTEMPT"], "1")
        self.assertEqual(captured["env"]["NODE_RANK"], "1")
        self.assertEqual(captured["env"]["WORLD_SIZE"], "16")
        self.assertEqual(
            captured["env"]["RANK_TABLE_FILE"],
            "/etc/kcc/ranktable/hccl.json",
        )
        self.assertTrue(
            captured["env"]["KCC_FAILURE_REPORT_PATH"].endswith(
                "/logs/node-rank-1/failure-report.json"
            )
        )

    def test_worker_uses_structured_hardware_failure_hint(self):
        class FailedProcess:
            pid = 12345

            def poll(self):
                return 1

            def wait(self):
                return 1

        def start_process(_argv, **kwargs):
            Path(kwargs["env"]["KCC_FAILURE_REPORT_PATH"]).write_text(
                json.dumps({"failureScope": "hardware"}), encoding="utf-8"
            )
            return FailedProcess()

        identity = {
            "NODE_NAME": "node-a",
            "POD_NAME": "worker-a",
            "POD_IP": "10.0.0.2",
            "HOST_IP": "10.0.0.1",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            with patch.dict(os.environ, identity), patch(
                "kcc_training.runtime.worker.subprocess.Popen",
                side_effect=start_process,
            ), patch.object(StructuredWorker, "_terminate"):
                outcome = StructuredWorker().run(
                    node_rank=0,
                    workers=2,
                    devices=8,
                    master_addr="10.0.0.3",
                    master_port=29501,
                    command=("python", "train.py"),
                    cwd=str(root),
                    environment={},
                    ranktable="/etc/kcc/ranktable/hccl.json",
                    log_root=str(root / "logs"),
                    checkpoint_root="/workspace/checkpoints/run-1",
                    output_root="/workspace/runs/run-1",
                    attempt_root="/workspace/runs/run-1/attempt-00",
                    attempt=0,
                    resume_from=None,
                    no_progress_seconds=0,
                )
        self.assertEqual(outcome["status"], "FAIL")
        self.assertEqual(outcome["failureScope"], "hardware")

    def test_ping_setup_failure_is_infrastructure(self):
        payload = {
            "status": "FAIL",
            "failed_stage": "ping",
            "failure": "hccn_tool is missing or not executable",
        }
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ):
                with self.assertRaises(CoordinatorError) as raised:
                    run_hccl_gate(spec, Path(directory))
        self.assertEqual(raised.exception.scope, "infrastructure")
        self.assertEqual(raised.exception.failed_nodes, ())

    def test_hccl_hardware_scope_requires_non_peer_failed_node(self):
        payload = {
            "status": "FAIL",
            "failed_stage": "hccl",
            "failure": "one rank failed",
            "stages": [
                {
                    "name": "hccl",
                    "result": {
                        "preflight": {
                            "workers": [
                                {"node_name": "node-a", "rank_start": 0},
                                {"node_name": "node-b", "rank_start": 8},
                            ]
                        },
                        "workers": [
                            {"status": "FAIL", "rank_start": 0, "failure": "allreduce failed"},
                            {"status": "FAIL", "rank_start": 8, "failure": "stop requested by Ray driver"},
                        ],
                    },
                }
            ],
        }
        spec = SimpleNamespace(
            namespace="training",
            run_name="run-1",
            attempt=0,
            workers=2,
            devices_per_node=8,
            ranktable_path=Path("/etc/kcc/ranktable/hccl.json"),
        )
        completed = SimpleNamespace(
            returncode=1,
            stdout=json.dumps(payload),
            stderr="",
        )
        with tempfile.TemporaryDirectory() as directory:
            with patch(
                "kcc_training.runtime.coordinator.subprocess.run",
                return_value=completed,
            ):
                with self.assertRaises(CoordinatorError) as raised:
                    run_hccl_gate(spec, Path(directory))
        self.assertEqual(raised.exception.scope, "hardware")
        self.assertEqual(raised.exception.failed_nodes, ("node-a",))


    def test_hccn_tool_preflight_allows_non_root_execution(self):
        with tempfile.TemporaryDirectory() as directory:
            tool = Path(directory) / "hccn_tool"
            tool.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            tool.chmod(0o755)
            with patch.object(ping_module.os, "geteuid", return_value=65532):
                ping_module.require_hccn_tool(str(tool))

if __name__ == "__main__":
    unittest.main()
