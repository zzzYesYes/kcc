#!/usr/bin/env python3
"""Run topology-frozen training scripts on whole NPU workers through Ray."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Mapping, Sequence


RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
MAX_SCRIPT_BYTES = 512 * 1024
MAX_LOG_TAIL_BYTES = 64 * 1024
MAX_TRACKER_BYTES = 256
ACTOR_PREFLIGHT_TIMEOUT_SECONDS = 300
RANK_TABLE_PATH = "/user/serverid/devindex/config/hccl.json"
MS_TORCHRUN = "/root/miniconda3/envs/ms/bin/torchrun"
CHECKPOINT_TRACKER = "latest_checkpointed_iteration.txt"
FRESH_ARCHIVE_ROOT = "/mnt/models/pretrain-ray-platform/archive"
PROCESS_SUPERVISOR = r"""
import ctypes
import os
import signal
import subprocess
import sys
import time

child = None
terminating = False


def terminate_group(_signum, _frame):
    global terminating
    if terminating:
        return
    terminating = True
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    try:
        os.killpg(os.getpgrp(), signal.SIGTERM)
    except ProcessLookupError:
        pass
    time.sleep(10.0)
    try:
        os.killpg(os.getpgrp(), signal.SIGKILL)
    except ProcessLookupError:
        raise SystemExit(143)


expected_parent = int(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(1, signal.SIGTERM, 0, 0, 0) != 0:
    raise OSError(ctypes.get_errno(), "prctl(PR_SET_PDEATHSIG) failed")
signal.signal(signal.SIGTERM, terminate_group)
if os.getppid() != expected_parent:
    terminate_group(signal.SIGTERM, None)
child = subprocess.Popen(
    ["/bin/bash", "-o", "pipefail", "-s"],
    stdin=sys.stdin,
    stdout=sys.stdout,
    stderr=sys.stderr,
    text=True,
)
raise SystemExit(child.wait())
"""


class DriverError(RuntimeError):
    pass


class CheckpointError(DriverError):
    pass


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_file(path: Path, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise DriverError(f"{label} is not a regular file: {path}")


def validate_ranktable_mount(path: Path, expected_sha256: str) -> str:
    # Kubernetes projects ConfigMap keys as kubelet-managed symlinks.  Follow
    # that link, then pin the mounted bytes to the HCCL-validated digest.
    if not path.is_file():
        raise RuntimeError(f"RankTable is not a regular file: {path}")
    actual_sha256 = sha256_file(path)
    if actual_sha256 != expected_sha256:
        raise RuntimeError("mounted RankTable digest differs from HCCL evidence")
    return actual_sha256


def require_mapping(value: Any, label: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise DriverError(f"{label} must be an object")
    return value


def expected_legacy_rank_dirs(
    tensor_parallel_size: int,
    pipeline_parallel_size: int,
) -> tuple[str, ...]:
    if tensor_parallel_size <= 0 or pipeline_parallel_size <= 0:
        raise CheckpointError("checkpoint TP and PP sizes must be positive")
    names: list[str] = []
    for tensor_rank in range(tensor_parallel_size):
        for pipeline_rank in range(pipeline_parallel_size):
            if pipeline_parallel_size == 1:
                names.append(f"mp_rank_{tensor_rank:02d}")
            else:
                names.append(f"mp_rank_{tensor_rank:02d}_{pipeline_rank:03d}")
    return tuple(names)


def inspect_committed_checkpoint(policy: Mapping[str, Any]) -> dict[str, Any]:
    """Inspect the checkpoint selected by Megatron's commit tracker.

    This intentionally does not scan for the numerically largest ``iter_*``
    directory.  A failed save may leave such a directory behind; Megatron
    updates the tracker only after the checkpoint save has completed.
    """

    load_dir_value = policy.get("loadDir")
    if not isinstance(load_dir_value, str) or not (
        load_dir_value.startswith("/") or Path(load_dir_value).is_absolute()
    ):
        raise CheckpointError("checkpoint loadDir is not an absolute worker path")
    if policy.get("trackerFilename") != CHECKPOINT_TRACKER:
        raise CheckpointError("checkpoint tracker filename is unsupported")
    if policy.get("selection") != "megatron-tracker":
        raise CheckpointError("checkpoint selection policy is unsupported")
    if policy.get("format") != "torch":
        raise CheckpointError("strict recovery supports legacy torch checkpoints only")
    tensor_parallel_size = policy.get("tensorParallelSize")
    pipeline_parallel_size = policy.get("pipelineParallelSize")
    if not isinstance(tensor_parallel_size, int) or isinstance(
        tensor_parallel_size, bool
    ):
        raise CheckpointError("checkpoint tensorParallelSize is invalid")
    if not isinstance(pipeline_parallel_size, int) or isinstance(
        pipeline_parallel_size, bool
    ):
        raise CheckpointError("checkpoint pipelineParallelSize is invalid")
    distributed_optimizer = policy.get("distributedOptimizer")
    if not isinstance(distributed_optimizer, bool):
        raise CheckpointError("checkpoint distributedOptimizer flag is invalid")

    load_dir = Path(load_dir_value)
    tracker = load_dir / CHECKPOINT_TRACKER
    if not tracker.is_file():
        raise CheckpointError(f"checkpoint tracker is missing: {tracker}")
    try:
        if tracker.stat().st_size > MAX_TRACKER_BYTES:
            raise CheckpointError("checkpoint tracker is unexpectedly large")
        tracker_bytes = tracker.read_bytes()
        tracker_value = tracker_bytes.decode("utf-8").strip()
    except (OSError, UnicodeError) as error:
        raise CheckpointError(f"cannot read checkpoint tracker: {error}") from error
    if tracker_value == "release":
        if policy.get("requiredForRecovery") is True:
            raise CheckpointError(
                "release checkpoint is not a resumable training checkpoint"
            )
        iteration: int | None = None
        candidate_name = "release"
    else:
        try:
            iteration = int(tracker_value)
        except ValueError as error:
            raise CheckpointError("checkpoint tracker is not a valid iteration") from error
        if iteration <= 0:
            raise CheckpointError("checkpoint tracker iteration must be positive")
        candidate_name = f"iter_{iteration:07d}"

    candidate = load_dir / candidate_name
    if not candidate.is_dir():
        raise CheckpointError(
            f"checkpoint tracker target directory is missing: {candidate}"
        )

    files: list[dict[str, Any]] = []
    for rank_dir_name in expected_legacy_rank_dirs(
        tensor_parallel_size,
        pipeline_parallel_size,
    ):
        rank_dir = candidate / rank_dir_name
        if not rank_dir.is_dir():
            raise CheckpointError(f"checkpoint rank directory is missing: {rank_dir}")
        required_names = ["model_optim_rng.pt"]
        if distributed_optimizer:
            required_names.append("distrib_optim.pt")
        for filename in required_names:
            checkpoint_file = rank_dir / filename
            if not checkpoint_file.is_file():
                raise CheckpointError(
                    f"checkpoint shard is missing: {checkpoint_file}"
                )
            try:
                size = checkpoint_file.stat().st_size
                with checkpoint_file.open("rb") as stream:
                    first_byte = stream.read(1)
            except OSError as error:
                raise CheckpointError(
                    f"checkpoint shard is unreadable: {checkpoint_file}: {error}"
                ) from error
            if size <= 0 or not first_byte:
                raise CheckpointError(f"checkpoint shard is empty: {checkpoint_file}")
            files.append(
                {
                    "path": str(checkpoint_file.relative_to(load_dir)),
                    "size": size,
                }
            )

    return {
        "status": "AVAILABLE_RESUME",
        "loadDir": load_dir_value,
        "tracker": str(tracker),
        "trackerValue": tracker_value,
        "trackerSha256": sha256_bytes(tracker_bytes),
        "iteration": iteration,
        "selectedDir": str(candidate),
        "files": files,
    }


def ensure_matching_checkpoint_views(
    views: Sequence[Mapping[str, Any]],
    *,
    expected_workers: int,
) -> None:
    if len(views) != expected_workers or not views:
        raise CheckpointError("checkpoint evidence is missing from one or more workers")
    comparable = {
        json.dumps(
            {
                "status": view.get("status"),
                "loadDir": view.get("loadDir"),
                "trackerValue": view.get("trackerValue"),
                "trackerSha256": view.get("trackerSha256"),
                "iteration": view.get("iteration"),
                "selectedDir": view.get("selectedDir"),
                "files": view.get("files"),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
        for view in views
    }
    if len(comparable) != 1:
        raise CheckpointError("active workers see different checkpoint snapshots")


def create_fresh_archive(archive_root: Path) -> str:
    archive_root.parent.mkdir(parents=True, exist_ok=True)
    try:
        archive_root.mkdir(exist_ok=False)
        (archive_root / "checkpoints").mkdir()
        logs_dir = archive_root / "logs"
        logs_dir.mkdir()
        (logs_dir / "wandb").mkdir()
        (logs_dir / "tensorboard").mkdir()
    except FileExistsError as error:
        raise DriverError(
            f"fresh archive already exists; choose a new run ID: {archive_root}"
        ) from error
    except OSError as error:
        raise DriverError(
            f"cannot create fresh archive {archive_root}: {error}"
        ) from error
    return str(archive_root)


def load_injection(
    injection_path: Path,
    scripts_dir: Path,
) -> tuple[
    Mapping[str, Any],
    dict[str, dict[str, Any]],
    dict[str, str],
]:
    require_file(injection_path, "injection manifest")
    try:
        injection = json.loads(injection_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise DriverError(f"cannot parse injection manifest: {error}") from error
    injection = require_mapping(injection, "injection manifest")
    if injection.get("schemaVersion") != "training-injection/v1":
        raise DriverError("unsupported injection manifest schema")
    run_id = injection.get("runId")
    if not isinstance(run_id, str) or RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise DriverError("injection runId is invalid")
    launch_mode = injection.get("launchMode", "resume")
    if launch_mode not in {"resume", "fresh"}:
        raise DriverError("injection launch mode is invalid")

    topology = require_mapping(injection.get("topology"), "topology")
    worker_count = topology.get("workers")
    npus_per_worker = topology.get("npusPerWorker")
    world_size = topology.get("worldSize")
    if (
        not isinstance(worker_count, int)
        or worker_count <= 0
        or not isinstance(npus_per_worker, int)
        or npus_per_worker <= 0
        or world_size != worker_count * npus_per_worker
    ):
        raise DriverError("injection topology is invalid")
    master_port = topology.get("masterPort")
    if not isinstance(master_port, int) or not 1024 <= master_port <= 65535:
        raise DriverError("injection master port is invalid")
    if topology.get("rankTablePath") != RANK_TABLE_PATH:
        raise DriverError("injection RankTable path differs from the mounted path")
    ranktable_sha256 = topology.get("rankTableSha256")
    if (
        not isinstance(ranktable_sha256, str)
        or SHA256_PATTERN.fullmatch(ranktable_sha256) is None
    ):
        raise DriverError("injection RankTable SHA256 is invalid")

    runtime = require_mapping(injection.get("runtime"), "runtime")
    training_cwd = runtime.get("trainingCwd")
    if not isinstance(training_cwd, str) or not training_cwd.startswith("/"):
        raise DriverError("training cwd is not an absolute worker path")
    if runtime.get("torchrun") != MS_TORCHRUN:
        raise DriverError("injection does not select the ms torchrun")
    if launch_mode == "fresh":
        expected_archive_root = f"{FRESH_ARCHIVE_ROOT}/{run_id}"
        expected_log_root = f"{expected_archive_root}/logs"
        if runtime.get("archiveRoot") != expected_archive_root:
            raise DriverError("fresh archive root is not the run-specific path")
    else:
        expected_archive_root = None
        expected_log_root = f"/mnt/models/pretrain-ray-platform/log/{run_id}"
        if runtime.get("archiveRoot") not in (None,):
            raise DriverError("resume injection unexpectedly defines an archive root")
    if runtime.get("logRoot") != expected_log_root:
        raise DriverError("injection log root is not the run-specific path")

    raw_checkpoint_load = injection.get("checkpointLoad")
    if raw_checkpoint_load is None:
        raw_checkpoint_load = {
            "enabled": False,
            "requiredForRecovery": False,
        }
        injection["checkpointLoad"] = raw_checkpoint_load
    checkpoint_load = require_mapping(raw_checkpoint_load, "checkpointLoad")
    checkpoint_enabled = checkpoint_load.get("enabled")
    checkpoint_required = checkpoint_load.get("requiredForRecovery")
    if not isinstance(checkpoint_enabled, bool) or not isinstance(
        checkpoint_required, bool
    ):
        raise DriverError("checkpointLoad enable/require flags are invalid")
    if checkpoint_required and not checkpoint_enabled:
        raise DriverError("formal recovery injection has checkpoint loading disabled")
    raw_nodes = injection.get("nodes")
    if not isinstance(raw_nodes, list) or len(raw_nodes) != worker_count:
        raise DriverError("injection node count differs from the topology")
    nodes_by_pod: dict[str, dict[str, Any]] = {}
    scripts_by_pod: dict[str, str] = {}
    ranks: set[int] = set()
    pod_ips: set[str] = set()
    for raw_node in raw_nodes:
        node = dict(require_mapping(raw_node, "injection node"))
        node_rank = node.get("nodeRank")
        if (
            not isinstance(node_rank, int)
            or not 0 <= node_rank < worker_count
            or node_rank in ranks
        ):
            raise DriverError("injection node ranks are invalid or duplicated")
        ranks.add(node_rank)
        if node.get("rankStart") != node_rank * npus_per_worker:
            raise DriverError(f"node rank {node_rank} has the wrong rank start")
        pod_name = node.get("podName")
        if (
            not isinstance(pod_name, str)
            or not pod_name
            or pod_name in nodes_by_pod
        ):
            raise DriverError("injection Pod names are invalid or duplicated")
        try:
            pod_ip = str(ipaddress.ip_address(node.get("podIp")))
        except (TypeError, ValueError) as error:
            raise DriverError(f"injection Pod IP is invalid: {node.get('podIp')}") from error
        if pod_ip != node.get("podIp") or pod_ip in pod_ips:
            raise DriverError("injection Pod IP is not normalized or is duplicated")
        pod_ips.add(pod_ip)

        script_name = node.get("script")
        if not isinstance(script_name, str) or Path(script_name).name != script_name:
            raise DriverError("injection script name is invalid")
        script_path = scripts_dir / script_name
        require_file(script_path, f"node rank {node_rank} script")
        if script_path.stat().st_size > MAX_SCRIPT_BYTES:
            raise DriverError(f"node rank {node_rank} script is too large")
        expected_sha256 = node.get("scriptSha256")
        if (
            not isinstance(expected_sha256, str)
            or SHA256_PATTERN.fullmatch(expected_sha256) is None
            or sha256_file(script_path) != expected_sha256
        ):
            raise DriverError(f"node rank {node_rank} script digest differs")
        script = script_path.read_text(encoding="utf-8")
        required_fragments = (
            f"export RANK_TABLE_FILE={RANK_TABLE_PATH}",
            f"NPUS_PER_NODE={npus_per_worker}",
            f"MASTER_ADDR={topology.get('masterAddr')}",
            f"MASTER_PORT={master_port}",
            f"NNODES={worker_count}",
            f"NODE_RANK={node_rank}",
            MS_TORCHRUN,
            f'LOG_FILE="{expected_log_root}/logs/node-rank-{node_rank}.log"',
        )
        if launch_mode == "fresh":
            required_fragments = (
                *required_fragments[:-1],
                f'LOG_FILE="{expected_log_root}/node-rank-{node_rank}.log"',
                f'CKPT_SAVE_DIR="{expected_archive_root}/checkpoints"',
            )
        missing = [fragment for fragment in required_fragments if fragment not in script]
        if missing:
            raise DriverError(
                f"node rank {node_rank} script lacks injected fields: {missing}"
            )
        node["podIp"] = pod_ip
        nodes_by_pod[pod_name] = node
        scripts_by_pod[pod_name] = script

    if ranks != set(range(worker_count)):
        raise DriverError("injection node ranks are not contiguous")
    rank_zero = next(node for node in nodes_by_pod.values() if node["nodeRank"] == 0)
    if topology.get("masterAddr") != rank_zero["podIp"]:
        raise DriverError("MASTER_ADDR is not the rank-0 worker Pod IP")
    return injection, nodes_by_pod, scripts_by_pod


def tail_text(path: Path) -> str:
    try:
        with path.open("rb") as stream:
            stream.seek(0, os.SEEK_END)
            size = stream.tell()
            stream.seek(max(0, size - MAX_LOG_TAIL_BYTES), os.SEEK_SET)
            return stream.read().decode("utf-8", errors="replace")
    except OSError as error:
        return f"<cannot read log tail: {error}>"


def training_environment(base: Mapping[str, str]) -> dict[str, str]:
    environment = dict(base)
    ms_bin = str(Path(MS_TORCHRUN).parent)
    current_path = environment.get("PATH", "")
    environment["PATH"] = f"{ms_bin}:{current_path}" if current_path else ms_bin
    environment["CONDA_PREFIX"] = str(Path(MS_TORCHRUN).parents[1])
    environment["CONDA_DEFAULT_ENV"] = "ms"
    environment.pop("PYTHONHOME", None)
    environment["PYTHONUNBUFFERED"] = "1"
    return environment


def execute_on_ray(
    injection: Mapping[str, Any],
    nodes_by_pod: Mapping[str, Mapping[str, Any]],
    scripts_by_pod: Mapping[str, str],
    *,
    timeout_seconds: int,
) -> dict[str, Any]:
    try:
        import ray
    except ModuleNotFoundError as error:
        raise DriverError("Ray is not installed in the head image") from error

    class TrainingActor:
        def __init__(self) -> None:
            self._process: subprocess.Popen[str] | None = None
            self._lock = threading.Lock()
            self._stop_reason: str | None = None

        def identity(self) -> dict[str, str]:
            names = ("POD_NAME", "POD_IP", "NODE_NAME", "HOST_IP")
            identity = {name: os.environ.get(name, "") for name in names}
            if any(not value for value in identity.values()):
                raise RuntimeError(f"worker identity is incomplete: {identity}")
            return identity

        @staticmethod
        def _terminate_process_group(
            process: subprocess.Popen[str],
            *,
            grace_seconds: float = 15.0,
        ) -> None:
            process_group_id = process.pid
            try:
                os.killpg(process_group_id, signal.SIGTERM)
            except ProcessLookupError:
                return
            deadline = time.monotonic() + grace_seconds
            while time.monotonic() < deadline:
                process.poll()
                try:
                    os.killpg(process_group_id, 0)
                except ProcessLookupError:
                    return
                time.sleep(0.2)
            try:
                os.killpg(process_group_id, signal.SIGKILL)
            except ProcessLookupError:
                pass

        def preflight(
            self,
            expected: Mapping[str, Any],
            topology: Mapping[str, Any],
            runtime: Mapping[str, Any],
        ) -> dict[str, Any]:
            identity = self.identity()
            for env_name, expected_field in (
                ("POD_NAME", "podName"),
                ("POD_IP", "podIp"),
                ("NODE_NAME", "nodeName"),
                ("HOST_IP", "serverId"),
            ):
                if identity[env_name] != expected.get(expected_field):
                    raise RuntimeError(
                        f"{env_name} differs from frozen topology: "
                        f"{identity[env_name]} != {expected.get(expected_field)}"
                    )
            training_cwd = Path(str(runtime["trainingCwd"]))
            pretrain_gpt = training_cwd / "pretrain_gpt.py"
            if (
                not training_cwd.is_dir()
                or training_cwd.is_symlink()
                or not pretrain_gpt.is_file()
                or pretrain_gpt.is_symlink()
            ):
                raise RuntimeError(
                    f"training cwd lacks a regular pretrain_gpt.py: {training_cwd}"
                )
            torchrun = Path(str(runtime["torchrun"]))
            if (
                not torchrun.is_file()
                or torchrun.is_symlink()
                or not os.access(torchrun, os.X_OK)
            ):
                raise RuntimeError(f"ms torchrun is not executable: {torchrun}")
            ranktable = Path(str(topology["rankTablePath"]))
            actual_ranktable_sha256 = validate_ranktable_mount(
                ranktable, str(topology["rankTableSha256"])
            )
            npus_per_worker = int(topology["npusPerWorker"])
            missing_devices = [
                str(Path("/dev") / f"davinci{device_id}")
                for device_id in range(npus_per_worker)
                if not (Path("/dev") / f"davinci{device_id}").exists()
            ]
            if missing_devices:
                raise RuntimeError(f"worker NPU devices are missing: {missing_devices}")
            if expected["nodeRank"] == 0:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
                    listener.bind(
                        (
                            str(topology["masterAddr"]),
                            int(topology["masterPort"]),
                        )
                    )
            return {
                "status": "PASS",
                "identity": identity,
                "trainingCwd": str(training_cwd),
                "torchrun": str(torchrun),
                "rankTableSha256": actual_ranktable_sha256,
                "npuDevices": npus_per_worker,
            }

        def prepare_fresh_archive(
            self,
            archive_root: str,
        ) -> str:
            return create_fresh_archive(Path(archive_root))

        def checkpoint_preflight(
            self,
            policy: Mapping[str, Any],
        ) -> dict[str, Any]:
            identity = self.identity()
            try:
                report = inspect_committed_checkpoint(policy)
            except CheckpointError as error:
                return {
                    "status": "CHECKPOINT_UNAVAILABLE",
                    "identity": identity,
                    "failure": str(error),
                }
            report["identity"] = identity
            return report

        def run(
            self,
            *,
            expected: Mapping[str, Any],
            script: str,
            script_sha256: str,
            runtime: Mapping[str, Any],
            run_id: str,
            timeout: int,
        ) -> dict[str, Any]:
            started_at = utc_now()
            started = time.monotonic()
            if sha256_bytes(script.encode("utf-8")) != script_sha256:
                raise RuntimeError("script payload digest differs from injection")
            node_rank = int(expected["nodeRank"])
            rank_dir = (
                Path(str(runtime["logRoot"]))
                / "ray-driver"
                / f"node-rank-{node_rank}"
            )
            if rank_dir.exists() or rank_dir.is_symlink():
                raise RuntimeError(f"run log directory already exists: {rank_dir}")
            rank_dir.mkdir(parents=True, exist_ok=False)
            stdout_path = rank_dir / "stdout.log"
            stderr_path = rank_dir / "stderr.log"
            environment = training_environment(os.environ)
            environment["TRAINCTL_RUN_ID"] = run_id
            with (
                stdout_path.open("x", encoding="utf-8") as stdout_stream,
                stderr_path.open("x", encoding="utf-8") as stderr_stream,
            ):
                with self._lock:
                    if self._stop_reason is not None:
                        raise RuntimeError(
                            f"training was stopped before launch: {self._stop_reason}"
                        )
                    process = subprocess.Popen(
                        [
                            sys.executable,
                            "-c",
                            PROCESS_SUPERVISOR,
                            str(os.getpid()),
                        ],
                        stdin=subprocess.PIPE,
                        stdout=stdout_stream,
                        stderr=stderr_stream,
                        text=True,
                        cwd=str(runtime["trainingCwd"]),
                        env=environment,
                        start_new_session=True,
                    )
                    self._process = process
                timed_out = False
                try:
                    process.communicate(
                        input=script,
                        timeout=None if timeout == 0 else timeout,
                    )
                except subprocess.TimeoutExpired:
                    timed_out = True
                    self._terminate_process_group(process)
                    process.wait()
                finally:
                    self._terminate_process_group(process, grace_seconds=3.0)
                    with self._lock:
                        if self._process is process:
                            self._process = None
            returncode = process.returncode
            status = "PASS" if returncode == 0 else "FAIL"
            if timed_out:
                status = "TIMEOUT"
            elif self._stop_reason is not None and returncode != 0:
                status = "STOPPED"
            return {
                "status": status,
                "nodeRank": node_rank,
                "podName": expected["podName"],
                "nodeName": expected["nodeName"],
                "returncode": returncode,
                "stopReason": self._stop_reason,
                "startedAt": started_at,
                "finishedAt": utc_now(),
                "durationSeconds": round(time.monotonic() - started, 3),
                "stdoutLog": str(stdout_path),
                "stderrLog": str(stderr_path),
                "stdoutTail": tail_text(stdout_path),
                "stderrTail": tail_text(stderr_path),
            }

        def stop(self, reason: str) -> dict[str, Any]:
            self._stop_reason = reason
            with self._lock:
                process = self._process
            if process is not None:
                self._terminate_process_group(process)
            return {"status": "STOP_REQUESTED", "reason": reason}

    topology = require_mapping(injection.get("topology"), "topology")
    runtime = require_mapping(injection.get("runtime"), "runtime")
    run_id = str(injection["runId"])
    launch_mode = str(injection.get("launchMode", "resume"))
    worker_count = int(topology["workers"])
    npus_per_worker = int(topology["npusPerWorker"])
    actors: list[Any] = []
    outcomes: list[dict[str, Any]] = []
    preflights: list[dict[str, Any]] = []
    checkpoint_result: dict[str, Any] = {
        "status": "NOT_CHECKED",
    }

    ray.init(address="auto")
    try:
        remote_actor = ray.remote(
            num_cpus=1,
            resources={"NPU": npus_per_worker, "trainctl_worker": 1},
            max_concurrency=2,
        )(TrainingActor)
        actors = [remote_actor.remote() for _ in range(worker_count)]
        try:
            identities = ray.get(
                [actor.identity.remote() for actor in actors],
                timeout=ACTOR_PREFLIGHT_TIMEOUT_SECONDS,
            )
        except Exception as error:
            return {
                "schemaVersion": "ray-training-result/v1",
                "runId": run_id,
                "status": "FAIL",
                "failureClass": "WORKER_RUNTIME_FAILURE",
                "failurePhase": "actor-identity",
                "failure": f"{type(error).__name__}: {error}",
                "topology": dict(topology),
                "preflights": [],
                "checkpoint": {"status": "NOT_CHECKED"},
                "nodes": [],
            }
        actor_by_pod: dict[str, Any] = {}
        for actor, identity in zip(actors, identities):
            pod_name = identity.get("POD_NAME")
            if not isinstance(pod_name, str) or pod_name in actor_by_pod:
                raise DriverError("Ray scheduled duplicate or unidentified workers")
            actor_by_pod[pod_name] = actor
        if set(actor_by_pod) != set(nodes_by_pod):
            raise DriverError(
                "Ray actor workers differ from the HCCL-frozen worker Pods"
            )

        ordered_nodes = sorted(
            nodes_by_pod.values(),
            key=lambda item: int(item["nodeRank"]),
        )
        ordered_actors = [actor_by_pod[str(node["podName"])] for node in ordered_nodes]
        try:
            preflights = ray.get(
                [
                    actor.preflight.remote(node, topology, runtime)
                    for actor, node in zip(ordered_actors, ordered_nodes)
                ],
                timeout=ACTOR_PREFLIGHT_TIMEOUT_SECONDS,
            )
        except Exception as error:
            return {
                "schemaVersion": "ray-training-result/v1",
                "runId": run_id,
                "status": "FAIL",
                "failureClass": "WORKER_RUNTIME_FAILURE",
                "failurePhase": "actor-preflight",
                "failure": f"{type(error).__name__}: {error}",
                "topology": dict(topology),
                "preflights": [],
                "checkpoint": {"status": "NOT_CHECKED"},
                "nodes": [],
            }
        if launch_mode == "fresh":
            archive_root = str(runtime["archiveRoot"])
            try:
                ray.get(
                    ordered_actors[0].prepare_fresh_archive.remote(
                        archive_root,
                    ),
                    timeout=ACTOR_PREFLIGHT_TIMEOUT_SECONDS,
                )
            except Exception as error:
                return {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": run_id,
                    "status": "FAIL",
                    "failureClass": "WORKER_RUNTIME_FAILURE",
                    "failurePhase": "fresh-archive-preparation",
                    "failure": f"{type(error).__name__}: {error}",
                    "topology": dict(topology),
                    "preflights": preflights,
                    "checkpoint": {"status": "NOT_CHECKED"},
                    "nodes": [],
                }
        checkpoint_policy = require_mapping(
            injection.get("checkpointLoad"),
            "checkpointLoad",
        )
        if checkpoint_policy.get("requiredForRecovery") is True:
            checkpoint_refs = [
                actor.checkpoint_preflight.remote(checkpoint_policy)
                for actor in ordered_actors
            ]
            checkpoint_views: list[dict[str, Any]] = []
            checkpoint_failures: list[dict[str, Any]] = []
            worker_runtime_failures: list[dict[str, Any]] = []
            protocol_failures: list[dict[str, Any]] = []
            checkpoint_wait_failure: str | None = None
            try:
                ready_checkpoint_refs, _ = ray.wait(
                    checkpoint_refs,
                    num_returns=len(checkpoint_refs),
                    timeout=ACTOR_PREFLIGHT_TIMEOUT_SECONDS,
                )
                ready_checkpoint_set = set(ready_checkpoint_refs)
            except Exception as error:
                ready_checkpoint_set = set()
                checkpoint_wait_failure = f"{type(error).__name__}: {error}"
            for node, reference in zip(ordered_nodes, checkpoint_refs):
                if reference not in ready_checkpoint_set:
                    failure = checkpoint_wait_failure or (
                        "checkpoint preflight exceeded "
                        f"{ACTOR_PREFLIGHT_TIMEOUT_SECONDS}s"
                    )
                    worker_runtime_failures.append(
                        {
                            "nodeRank": node["nodeRank"],
                            "podName": node["podName"],
                            "nodeName": node["nodeName"],
                            "failure": failure,
                        }
                    )
                    continue
                try:
                    observation = dict(ray.get(reference))
                except Exception as error:
                    worker_runtime_failures.append(
                        {
                            "nodeRank": node["nodeRank"],
                            "podName": node["podName"],
                            "nodeName": node["nodeName"],
                            "failure": f"{type(error).__name__}: {error}",
                        }
                    )
                    continue
                if observation.get("status") == "AVAILABLE_RESUME":
                    checkpoint_views.append(observation)
                elif observation.get("status") == "CHECKPOINT_UNAVAILABLE":
                    checkpoint_failures.append(
                        {
                            "nodeRank": node["nodeRank"],
                            "podName": node["podName"],
                            "nodeName": node["nodeName"],
                            "failure": observation.get("failure"),
                            "identity": observation.get("identity"),
                        }
                    )
                else:
                    protocol_failures.append(
                        {
                            "nodeRank": node["nodeRank"],
                            "podName": node["podName"],
                            "nodeName": node["nodeName"],
                            "failure": "worker returned invalid checkpoint evidence",
                        }
                    )
            if protocol_failures:
                checkpoint_result = {
                    "status": "INVALID_EVIDENCE",
                    "policy": dict(checkpoint_policy),
                    "nodes": checkpoint_views,
                    "protocolFailures": protocol_failures,
                    "workerRuntimeFailures": worker_runtime_failures,
                }
                return {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": run_id,
                    "status": "FAIL",
                    "failureClass": "DRIVER_PROTOCOL_FAILURE",
                    "topology": dict(topology),
                    "preflights": preflights,
                    "checkpoint": checkpoint_result,
                    "nodes": [],
                }
            if worker_runtime_failures:
                checkpoint_result = {
                    "status": "INTERRUPTED",
                    "policy": dict(checkpoint_policy),
                    "nodes": checkpoint_views,
                    "checkpointFailures": checkpoint_failures,
                    "workerRuntimeFailures": worker_runtime_failures,
                }
                return {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": run_id,
                    "status": "FAIL",
                    "failureClass": "WORKER_RUNTIME_FAILURE",
                    "topology": dict(topology),
                    "preflights": preflights,
                    "checkpoint": checkpoint_result,
                    "nodes": [],
                }
            if not checkpoint_failures:
                try:
                    ensure_matching_checkpoint_views(
                        checkpoint_views,
                        expected_workers=worker_count,
                    )
                except CheckpointError as error:
                    checkpoint_failures.append({"failure": str(error)})
            if checkpoint_failures:
                checkpoint_result = {
                    "status": "FAIL",
                    "policy": dict(checkpoint_policy),
                    "nodes": checkpoint_views,
                    "failures": checkpoint_failures,
                }
                return {
                    "schemaVersion": "ray-training-result/v1",
                    "runId": run_id,
                    "status": "FAIL",
                    "failureClass": "CHECKPOINT_UNAVAILABLE",
                    "topology": dict(topology),
                    "preflights": preflights,
                    "checkpoint": checkpoint_result,
                    "nodes": [],
                }
            checkpoint_result = {
                "status": "PASS",
                "policy": dict(checkpoint_policy),
                "selectedIteration": checkpoint_views[0]["iteration"],
                "selectedDir": checkpoint_views[0]["selectedDir"],
                "nodes": checkpoint_views,
            }
        elif checkpoint_policy.get("enabled") is True:
            checkpoint_result = {
                "status": "NOT_REQUIRED",
                "policy": dict(checkpoint_policy),
            }
        else:
            checkpoint_result = {
                "status": "DISABLED",
                "policy": dict(checkpoint_policy),
            }

        run_refs: dict[Any, tuple[Any, Mapping[str, Any]]] = {}
        for actor, node in zip(ordered_actors, ordered_nodes):
            pod_name = str(node["podName"])
            ref = actor.run.remote(
                expected=node,
                script=scripts_by_pod[pod_name],
                script_sha256=str(node["scriptSha256"]),
                runtime=runtime,
                run_id=run_id,
                timeout=timeout_seconds,
            )
            run_refs[ref] = (actor, node)

        failure_seen = False
        while run_refs:
            ready, _ = ray.wait(list(run_refs), num_returns=1)
            ref = ready[0]
            actor, node = run_refs.pop(ref)
            try:
                outcome = ray.get(ref)
            except Exception as error:  # Ray wraps remote exceptions.
                outcome = {
                    "status": "FAIL",
                    "nodeRank": node["nodeRank"],
                    "podName": node["podName"],
                    "nodeName": node["nodeName"],
                    "returncode": None,
                    "failure": f"{type(error).__name__}: {error}",
                }
            outcomes.append(dict(outcome))
            if outcome.get("status") != "PASS" and not failure_seen:
                failure_seen = True
                reason = (
                    f"peer node rank {node['nodeRank']} ended with "
                    f"{outcome.get('status')}"
                )
                for pending_actor, _pending_node in run_refs.values():
                    pending_actor.stop.remote(reason)
        outcomes.sort(key=lambda item: int(item["nodeRank"]))
        status = "PASS" if all(item["status"] == "PASS" for item in outcomes) else "FAIL"
        return {
            "schemaVersion": "ray-training-result/v1",
            "runId": run_id,
            "status": status,
            "topology": dict(topology),
            "preflights": preflights,
            "checkpoint": checkpoint_result,
            "nodes": outcomes,
        }
    finally:
        stop_refs: list[Any] = []
        for actor in actors:
            try:
                stop_refs.append(actor.stop.remote("driver finalization"))
            except Exception:
                pass
        if stop_refs:
            try:
                ray.get(stop_refs, timeout=30)
            except Exception:
                pass
        for actor in actors:
            try:
                ray.kill(actor, no_restart=True)
            except Exception:
                pass
        ray.shutdown()


def write_result_create_only(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
    except FileExistsError as error:
        raise DriverError(f"refusing to overwrite result: {path}") from error


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--injection", type=Path, required=True)
    parser.add_argument("--scripts-dir", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=0,
        help="per-worker training timeout; 0 means no timeout",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    started_at = utc_now()
    status = 1
    run_id: str | None = None
    try:
        if args.timeout_seconds < 0:
            raise DriverError("timeout must be zero or positive")
        injection, nodes_by_pod, scripts_by_pod = load_injection(
            args.injection.resolve(),
            args.scripts_dir.resolve(),
        )
        run_id = str(injection["runId"])
        result = execute_on_ray(
            injection,
            nodes_by_pod,
            scripts_by_pod,
            timeout_seconds=args.timeout_seconds,
        )
        result["startedAt"] = started_at
        result["finishedAt"] = utc_now()
        status = 0 if result.get("status") == "PASS" else 1
    except (Exception, KeyboardInterrupt) as error:
        result = {
            "schemaVersion": "ray-training-result/v1",
            "status": "FAIL",
            "failureClass": (
                "INTERRUPTED"
                if isinstance(error, KeyboardInterrupt)
                else "DRIVER_INTERNAL_FAILURE"
            ),
            "startedAt": started_at,
            "finishedAt": utc_now(),
            "failure": f"{type(error).__name__}: {error}",
        }
        if run_id is not None:
            result["runId"] = run_id
        status = 130 if isinstance(error, KeyboardInterrupt) else 1
    try:
        write_result_create_only(args.result.resolve(), result)
    except (DriverError, OSError) as error:
        print(f"STOP: cannot save training result: {error}", file=sys.stderr)
        return 1
    if status == 0:
        print(f"PASS: formal training completed; result={args.result.resolve()}")
    else:
        print(f"STOP: formal training failed; result={args.result.resolve()}", file=sys.stderr)
    return status


if __name__ == "__main__":
    raise SystemExit(main())
