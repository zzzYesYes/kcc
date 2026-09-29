"""Run environment, rank-table, and HCCL checks as one fail-closed pipeline.

The default mode only renders a plan.  ``--execute`` is the sole switch that
allows child stages to connect to Ray/Kubernetes or access devices.
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import hashlib
import json
import os
from pathlib import Path
import re
import secrets
import signal
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "1.0"
DEFAULT_HCCN_TOOL = "/usr/local/Ascend/driver/tools/hccn_tool"
DEFAULT_RANK_TABLE = "/user/serverid/devindex/config/hccl.json"
DEFAULT_PROBE_BINARY = "/opt/kcc-hccl/bin/ranktable_allreduce_probe"
DEFAULT_LOG_ROOT = "/var/log/hccl-check"
MAX_DIAGNOSTIC_TEXT = 64 * 1024
MAX_STAGE_OUTPUT_BYTES = 32 * 1024 * 1024
MAX_HCCL_COUNT = 1024 * 1024
MAX_HCCL_TIMEOUT = 86400
MAX_HCCL_COMMAND_TIMEOUT = 300.0
MIN_KILL_GRACE = 0.1
MAX_KILL_GRACE = 60.0
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class PipelineError(RuntimeError):
    """The requested run or a stage result is not safe to accept."""


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _nonnegative_int(value: str) -> int:
    parsed = int(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("must be non-negative")
    return parsed


def _positive_float(value: str) -> float:
    parsed = float(value)
    if not parsed > 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def _new_run_id() -> str:
    stamp = _datetime.datetime.now(_datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"check-{stamp}-{secrets.token_hex(4)}"


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Preview or execute: HCCN ping PASS -> rank table ready -> "
            "rank-table-driven HCCL AllReduce PASS."
        )
    )
    parser.add_argument("--execute", action="store_true", help="run the three stages")
    parser.add_argument(
        "--output-dir",
        type=Path,
        help="new result directory; required with --execute and must not exist",
    )
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--raycluster", required=True)
    parser.add_argument("--ray-address", default="auto")
    parser.add_argument("--resource", default="NPU")
    parser.add_argument("--hccn-tool", default=DEFAULT_HCCN_TOOL)
    parser.add_argument("--device-ids")
    parser.add_argument("--matrix", choices=("same-device", "all"), default="same-device")
    parser.add_argument("--rank-table-path", default=DEFAULT_RANK_TABLE)
    parser.add_argument(
        "--probe-binary",
        default=os.environ.get("HCCL_CHECK_PROBE", DEFAULT_PROBE_BINARY),
    )
    parser.add_argument(
        "--server-id-env",
        default="HOST_IP",
        help=(
            "one authoritative worker environment variable matching ranktable "
            "server_id or optional host_ip"
        ),
    )
    parser.add_argument("--kubectl-command", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--container", default="ray-worker")
    parser.add_argument("--cann-env-script")
    parser.add_argument("--expected-workers", type=_nonnegative_int)
    parser.add_argument("--expected-world-size", type=_nonnegative_int)
    parser.add_argument("--run-id")
    parser.add_argument("--count", type=_positive_int, default=4096)
    parser.add_argument("--log-root", default=DEFAULT_LOG_ROOT)

    parser.add_argument("--ping-timeout", type=_positive_int, default=30)
    parser.add_argument("--ping-stage-timeout", type=_positive_float, default=900.0)
    parser.add_argument("--ranktable-wait-timeout", type=_positive_int, default=300)
    parser.add_argument("--ranktable-poll-interval", type=_positive_int, default=2)
    parser.add_argument("--ranktable-mount-timeout", type=_positive_int, default=120)
    parser.add_argument("--ranktable-stage-timeout", type=_positive_float, default=600.0)
    parser.add_argument("--connect-timeout", type=_positive_int, default=120)
    parser.add_argument("--exec-timeout", type=_positive_int, default=300)
    parser.add_argument("--hccl-command-timeout", type=_positive_float, default=30.0)
    parser.add_argument("--hccl-prepare-timeout", type=_positive_float, default=300.0)
    parser.add_argument("--hccl-test-timeout", type=_positive_float, default=600.0)
    parser.add_argument("--hccl-overall-timeout", type=_positive_float, default=660.0)
    parser.add_argument("--hccl-stage-timeout", type=_positive_float, default=1200.0)
    parser.add_argument("--kill-grace", type=_positive_float, default=10.0)
    return parser


def _bounded_text(value: str, name: str) -> str:
    if not value or len(value) > 4096 or any(c in value for c in ("\x00", "\r", "\n")):
        raise PipelineError(f"--{name} must be a bounded, non-empty single-line value")
    return value


def _absolute_path(value: str | Path, name: str) -> str:
    text = _bounded_text(str(value), name)
    if not Path(text).is_absolute():
        raise PipelineError(f"--{name} must be an absolute path")
    return text


def _required_hccl_stage_timeout(args: argparse.Namespace) -> float:
    """Bound the native stage including both actor-stop passes and drain time."""

    stop_budget = max(30.0, 2.0 * args.kill_grace + 20.0)
    drain_budget = max(10.0, args.kill_grace + 5.0)
    return (
        args.hccl_prepare_timeout
        + args.hccl_overall_timeout
        + 2.0 * stop_budget
        + drain_budget
        + 30.0
    )


def validate_args(args: argparse.Namespace) -> None:
    for field in (
        "namespace",
        "raycluster",
        "ray_address",
        "resource",
        "kubectl_command",
        "container",
    ):
        _bounded_text(str(getattr(args, field)), field.replace("_", "-"))
    for field in ("hccn_tool", "rank_table_path", "probe_binary", "log_root"):
        setattr(
            args,
            field,
            _absolute_path(getattr(args, field), field.replace("_", "-")),
        )
    if args.kubeconfig is not None:
        args.kubeconfig = Path(_absolute_path(args.kubeconfig, "kubeconfig"))
    if args.cann_env_script is not None:
        args.cann_env_script = _absolute_path(args.cann_env_script, "cann-env-script")
    if args.device_ids is not None:
        _bounded_text(args.device_ids, "device-ids")
    if not _ENV_NAME_RE.fullmatch(args.server_id_env):
        raise PipelineError("--server-id-env must be one environment variable name")
    args.run_id = args.run_id or _new_run_id()
    if not _RUN_ID_RE.fullmatch(args.run_id):
        raise PipelineError("--run-id contains unsupported characters")
    if not 1 <= args.count <= MAX_HCCL_COUNT:
        raise PipelineError(f"--count must be in [1, {MAX_HCCL_COUNT}]")
    for name in ("connect_timeout", "exec_timeout"):
        value = getattr(args, name)
        if not 1 <= value <= MAX_HCCL_TIMEOUT:
            raise PipelineError(
                f"--{name.replace('_', '-')} must be in [1, {MAX_HCCL_TIMEOUT}]"
            )
    if not 1.0 <= args.hccl_command_timeout <= MAX_HCCL_COMMAND_TIMEOUT:
        raise PipelineError(
            f"--hccl-command-timeout must be in [1, {MAX_HCCL_COMMAND_TIMEOUT:g}]"
        )
    for name in ("hccl_prepare_timeout", "hccl_test_timeout", "hccl_overall_timeout"):
        value = getattr(args, name)
        if not 1.0 <= value <= MAX_HCCL_TIMEOUT:
            raise PipelineError(
                f"--{name.replace('_', '-')} must be in [1, {MAX_HCCL_TIMEOUT}]"
            )
    if not MIN_KILL_GRACE <= args.kill_grace <= MAX_KILL_GRACE:
        raise PipelineError(
            f"--kill-grace must be in [{MIN_KILL_GRACE:g}, {MAX_KILL_GRACE:g}]"
        )
    if args.hccl_overall_timeout <= args.hccl_test_timeout:
        raise PipelineError("--hccl-overall-timeout must exceed --hccl-test-timeout")
    required_hccl_stage_timeout = _required_hccl_stage_timeout(args)
    if args.hccl_stage_timeout < required_hccl_stage_timeout:
        raise PipelineError(
            "--hccl-stage-timeout must cover prepare + overall + cleanup budget "
            f"({required_hccl_stage_timeout:g}s)"
        )
    if args.ranktable_stage_timeout <= (
        args.ranktable_wait_timeout + args.ranktable_mount_timeout
    ):
        raise PipelineError(
            "--ranktable-stage-timeout must exceed wait-timeout plus mount-timeout"
        )
    if args.execute and args.output_dir is None:
        raise PipelineError("--output-dir is required with --execute")


def build_stage_commands(args: argparse.Namespace) -> list[dict[str, Any]]:
    python = sys.executable
    ping = [
        python,
        "-m",
        "hccl_check._ping",
        "--address",
        args.ray_address,
        "--hccn-tool",
        args.hccn_tool,
        "--resource",
        args.resource,
        "--matrix",
        args.matrix,
        "--timeout",
        str(args.ping_timeout),
        "--execute",
    ]
    if args.device_ids is not None:
        ping.extend(("--device-ids", args.device_ids))
    if args.expected_workers:
        ping.extend(("--expected-workers", str(args.expected_workers)))

    ranktable = [
        python,
        "-m",
        "hccl_check._ranktable",
        "prepare",
        "--namespace",
        args.namespace,
        "--raycluster",
        args.raycluster,
        "--kubectl-command",
        args.kubectl_command,
        "--wait-timeout",
        str(args.ranktable_wait_timeout),
        "--poll-interval",
        str(args.ranktable_poll_interval),
        "--mount-timeout",
        str(args.ranktable_mount_timeout),
        "--container",
        args.container,
        "--mount-path",
        args.rank_table_path,
    ]
    if args.kubeconfig is not None:
        ranktable.extend(("--kubeconfig", str(args.kubeconfig)))

    hccl = [
        python,
        "-m",
        "hccl_check._hccl",
        "--address",
        args.ray_address,
        "--rank-table",
        args.rank_table_path,
        "--probe-binary",
        args.probe_binary,
        "--resource",
        args.resource,
        "--server-id-env",
        args.server_id_env,
        "--hccn-tool",
        args.hccn_tool,
        "--count",
        str(args.count),
        "--connect-timeout",
        str(args.connect_timeout),
        "--exec-timeout",
        str(args.exec_timeout),
        "--command-timeout",
        str(args.hccl_command_timeout),
        "--prepare-timeout",
        str(args.hccl_prepare_timeout),
        "--test-timeout",
        str(args.hccl_test_timeout),
        "--overall-timeout",
        str(args.hccl_overall_timeout),
        "--kill-grace",
        str(args.kill_grace),
        "--log-root",
        args.log_root,
        "--run-id",
        args.run_id,
        "--execute",
    ]
    if args.cann_env_script is not None:
        hccl.extend(("--cann-env-script", args.cann_env_script))
    if args.expected_workers:
        hccl.extend(("--expected-workers", str(args.expected_workers)))
    if args.expected_world_size:
        hccl.extend(("--expected-world-size", str(args.expected_world_size)))

    return [
        {
            "name": "ping",
            "command": ping,
            "timeout_seconds": args.ping_stage_timeout,
            "termination_grace_seconds": args.kill_grace,
        },
        {
            "name": "ranktable",
            "command": ranktable,
            "timeout_seconds": args.ranktable_stage_timeout,
            "termination_grace_seconds": args.kill_grace,
        },
        {
            "name": "hccl",
            "command": hccl,
            "timeout_seconds": args.hccl_stage_timeout,
            "termination_grace_seconds": args.kill_grace,
        },
    ]


def _sha256_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def build_provenance(args: argparse.Namespace) -> dict[str, Any]:
    package = Path(__file__).resolve().parent
    sources: dict[str, str] = {}
    for name in ("__init__.py", "__main__.py", "pipeline.py", "_ping.py", "_ranktable.py", "_hccl.py"):
        path = package / name
        if path.is_file():
            sources[f"hccl_check/{name}"] = _sha256_file(path)
    probe_path = Path(args.probe_binary)
    probe: dict[str, Any] = {"path": args.probe_binary, "driver_sha256": None}
    if probe_path.is_file():
        try:
            probe["driver_sha256"] = _sha256_file(probe_path)
        except OSError:
            pass
    return {
        "entrypoint": "python -m hccl_check",
        "python_executable": sys.executable,
        "python_version": sys.version.split()[0],
        "source_sha256": sources,
        "probe": probe,
        "rank_table": {
            "path": args.rank_table_path,
            "sha256_source": "HCCL worker preflight",
        },
    }


def _plan(args: argparse.Namespace, stages: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "run_id": args.run_id,
        "mode": "execute" if args.execute else "preview",
        "order": [stage["name"] for stage in stages],
        "fail_closed": True,
        "stages": [
            {
                "name": stage["name"],
                "status": "PENDING" if args.execute else "PLANNED",
                "command": list(stage["command"]),
                "timeout_seconds": stage["timeout_seconds"],
                "termination_grace_seconds": stage["termination_grace_seconds"],
            }
            for stage in stages
        ],
        "provenance": build_provenance(args),
    }


def _encode_json(value: Mapping[str, Any]) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, sort_keys=False) + "\n").encode(
        "utf-8"
    )


def _atomic_write_json(path: Path, value: Mapping[str, Any]) -> str:
    payload = _encode_json(value)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.{secrets.token_hex(4)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor: int | None = None
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "wb", closefd=True) as handle:
            descriptor = None
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass
    return _sha256_bytes(payload)


def _new_output_directory(path: Path) -> Path:
    destination = path.resolve(strict=False)
    if destination == Path(destination.anchor):
        raise PipelineError("--output-dir must not be a filesystem root")
    destination.parent.mkdir(parents=True, exist_ok=True)
    try:
        destination.mkdir(mode=0o700)
    except FileExistsError as error:
        raise PipelineError(f"--output-dir already exists: {destination}") from error
    return destination


def _parse_json_stdout(stdout: str, stage: str) -> dict[str, Any]:
    try:
        value = json.loads(stdout)
    except json.JSONDecodeError as error:
        raise PipelineError(f"{stage} did not emit one valid JSON document: {error}") from error
    if not isinstance(value, dict):
        raise PipelineError(f"{stage} JSON result must be an object")
    return value


def _integer(value: Any, where: str, *, positive: bool = False) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise PipelineError(f"{where} must be an integer")
    if value < (1 if positive else 0):
        raise PipelineError(f"{where} is outside the accepted range")
    return value


def _validate_ping(payload: Mapping[str, Any], args: argparse.Namespace) -> None:
    if payload.get("status") != "PASS" or payload.get("mode") != "execute":
        raise PipelineError("ping stage did not report execute/PASS")
    workers = _integer(payload.get("worker_count"), "ping.worker_count", positive=True)
    if args.expected_workers and workers != args.expected_workers:
        raise PipelineError("ping worker_count differs from --expected-workers")
    worker_items = payload.get("workers")
    result_items = payload.get("results")
    if not isinstance(worker_items, list) or len(worker_items) != workers:
        raise PipelineError("ping workers do not match worker_count")
    if not isinstance(result_items, list) or len(result_items) != workers:
        raise PipelineError("ping results do not match worker_count")
    worker_pods = [
        item.get("pod") if isinstance(item, dict) else None for item in worker_items
    ]
    result_pods = [
        item.get("pod") if isinstance(item, dict) else None for item in result_items
    ]
    if any(not isinstance(pod, str) or not pod for pod in worker_pods):
        raise PipelineError("ping workers contain an invalid pod identity")
    if len(set(worker_pods)) != workers or set(result_pods) != set(worker_pods):
        raise PipelineError("ping worker and result pod identities differ")
    if any(
        not isinstance(item, dict) or item.get("status") != "PASS"
        for item in result_items
    ):
        raise PipelineError("one or more per-worker ping results did not PASS")


def _validate_ranktable(
    payload: Mapping[str, Any],
    args: argparse.Namespace,
    ping_payload: Mapping[str, Any],
) -> None:
    if payload.get("status") != "ready":
        raise PipelineError("ranktable stage did not report ready")
    workers = _integer(payload.get("server_count"), "ranktable.server_count", positive=True)
    world = _integer(payload.get("world_size"), "ranktable.world_size", positive=True)
    if payload.get("mount_path") != args.rank_table_path:
        raise PipelineError("ranktable stage verified a different mount path")
    digest = payload.get("ranktable_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise PipelineError("ranktable stage did not report a valid SHA256")
    if args.expected_workers and workers != args.expected_workers:
        raise PipelineError("ranktable server_count differs from --expected-workers")
    if args.expected_world_size and world != args.expected_world_size:
        raise PipelineError("ranktable world_size differs from --expected-world-size")
    if workers != ping_payload.get("worker_count"):
        raise PipelineError("ranktable server_count differs from the Ping worker set")
    ping_workers = ping_payload.get("workers")
    mounted_pods = payload.get("mounted_pods")
    if not isinstance(ping_workers, list) or not isinstance(mounted_pods, list):
        raise PipelineError("Ping/ranktable pod evidence is missing")
    ping_pods = {
        item.get("pod") for item in ping_workers if isinstance(item, dict)
    }
    if (
        len(mounted_pods) != workers
        or any(not isinstance(pod, str) or not pod for pod in mounted_pods)
        or len(set(mounted_pods)) != workers
        or set(mounted_pods) != ping_pods
    ):
        raise PipelineError("ranktable mounted pods differ from the Ping worker set")


def _validate_hccl(
    payload: Mapping[str, Any],
    args: argparse.Namespace,
    ranktable_payload: Mapping[str, Any],
) -> None:
    if payload.get("status") != "PASS" or payload.get("mode") != "execute":
        raise PipelineError("HCCL stage did not report execute/PASS")
    if payload.get("validation_kind") != "public_hccl_api_ranktable_smoke":
        raise PipelineError("HCCL stage reported an unexpected validation kind")
    if payload.get("mpi_used") is not False:
        raise PipelineError("HCCL stage must not use MPI")
    preflight = payload.get("preflight")
    acceptance = payload.get("acceptance")
    final_cleanup = payload.get("final_cleanup")
    if not isinstance(preflight, dict) or preflight.get("status") != "PASS":
        raise PipelineError("HCCL preflight did not PASS")
    if not isinstance(acceptance, dict) or acceptance.get("all_ranks_pass") is not True:
        raise PipelineError("not every HCCL rank passed AllReduce")
    if not isinstance(final_cleanup, dict) or final_cleanup.get("all_process_groups_gone") is not True:
        raise PipelineError("HCCL child process cleanup is unconfirmed")
    workers = _integer(preflight.get("server_count"), "hccl.preflight.server_count", positive=True)
    world = _integer(preflight.get("world_size"), "hccl.preflight.world_size", positive=True)
    if workers != ranktable_payload.get("server_count"):
        raise PipelineError("HCCL server_count differs from prepared ranktable")
    if world != ranktable_payload.get("world_size"):
        raise PipelineError("HCCL world_size differs from prepared ranktable")
    digest = preflight.get("ranktable_sha256")
    if not isinstance(digest, str) or re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise PipelineError("HCCL preflight did not report a ranktable SHA256")
    if digest != ranktable_payload.get("ranktable_sha256"):
        raise PipelineError("HCCL workers tested different ranktable bytes than prepare verified")
    prepared_pods = ranktable_payload.get("mounted_pods")
    preflight_workers = preflight.get("workers")
    if not isinstance(prepared_pods, list) or not isinstance(preflight_workers, list):
        raise PipelineError("HCCL/ranktable worker identity evidence is missing")
    hccl_pods = [
        item.get("pod_name") if isinstance(item, dict) else None
        for item in preflight_workers
    ]
    if (
        len(hccl_pods) != workers
        or any(not isinstance(pod, str) or not pod for pod in hccl_pods)
        or len(set(hccl_pods)) != workers
        or set(hccl_pods) != set(prepared_pods)
    ):
        raise PipelineError("HCCL workers differ from the prepared ranktable pod set")
    passed = _integer(acceptance.get("passed_rank_count"), "hccl.passed_rank_count")
    expected = _integer(acceptance.get("expected_rank_count"), "hccl.expected_rank_count", positive=True)
    if passed != world or expected != world:
        raise PipelineError("HCCL accepted rank counts differ from ranktable world_size")


def _diagnostic_text(value: str | bytes | None) -> tuple[str, bool]:
    if value is None:
        return "", False
    text = value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value
    if len(text) <= MAX_DIAGNOSTIC_TEXT:
        return text, False
    return text[-MAX_DIAGNOSTIC_TEXT:], True


def _as_text(value: str | bytes | None) -> str:
    if value is None:
        return ""
    return value.decode("utf-8", errors="replace") if isinstance(value, bytes) else value


def _process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _timeout_cleanup(
    process: subprocess.Popen[str], grace_seconds: float
) -> tuple[str, str, dict[str, Any]]:
    """Terminate the dedicated local process group and audit the outcome."""

    audit: dict[str, Any] = {
        "pgid": process.pid,
        "signals": [],
        "errors": [],
        "process_group_gone": False,
    }
    stdout = ""
    stderr = ""
    try:
        os.killpg(process.pid, signal.SIGTERM)
        audit["signals"].append("SIGTERM")
    except ProcessLookupError:
        pass
    except OSError as error:
        audit["errors"].append(f"SIGTERM: {type(error).__name__}: {error}")
    try:
        stdout, stderr = process.communicate(timeout=grace_seconds)
    except subprocess.TimeoutExpired as error:
        stdout = _as_text(error.stdout)
        stderr = _as_text(error.stderr)
    if _process_group_exists(process.pid):
        try:
            os.killpg(process.pid, signal.SIGKILL)
            audit["signals"].append("SIGKILL")
        except ProcessLookupError:
            pass
        except OSError as error:
            audit["errors"].append(f"SIGKILL: {type(error).__name__}: {error}")
        try:
            final_stdout, final_stderr = process.communicate(timeout=grace_seconds)
            stdout = final_stdout or stdout
            stderr = final_stderr or stderr
        except subprocess.TimeoutExpired as error:
            stdout = _as_text(error.stdout) or stdout
            stderr = _as_text(error.stderr) or stderr
            audit["errors"].append("process group did not close its pipes after SIGKILL")
    audit["process_group_gone"] = not _process_group_exists(process.pid)
    if not audit["process_group_gone"]:
        audit["errors"].append("local process group could not be confirmed gone")
    return _as_text(stdout), _as_text(stderr), audit


def _run_stage(stage: Mapping[str, Any]) -> dict[str, Any]:
    command = list(stage["command"])
    timeout = float(stage["timeout_seconds"])
    started_at = _datetime.datetime.now(_datetime.timezone.utc).isoformat()
    started = time.monotonic()
    try:
        process = subprocess.Popen(
            command,
            shell=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            start_new_session=True,
            close_fds=True,
        )
    except OSError as error:
        return {
            "name": stage["name"],
            "command": command,
            "timeout_seconds": timeout,
            "started_at": started_at,
            "finished_at": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
            "duration_seconds": round(time.monotonic() - started, 6),
            "returncode": None,
            "stdout_sha256": _sha256_bytes(b""),
            "stderr_sha256": _sha256_bytes(b""),
            "stderr": "",
            "stderr_truncated": False,
            "status": "FAIL",
            "failure": f"could not start stage: {type(error).__name__}: {error}",
        }
    timeout_cleanup: dict[str, Any] | None = None
    try:
        stdout, stderr = process.communicate(timeout=timeout)
        timed_out = False
    except subprocess.TimeoutExpired:
        stdout, stderr, timeout_cleanup = _timeout_cleanup(
            process, float(stage["termination_grace_seconds"])
        )
        timed_out = True
    stdout = _as_text(stdout)
    stderr = _as_text(stderr)
    stdout_excerpt, stdout_truncated = _diagnostic_text(stdout)
    stderr_text, stderr_truncated = _diagnostic_text(stderr)
    oversized = (
        len(stdout.encode("utf-8")) > MAX_STAGE_OUTPUT_BYTES
        or len(stderr.encode("utf-8")) > MAX_STAGE_OUTPUT_BYTES
    )
    record: dict[str, Any] = {
        "name": stage["name"],
        "command": command,
        "timeout_seconds": timeout,
        "started_at": started_at,
        "finished_at": _datetime.datetime.now(_datetime.timezone.utc).isoformat(),
        "duration_seconds": round(time.monotonic() - started, 6),
        "returncode": process.returncode,
        "stdout_sha256": _sha256_bytes(stdout.encode("utf-8")),
        "stderr_sha256": _sha256_bytes(stderr.encode("utf-8")),
        "stderr": stderr_text,
        "stderr_truncated": stderr_truncated,
    }
    if timed_out:
        record.update(
            {
                "status": "TIMEOUT",
                "failure": f"stage exceeded {timeout} seconds",
                "stdout_excerpt": stdout_excerpt,
                "stdout_truncated": stdout_truncated,
                "timeout_cleanup": timeout_cleanup,
            }
        )
        if not timeout_cleanup or timeout_cleanup.get("process_group_gone") is not True:
            record["failure"] += "; local process-group cleanup is unconfirmed"
        return record
    if oversized:
        record.update(
            {
                "status": "FAIL",
                "failure": f"stage output exceeded {MAX_STAGE_OUTPUT_BYTES} bytes",
                "stdout_excerpt": stdout_excerpt,
                "stdout_truncated": stdout_truncated,
            }
        )
        return record
    try:
        payload = _parse_json_stdout(stdout, str(stage["name"]))
    except PipelineError as error:
        failure = str(error)
        if stderr_text:
            failure = f"{failure}; stderr: {stderr_text[-4000:]}"
        record.update(
            {
                "status": "FAIL",
                "failure": failure,
                "stdout_excerpt": stdout_excerpt,
                "stdout_truncated": stdout_truncated,
            }
        )
        return record
    record["result"] = payload
    if process.returncode != 0:
        record.update(
            {"status": "FAIL", "failure": f"stage exited with code {process.returncode}"}
        )
    else:
        record["status"] = "PASS"
    return record


def execute_pipeline(args: argparse.Namespace, stages: list[dict[str, Any]]) -> dict[str, Any]:
    assert args.output_dir is not None
    output = _new_output_directory(args.output_dir)
    plan = _plan(args, stages)
    plan_sha = _atomic_write_json(output / "plan.json", plan)
    completed: list[dict[str, Any]] = []
    ping_payload: Mapping[str, Any] | None = None
    ranktable_payload: Mapping[str, Any] | None = None
    failure: str | None = None
    failed_stage: str | None = None
    for index, stage in enumerate(stages, start=1):
        record = _run_stage(stage)
        if record["status"] == "PASS":
            try:
                payload = record["result"]
                if stage["name"] == "hccl":
                    if ranktable_payload is None:
                        raise PipelineError("ranktable evidence is unavailable")
                    _validate_hccl(payload, args, ranktable_payload)
                elif stage["name"] == "ping":
                    _validate_ping(payload, args)
                    ping_payload = payload
                else:
                    if ping_payload is None:
                        raise PipelineError("Ping evidence is unavailable")
                    _validate_ranktable(payload, args, ping_payload)
                    ranktable_payload = payload
            except PipelineError as error:
                record["status"] = "FAIL"
                record["failure"] = str(error)
        artifact = output / f"{index:02d}-{stage['name']}.json"
        record["artifact"] = artifact.name
        record["artifact_sha256"] = _atomic_write_json(artifact, record)
        completed.append(record)
        if record["status"] != "PASS":
            failed_stage = str(stage["name"])
            failure = str(record.get("failure") or f"{failed_stage} failed")
            break

    if failed_stage is not None:
        already = {str(item["name"]) for item in completed}
        for stage in stages:
            if str(stage["name"]) not in already:
                completed.append(
                    {
                        "name": stage["name"],
                        "status": "BLOCKED",
                        "blocked_by": failed_stage,
                        "command": list(stage["command"]),
                        "timeout_seconds": stage["timeout_seconds"],
                    }
                )

    hccl_payload = next(
        (
            item.get("result")
            for item in completed
            if item.get("name") == "hccl" and item.get("status") == "PASS"
        ),
        None,
    )
    ranktable_sha = None
    if isinstance(hccl_payload, dict):
        preflight = hccl_payload.get("preflight")
        if isinstance(preflight, dict):
            ranktable_sha = preflight.get("ranktable_sha256")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "run_id": args.run_id,
        "mode": "execute",
        "status": "PASS" if failure is None and len(completed) == len(stages) else "FAIL",
        "failure": failure,
        "failed_stage": failed_stage,
        "output_dir": str(output),
        "plan_sha256": plan_sha,
        "ranktable_sha256": ranktable_sha,
        "stages": completed,
        "provenance": plan["provenance"],
    }
    _atomic_write_json(output / "result.json", result)
    return result


def preview(args: argparse.Namespace, stages: list[dict[str, Any]]) -> dict[str, Any]:
    plan = _plan(args, stages)
    plan.update(
        {
            "status": "DRY_RUN",
            "external_access": {
                "ray_connected": False,
                "kubernetes_accessed": False,
                "npu_accessed": False,
            },
            "message": "add --execute and a new --output-dir only after target devices are idle",
        }
    )
    return plan


def main(argv: Sequence[str] | None = None) -> int:
    parser = make_parser()
    args = parser.parse_args(argv)
    try:
        validate_args(args)
        stages = build_stage_commands(args)
        result = execute_pipeline(args, stages) if args.execute else preview(args, stages)
        print(json.dumps(result, ensure_ascii=False, indent=2) + "\n", end="")
        return 0 if result["status"] in {"PASS", "DRY_RUN"} else 1
    except (PipelineError, OSError) as error:
        print(f"hccl-check: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("hccl-check: interrupted", file=sys.stderr)
        return 130


__all__ = [
    "PipelineError",
    "build_stage_commands",
    "execute_pipeline",
    "main",
    "make_parser",
    "preview",
    "validate_args",
]
