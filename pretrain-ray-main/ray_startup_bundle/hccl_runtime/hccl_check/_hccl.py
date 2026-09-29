#!/usr/bin/env python3
"""Run a rank-table-driven HCCL AllReduce smoke test through Ray, without MPI.

Ray owns placement, whole-node NPU reservations, concurrent launch, timeout,
and cleanup.  One native probe process is launched for every rank-table Device.
The native process calls ``HcclCommInitClusterInfo`` directly and verifies an
INT32/SUM AllReduce result.

Dry-run is the default.  ``--execute`` is required before this program imports
or connects to Ray or accesses an NPU.
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import glob
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import secrets
import signal
import socket
import subprocess
import sys
import threading
import time
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "1.0"
DEFAULT_RANK_TABLE = "/user/serverid/devindex/config/hccl.json"
DEFAULT_PROBE_BINARY = "/opt/hccl-check/bin/ranktable_allreduce_probe"
DEFAULT_HCCN_TOOL = "/usr/local/Ascend/driver/tools/hccn_tool"
DEFAULT_LOG_ROOT = "/var/log/hccl-check/allreduce"
MAX_RANK_TABLE_BYTES = 32 * 1024 * 1024
MAX_LOG_TAIL_BYTES = 64 * 1024

_ROOT_KEYS = {"status", "version", "server_count", "server_list"}
_SERVER_REQUIRED_KEYS = {"server_id", "device"}
_SERVER_OPTIONAL_KEYS = {"host_ip"}
_DEVICE_REQUIRED_KEYS = {"device_id", "device_ip", "rank_id"}
_DEVICE_OPTIONAL_KEYS = {"device_port", "host_port"}
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
_DAVINCI_RE = re.compile(r"^/dev/davinci([0-9]+)$")
_HCCN_IP_RE = re.compile(
    r"^\s*ipaddr\s*:\s*([^\s]+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
_ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ValidationError(RuntimeError):
    """Input, topology, or worker state is unsafe for this test."""


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValidationError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _mapping(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ValidationError(f"{where} must be a JSON object")
    return value


def _list(value: Any, where: str) -> list[Any]:
    if not isinstance(value, list):
        raise ValidationError(f"{where} must be a JSON array")
    return value


def _nonempty_text(value: Any, where: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 4096
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise ValidationError(f"{where} must be a bounded non-empty string")
    return value


def _decimal(value: Any, where: str) -> int:
    if isinstance(value, bool):
        raise ValidationError(f"{where} must be a non-negative integer")
    if isinstance(value, int):
        result = value
    elif isinstance(value, str) and re.fullmatch(r"0|[1-9][0-9]*", value):
        result = int(value, 10)
    else:
        raise ValidationError(f"{where} must be a decimal integer or integer string")
    if result < 0:
        raise ValidationError(f"{where} must be non-negative")
    return result


def _ip_address(value: Any, where: str) -> str:
    text = _nonempty_text(value, where)
    try:
        address = ipaddress.ip_address(text)
    except ValueError as error:
        raise ValidationError(f"{where} is not an IP address: {text!r}") from error
    if address.is_unspecified:
        raise ValidationError(f"{where} must be a non-unspecified IP address")
    return str(address)


def _ipv4(value: Any, where: str) -> str:
    normalized = _ip_address(value, where)
    if not isinstance(ipaddress.ip_address(normalized), ipaddress.IPv4Address):
        raise ValidationError(f"{where} must be an IPv4 address")
    return normalized


def _server_id(value: Any, where: str) -> str:
    result = _nonempty_text(value, where)
    if len(result) > 64:
        raise ValidationError(f"{where} must be at most 64 characters")
    return result


def _server_log_directory_name(server_id: str) -> str:
    digest = hashlib.sha256(server_id.encode("utf-8")).hexdigest()
    return f"server-{digest}"


def resolve_server_identity(
    environment_name: str,
    environment: Mapping[str, str] | None = None,
) -> tuple[str, dict[str, str]]:
    """Resolve the rank-table server identity from one configured worker env."""

    if not _ENV_NAME_RE.fullmatch(environment_name):
        raise ValidationError("server identity source is not a valid environment name")
    source = os.environ if environment is None else environment
    raw_value = source.get(environment_name)
    if not raw_value:
        raise ValidationError(
            f"configured server identity variable {environment_name} is unset"
        )
    server_id = _server_id(raw_value, environment_name)
    return server_id, {environment_name: server_id}


def _port(value: Any, where: str) -> int:
    result = _decimal(value, where)
    if not 1 <= result <= 65535:
        raise ValidationError(f"{where} must be in [1, 65535]")
    return result


def parse_rank_table_bytes(payload: bytes) -> dict[str, Any]:
    """Validate a CANN AI Server v1.0 rank table and normalize its mapping."""

    if not payload or len(payload) > MAX_RANK_TABLE_BYTES:
        raise ValidationError(
            f"rank table must contain 1..{MAX_RANK_TABLE_BYTES} bytes"
        )
    try:
        document = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_keys
        )
    except ValidationError:
        raise
    except (UnicodeError, json.JSONDecodeError) as error:
        raise ValidationError(f"rank table is not valid UTF-8 JSON: {error}") from error

    root = _mapping(document, "rank_table")
    if set(root) != _ROOT_KEYS:
        raise ValidationError(
            "rank table root must contain exactly status, version, server_count, server_list"
        )
    if root.get("status") != "completed":
        raise ValidationError("rank table status must be 'completed'")
    if root.get("version") != "1.0":
        raise ValidationError("rank table version must be '1.0'")
    servers_raw = _list(root.get("server_list"), "rank_table.server_list")
    if not servers_raw:
        raise ValidationError("rank table server_list must not be empty")
    if _decimal(root.get("server_count"), "rank_table.server_count") != len(servers_raw):
        raise ValidationError("rank table server_count does not match server_list")

    servers: list[dict[str, Any]] = []
    seen_server_ids: set[str] = set()
    seen_host_ips: set[str] = set()
    seen_device_ips: set[str] = set()
    seen_rank_ids: set[int] = set()
    flattened_ranks: list[int] = []
    for server_index, raw_server in enumerate(servers_raw):
        where = f"rank_table.server_list[{server_index}]"
        server = _mapping(raw_server, where)
        keys = set(server)
        if not _SERVER_REQUIRED_KEYS <= keys or not keys <= (
            _SERVER_REQUIRED_KEYS | _SERVER_OPTIONAL_KEYS
        ):
            raise ValidationError(f"{where} has missing or unsupported fields")
        server_id = _server_id(server.get("server_id"), f"{where}.server_id")
        if server_id in seen_server_ids:
            raise ValidationError(f"duplicate rank-table server_id: {server_id}")
        if server_id in seen_host_ips:
            raise ValidationError(
                f"rank-table server_id {server_id!r} conflicts with another host_ip"
            )
        seen_server_ids.add(server_id)
        host_ip = (
            _ipv4(server.get("host_ip"), f"{where}.host_ip")
            if "host_ip" in server
            else None
        )
        if host_ip is not None:
            if host_ip in seen_host_ips:
                raise ValidationError(f"duplicate rank-table host_ip: {host_ip}")
            if host_ip in seen_server_ids and host_ip != server_id:
                raise ValidationError(
                    f"rank-table host_ip {host_ip!r} conflicts with another server_id"
                )
            seen_host_ips.add(host_ip)

        devices_raw = _list(server.get("device"), f"{where}.device")
        if not devices_raw:
            raise ValidationError(f"{where}.device must not be empty")
        devices: list[dict[str, Any]] = []
        seen_local_devices: set[int] = set()
        seen_local_host_ports: set[int] = set()
        for device_index, raw_device in enumerate(devices_raw):
            device_where = f"{where}.device[{device_index}]"
            device = _mapping(raw_device, device_where)
            device_keys = set(device)
            if not _DEVICE_REQUIRED_KEYS <= device_keys or not device_keys <= (
                _DEVICE_REQUIRED_KEYS | _DEVICE_OPTIONAL_KEYS
            ):
                raise ValidationError(
                    f"{device_where} has missing or unsupported fields"
                )
            device_id = _decimal(device.get("device_id"), f"{device_where}.device_id")
            device_ip = _ip_address(
                device.get("device_ip"), f"{device_where}.device_ip"
            )
            rank_id = _decimal(device.get("rank_id"), f"{device_where}.rank_id")
            normalized_device = {
                "device_id": device_id,
                "device_ip": device_ip,
                "rank_id": rank_id,
            }
            if "device_port" in device:
                normalized_device["device_port"] = _port(
                    device.get("device_port"), f"{device_where}.device_port"
                )
            if "host_port" in device:
                host_port = _port(
                    device.get("host_port"), f"{device_where}.host_port"
                )
                if host_port in seen_local_host_ports:
                    raise ValidationError(
                        f"duplicate host_port {host_port} on server {server_id}"
                    )
                seen_local_host_ports.add(host_port)
                normalized_device["host_port"] = host_port
            if device_id in seen_local_devices:
                raise ValidationError(
                    f"duplicate device_id {device_id} on server {server_id}"
                )
            if device_ip in seen_device_ips:
                raise ValidationError(f"duplicate rank-table device_ip: {device_ip}")
            if rank_id in seen_rank_ids:
                raise ValidationError(f"duplicate rank-table rank_id: {rank_id}")
            seen_local_devices.add(device_id)
            seen_device_ips.add(device_ip)
            seen_rank_ids.add(rank_id)
            devices.append(normalized_device)

        # Explicit rank_id is authoritative.  Array order and local Device ID
        # are not used to invent a global rank.
        devices.sort(key=lambda item: item["rank_id"])
        local_ranks = [item["rank_id"] for item in devices]
        normalized_server = {
            "server_id": server_id,
            "rank_start": local_ranks[0],
            "rank_end_exclusive": local_ranks[-1] + 1,
            "devices": devices,
        }
        if host_ip is not None:
            normalized_server["host_ip"] = host_ip
        servers.append(normalized_server)

    servers.sort(key=lambda item: item["rank_start"])
    flattened_ranks = sorted(seen_rank_ids)
    world_size = len(flattened_ranks)
    if flattened_ranks != list(range(world_size)):
        raise ValidationError(
            "rank-table global ranks must cover 0..world_size-1 exactly once"
        )
    return {
        "sha256": hashlib.sha256(payload).hexdigest(),
        "server_count": len(servers),
        "world_size": world_size,
        "servers": servers,
    }


def read_rank_table(path: str) -> tuple[bytes, dict[str, Any]]:
    rank_table = Path(path)
    if not rank_table.is_absolute():
        raise ValidationError("rank-table path must be absolute")
    try:
        stat_result = rank_table.stat()
        if not rank_table.is_file() or not 0 < stat_result.st_size <= MAX_RANK_TABLE_BYTES:
            raise ValidationError("rank table is not a bounded non-empty regular file")
        payload = rank_table.read_bytes()
    except ValidationError:
        raise
    except OSError as error:
        raise ValidationError(f"cannot read rank table {rank_table}: {error}") from error
    return payload, parse_rank_table_bytes(payload)


def select_rank_table_server(
    table: Mapping[str, Any], identity: str
) -> tuple[Mapping[str, Any], str]:
    """Select one server by official server_id or optional host_ip."""

    matches: list[tuple[Mapping[str, Any], list[str]]] = []
    for raw_server in table.get("servers", []):
        server = _mapping(raw_server, "table.servers[]")
        fields: list[str] = []
        if server.get("server_id") == identity:
            fields.append("server_id")
        if server.get("host_ip") == identity:
            fields.append("host_ip")
        if fields:
            matches.append((server, fields))
    if len(matches) != 1:
        raise ValidationError(
            f"worker identity {identity!r} matched {len(matches)} rank-table servers; "
            "expected exactly one server_id/host_ip match"
        )
    server, fields = matches[0]
    return server, "+".join(fields)


def parse_hccn_ip(output: str) -> str:
    matches = _HCCN_IP_RE.findall(output)
    normalized: list[str] = []
    for match in matches:
        try:
            value = _ip_address(match, "hccn_tool ipaddr")
        except ValidationError:
            continue
        if value not in normalized:
            normalized.append(value)
    if len(normalized) != 1:
        raise ValidationError(
            f"expected one unambiguous hccn_tool ipaddr, found {normalized}"
        )
    return normalized[0]


def detect_local_device_ids() -> list[int]:
    device_ids: list[int] = []
    for path in glob.glob("/dev/davinci[0-9]*"):
        match = _DAVINCI_RE.fullmatch(path)
        if match:
            device_ids.append(int(match.group(1), 10))
    result = sorted(set(device_ids))
    return result


def parse_visible_device_ids(value: str, where: str) -> list[int]:
    result: list[int] = []
    seen: set[int] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            raise ValidationError(f"{where} contains an empty Device item")
        if "-" not in part:
            expanded = [_decimal(part, where)]
        else:
            bounds = part.split("-")
            if len(bounds) != 2:
                raise ValidationError(
                    f"{where} contains an invalid Device range: {part!r}"
                )
            start = _decimal(bounds[0], where)
            end = _decimal(bounds[1], where)
            if start > end:
                raise ValidationError(
                    f"{where} contains a reversed Device range: {part!r}"
                )
            expanded = list(range(start, end + 1))
        for device_id in expanded:
            if device_id in seen:
                raise ValidationError(f"{where} contains duplicate Device {device_id}")
            seen.add(device_id)
            result.append(device_id)
    if not result:
        raise ValidationError(f"{where} is empty")
    return result


def validate_visible_device_environment(
    expected: Sequence[int], environment: Mapping[str, str] | None = None
) -> dict[str, list[int]]:
    mappings: dict[str, list[int]] = {}
    expected_list = list(expected)
    source = os.environ if environment is None else environment
    for name in ("ASCEND_RT_VISIBLE_DEVICES", "ASCEND_VISIBLE_DEVICES"):
        value = source.get(name)
        if not value:
            continue
        parsed = parse_visible_device_ids(value, name)
        if parsed != expected_list:
            raise ValidationError(
                f"{name}={value!r} is not the required identity mapping {expected_list}"
            )
        mappings[name] = parsed
    return mappings


def validate_acl_device_mapping(
    expected: Sequence[int], environment: Mapping[str, str] | None = None
) -> dict[str, list[int]]:
    """Fail closed unless rank-table IDs are ACL identity-map user indices."""

    expected_list = list(expected)
    identity = list(range(len(expected_list)))
    if expected_list != identity:
        raise ValidationError(
            "rank-table device IDs are not the ACL identity mapping 0..N-1; "
            "sparse or ASCEND_RT_VISIBLE_DEVICES-remapped execution is not "
            "implemented"
        )
    return validate_visible_device_environment(identity, environment)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def write_rank_table_snapshot(
    payload: bytes, destination: Path, expected_sha256: str
) -> str:
    """Persist the exact validated bytes in a run-scoped, read-only file."""

    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = -1
    try:
        descriptor = os.open(str(destination), flags, 0o400)
        stream = os.fdopen(descriptor, "wb")
        descriptor = -1
        with stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(destination, 0o400)
        actual_sha256 = sha256_file(destination)
        if actual_sha256 != expected_sha256:
            raise ValidationError(
                "run-scoped rank-table snapshot differs from the validated source"
            )
        return actual_sha256
    except Exception:
        if descriptor >= 0:
            os.close(descriptor)
        try:
            destination.unlink()
        except FileNotFoundError:
            pass
        raise


def _bounded_tail(path: Path, limit: int = MAX_LOG_TAIL_BYTES) -> str:
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > limit:
                handle.seek(size - limit)
            data = handle.read(limit)
        prefix = f"... <{size - limit} bytes omitted> ...\n" if size > limit else ""
        return prefix + data.decode("utf-8", errors="replace")
    except OSError as error:
        return f"<could not read log: {error}>"


def _cann_environment_candidates(
    setup_script: str | None,
    environment: Mapping[str, str] | None = None,
) -> tuple[Path, ...]:
    """Return explicit or conventional CANN environment script candidates."""

    if setup_script is not None:
        explicit = Path(setup_script)
        if not explicit.is_absolute():
            raise ValidationError("CANN environment script path must be absolute")
        return (explicit,)

    source = os.environ if environment is None else environment
    candidates: list[Path] = []
    for variable in ("ASCEND_HOME_PATH", "ASCEND_TOOLKIT_HOME"):
        raw_root = source.get(variable)
        if not raw_root:
            continue
        root = Path(raw_root)
        candidates.extend((root / "bin/setenv.bash", root / "set_env.sh"))
    candidates.extend(
        (
            Path("/usr/local/Ascend/ascend-toolkit/latest/bin/setenv.bash"),
            Path("/usr/local/Ascend/ascend-toolkit/latest/set_env.sh"),
            Path("/usr/local/Ascend/ascend-toolkit/set_env.sh"),
            Path("/usr/local/Ascend/cann/ascend-toolkit/latest/bin/setenv.bash"),
            Path("/usr/local/Ascend/cann/ascend-toolkit/latest/set_env.sh"),
            Path("/usr/local/Ascend/cann/ascend-toolkit/set_env.sh"),
        )
    )
    return tuple(dict.fromkeys(candidates))


def _load_cann_environment(
    command_runner: Any = subprocess.run,
    setup_script: str | None = None,
) -> dict[str, str]:
    candidates = _cann_environment_candidates(setup_script)
    setup = next((candidate for candidate in candidates if candidate.is_file()), None)
    if setup is None:
        raise ValidationError(f"CANN environment script is missing; checked {candidates}")
    completed = command_runner(
        ["bash", "-c", 'source "$CANN_SETUP" >/dev/null && env -0'],
        env={**os.environ, "CANN_SETUP": str(setup)},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=30,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace")
        raise ValidationError(f"could not load CANN environment: {detail.strip()}")
    environment = os.environ.copy()
    for item in completed.stdout.split(b"\0"):
        if item and b"=" in item:
            key, value = item.split(b"=", 1)
            environment[key.decode()] = value.decode()
    environment.pop("CANN_SETUP", None)
    driver_paths = [
        "/usr/local/Ascend/driver/lib64",
        "/usr/local/Ascend/driver/lib64/common",
        "/usr/local/Ascend/driver/lib64/driver",
    ]
    existing = environment.get("LD_LIBRARY_PATH", "")
    environment["LD_LIBRARY_PATH"] = ":".join(
        [*driver_paths, *([existing] if existing else [])]
    )
    # This execution path is deliberately independent of MPI/PMI rank state.
    for key in list(environment):
        if key.startswith(("OMPI_", "PMI_", "PMIX_", "MPI_", "MV2_")):
            environment.pop(key, None)
    for key in (
        "LOCAL_RANK",
        "LOCAL_WORLD_SIZE",
        "NODE_RANK",
        "WORLD_SIZE",
        "MASTER_ADDR",
        "MASTER_PORT",
    ):
        environment.pop(key, None)
    return environment


def build_probe_specs(
    local_server: Mapping[str, Any],
    *,
    binary: str,
    rank_table: str,
    world_size: int,
    count: int,
) -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for device in local_server["devices"]:
        rank_id = int(device["rank_id"])
        device_id = int(device["device_id"])
        specs.append(
            {
                "rank_id": rank_id,
                "device_id": device_id,
                "device_ip": str(device["device_ip"]),
                "argv": [
                    binary,
                    "--rank-table",
                    rank_table,
                    "--rank-id",
                    str(rank_id),
                    "--device-id",
                    str(device_id),
                    "--world-size",
                    str(world_size),
                    "--count",
                    str(count),
                ],
            }
        )
    return specs


def validate_preparations(
    preparations: Sequence[Mapping[str, Any]],
    resource_nodes: Sequence[tuple[str, int]],
    *,
    expected_workers: int,
    expected_world_size: int,
) -> dict[str, Any]:
    if len(preparations) != len(resource_nodes):
        raise ValidationError("Ray preparation count differs from selected resource nodes")
    if expected_workers and len(preparations) != expected_workers:
        raise ValidationError(
            f"expected {expected_workers} workers, prepared {len(preparations)}"
        )
    node_ids = [str(item["ray_node_id"]) for item in preparations]
    if len(set(node_ids)) != len(node_ids):
        raise ValidationError("multiple preparations came from the same Ray node")
    if set(node_ids) != {node_id for node_id, _ in resource_nodes}:
        raise ValidationError("prepared Ray nodes differ from selected Ray nodes")
    expected_resources = dict(resource_nodes)
    for item in preparations:
        node_id = str(item["ray_node_id"])
        if int(item["npu_count"]) != expected_resources[node_id]:
            raise ValidationError(
                f"prepared NPU count for Ray node {node_id} differs from discovery"
            )

    rank_hashes = {str(item["ranktable_sha256"]) for item in preparations}
    binary_hashes = {str(item["probe_binary_sha256"]) for item in preparations}
    world_sizes = {int(item["world_size"]) for item in preparations}
    server_counts = {int(item["server_count"]) for item in preparations}
    table_summaries = {
        json.dumps(item["table_servers"], sort_keys=True, separators=(",", ":"))
        for item in preparations
    }
    if len(rank_hashes) != 1:
        raise ValidationError(f"workers mounted different rank-table bytes: {rank_hashes}")
    if len(binary_hashes) != 1:
        raise ValidationError(f"workers have different probe binaries: {binary_hashes}")
    if len(world_sizes) != 1 or len(server_counts) != 1 or len(table_summaries) != 1:
        raise ValidationError("workers parsed different rank-table topology")
    world_size = next(iter(world_sizes))
    server_count = next(iter(server_counts))
    if server_count != len(preparations):
        raise ValidationError(
            f"rank table has {server_count} servers but Ray prepared {len(preparations)}"
        )
    if expected_world_size and world_size != expected_world_size:
        raise ValidationError(
            f"expected world size {expected_world_size}, rank table has {world_size}"
        )
    if sum(count for _, count in resource_nodes) != world_size:
        raise ValidationError("Ray resource sum differs from rank-table world size")
    server_ids = [str(item["server_id"]) for item in preparations]
    if len(set(server_ids)) != server_count:
        raise ValidationError("Ray workers did not map one-to-one to rank-table servers")
    canonical_servers = preparations[0]["table_servers"]
    blocks = {str(item["server_id"]): item for item in canonical_servers}
    if set(server_ids) != set(blocks):
        raise ValidationError("prepared server IDs differ from rank-table server IDs")
    for item in preparations:
        server_id = str(item["server_id"])
        block = blocks[server_id]
        expected_devices = list(block["devices"])
        if [int(value) for value in item["rank_ids"]] != [
            int(device["rank_id"]) for device in expected_devices
        ]:
            raise ValidationError(f"prepared ranks differ from table block for {server_id}")
        if [int(value) for value in item["device_ids"]] != [
            int(device["device_id"]) for device in expected_devices
        ]:
            raise ValidationError(
                f"prepared Device IDs differ from table block for {server_id}"
            )
        if [str(value) for value in item["device_ips"]] != [
            str(device["device_ip"]) for device in expected_devices
        ]:
            raise ValidationError(
                f"prepared Device IPs differ from table block for {server_id}"
            )
    ranks = sorted(
        int(rank)
        for item in preparations
        for rank in item["rank_ids"]
    )
    if ranks != list(range(world_size)):
        raise ValidationError("prepared workers do not cover every rank exactly once")
    return {
        "ranktable_sha256": next(iter(rank_hashes)),
        "probe_binary_sha256": next(iter(binary_hashes)),
        "server_count": server_count,
        "world_size": world_size,
        "server_ids": [str(item["server_id"]) for item in canonical_servers],
    }


def _terminate_processes(
    processes: Sequence[subprocess.Popen[Any]], grace_seconds: float
) -> list[dict[str, Any]]:
    unique = {process.pid: process for process in processes}
    audit = {
        pid: {
            "pid": pid,
            "pgid": pid,
            "signals": [],
            "errors": [],
            "leader_exit_code": None,
            "confirmed_gone": False,
        }
        for pid in unique
    }

    def group_exists(pgid: int) -> bool:
        try:
            os.killpg(pgid, 0)
            return True
        except ProcessLookupError:
            return False
        except PermissionError:
            return True

    def signal_existing_groups(signum: int, name: str) -> None:
        for pid in sorted(unique):
            if not group_exists(pid):
                continue
            try:
                os.killpg(pid, signum)
                audit[pid]["signals"].append(name)
            except ProcessLookupError:
                pass
            except OSError as error:
                audit[pid]["errors"].append(f"{name}: {error}")

    def wait_for_groups(deadline: float) -> bool:
        while True:
            for process in unique.values():
                process.poll()
            if all(not group_exists(pid) for pid in unique):
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            time.sleep(min(0.1, remaining))

    signal_existing_groups(signal.SIGTERM, "SIGTERM")
    all_gone = wait_for_groups(time.monotonic() + grace_seconds)
    if not all_gone:
        signal_existing_groups(signal.SIGKILL, "SIGKILL")
        wait_for_groups(time.monotonic() + 5.0)

    for pid, process in unique.items():
        audit[pid]["leader_exit_code"] = process.poll()
        audit[pid]["confirmed_gone"] = not group_exists(pid)
        if not audit[pid]["confirmed_gone"]:
            audit[pid]["errors"].append("process group still exists after SIGKILL")
    return [audit[pid] for pid in sorted(audit)]


class _RankTableHcclActor:
    """One hard-pinned actor reserves and supervises all NPUs on one node."""

    def __init__(self, expected_ray_node_id: str, npu_count: int) -> None:
        import ray

        self.expected_ray_node_id = expected_ray_node_id
        self.npu_count = npu_count
        self.actual_ray_node_id = str(ray.get_runtime_context().get_node_id())
        if self.actual_ray_node_id != expected_ray_node_id:
            raise RuntimeError(
                f"actor landed on {self.actual_ray_node_id}, expected {expected_ray_node_id}"
            )
        self._prepared: dict[str, Any] | None = None
        self._processes: dict[int, subprocess.Popen[bytes]] = {}
        self._aux_processes: dict[int, subprocess.Popen[Any]] = {}
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._cleanup_lock = threading.Lock()
        self._stop_requested = False
        self._state = "NEW"
        self._active_calls = 0

    def _call_with_activity(self, function: Any, *args: Any) -> Any:
        with self._condition:
            self._active_calls += 1
        try:
            return function(*args)
        finally:
            with self._condition:
                self._active_calls -= 1
                self._condition.notify_all()

    def _run_supervised_command(
        self,
        argv: Sequence[str],
        *,
        env: Mapping[str, str] | None = None,
        stdout: Any = subprocess.PIPE,
        stderr: Any = subprocess.PIPE,
        timeout: float,
        check: bool = False,
        text: bool = False,
    ) -> subprocess.CompletedProcess[Any]:
        with self._lock:
            if self._stop_requested:
                raise RuntimeError("stop requested before preflight command launch")
            process = subprocess.Popen(
                list(argv),
                env=dict(env) if env is not None else None,
                stdin=subprocess.DEVNULL,
                stdout=stdout,
                stderr=stderr,
                text=text,
                shell=False,
                close_fds=True,
                start_new_session=True,
            )
            self._aux_processes[process.pid] = process
        try:
            output, error_output = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            with self._cleanup_lock:
                _terminate_processes([process], min(5.0, max(0.1, timeout)))
            raise
        finally:
            with self._cleanup_lock:
                audit = _terminate_processes([process], 0.1)
            if audit and audit[0]["confirmed_gone"]:
                with self._lock:
                    self._aux_processes.pop(process.pid, None)
        completed = subprocess.CompletedProcess(
            list(argv), process.returncode, output, error_output
        )
        if check and completed.returncode != 0:
            raise subprocess.CalledProcessError(
                completed.returncode,
                completed.args,
                output=completed.stdout,
                stderr=completed.stderr,
            )
        return completed

    def prepare(self, request: Mapping[str, Any]) -> dict[str, Any]:
        return self._call_with_activity(self._prepare, request)

    def _prepare(self, request: Mapping[str, Any]) -> dict[str, Any]:
        with self._lock:
            if self._state != "NEW":
                raise RuntimeError("actor is single-use and is not in NEW state")
            self._state = "PREPARING"
        rank_table_path = _nonempty_text(request.get("rank_table"), "request.rank_table")
        probe_binary_text = _nonempty_text(
            request.get("probe_binary"), "request.probe_binary"
        )
        server_id_env = _nonempty_text(
            request.get("server_id_env"), "request.server_id_env"
        )
        worker_identity, identity_values = resolve_server_identity(server_id_env)

        rank_table_payload, table = read_rank_table(rank_table_path)
        local_server, identity_matched_field = select_rank_table_server(
            table, worker_identity
        )
        rank_table_server_id = str(local_server["server_id"])
        if len(local_server["devices"]) != self.npu_count:
            raise ValidationError(
                f"Ray reserves {self.npu_count} NPUs but rank table assigns "
                f"{len(local_server['devices'])} to {rank_table_server_id}"
            )
        expected_device_ids = sorted(
            int(item["device_id"]) for item in local_server["devices"]
        )
        actual_device_ids = detect_local_device_ids()
        if actual_device_ids != expected_device_ids:
            raise ValidationError(
                f"visible Device IDs {actual_device_ids} differ from rank table "
                f"{expected_device_ids} on {rank_table_server_id}"
            )
        visible_device_environment = validate_acl_device_mapping(
            expected_device_ids
        )

        command_timeout = float(request.get("command_timeout", 30.0))
        if not 1.0 <= command_timeout <= 300.0:
            raise ValidationError("command_timeout must be in [1, 300] seconds")

        # Normalize the CANN/driver environment before invoking any Ascend
        # binary.  In particular, hccn_tool depends on driver libraries that
        # are not necessarily present in the Ray worker's inherited
        # LD_LIBRARY_PATH even when the CANN setup script is available.
        raw_cann_env_script = request.get("cann_env_script")
        cann_env_script = (
            None
            if raw_cann_env_script is None
            else _nonempty_text(raw_cann_env_script, "request.cann_env_script")
        )
        child_environment = _load_cann_environment(
            self._run_supervised_command, cann_env_script
        )
        final_visible_device_environment = validate_acl_device_mapping(
            expected_device_ids, child_environment
        )

        verify_device_ips = request.get("verify_device_ips") is True
        actual_ips: dict[int, str] = {}
        if verify_device_ips:
            hccn_tool = Path(
                _nonempty_text(request.get("hccn_tool"), "request.hccn_tool")
            )
            if not hccn_tool.is_file() or not os.access(hccn_tool, os.X_OK):
                raise ValidationError(f"hccn_tool is not executable: {hccn_tool}")
            for device in local_server["devices"]:
                device_id = int(device["device_id"])
                completed = self._run_supervised_command(
                    [str(hccn_tool), "-i", str(device_id), "-ip", "-g"],
                    env=child_environment,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    timeout=command_timeout,
                    check=False,
                )
                if completed.returncode != 0:
                    raise ValidationError(
                        f"hccn_tool IP query failed for Device {device_id}: "
                        f"{completed.stdout.strip()}"
                    )
                actual_ip = parse_hccn_ip(completed.stdout)
                expected_ip = str(device["device_ip"])
                if actual_ip != expected_ip:
                    raise ValidationError(
                        f"Device {device_id} IP is {actual_ip}, rank table says {expected_ip}"
                    )
                actual_ips[device_id] = actual_ip

        probe_binary = Path(probe_binary_text).resolve()
        if not probe_binary.is_file() or not os.access(probe_binary, os.X_OK):
            raise ValidationError(f"probe binary is not executable: {probe_binary}")
        binary_sha256 = sha256_file(probe_binary)
        run_id = _nonempty_text(request.get("run_id"), "request.run_id")
        if not _RUN_ID_RE.fullmatch(run_id):
            raise ValidationError("run_id contains unsupported characters")
        log_root = Path(_nonempty_text(request.get("log_root"), "request.log_root"))
        if not log_root.is_absolute():
            raise ValidationError("log_root must be absolute")
        run_directory = (
            log_root / run_id / _server_log_directory_name(rank_table_server_id)
        )
        run_directory.mkdir(mode=0o700, parents=True, exist_ok=False)
        rank_table_snapshot = run_directory / "ranktable.json"
        write_rank_table_snapshot(
            rank_table_payload, rank_table_snapshot, table["sha256"]
        )

        count = int(request.get("count", 4096))
        test_timeout = float(request.get("test_timeout", 600.0))
        kill_grace = float(request.get("kill_grace", 10.0))
        connect_timeout = int(request.get("connect_timeout", 120))
        exec_timeout = int(request.get("exec_timeout", 300))
        if not 1 <= count <= 1024 * 1024:
            raise ValidationError("probe count must be in [1, 1048576]")
        if not 1.0 <= test_timeout <= 86400.0:
            raise ValidationError("test timeout must be in [1, 86400] seconds")
        if not 0.1 <= kill_grace <= 60.0:
            raise ValidationError("kill grace must be in [0.1, 60] seconds")
        if not 1 <= connect_timeout <= 86400 or not 1 <= exec_timeout <= 86400:
            raise ValidationError("HCCL timeouts must be in [1, 86400] seconds")

        child_environment["RANK_TABLE_FILE"] = str(rank_table_snapshot)
        child_environment["HCCL_CONNECT_TIMEOUT"] = str(connect_timeout)
        child_environment["HCCL_EXEC_TIMEOUT"] = str(exec_timeout)
        specs = build_probe_specs(
            local_server,
            binary=str(probe_binary),
            rank_table=str(rank_table_snapshot),
            world_size=int(table["world_size"]),
            count=count,
        )
        log_paths: dict[int, str] = {}
        for spec in specs:
            rank_id = int(spec["rank_id"])
            log_path = run_directory / f"rank-{rank_id:05d}.log"
            with log_path.open("xb"):
                pass
            os.chmod(log_path, 0o600)
            log_paths[rank_id] = str(log_path)

        prepared = {
            "source_rank_table": rank_table_path,
            "rank_table": str(rank_table_snapshot),
            "ranktable_sha256": table["sha256"],
            "probe_binary": str(probe_binary),
            "probe_binary_sha256": binary_sha256,
            "server_id": rank_table_server_id,
            "server_id_source": server_id_env,
            "worker_identity": worker_identity,
            "server_identity_matched_field": identity_matched_field,
            "rank_table_host_ip": local_server.get("host_ip"),
            "rank_start": local_server["rank_start"],
            "local_server": local_server,
            "table": table,
            "run_directory": str(run_directory),
            "count": count,
            "test_timeout": test_timeout,
            "kill_grace": kill_grace,
            "connect_timeout": connect_timeout,
            "exec_timeout": exec_timeout,
            "child_environment": child_environment,
            "specs": specs,
            "log_paths": log_paths,
        }
        with self._lock:
            if self._stop_requested:
                self._state = "STOPPED"
                raise RuntimeError("stop requested while actor was preparing")
            self._prepared = prepared
            self._state = "PREPARED"
        return {
            "ray_node_id": self.actual_ray_node_id,
            "pod_name": os.environ.get("POD_NAME") or socket.gethostname(),
            "node_name": os.environ.get("NODE_NAME"),
            "server_id": rank_table_server_id,
            "server_id_source": server_id_env,
            "worker_identity": worker_identity,
            "server_identity_matched_field": identity_matched_field,
            "rank_table_host_ip": local_server.get("host_ip"),
            "identity_values": identity_values,
            "npu_count": self.npu_count,
            "rank_start": int(local_server["rank_start"]),
            "rank_ids": [item["rank_id"] for item in local_server["devices"]],
            "device_ids": [item["device_id"] for item in local_server["devices"]],
            "device_ips": [item["device_ip"] for item in local_server["devices"]],
            "actual_device_ips": actual_ips,
            "device_ip_verified": verify_device_ips,
            "visible_device_environment": visible_device_environment,
            "final_visible_device_environment": final_visible_device_environment,
            "acl_device_mapping": "identity-only",
            "ranktable_sha256": table["sha256"],
            "probe_binary_sha256": binary_sha256,
            "world_size": table["world_size"],
            "server_count": table["server_count"],
            "table_servers": table["servers"],
            "rank_table_snapshot": str(rank_table_snapshot),
            "run_directory": str(run_directory),
        }

    def run(self) -> dict[str, Any]:
        return self._call_with_activity(self._run)

    def _run(self) -> dict[str, Any]:
        with self._lock:
            if self._prepared is None or self._state != "PREPARED":
                raise RuntimeError("actor must be PREPARED before run")
            if self._stop_requested:
                raise RuntimeError("actor was stopped before run")
            self._state = "RUNNING"
            prepared = self._prepared
        _, current_table = read_rank_table(prepared["rank_table"])
        if current_table["sha256"] != prepared["ranktable_sha256"]:
            raise ValidationError("rank table changed between prepare and run")
        if sha256_file(Path(prepared["probe_binary"])) != prepared["probe_binary_sha256"]:
            raise ValidationError("probe binary changed between prepare and run")
        test_timeout = float(prepared["test_timeout"])
        kill_grace = float(prepared["kill_grace"])
        environment = dict(prepared["child_environment"])
        specs = list(prepared["specs"])
        processes: dict[int, subprocess.Popen[bytes]] = {}
        handles: dict[int, Any] = {}
        log_paths: dict[int, Path] = {}
        failure: str | None = None
        timed_out = False
        cleanup_audit: list[dict[str, Any]] = []
        try:
            for spec in specs:
                rank_id = int(spec["rank_id"])
                log_path = Path(prepared["log_paths"][rank_id])
                handle = log_path.open("ab")
                handles[rank_id] = handle
                log_paths[rank_id] = log_path
                child_env = environment.copy()
                child_env.update(
                    {
                        "RANK_ID": str(rank_id),
                        "RANK_SIZE": str(prepared["table"]["world_size"]),
                        "DEVICE_ID": str(spec["device_id"]),
                    }
                )
                with self._lock:
                    if self._stop_requested:
                        failure = "stop requested before all local ranks started"
                        break
                    process = subprocess.Popen(
                        spec["argv"],
                        cwd=str(Path(prepared["probe_binary"]).parent),
                        env=child_env,
                        stdin=subprocess.DEVNULL,
                        stdout=handle,
                        stderr=subprocess.STDOUT,
                        shell=False,
                        close_fds=True,
                        start_new_session=True,
                    )
                    processes[rank_id] = process
                    self._processes[rank_id] = process

            deadline = time.monotonic() + test_timeout
            while failure is None and processes:
                states = {rank: process.poll() for rank, process in processes.items()}
                bad = {rank: code for rank, code in states.items() if code not in (None, 0)}
                if bad:
                    failure = f"one or more local ranks failed: {bad}"
                    break
                if all(code == 0 for code in states.values()):
                    break
                with self._lock:
                    if self._stop_requested:
                        failure = "stop requested by Ray driver"
                        break
                if time.monotonic() >= deadline:
                    failure = f"local HCCL test timed out after {test_timeout} seconds"
                    timed_out = True
                    break
                time.sleep(0.2)
        finally:
            with self._cleanup_lock:
                final_cleanup = _terminate_processes(
                    list(processes.values()), kill_grace
                )
            earlier = {item["pid"]: item for item in cleanup_audit}
            for item in final_cleanup:
                previous = earlier.get(item["pid"])
                if previous is not None:
                    item["signals"] = list(
                        dict.fromkeys([*previous["signals"], *item["signals"]])
                    )
                    item["errors"] = [*previous["errors"], *item["errors"]]
            cleanup_audit = final_cleanup
            for handle in handles.values():
                handle.close()
            cleanup_confirmed = all(
                item["confirmed_gone"] for item in final_cleanup
            )
            with self._lock:
                if cleanup_confirmed:
                    self._processes.clear()
                    self._state = (
                        "STOPPED" if self._stop_requested else "FINISHED"
                    )
                else:
                    self._state = "CLEANUP_UNCONFIRMED"

        rank_results: list[dict[str, Any]] = []
        for spec in specs:
            rank_id = int(spec["rank_id"])
            process = processes.get(rank_id)
            log_path = log_paths.get(rank_id)
            log_tail = _bounded_tail(log_path) if log_path is not None else "<not started>"
            marker = f"RANKTABLE_HCCL_PASS rank={rank_id} "
            marker_found = marker in log_tail
            exit_code = process.returncode if process is not None else None
            rank_results.append(
                {
                    "rank_id": rank_id,
                    "device_id": spec["device_id"],
                    "device_ip": spec["device_ip"],
                    "exit_code": exit_code,
                    "pass_marker": marker_found,
                    "status": "PASS" if exit_code == 0 and marker_found else "FAIL",
                    "log_path": str(log_path) if log_path is not None else None,
                    "log_tail": log_tail,
                }
            )
        if any(item["status"] != "PASS" for item in rank_results) and failure is None:
            failure = "one or more ranks lacked a successful result marker"
        if not cleanup_confirmed:
            failure = "one or more local process groups could not be confirmed gone"

        _, final_table = read_rank_table(prepared["rank_table"])
        if final_table["sha256"] != prepared["ranktable_sha256"]:
            failure = "rank table changed during the HCCL test"
        return {
            "ray_node_id": self.actual_ray_node_id,
            "server_id": prepared["server_id"],
            "rank_start": prepared["local_server"]["rank_start"],
            "status": "PASS" if failure is None else "FAIL",
            "failure": failure,
            "timed_out": timed_out,
            "ranktable_sha256": prepared["ranktable_sha256"],
            "probe_binary_sha256": prepared["probe_binary_sha256"],
            "ranks": rank_results,
            "cleanup": cleanup_audit,
        }

    def stop(self, grace_seconds: float = 10.0) -> dict[str, Any]:
        with self._lock:
            self._stop_requested = True
            if self._state != "FINISHED":
                self._state = "STOPPING"
            processes = [
                *self._processes.values(),
                *self._aux_processes.values(),
            ]
        with self._cleanup_lock:
            audit = _terminate_processes(processes, grace_seconds)
        quiesce_deadline = time.monotonic() + grace_seconds + 5.0
        with self._condition:
            while self._active_calls:
                remaining = quiesce_deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._condition.wait(timeout=min(0.2, remaining))
            active_calls = self._active_calls
            remaining_processes = [
                *processes,
                *self._processes.values(),
                *self._aux_processes.values(),
            ]
        with self._cleanup_lock:
            final_audit = _terminate_processes(remaining_processes, 0.1)
        previous_by_pid = {item["pid"]: item for item in audit}
        for item in final_audit:
            previous = previous_by_pid.get(item["pid"])
            if previous is not None:
                item["signals"] = list(
                    dict.fromkeys([*previous["signals"], *item["signals"]])
                )
                item["errors"] = [*previous["errors"], *item["errors"]]
        audit = final_audit
        all_gone = (
            active_calls == 0
            and all(item["confirmed_gone"] for item in final_audit)
        )
        with self._lock:
            if all_gone:
                self._processes.clear()
                self._aux_processes.clear()
                self._state = "STOPPED"
            state = self._state
        return {
            "stopped_processes": len(processes),
            "all_process_groups_gone": all_gone,
            "active_calls": active_calls,
            "state": state,
            "audit": audit,
        }


def discover_resource_nodes(
    ray_module: Any, resource_name: str
) -> list[tuple[str, int]]:
    nodes: list[tuple[str, int]] = []
    for node in ray_module.nodes():
        if not node.get("Alive"):
            continue
        raw = node.get("Resources", {}).get(resource_name, 0)
        if not raw:
            continue
        if isinstance(raw, bool) or int(raw) != raw or raw < 1:
            raise ValidationError(
                f"Ray node {node.get('NodeID')} has invalid {resource_name}={raw}"
            )
        nodes.append((str(node["NodeID"]), int(raw)))
    nodes.sort()
    if not nodes:
        raise ValidationError(f"no alive Ray node advertises {resource_name!r}")
    return nodes


def _stop_actors(ray_module: Any, actors: Sequence[Any], grace: float) -> dict[str, Any]:
    references: dict[Any, int] = {}
    dispatch_errors: list[dict[str, Any]] = []
    for index, actor in enumerate(actors):
        try:
            references[actor.stop.remote(grace)] = index
        except Exception as error:
            dispatch_errors.append(
                {"actor_index": index, "error": f"{type(error).__name__}: {error}"}
            )

    results: list[dict[str, Any]] = []
    pending = dict(references)
    deadline = time.monotonic() + max(30.0, 2.0 * grace + 20.0)
    while pending:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        try:
            ready, _ = ray_module.wait(
                list(pending), num_returns=1, timeout=min(1.0, remaining)
            )
        except Exception as error:
            dispatch_errors.append(
                {"actor_index": None, "error": f"ray.wait: {type(error).__name__}: {error}"}
            )
            break
        if not ready:
            continue
        reference = ready[0]
        index = pending.pop(reference)
        try:
            result = dict(ray_module.get(reference))
            result["actor_index"] = index
            results.append(result)
        except Exception as error:
            dispatch_errors.append(
                {"actor_index": index, "error": f"{type(error).__name__}: {error}"}
            )

    timed_out_actor_indexes = sorted(pending.values())
    all_confirmed = (
        not dispatch_errors
        and not timed_out_actor_indexes
        and len(results) == len(actors)
        and all(item.get("all_process_groups_gone") is True for item in results)
    )
    return {
        "status": "PASS" if all_confirmed else "UNCONFIRMED",
        "all_process_groups_gone": all_confirmed,
        "workers": sorted(results, key=lambda item: item["actor_index"]),
        "dispatch_errors": dispatch_errors,
        "timed_out_actor_indexes": timed_out_actor_indexes,
    }


def execute(args: argparse.Namespace) -> dict[str, Any]:
    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except ImportError as error:
        raise ValidationError(f"Ray is required with --execute: {error}") from error

    started_here = False
    actors: list[Any] = []
    preparations: list[dict[str, Any]] = []
    run_results: list[dict[str, Any]] = []
    failure: str | None = None
    cleanup: dict[str, Any] = {"status": "NOT_NEEDED"}
    result_payload: dict[str, Any] | None = None
    try:
        if not ray.is_initialized():
            ray.init(address=args.address, log_to_driver=False)
            started_here = True
        resource_nodes = discover_resource_nodes(ray, args.resource)
        if args.expected_workers and len(resource_nodes) != args.expected_workers:
            raise ValidationError(
                f"expected {args.expected_workers} resource workers, "
                f"Ray has {len(resource_nodes)}"
            )

        RemoteActor = ray.remote(
            num_cpus=0,
            max_restarts=0,
            max_task_retries=0,
            max_concurrency=2,
        )(_RankTableHcclActor)
        for node_id, npu_count in resource_nodes:
            actors.append(
                RemoteActor.options(
                    resources={args.resource: float(npu_count)},
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        node_id=node_id, soft=False
                    ),
                ).remote(node_id, npu_count)
            )

        prepare_request = {
            "rank_table": args.rank_table,
            "probe_binary": args.probe_binary,
            "server_id_env": args.server_id_env,
            "cann_env_script": args.cann_env_script,
            "hccn_tool": args.hccn_tool,
            "verify_device_ips": not args.skip_device_ip_check,
            "command_timeout": args.command_timeout,
            "run_id": args.run_id,
            "log_root": args.log_root,
            "count": args.count,
            "test_timeout": args.test_timeout,
            "kill_grace": args.kill_grace,
            "connect_timeout": args.connect_timeout,
            "exec_timeout": args.exec_timeout,
        }
        prepare_refs = [actor.prepare.remote(prepare_request) for actor in actors]
        preparations = list(ray.get(prepare_refs, timeout=args.prepare_timeout))
        preflight = validate_preparations(
            preparations,
            resource_nodes,
            expected_workers=args.expected_workers,
            expected_world_size=args.expected_world_size,
        )

        pending = {actor.run.remote(): actor for actor in actors}
        deadline = time.monotonic() + args.overall_timeout
        while pending:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                failure = f"overall timeout after {args.overall_timeout} seconds"
                break
            ready, _ = ray.wait(
                list(pending), num_returns=1, timeout=min(1.0, remaining)
            )
            if not ready:
                continue
            reference = ready[0]
            pending.pop(reference)
            try:
                result = ray.get(reference)
            except Exception as error:
                result = {
                    "status": "FAIL",
                    "failure": f"{type(error).__name__}: {error}",
                    "ranks": [],
                }
            run_results.append(result)
            if result.get("status") != "PASS":
                failure = str(result.get("failure") or "worker HCCL test failed")
                break
        if failure is not None:
            cleanup = _stop_actors(ray, actors, args.kill_grace)
            if not cleanup["all_process_groups_gone"]:
                failure = f"{failure}; worker process-group cleanup is unconfirmed"

            drain_deadline = time.monotonic() + max(10.0, args.kill_grace + 5.0)
            while pending and time.monotonic() < drain_deadline:
                drain_remaining = drain_deadline - time.monotonic()
                if drain_remaining <= 0:
                    break
                ready, _ = ray.wait(
                    list(pending),
                    num_returns=1,
                    timeout=min(1.0, drain_remaining),
                )
                if not ready:
                    continue
                reference = ready[0]
                pending.pop(reference)
                try:
                    run_results.append(ray.get(reference))
                except Exception as error:
                    run_results.append(
                        {
                            "status": "FAIL",
                            "failure": f"{type(error).__name__}: {error}",
                            "ranks": [],
                        }
                    )
            if pending:
                cleanup["undrained_run_tasks"] = len(pending)
                cleanup["status"] = "UNCONFIRMED"
                cleanup["all_process_groups_gone"] = False

        reported_ranks = sorted(
            int(rank["rank_id"])
            for result in run_results
            for rank in result.get("ranks", [])
            if rank.get("status") == "PASS"
        )
        all_ranks_pass = (
            failure is None
            and reported_ranks == list(range(int(preflight["world_size"])))
            and len(run_results) == len(actors)
        )
        if not all_ranks_pass and failure is None:
            failure = "not every global rank reported PASS exactly once"
        result_payload = {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "mode": "execute",
            "validation_kind": "public_hccl_api_ranktable_smoke",
            "official_hccltest": False,
            "status": "PASS" if failure is None else "FAIL",
            "failure": failure,
            "mpi_used": False,
            "preflight": {
                "status": "PASS",
                **preflight,
                "workers": sorted(
                    preparations, key=lambda item: min(int(rank_id) for rank_id in item["rank_ids"])
                ),
            },
            "workers": sorted(
                run_results, key=lambda item: int(item.get("rank_start", 1 << 30))
            ),
            "cleanup": cleanup,
            "acceptance": {
                "expected_rank_count": int(preflight["world_size"]),
                "passed_rank_count": len(reported_ranks),
                "all_ranks_pass": all_ranks_pass,
            },
            "dropped_npu_reallocation": "TODO_NOT_IMPLEMENTED",
        }
        return result_payload
    except Exception as error:
        cleanup = _stop_actors(ray, actors, args.kill_grace)
        result_payload = {
            "schema_version": SCHEMA_VERSION,
            "run_id": args.run_id,
            "mode": "execute",
            "validation_kind": "public_hccl_api_ranktable_smoke",
            "official_hccltest": False,
            "status": "FAIL",
            "failure": f"{type(error).__name__}: {error}",
            "mpi_used": False,
            "preflight": {"status": "FAIL", "workers": preparations},
            "workers": run_results,
            "cleanup": cleanup,
            "dropped_npu_reallocation": "TODO_NOT_IMPLEMENTED",
        }
        return result_payload
    finally:
        final_cleanup = _stop_actors(ray, actors, args.kill_grace)
        confirmed_actor_indexes = {
            int(item["actor_index"])
            for item in final_cleanup["workers"]
            if item.get("all_process_groups_gone") is True
        }
        actor_kill_errors: list[dict[str, Any]] = []
        for index, actor in enumerate(actors):
            try:
                # Always release the Ray reservation.  A probe also requests a
                # parent-death signal, while an unconfirmed process group still
                # keeps the overall result in FAIL state for operator review.
                ray.kill(actor, no_restart=True)
            except Exception as error:
                actor_kill_errors.append(
                    {
                        "actor_index": index,
                        "error": f"{type(error).__name__}: {error}",
                    }
                )
        final_cleanup["actors_with_confirmed_process_cleanup"] = sorted(
            confirmed_actor_indexes
        )
        final_cleanup["actor_kill_requested"] = len(actors)
        final_cleanup["actor_kill_errors"] = actor_kill_errors
        final_cleanup["ray_shutdown"] = False
        if result_payload is not None:
            result_payload["final_cleanup"] = final_cleanup
            if (
                not final_cleanup["all_process_groups_gone"]
                or actor_kill_errors
            ):
                previous_failure = result_payload.get("failure")
                cleanup_failure = "final worker cleanup is unconfirmed"
                result_payload["failure"] = (
                    f"{previous_failure}; {cleanup_failure}"
                    if previous_failure
                    else cleanup_failure
                )
                result_payload["status"] = "FAIL"
                if "acceptance" in result_payload:
                    result_payload["acceptance"]["all_ranks_pass"] = False
        if started_here and final_cleanup["all_process_groups_gone"]:
            try:
                ray.shutdown()
                final_cleanup["ray_shutdown"] = True
            except Exception as error:
                final_cleanup["ray_shutdown_error"] = (
                    f"{type(error).__name__}: {error}"
                )
                if result_payload is not None:
                    result_payload["status"] = "FAIL"
                    result_payload["failure"] = "Ray shutdown failed after worker cleanup"


def _new_run_id() -> str:
    timestamp = _datetime.datetime.now(_datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"hccl-{timestamp}-{secrets.token_hex(4)}"


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="auto", help="Ray address")
    parser.add_argument("--rank-table", default=DEFAULT_RANK_TABLE)
    parser.add_argument("--probe-binary", default=DEFAULT_PROBE_BINARY)
    parser.add_argument(
        "--resource",
        default="NPU",
        help="Ray resource name used to discover and reserve accelerator devices",
    )
    parser.add_argument(
        "--server-id-env",
        default="HOST_IP",
        help=(
            "worker environment variable whose value matches rank-table "
            "server_id or optional host_ip"
        ),
    )
    parser.add_argument(
        "--cann-env-script",
        default=None,
        help="absolute CANN setup script path; default: discover on each worker",
    )
    parser.add_argument("--hccn-tool", default=DEFAULT_HCCN_TOOL)
    parser.add_argument("--expected-workers", type=int, default=0)
    parser.add_argument("--expected-world-size", type=int, default=0)
    parser.add_argument("--count", type=int, default=4096)
    parser.add_argument("--connect-timeout", type=int, default=120)
    parser.add_argument("--exec-timeout", type=int, default=300)
    parser.add_argument("--command-timeout", type=float, default=30.0)
    parser.add_argument("--prepare-timeout", type=float, default=300.0)
    parser.add_argument("--test-timeout", type=float, default=600.0)
    parser.add_argument("--overall-timeout", type=float, default=660.0)
    parser.add_argument("--kill-grace", type=float, default=10.0)
    parser.add_argument("--log-root", default=DEFAULT_LOG_ROOT)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--skip-device-ip-check",
        action="store_true",
        help="skip hccn_tool IP comparison (not recommended)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="reserve NPUs and run HCCL; default is a non-connecting dry-run",
    )
    return parser


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    return make_parser().parse_args(argv)


def validate_args(args: argparse.Namespace) -> None:
    args.run_id = args.run_id or _new_run_id()
    if not _RUN_ID_RE.fullmatch(args.run_id):
        raise ValidationError("--run-id contains unsupported characters")
    if args.expected_workers < 0 or args.expected_world_size < 0:
        raise ValidationError("expected counts must be non-negative")
    if not isinstance(args.resource, str) or not args.resource.strip():
        raise ValidationError("--resource must be a non-empty Ray resource name")
    if args.resource != args.resource.strip():
        raise ValidationError("--resource must not contain leading or trailing whitespace")
    if not isinstance(args.server_id_env, str) or not _ENV_NAME_RE.fullmatch(
        args.server_id_env
    ):
        raise ValidationError("--server-id-env must be a valid environment name")
    if args.cann_env_script is not None and not Path(
        args.cann_env_script
    ).is_absolute():
        raise ValidationError("--cann-env-script must be absolute")
    if not 1 <= args.count <= 1024 * 1024:
        raise ValidationError("--count must be in [1, 1048576]")
    if not 1 <= args.connect_timeout <= 86400:
        raise ValidationError("--connect-timeout must be in [1, 86400]")
    if not 1 <= args.exec_timeout <= 86400:
        raise ValidationError("--exec-timeout must be in [1, 86400]")
    if not 1.0 <= float(args.command_timeout) <= 300.0:
        raise ValidationError("--command-timeout must be in [1, 300]")
    for name in ("prepare_timeout", "test_timeout", "overall_timeout"):
        value = float(getattr(args, name))
        if not 1.0 <= value <= 86400.0:
            raise ValidationError(f"--{name.replace('_', '-')} must be in [1, 86400]")
    if args.overall_timeout <= args.test_timeout:
        raise ValidationError("--overall-timeout must be greater than --test-timeout")
    if not 0.1 <= args.kill_grace <= 60.0:
        raise ValidationError("--kill-grace must be in [0.1, 60]")
    if args.execute and args.skip_device_ip_check:
        raise ValidationError("--skip-device-ip-check is diagnostic-only and cannot use --execute")
    for path_name in ("rank_table", "probe_binary", "hccn_tool", "log_root"):
        if not Path(getattr(args, path_name)).is_absolute():
            raise ValidationError(f"--{path_name.replace('_', '-')} must be absolute")


def _write_output(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        validate_args(args)
        if args.execute:
            result = execute(args)
        else:
            result = {
                "schema_version": SCHEMA_VERSION,
                "run_id": args.run_id,
                "mode": "dry-run",
                "validation_kind": "configuration_preview",
                "official_hccltest": False,
                "status": "DRY_RUN",
                "mpi_used": False,
                "ray_connected": False,
                "npu_accessed": False,
                "rank_table": args.rank_table,
                "probe_binary": args.probe_binary,
                "resource": args.resource,
                "server_id_env": args.server_id_env,
                "cann_env_script": args.cann_env_script or "auto",
                "expected_workers": args.expected_workers,
                "expected_world_size": args.expected_world_size,
                "count": args.count,
                "device_ip_check": not args.skip_device_ip_check,
                "message": "add --execute only after all target NPUs are idle",
            }
        text = json.dumps(result, ensure_ascii=False, indent=2) + "\n"
        if args.output is not None:
            _write_output(args.output, text)
        print(text, end="")
        return 0 if result["status"] in {"PASS", "DRY_RUN"} else 1
    except ValidationError as error:
        print(f"ray_ranktable_hccl: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("ray_ranktable_hccl: interrupted; worker cleanup was requested", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
