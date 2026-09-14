"""Convert ClusterD HCCL discovery output to an official AI Server rank table."""

from __future__ import annotations

import ipaddress
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path


DECIMAL = re.compile(r"^(0|[1-9][0-9]*)$")
RANKTABLE_PROFILE = "ai-server-v1"
RANKTABLE_VERSION = "1.0"


class HcclCleanError(ValueError):
    """Raised when ClusterD output is unsafe to publish."""


class HcclNotReadyError(HcclCleanError):
    """Raised when ClusterD has not marked the topology complete yet."""


@dataclass(frozen=True)
class CleanResult:
    hccl: dict[str, object]
    rank_offset: int
    server_count: int
    world_size: int
    pod_names: tuple[str, ...]
    profile: str = RANKTABLE_PROFILE
    ranktable_version: str = RANKTABLE_VERSION


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    value: dict[str, object] = {}
    for key, item in pairs:
        if key in value:
            raise HcclCleanError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def parse_hccl_text(text: str) -> dict[str, object]:
    """Parse JSON without silently accepting duplicate object keys."""

    try:
        value = json.loads(text, object_pairs_hook=_reject_duplicate_keys)
    except json.JSONDecodeError as error:
        raise HcclCleanError(f"invalid hccl.json: {error}") from error
    if not isinstance(value, dict):
        raise HcclCleanError("hccl.json root must be an object")
    return value


def _required_text(value: object, field: str) -> str:
    if (
        not isinstance(value, str)
        or not value
        or any(character in value for character in ("\x00", "\r", "\n"))
    ):
        raise HcclCleanError(f"{field} must be a non-empty string")
    return value


def _parse_rank(value: object, field: str) -> int:
    if not isinstance(value, str) or not DECIMAL.fullmatch(value):
        raise HcclCleanError(f"{field} must be a non-negative decimal string")
    return int(value)


def _parse_port(value: object, field: str) -> str:
    if isinstance(value, bool):
        raise HcclCleanError(f"{field} must be a decimal port")
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and DECIMAL.fullmatch(value):
        parsed = int(value)
    else:
        raise HcclCleanError(f"{field} must be a decimal port")
    if not 1 <= parsed <= 65535:
        raise HcclCleanError(f"{field} must be in [1, 65535]")
    return str(parsed)


def _parse_ip(value: object, field: str, *, ipv4_only: bool = False) -> str:
    text = _required_text(value, field)
    try:
        address = ipaddress.ip_address(text)
    except ValueError as error:
        raise HcclCleanError(f"{field} is invalid: {text}") from error
    if address.is_unspecified:
        raise HcclCleanError(f"{field} must not be unspecified")
    if ipv4_only and not isinstance(address, ipaddress.IPv4Address):
        raise HcclCleanError(f"{field} must be an IPv4 address")
    return str(address)


def clean_clusterd_hccl(hccl: dict[str, object]) -> CleanResult:
    """Return a validated CANN AI Server v1.0 rank table.

    Duplicate ranks are rejected.  Valid unique ranks are compacted in their
    original order so stale gaps left by ClusterD do not reach HCCL.  ClusterD
    metadata is deliberately omitted from the returned HCCL table.
    """

    if hccl.get("status") != "complete":
        raise HcclNotReadyError(
            f"ClusterD status is {hccl.get('status')!r}; expected 'complete'"
        )

    servers_value = hccl.get("server_list")
    if not isinstance(servers_value, list) or not servers_value:
        raise HcclCleanError("server_list must be a non-empty list")

    source_server_count = _parse_rank(hccl.get("server_count"), "server_count")
    if source_server_count != len(servers_value):
        raise HcclCleanError(
            "server_count does not match the number of ClusterD servers"
        )

    super_pod_list = hccl.get("super_pod_list")
    if super_pod_list not in (None, []):
        raise HcclCleanError(
            "SuperPod topology cannot be converted with the ai-server-v1 profile"
        )

    seen_server_ids: set[str] = set()
    seen_host_ips: set[str] = set()
    seen_pod_names: set[str] = set()
    seen_device_ips: set[str] = set()
    seen_ranks: set[int] = set()
    indexed_servers: list[tuple[int, str, dict[str, object]]] = []

    for server_index, server in enumerate(servers_value):
        prefix = f"server_list[{server_index}]"
        if not isinstance(server, dict):
            raise HcclCleanError(f"{prefix} must be an object")

        server_id = _required_text(server.get("server_id"), f"{prefix}.server_id")
        pod_name = _required_text(server.get("pod_name"), f"{prefix}.pod_name")
        if len(server_id) > 64:
            raise HcclCleanError(f"{prefix}.server_id must be at most 64 characters")
        if server.get("super_pod_id") not in (None, -1, "-1"):
            raise HcclCleanError(
                f"{prefix}.super_pod_id describes a SuperPod; "
                "ai-server-v1 only supports typical AI Server topology"
            )
        if server_id in seen_server_ids:
            raise HcclCleanError(f"duplicate server_id: {server_id}")
        if server_id in seen_host_ips:
            raise HcclCleanError(
                f"server_id {server_id!r} conflicts with another host_ip"
            )
        if pod_name in seen_pod_names:
            raise HcclCleanError(f"duplicate pod_name: {pod_name}")
        seen_server_ids.add(server_id)
        seen_pod_names.add(pod_name)
        host_ip = (
            _parse_ip(server.get("host_ip"), f"{prefix}.host_ip", ipv4_only=True)
            if "host_ip" in server
            else None
        )
        if host_ip is not None:
            if host_ip in seen_host_ips:
                raise HcclCleanError(f"duplicate host_ip: {host_ip}")
            if host_ip in seen_server_ids and host_ip != server_id:
                raise HcclCleanError(
                    f"host_ip {host_ip!r} conflicts with another server_id"
                )
            seen_host_ips.add(host_ip)

        devices = server.get("device")
        if not isinstance(devices, list) or not devices:
            raise HcclCleanError(f"{prefix}.device must be a non-empty list")
        local_device_ids: set[str] = set()
        local_host_ports: set[str] = set()
        indexed_devices: list[tuple[int, dict[str, object]]] = []
        for device_index, device in enumerate(devices):
            device_prefix = f"{prefix}.device[{device_index}]"
            if not isinstance(device, dict):
                raise HcclCleanError(f"{device_prefix} must be an object")
            device_id_value = _parse_rank(
                device.get("device_id"), f"{device_prefix}.device_id"
            )
            device_id = str(device_id_value)
            device_ip = _parse_ip(
                device.get("device_ip"), f"{device_prefix}.device_ip"
            )
            rank = _parse_rank(device.get("rank_id"), f"{device_prefix}.rank_id")
            official_device: dict[str, object] = {
                "device_id": device_id,
                "device_ip": device_ip,
                "rank_id": str(rank),
            }
            for port_name in ("device_port", "host_port"):
                if port_name in device:
                    official_device[port_name] = _parse_port(
                        device.get(port_name), f"{device_prefix}.{port_name}"
                    )
            host_port = official_device.get("host_port")
            if isinstance(host_port, str):
                if host_port in local_host_ports:
                    raise HcclCleanError(
                        f"duplicate host_port {host_port} on server {server_id}"
                    )
                local_host_ports.add(host_port)

            if device_id in local_device_ids:
                raise HcclCleanError(
                    f"duplicate device_id {device_id} on server {server_id}"
                )
            if device_ip in seen_device_ips:
                raise HcclCleanError(f"duplicate device_ip: {device_ip}")
            if rank in seen_ranks:
                raise HcclCleanError(f"duplicate rank_id: {rank}")
            local_device_ids.add(device_id)
            seen_device_ips.add(device_ip)
            seen_ranks.add(rank)
            indexed_devices.append(
                (
                    rank,
                    official_device,
                )
            )

        indexed_devices.sort(key=lambda item: item[0])
        local_ranks = [rank for rank, _ in indexed_devices]
        official_server: dict[str, object] = {
            "server_id": server_id,
            "device": [device for _, device in indexed_devices],
        }
        if host_ip is not None:
            official_server["host_ip"] = host_ip
        indexed_servers.append((local_ranks[0], pod_name, official_server))

    ordered_ranks = sorted(seen_ranks)
    rank_offset = ordered_ranks[0]
    normalized_by_source_rank = {
        source_rank: normalized_rank
        for normalized_rank, source_rank in enumerate(ordered_ranks)
    }

    indexed_servers.sort(key=lambda item: item[0])
    official_servers = [server for _, _, server in indexed_servers]
    for server in official_servers:
        devices = server["device"]
        assert isinstance(devices, list)
        for device in devices:
            assert isinstance(device, dict)
            old_rank = _parse_rank(device["rank_id"], "rank_id")
            device["rank_id"] = str(normalized_by_source_rank[old_rank])

    normalized_ranks = sorted(
        int(device["rank_id"])
        for server in official_servers
        for device in server["device"]
    )
    if normalized_ranks != list(range(len(normalized_ranks))):
        raise HcclCleanError(
            "internal error: cleaned ranks are not continuous from zero"
        )

    official_hccl: dict[str, object] = {
        "status": "completed",
        "version": RANKTABLE_VERSION,
        "server_count": str(len(official_servers)),
        "server_list": official_servers,
    }

    return CleanResult(
        hccl=official_hccl,
        rank_offset=rank_offset,
        server_count=len(indexed_servers),
        world_size=len(normalized_ranks),
        pod_names=tuple(pod_name for _, pod_name, _ in indexed_servers),
    )


def hccl_json_text(hccl: dict[str, object]) -> str:
    return json.dumps(hccl, ensure_ascii=False, indent=2, sort_keys=False) + "\n"


def atomic_write(path: Path, text: str) -> None:
    """Write a derived local file without ever modifying the source file."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    temporary.replace(path)
