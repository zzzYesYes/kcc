#!/usr/bin/env python3
"""Discover Ray NPU worker device IPs and test their connectivity."""

from __future__ import annotations

import argparse
from dataclasses import asdict, dataclass
import ipaddress
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import sys
from typing import Any, Sequence


DEFAULT_HCCN_TOOL = "/usr/local/Ascend/driver/tools/hccn_tool"
PACKET_LOSS_SUCCESS = re.compile(
    r"(?:^|\s)0(?:\.0+)?%\s+packet loss", re.IGNORECASE
)
IPADDR_PATTERN = re.compile(
    r"^\s*ipaddr\s*:\s*([^\s]+)", re.IGNORECASE | re.MULTILINE
)
NETMASK_PATTERN = re.compile(
    r"^\s*netmask\s*:\s*([^\s]+)", re.IGNORECASE | re.MULTILINE
)
GATEWAY_PATTERN = re.compile(
    r"^\s*(?:default\s+)?gateway(?:\s+(?:address|addr))?\s*[:=]\s*([^\s]+)",
    re.IGNORECASE | re.MULTILINE,
)
IP_TOKEN_PATTERN = re.compile(r"[0-9A-Fa-f:.%]+")
TLS_SWITCH_PATTERNS = (
    re.compile(r"tls\s+switch\s*\[\s*([01])\s*\]", re.IGNORECASE),
    re.compile(r"tls\s+switch\s*[:=]\s*([01])", re.IGNORECASE),
)
LLDP_IFNAME_PATTERN = re.compile(
    r"^\s*Ifname\s*:\s*(.+?)\s*$", re.IGNORECASE | re.MULTILINE
)
LLDP_CHASSIS_MAC_PATTERN = re.compile(
    r"^\s*MAC\s*:\s*([^\s]+)", re.IGNORECASE | re.MULTILINE
)
LLDP_VLAN_PATTERN = re.compile(
    r"^\s*(?:PVID|Port\s+VLAN\s+ID|VLAN\s+ID)\s*[:=]\s*(\d+)\s*$",
    re.IGNORECASE | re.MULTILINE,
)
DAVINCI_DEVICE_PATTERN = re.compile(r"^davinci(\d+)$")


class HccnCheckError(ValueError):
    """The worker inventory or hccn_tool result is unsafe to use."""


@dataclass(frozen=True)
class DeviceAddress:
    device_id: int
    device_ip: str
    netmask: str


@dataclass(frozen=True)
class WorkerInfo:
    node_id: str
    pod: str
    ray_node_ip: str
    resource_count: int
    device_id_source: str
    devices: tuple[DeviceAddress, ...]


@dataclass(frozen=True)
class PingTarget:
    local_device_id: int
    local_device_ip: str
    local_netmask: str
    peer_pod: str
    peer_ray_node_ip: str
    peer_device_id: int
    peer_device_ip: str
    peer_netmask: str


def _nonnegative_int(value: str, field: str) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise HccnCheckError(f"{field} must be an integer: {value!r}") from error
    if parsed < 0:
        raise HccnCheckError(f"{field} must be non-negative: {parsed}")
    return parsed


def parse_device_ids(value: str) -> tuple[int, ...]:
    """Parse values such as ``0-3,6,7`` into unique sorted device IDs."""
    device_ids: set[int] = set()
    for raw_part in value.split(","):
        part = raw_part.strip()
        if not part:
            continue
        if "-" not in part:
            device_ids.add(_nonnegative_int(part, "device ID"))
            continue
        bounds = part.split("-")
        if len(bounds) != 2:
            raise HccnCheckError(f"invalid device range: {part!r}")
        start = _nonnegative_int(bounds[0].strip(), "range start")
        end = _nonnegative_int(bounds[1].strip(), "range end")
        if start > end:
            raise HccnCheckError(f"device range is reversed: {part!r}")
        device_ids.update(range(start, end + 1))
    if not device_ids:
        raise HccnCheckError("device ID list is empty")
    return tuple(sorted(device_ids))


def detect_device_ids(explicit: str | None) -> tuple[tuple[int, ...], str]:
    if explicit:
        return parse_device_ids(explicit), "command-line"

    device_nodes: list[int] = []
    for path in Path("/dev").glob("davinci*"):
        match = DAVINCI_DEVICE_PATTERN.fullmatch(path.name)
        if match:
            device_nodes.append(int(match.group(1)))
    if device_nodes:
        return tuple(sorted(set(device_nodes))), "/dev/davinci*"

    for variable in ("ASCEND_RT_VISIBLE_DEVICES", "ASCEND_VISIBLE_DEVICES"):
        value = os.environ.get(variable)
        if value:
            return parse_device_ids(value), variable
    raise HccnCheckError(
        "cannot discover device IDs; mount /dev/davinci* or pass --device-ids"
    )


def _parse_ip(value: str, field: str, *, allow_unspecified: bool = False) -> str:
    try:
        address = ipaddress.ip_address(value)
    except ValueError as error:
        raise HccnCheckError(
            f"hccn_tool returned an invalid {field}: {value}"
        ) from error
    if address.is_unspecified and not allow_unspecified:
        raise HccnCheckError(f"hccn_tool returned an unspecified {field}: {value}")
    return str(address)


def _interface_from_mask(
    address: ipaddress.IPv4Address | ipaddress.IPv6Address,
    raw_mask: str,
) -> ipaddress.IPv4Interface | ipaddress.IPv6Interface:
    mask = raw_mask.removeprefix("/")
    try:
        if mask.isdecimal():
            prefix = int(mask)
            if not 0 <= prefix <= address.max_prefixlen:
                raise ValueError("prefix is outside the address-family range")
        elif isinstance(address, ipaddress.IPv4Address):
            return ipaddress.IPv4Interface(f"{address}/{mask}")
        else:
            mask_value = int(ipaddress.IPv6Address(mask))
            bits = f"{mask_value:0128b}"
            if "01" in bits:
                raise ValueError("IPv6 netmask is not contiguous")
            prefix = bits.find("0")
            if prefix < 0:
                prefix = 128
        return ipaddress.ip_interface(f"{address}/{prefix}")
    except ValueError as error:
        raise HccnCheckError(
            f"hccn_tool returned an invalid or non-contiguous netmask: {raw_mask}"
        ) from error


def parse_hccn_device_network(output: str) -> tuple[str, str]:
    """Parse an IPv4/IPv6 Device address and a contiguous netmask/prefix."""
    ip_match = IPADDR_PATTERN.search(output)
    if not ip_match:
        raise HccnCheckError("hccn_tool -ip output does not contain ipaddr")
    mask_match = NETMASK_PATTERN.search(output)
    if not mask_match:
        raise HccnCheckError("hccn_tool -ip output does not contain netmask")

    device_ip = _parse_ip(ip_match.group(1), "device IP")
    address = ipaddress.ip_address(device_ip)
    interface = _interface_from_mask(address, mask_match.group(1))
    if isinstance(interface, ipaddress.IPv4Interface) and (
        interface.network.prefixlen <= 30
        and interface.ip
        in {
            interface.network.network_address,
            interface.network.broadcast_address,
        }
    ):
        raise HccnCheckError(
            f"device IP {device_ip} is a network or broadcast address for "
            f"{mask_match.group(1)}"
        )
    normalized_mask = (
        str(interface.netmask)
        if isinstance(interface, ipaddress.IPv4Interface)
        else str(interface.network.prefixlen)
    )
    return str(interface.ip), normalized_mask


def parse_hccn_device_ip(output: str) -> str:
    """Compatibility wrapper for callers that only need the Device IP."""
    return parse_hccn_device_network(output)[0]


def parse_hccn_gateway(output: str) -> str:
    match = GATEWAY_PATTERN.search(output)
    if match:
        return _parse_ip(match.group(1), "gateway", allow_unspecified=True)
    candidates: list[str] = []
    for value in IP_TOKEN_PATTERN.findall(output):
        try:
            parsed = _parse_ip(value, "gateway", allow_unspecified=True)
        except HccnCheckError:
            continue
        if parsed not in candidates:
            candidates.append(parsed)
    if len(candidates) == 1:
        return candidates[0]
    raise HccnCheckError("hccn_tool -gateway output does not contain one gateway")


def parse_hccn_tls_switch(output: str) -> int:
    for pattern in TLS_SWITCH_PATTERNS:
        match = pattern.search(output)
        if match:
            return int(match.group(1))
    raise HccnCheckError("hccn_tool -tls output does not contain TLS switch")


def parse_hccn_lldp(output: str) -> dict[str, Any]:
    """Extract only stable LLDP fields; VLAN may not be advertised by a switch."""
    ifnames = tuple(dict.fromkeys(LLDP_IFNAME_PATTERN.findall(output)))
    chassis_macs = tuple(dict.fromkeys(LLDP_CHASSIS_MAC_PATTERN.findall(output)))
    vlan_ids = tuple(
        dict.fromkeys(int(value) for value in LLDP_VLAN_PATTERN.findall(output))
    )
    if not ifnames and not chassis_macs and not vlan_ids:
        raise HccnCheckError(
            "hccn_tool -lldp output does not contain a recognizable switch neighbor"
        )
    return {
        "ifnames": list(ifnames),
        "chassis_macs": list(chassis_macs),
        "vlan_ids": list(vlan_ids),
    }


def ping_succeeded(returncode: int, output: str) -> bool:
    return returncode == 0 and PACKET_LOSS_SUCCESS.search(output) is not None


def run_command(command: list[str], timeout: int) -> tuple[int, str]:
    try:
        completed = subprocess.run(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as error:
        captured = error.stdout or ""
        if isinstance(captured, bytes):
            captured = captured.decode(errors="replace")
        return 124, f"{captured}\ncommand timed out after {timeout}s".strip()
    return completed.returncode, completed.stdout


def require_hccn_tool(hccn_tool: str) -> None:
    path = Path(hccn_tool)
    if not path.is_file() or not os.access(path, os.X_OK):
        raise HccnCheckError(f"hccn_tool is missing or not executable: {hccn_tool}")


def discover_current_worker(
    node_id: str,
    resource_count: int,
    hccn_tool: str,
    timeout: int,
    explicit_device_ids: str | None,
) -> dict[str, Any]:
    """Query local device IDs and IPs without using a rank table."""
    require_hccn_tool(hccn_tool)
    device_ids, source = detect_device_ids(explicit_device_ids)
    if len(device_ids) != resource_count:
        raise HccnCheckError(
            f"Ray advertises {resource_count} accelerator resources, but {source} found "
            f"{len(device_ids)} device IDs: {list(device_ids)}"
        )

    devices: list[DeviceAddress] = []
    seen_ips: set[str] = set()
    for device_id in device_ids:
        command = [hccn_tool, "-i", str(device_id), "-ip", "-g"]
        returncode, output = run_command(command, timeout)
        if returncode != 0:
            raise HccnCheckError(
                f"device {device_id} IP query failed with exit {returncode}: {output}"
            )
        device_ip, netmask = parse_hccn_device_network(output)
        if device_ip in seen_ips:
            raise HccnCheckError(f"worker returned duplicate device IP: {device_ip}")
        seen_ips.add(device_ip)
        devices.append(DeviceAddress(device_id, device_ip, netmask))

    pod = (
        os.environ.get("POD_NAME")
        or os.environ.get("HOSTNAME")
        or socket.gethostname()
    )
    try:
        ray_node_ip = socket.gethostbyname(socket.gethostname())
    except OSError:
        ray_node_ip = "unknown"
    return asdict(
        WorkerInfo(
            node_id=node_id,
            pod=pod,
            ray_node_ip=ray_node_ip,
            resource_count=resource_count,
            device_id_source=source,
            devices=tuple(devices),
        )
    )


def worker_from_dict(value: dict[str, Any]) -> WorkerInfo:
    return WorkerInfo(
        node_id=str(value["node_id"]),
        pod=str(value["pod"]),
        ray_node_ip=str(value["ray_node_ip"]),
        resource_count=int(value["resource_count"]),
        device_id_source=str(value["device_id_source"]),
        devices=tuple(
            DeviceAddress(
                int(device["device_id"]),
                str(device["device_ip"]),
                str(device["netmask"]),
            )
            for device in value["devices"]
        ),
    )


def validate_workers(workers: tuple[WorkerInfo, ...]) -> None:
    if not workers:
        raise HccnCheckError("at least one accelerator worker is required")
    if len({worker.node_id for worker in workers}) != len(workers):
        raise HccnCheckError("duplicate Ray node ID in worker inventory")
    if len({worker.pod for worker in workers}) != len(workers):
        raise HccnCheckError("duplicate Pod name in worker inventory")
    all_ips = [device.device_ip for worker in workers for device in worker.devices]
    if len(set(all_ips)) != len(all_ips):
        raise HccnCheckError("duplicate device IP across workers")


def build_ping_plans(
    workers: tuple[WorkerInfo, ...], matrix: str
) -> dict[str, tuple[PingTarget, ...]]:
    """Build directed checks for matching device planes or all device pairs."""
    validate_workers(workers)
    if matrix not in {"same-device", "all"}:
        raise HccnCheckError(f"unsupported matrix: {matrix}")

    plans: dict[str, tuple[PingTarget, ...]] = {}
    for local in workers:
        targets: list[PingTarget] = []
        local_ids = {device.device_id for device in local.devices}
        for peer in workers:
            if peer.node_id == local.node_id:
                continue
            peer_by_id = {device.device_id: device for device in peer.devices}
            if matrix == "same-device" and set(peer_by_id) != local_ids:
                raise HccnCheckError(
                    f"device planes differ between {local.pod} and {peer.pod}: "
                    f"local={sorted(local_ids)}, peer={sorted(peer_by_id)}; "
                    "use --matrix all only when an all-to-all test is intended"
                )
            for local_device in local.devices:
                peer_devices = (
                    (peer_by_id[local_device.device_id],)
                    if matrix == "same-device"
                    else peer.devices
                )
                for peer_device in peer_devices:
                    targets.append(
                        PingTarget(
                            local_device_id=local_device.device_id,
                            local_device_ip=local_device.device_ip,
                            local_netmask=local_device.netmask,
                            peer_pod=peer.pod,
                            peer_ray_node_ip=peer.ray_node_ip,
                            peer_device_id=peer_device.device_id,
                            peer_device_ip=peer_device.device_ip,
                            peer_netmask=peer_device.netmask,
                        )
                    )
        plans[local.node_id] = tuple(targets)
    return plans


def summarize_inter_worker_checks(
    plans: dict[str, tuple[PingTarget, ...]], *, execute: bool, status: str
) -> dict[str, Any]:
    planned_count = sum(len(targets) for targets in plans.values())
    if planned_count == 0:
        check_status = "NOT_APPLICABLE"
    elif execute:
        check_status = status
    else:
        check_status = "DRY_RUN"
    return {"planned_count": planned_count, "status": check_status}


def ping_current_worker(
    expected_pod: str,
    targets: tuple[PingTarget, ...],
    hccn_tool: str,
    timeout: int,
) -> dict[str, Any]:
    """Run hccn_tool sequentially; it does not support local concurrency."""
    require_hccn_tool(hccn_tool)
    actual_pod = (
        os.environ.get("POD_NAME")
        or os.environ.get("HOSTNAME")
        or socket.gethostname()
    )
    if actual_pod != expected_pod:
        raise HccnCheckError(
            f"task landed on unexpected worker: expected={expected_pod}, "
            f"actual={actual_pod}"
        )

    checks: list[dict[str, Any]] = []
    for target in targets:
        command = [
            hccn_tool,
            "-i",
            str(target.local_device_id),
            "-ping",
            "-g",
            "address",
            target.peer_device_ip,
        ]
        returncode, output = run_command(command, timeout)
        checks.append(
            {
                **asdict(target),
                "command": command,
                "returncode": returncode,
                "passed": ping_succeeded(returncode, output),
                "output": output,
            }
        )
    return {
        "pod": actual_pod,
        "status": "PASS" if all(check["passed"] for check in checks) else "FAIL",
        "checks": checks,
    }


def diagnose_current_worker(
    expected_pod: str,
    device_ids: tuple[int, ...],
    hccn_tool: str,
    timeout: int,
) -> dict[str, Any]:
    """Collect read-only network diagnostics after a ping failure."""
    require_hccn_tool(hccn_tool)
    actual_pod = (
        os.environ.get("POD_NAME")
        or os.environ.get("HOSTNAME")
        or socket.gethostname()
    )
    if actual_pod != expected_pod:
        raise HccnCheckError(
            f"diagnostic task landed on unexpected worker: expected={expected_pod}, "
            f"actual={actual_pod}"
        )

    query_specs = (
        ("ip", ("-ip", "-g")),
        ("gateway", ("-gateway", "-g")),
        ("tls", ("-tls", "-g")),
        ("lldp", ("-lldp", "-g")),
    )
    devices: list[dict[str, Any]] = []
    for device_id in device_ids:
        commands: dict[str, dict[str, Any]] = {}
        for name, arguments in query_specs:
            command = [hccn_tool, "-i", str(device_id), *arguments]
            returncode, output = run_command(command, timeout)
            commands[name] = {
                "command": command,
                "returncode": returncode,
                "output": output,
            }

        parsed: dict[str, Any] = {}
        parse_errors: dict[str, str] = {}
        if commands["ip"]["returncode"] == 0:
            try:
                parsed["device_ip"], parsed["netmask"] = (
                    parse_hccn_device_network(commands["ip"]["output"])
                )
            except HccnCheckError as error:
                parse_errors["ip"] = str(error)
        else:
            parse_errors["ip"] = (
                f"query exited with {commands['ip']['returncode']}"
            )

        if commands["gateway"]["returncode"] == 0:
            try:
                parsed["gateway"] = parse_hccn_gateway(
                    commands["gateway"]["output"]
                )
            except HccnCheckError as error:
                parse_errors["gateway"] = str(error)
        else:
            parse_errors["gateway"] = (
                f"query exited with {commands['gateway']['returncode']}"
            )

        if commands["tls"]["returncode"] == 0:
            try:
                parsed["tls_switch"] = parse_hccn_tls_switch(
                    commands["tls"]["output"]
                )
            except HccnCheckError as error:
                parse_errors["tls"] = str(error)
        else:
            parse_errors["tls"] = (
                f"query exited with {commands['tls']['returncode']}"
            )

        if commands["lldp"]["returncode"] == 0:
            try:
                parsed["lldp"] = parse_hccn_lldp(commands["lldp"]["output"])
            except HccnCheckError as error:
                parse_errors["lldp"] = str(error)
        else:
            parse_errors["lldp"] = (
                f"query exited with {commands['lldp']['returncode']}"
            )

        devices.append(
            {
                "device_id": device_id,
                **parsed,
                "parse_errors": parse_errors,
                "commands": commands,
            }
        )

    return {
        "pod": actual_pod,
        "status": (
            "COLLECTED"
            if all(not device["parse_errors"] for device in devices)
            else "PARTIAL"
        ),
        "read_only": True,
        "devices": devices,
    }


def _diagnostic_status(findings: list[dict[str, Any]]) -> str:
    severities = {finding["severity"] for finding in findings}
    if "ERROR" in severities:
        return "FAIL"
    if "INCOMPLETE" in severities:
        return "INCOMPLETE"
    if "WARNING" in severities:
        return "WARN"
    return "PASS"


def analyze_failure_diagnostics(
    workers: tuple[WorkerInfo, ...],
    diagnostic_results: list[dict[str, Any]],
) -> dict[str, Any]:
    """Compare per-device query results across workers without changing state."""
    expected = {
        (worker.pod, device.device_id): device
        for worker in workers
        for device in worker.devices
    }
    observed = {
        (str(result["pod"]), int(device["device_id"])): device
        for result in diagnostic_results
        for device in result.get("devices", [])
    }

    ip_findings: list[dict[str, Any]] = []
    gateway_findings: list[dict[str, Any]] = []
    tls_findings: list[dict[str, Any]] = []
    vlan_findings: list[dict[str, Any]] = []
    interfaces: dict[
        tuple[str, int], ipaddress.IPv4Interface | ipaddress.IPv6Interface
    ] = {}
    gateways: dict[tuple[str, int], str | None] = {}
    tls_switches: dict[tuple[str, int], int] = {}
    lldp_values: dict[tuple[str, int], dict[str, Any]] = {}

    for key, expected_device in expected.items():
        pod, device_id = key
        device = observed.get(key)
        context = {"pod": pod, "device_id": device_id}
        if device is None:
            message = "no diagnostic result was returned for this Device"
            for findings in (
                ip_findings,
                gateway_findings,
                tls_findings,
                vlan_findings,
            ):
                findings.append(
                    {"severity": "INCOMPLETE", "message": message, **context}
                )
            continue

        device_ip = device.get("device_ip")
        netmask = device.get("netmask")
        if not device_ip or not netmask:
            ip_findings.append(
                {
                    "severity": "INCOMPLETE",
                    "message": device.get("parse_errors", {}).get(
                        "ip", "IP/netmask could not be parsed"
                    ),
                    **context,
                }
            )
        else:
            try:
                interface = ipaddress.ip_interface(f"{device_ip}/{netmask}")
                interfaces[key] = interface
            except ValueError as error:
                ip_findings.append(
                    {"severity": "ERROR", "message": str(error), **context}
                )
            if device_ip != expected_device.device_ip or netmask != expected_device.netmask:
                ip_findings.append(
                    {
                        "severity": "ERROR",
                        "message": (
                            "IP configuration changed between discovery and failure "
                            f"diagnosis: discovered={expected_device.device_ip}/"
                            f"{expected_device.netmask}, current={device_ip}/{netmask}"
                        ),
                        **context,
                    }
                )

        gateway = device.get("gateway")
        if gateway is None:
            gateway_findings.append(
                {
                    "severity": "INCOMPLETE",
                    "message": device.get("parse_errors", {}).get(
                        "gateway", "gateway could not be parsed"
                    ),
                    **context,
                }
            )
        else:
            try:
                gateway_address = ipaddress.ip_address(str(gateway))
            except ValueError as error:
                gateway_findings.append(
                    {
                        "severity": "ERROR",
                        "message": f"gateway is invalid: {error}",
                        **context,
                    }
                )
            else:
                normalized_gateway = (
                    None if gateway_address.is_unspecified else str(gateway_address)
                )
                gateways[key] = normalized_gateway
                interface = interfaces.get(key)
                if normalized_gateway is not None and interface is not None:
                    if gateway_address.version != interface.version:
                        gateway_findings.append(
                            {
                                "severity": "ERROR",
                                "message": (
                                    f"gateway {normalized_gateway} and Device network "
                                    f"{interface.network} use different address families"
                                ),
                                **context,
                            }
                        )
                    elif gateway_address not in interface.network:
                        gateway_findings.append(
                            {
                                "severity": "ERROR",
                                "message": (
                                    f"gateway {normalized_gateway} is outside Device "
                                    f"network {interface.network}"
                                ),
                                **context,
                            }
                        )

        tls_switch = device.get("tls_switch")
        if tls_switch not in {0, 1}:
            tls_findings.append(
                {
                    "severity": "INCOMPLETE",
                    "message": device.get("parse_errors", {}).get(
                        "tls", "TLS switch could not be parsed"
                    ),
                    **context,
                }
            )
        else:
            tls_switches[key] = int(tls_switch)

        lldp = device.get("lldp")
        if not isinstance(lldp, dict):
            vlan_findings.append(
                {
                    "severity": "INCOMPLETE",
                    "message": device.get("parse_errors", {}).get(
                        "lldp", "LLDP switch neighbor could not be parsed"
                    ),
                    **context,
                }
            )
        else:
            lldp_values[key] = lldp

    device_ids = sorted({device_id for _, device_id in expected})
    for device_id in device_ids:
        plane_keys = sorted(key for key in expected if key[1] == device_id)
        plane_interfaces = [interfaces[key] for key in plane_keys if key in interfaces]
        networks = {str(interface.network) for interface in plane_interfaces}
        if len(networks) > 1:
            ip_findings.append(
                {
                    "severity": "WARNING",
                    "message": (
                        f"Device plane {device_id} spans multiple subnets {sorted(networks)}; "
                        "confirm that routed RoCE is intentional"
                    ),
                    "device_id": device_id,
                }
            )

        plane_gateways = {
            gateways[key] for key in plane_keys if key in gateways
        }
        if len(plane_gateways) > 1:
            gateway_findings.append(
                {
                    "severity": "ERROR",
                    "message": (
                        f"Device plane {device_id} has inconsistent gateways: "
                        f"{sorted(str(value) for value in plane_gateways)}"
                    ),
                    "device_id": device_id,
                }
            )
        if len(networks) > 1 and None in plane_gateways:
            gateway_findings.append(
                {
                    "severity": "ERROR",
                    "message": (
                        f"Device plane {device_id} crosses subnets but at least one "
                        "Device has no configured gateway"
                    ),
                    "device_id": device_id,
                }
            )

        advertised_vlans: dict[tuple[str, int], tuple[int, ...]] = {}
        for key in plane_keys:
            lldp = lldp_values.get(key)
            if lldp is not None:
                advertised_vlans[key] = tuple(int(item) for item in lldp["vlan_ids"])
        complete_vlan_ids = {
            values[0]
            for values in advertised_vlans.values()
            if len(values) == 1
        }
        if len(complete_vlan_ids) > 1:
            vlan_findings.append(
                {
                    "severity": "ERROR",
                    "message": (
                        f"Device plane {device_id} advertises different LLDP VLAN IDs: "
                        f"{sorted(complete_vlan_ids)}"
                    ),
                    "device_id": device_id,
                }
            )

    gateways_by_network: dict[str, set[str | None]] = {}
    for key, interface in interfaces.items():
        if key in gateways:
            gateways_by_network.setdefault(str(interface.network), set()).add(
                gateways[key]
            )
    for network, network_gateways in sorted(gateways_by_network.items()):
        if len(network_gateways) > 1:
            gateway_findings.append(
                {
                    "severity": "ERROR",
                    "message": (
                        f"Devices in subnet {network} have inconsistent gateways: "
                        f"{sorted(str(value) for value in network_gateways)}"
                    ),
                    "network": network,
                }
            )

    if tls_switches:
        unique_tls = set(tls_switches.values())
        if len(unique_tls) > 1:
            tls_findings.append(
                {
                    "severity": "ERROR",
                    "message": "NPU TLS switches are inconsistent across workers/Devices",
                    "values": {
                        f"{pod}/device-{device_id}": value
                        for (pod, device_id), value in sorted(tls_switches.items())
                    },
                }
            )
        elif unique_tls == {1}:
            tls_findings.append(
                {
                    "severity": "WARNING",
                    "message": (
                        "NPU TLS is consistently enabled; disabled (0) is the usual "
                        "deployment setting, so verify certificates and system time"
                    ),
                }
            )

    all_vlan_advertised = bool(expected) and all(
        key in lldp_values and len(lldp_values[key]["vlan_ids"]) == 1
        for key in expected
    )
    if not all_vlan_advertised:
        switch_ports = [
            {
                "pod": pod,
                "device_id": device_id,
                "ifnames": lldp.get("ifnames", []),
                "chassis_macs": lldp.get("chassis_macs", []),
            }
            for (pod, device_id), lldp in sorted(lldp_values.items())
        ]
        vlan_findings.append(
            {
                "severity": "INCOMPLETE",
                "message": (
                    "LLDP does not expose a VLAN ID for every NPU port. hccn_tool can "
                    "identify adjacent switch ports but cannot prove their switch VLAN "
                    "membership; verify the listed ports on the switch"
                ),
                "switch_ports": switch_ports,
            }
        )
    elif not any(item["severity"] == "ERROR" for item in vlan_findings):
        advertised = {
            f"{pod}/device-{device_id}": lldp["vlan_ids"]
            for (pod, device_id), lldp in sorted(lldp_values.items())
        }
        vlan_findings.append(
            {
                "severity": "INCOMPLETE",
                "message": (
                    "LLDP-advertised VLAN IDs are consistent, but this does not prove "
                    "the complete tagged/untagged VLAN membership on the switch; "
                    "confirm the switch port configuration"
                ),
                "advertised_vlan_ids": advertised,
            }
        )

    checks = {
        "ip_configuration": {
            "status": _diagnostic_status(ip_findings),
            "findings": ip_findings,
        },
        "gateway_consistency": {
            "status": _diagnostic_status(gateway_findings),
            "findings": gateway_findings,
        },
        "switch_vlan": {
            "status": (
                "FAIL"
                if any(item["severity"] == "ERROR" for item in vlan_findings)
                else "MANUAL_CHECK_REQUIRED"
            ),
            "findings": vlan_findings,
        },
        "tls_consistency": {
            "status": _diagnostic_status(tls_findings),
            "findings": tls_findings,
            "expected_usual_value": 0,
        },
    }
    statuses = {item["status"] for item in checks.values()}
    if "FAIL" in statuses:
        status = "ISSUES_FOUND"
    elif statuses & {"INCOMPLETE", "MANUAL_CHECK_REQUIRED"}:
        status = "INCOMPLETE"
    elif "WARN" in statuses:
        status = "WARN"
    else:
        status = "NO_OBVIOUS_CONFIGURATION_MISMATCH"

    return {
        "status": status,
        "trigger": "one_or_more_hccn_ping_checks_failed",
        "read_only": True,
        "checks": checks,
        "workers": diagnostic_results,
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover Ray NPU worker IPs and run hccn_tool connectivity checks."
    )
    parser.add_argument("--hccn-tool", default=DEFAULT_HCCN_TOOL)
    parser.add_argument("--address", default="auto", help="Ray address")
    parser.add_argument("--resource", default="NPU")
    parser.add_argument("--expected-workers", type=int, default=0)
    parser.add_argument(
        "--device-ids",
        help="IDs on every worker, for example 0-3 or 0,2,4,6; default: auto",
    )
    parser.add_argument(
        "--matrix",
        choices=("same-device", "all"),
        default="same-device",
        help="matching device IDs (default), or every cross-worker device pair",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument(
        "--execute",
        action="store_true",
        help="run ping; without this flag only discover IPs and print the plan",
    )
    parser.add_argument("--output", help="optional JSON result path on the Ray driver")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.timeout < 1:
        print("--timeout must be at least 1 second", file=sys.stderr)
        return 2
    if args.expected_workers < 0:
        print("--expected-workers must be non-negative", file=sys.stderr)
        return 2
    if args.device_ids:
        try:
            parse_device_ids(args.device_ids)
        except HccnCheckError as error:
            print(str(error), file=sys.stderr)
            return 2

    try:
        import ray
        from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy
    except ImportError as error:
        print(f"Ray is required to run the cluster driver: {error}", file=sys.stderr)
        return 2

    ray.init(address=args.address, log_to_driver=False)
    resource_nodes: list[tuple[str, int]] = []
    for node in ray.nodes():
        resources = node.get("Resources", {})
        amount = resources.get(args.resource, 0)
        if node.get("Alive") and amount:
            if int(amount) != amount or amount < 1:
                print(
                    f"node {node['NodeID']} has invalid {args.resource}={amount}",
                    file=sys.stderr,
                )
                return 2
            resource_nodes.append((str(node["NodeID"]), int(amount)))
    resource_nodes.sort()
    if not resource_nodes:
        print(f"no live Ray nodes advertise {args.resource!r}", file=sys.stderr)
        return 2
    if args.expected_workers and len(resource_nodes) != args.expected_workers:
        print(
            f"expected {args.expected_workers} resource workers, "
            f"found {len(resource_nodes)}",
            file=sys.stderr,
        )
        return 2

    discover = ray.remote(num_cpus=0.1)(discover_current_worker)
    discovery_futures = [
        discover.options(
            resources={args.resource: count},
            scheduling_strategy=NodeAffinitySchedulingStrategy(node_id, soft=False),
        ).remote(node_id, count, args.hccn_tool, args.timeout, args.device_ids)
        for node_id, count in resource_nodes
    ]
    try:
        workers = tuple(worker_from_dict(item) for item in ray.get(discovery_futures))
        workers = tuple(sorted(workers, key=lambda item: (item.pod, item.node_id)))
        plans = build_ping_plans(workers, args.matrix)
    except Exception as error:
        print(f"worker IP discovery failed: {error}", file=sys.stderr)
        return 1

    results: list[dict[str, Any]]
    failure_diagnostics: dict[str, Any] | None = None
    if args.execute:
        ping = ray.remote(num_cpus=0.1)(ping_current_worker)
        ping_futures = [
            ping.options(
                resources={args.resource: worker.resource_count},
                scheduling_strategy=NodeAffinitySchedulingStrategy(
                    worker.node_id, soft=False
                ),
            ).remote(worker.pod, plans[worker.node_id], args.hccn_tool, args.timeout)
            for worker in workers
        ]
        try:
            results = list(ray.get(ping_futures))
        except Exception as error:
            print(f"worker ping failed: {error}", file=sys.stderr)
            return 1
        results.sort(key=lambda item: item["pod"])
        status = (
            "PASS" if all(item["status"] == "PASS" for item in results) else "FAIL"
        )
        if status == "FAIL":
            diagnose = ray.remote(num_cpus=0.1)(diagnose_current_worker)
            diagnostic_futures = [
                diagnose.options(
                    resources={args.resource: worker.resource_count},
                    scheduling_strategy=NodeAffinitySchedulingStrategy(
                        worker.node_id, soft=False
                    ),
                ).remote(
                    worker.pod,
                    tuple(device.device_id for device in worker.devices),
                    args.hccn_tool,
                    args.timeout,
                )
                for worker in workers
            ]
            try:
                diagnostic_results = list(ray.get(diagnostic_futures))
                diagnostic_results.sort(key=lambda item: item["pod"])
                failure_diagnostics = analyze_failure_diagnostics(
                    workers, diagnostic_results
                )
            except Exception as error:
                failure_diagnostics = {
                    "status": "UNAVAILABLE",
                    "trigger": "one_or_more_hccn_ping_checks_failed",
                    "read_only": True,
                    "error": str(error),
                }
    else:
        status = "DRY_RUN"
        results = [
            {
                "pod": worker.pod,
                "status": "DRY_RUN",
                "checks": [
                    {
                        **asdict(target),
                        "command": [
                            args.hccn_tool,
                            "-i",
                            str(target.local_device_id),
                            "-ping",
                            "-g",
                            "address",
                            target.peer_device_ip,
                        ],
                    }
                    for target in plans[worker.node_id]
                ],
            }
            for worker in workers
        ]

    summary = {
        "status": status,
        "mode": "execute" if args.execute else "dry-run",
        "matrix": args.matrix,
        "worker_count": len(workers),
        "workers": [asdict(worker) for worker in workers],
        "results": results,
        "inter_worker_checks": summarize_inter_worker_checks(
            plans, execute=args.execute, status=status
        ),
    }
    if failure_diagnostics is not None:
        summary["failure_diagnostics"] = failure_diagnostics
    rendered = json.dumps(summary, ensure_ascii=False, indent=2) + "\n"
    print(rendered, end="")
    if args.output:
        Path(args.output).write_text(rendered, encoding="utf-8")
    return 0 if status in {"PASS", "DRY_RUN"} else 1


if __name__ == "__main__":
    raise SystemExit(main())
