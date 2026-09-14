#!/usr/bin/env python3
"""Small, read-only environment checks run before Ray or training."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
import shlex
import subprocess
import sys
from typing import Any, Mapping, Sequence

import cluster_config


DEFAULT_NPU_RESOURCE = "huawei.com/Ascend910"
DEFAULT_EXPORTER_APP = "npu-exporter"
DEFAULT_EXPORTER_PORT = 8082
TERMINAL_PHASES = {"Succeeded", "Failed"}
LABEL_PATTERN = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="([^"]*)"')


class CheckError(RuntimeError):
    pass


def kubectl_json(
    kubectl_command: Sequence[str],
    kubeconfig: Path | None,
    resource: str,
) -> dict[str, Any]:
    command = list(kubectl_command)
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    command.extend(("get", resource))
    if resource == "pods":
        command.append("-A")
    command.extend(("-o", "json"))
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise CheckError(f"cannot execute kubectl: {error}") from error
    if result.returncode != 0:
        raise CheckError(
            f"kubectl get {resource} failed: {result.stderr.strip()}"
        )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise CheckError(f"kubectl returned invalid JSON: {error}") from error


def kubectl_raw(
    kubectl_command: Sequence[str],
    kubeconfig: Path | None,
    path: str,
) -> str:
    command = list(kubectl_command)
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    command.extend(("get", "--raw", path))
    try:
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise CheckError(f"cannot execute kubectl: {error}") from error
    if result.returncode != 0:
        raise CheckError(f"cannot read NPU exporter: {result.stderr.strip()}")
    return result.stdout


def npu_count(container: Mapping[str, Any], resource_name: str) -> int:
    resources = container.get("resources", {})
    requests = resources.get("requests", {})
    limits = resources.get("limits", {})
    raw_value = requests.get(resource_name, limits.get(resource_name, 0))
    try:
        return int(raw_value)
    except (TypeError, ValueError) as error:
        raise CheckError(
            f"invalid {resource_name} value in Pod resources: {raw_value!r}"
        ) from error


def pod_npu_count(pod: Mapping[str, Any], resource_name: str) -> int:
    spec = pod.get("spec", {})
    application = sum(
        npu_count(container, resource_name)
        for container in spec.get("containers", [])
    )
    init = max(
        (
            npu_count(container, resource_name)
            for container in spec.get("initContainers", [])
        ),
        default=0,
    )
    return max(application, init)


def check_kubernetes_npu_occupancy(
    nodes_document: Mapping[str, Any],
    pods_document: Mapping[str, Any],
    targets: Sequence[str],
    resource_name: str = DEFAULT_NPU_RESOURCE,
    expected_devices_per_node: int | None = None,
) -> dict[str, Any]:
    """Module 1: read node information and stop on active NPU ownership."""

    nodes_by_target: dict[str, Mapping[str, Any]] = {}
    for node in nodes_document.get("items", []):
        metadata = node.get("metadata", {})
        addresses = node.get("status", {}).get("addresses", [])
        names = {metadata.get("name")}
        names.update(
            address.get("address")
            for address in addresses
            if address.get("type") == "InternalIP"
        )
        for target in targets:
            if target in names:
                nodes_by_target[target] = node

    node_reports: list[dict[str, Any]] = []
    blocked_reasons: list[str] = []
    target_node_names: set[str] = set()
    for target in targets:
        node = nodes_by_target.get(target)
        if node is None:
            blocked_reasons.append(f"target node not found: {target}")
            continue
        metadata = node.get("metadata", {})
        status = node.get("status", {})
        name = metadata.get("name", target)
        target_node_names.add(name)
        conditions = {
            condition.get("type"): condition.get("status")
            for condition in status.get("conditions", [])
        }
        internal_ip = next(
            (
                address.get("address")
                for address in status.get("addresses", [])
                if address.get("type") == "InternalIP"
            ),
            "unknown",
        )
        capacity = int(status.get("capacity", {}).get(resource_name, 0))
        allocatable = int(status.get("allocatable", {}).get(resource_name, 0))
        ready = conditions.get("Ready") == "True"
        node_reports.append(
            {
                "name": name,
                "target": target,
                "ip": internal_ip,
                "ready": ready,
                "chip": metadata.get("labels", {}).get(
                    "node.kubernetes.io/npu.chip.name"
                ),
                "capacity": capacity,
                "allocatable": allocatable,
            }
        )
        if not ready:
            blocked_reasons.append(f"node is not Ready: {name}")
        if expected_devices_per_node is not None and allocatable < expected_devices_per_node:
            blocked_reasons.append(
                f"node has {allocatable} allocatable {resource_name}, "
                f"but the runtime profile requests {expected_devices_per_node}: {name}"
            )
        elif allocatable <= 0:
            blocked_reasons.append(f"node has no allocatable NPU: {name}")

    owners: list[dict[str, Any]] = []
    for pod in pods_document.get("items", []):
        node_name = pod.get("spec", {}).get("nodeName")
        phase = pod.get("status", {}).get("phase", "Unknown")
        if node_name not in target_node_names or phase in TERMINAL_PHASES:
            continue
        count = pod_npu_count(pod, resource_name)
        if count:
            metadata = pod.get("metadata", {})
            owners.append(
                {
                    "namespace": metadata.get("namespace", "default"),
                    "pod": metadata.get("name", "unknown"),
                    "node": node_name,
                    "phase": phase,
                    "npu": count,
                }
            )

    owners.sort(key=lambda item: (item["node"], item["namespace"], item["pod"]))
    return {
        "passed": not blocked_reasons and not owners,
        "nodes": node_reports,
        "owners": owners,
        "reasons": blocked_reasons,
    }


def print_occupancy_result(result: Mapping[str, Any]) -> None:
    print("=== Environment check: Kubernetes NPU occupancy ===")
    for node in result["nodes"]:
        address = node.get("ip") or node.get("target", "unknown")
        print(
            f"{node['name']} ({address}): "
            f"Ready={node['ready']}, chip={node['chip']}, "
            f"NPU={node['allocatable']}/{node['capacity']}"
        )
    for reason in result["reasons"]:
        print(f"WARNING: {reason}")
    if result["owners"]:
        print("WARNING: target node NPU is already occupied:")
        for owner in result["owners"]:
            print(
                f"  {owner['namespace']}/{owner['pod']} "
                f"node={owner['node']} phase={owner['phase']} "
                f"npu={owner['npu']}"
            )
    if result["passed"]:
        print("PASS: no active Kubernetes NPU owner was found.")
    else:
        print("STOP: environment check failed; Ray/training must not start.")


def metric_samples(text: str, metric_name: str) -> list[dict[str, Any]]:
    samples: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not (
            line.startswith(f"{metric_name}{{")
            or line.startswith(f"{metric_name} ")
        ):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        try:
            value = float(fields[1])
        except ValueError:
            continue
        samples.append(
            {
                "labels": dict(LABEL_PATTERN.findall(fields[0])),
                "value": value,
            }
        )
    return samples


def parse_npu_exporter_metrics(text: str) -> dict[str, Any]:
    machine_samples = metric_samples(text, "machine_npu_nums")
    health_samples = metric_samples(text, "npu_chip_info_health_status")
    count_samples = metric_samples(text, "npu_chip_info_process_info_num")
    process_samples = metric_samples(text, "npu_chip_info_process_info")

    visible = int(machine_samples[0]["value"]) if machine_samples else 0
    health = {
        sample["labels"].get("id", "?"): sample["value"] == 1
        for sample in health_samples
    }
    unhealthy = sorted(npu for npu, healthy in health.items() if not healthy)
    occupied = {
        sample["labels"].get("id", "?"): int(sample["value"])
        for sample in count_samples
        if sample["value"] > 0
    }
    processes = []
    for sample in process_samples:
        labels = sample["labels"]
        if not labels.get("process_id"):
            continue
        processes.append(
            {
                "npu": labels.get("id", "?"),
                "pid": labels["process_id"],
                "namespace": labels.get("namespace", ""),
                "pod": labels.get("pod_name", ""),
                "container": labels.get("container_name", ""),
                "memory_mb": int(sample["value"]),
            }
        )
    processes.sort(key=lambda item: (item["npu"], item["pid"]))
    return {
        "visible": visible,
        "health_samples": len(health_samples),
        "healthy": len(health_samples) - len(unhealthy),
        "health": health,
        "unhealthy": unhealthy,
        "process_count": sum(occupied.values()),
        "occupied": occupied,
        "processes": processes,
    }


def find_npu_exporters(
    pods_document: Mapping[str, Any],
    node_names: set[str],
    app_name: str,
) -> dict[str, Mapping[str, Any]]:
    exporters: dict[str, Mapping[str, Any]] = {}
    for pod in pods_document.get("items", []):
        metadata = pod.get("metadata", {})
        node_name = pod.get("spec", {}).get("nodeName")
        labels = metadata.get("labels", {})
        name = metadata.get("name", "")
        if (
            node_name in node_names
            and pod.get("status", {}).get("phase") == "Running"
            and (
                labels.get("app") == app_name
                or name.startswith(f"{app_name}-")
            )
        ):
            exporters[node_name] = pod
    return exporters


def check_actual_npu_state(
    kubectl_command: Sequence[str],
    kubeconfig: Path | None,
    pods_document: Mapping[str, Any],
    node_reports: Sequence[Mapping[str, Any]],
    exporter_app: str = DEFAULT_EXPORTER_APP,
    exporter_port: int = DEFAULT_EXPORTER_PORT,
) -> dict[str, Any]:
    """Module 2: stop when hardware is unhealthy or an NPU process exists."""

    exporters = find_npu_exporters(
        pods_document,
        {str(node["name"]) for node in node_reports},
        exporter_app,
    )
    reports: list[dict[str, Any]] = []
    reasons: list[str] = []

    for node in node_reports:
        node_name = str(node["name"])
        pod = exporters.get(node_name)
        if pod is None:
            reasons.append(f"NPU exporter not found on node: {node_name}")
            continue
        metadata = pod.get("metadata", {})
        namespace = metadata.get("namespace", "default")
        pod_name = metadata.get("name")
        path = (
            f"/api/v1/namespaces/{namespace}/pods/"
            f"{pod_name}:{exporter_port}/proxy/metrics"
        )
        try:
            state = parse_npu_exporter_metrics(
                kubectl_raw(kubectl_command, kubeconfig, path)
            )
        except CheckError as error:
            reasons.append(f"{node_name}: {error}")
            continue

        report = {
            **state,
            "name": node_name,
            "target": node["target"],
            "ip": node.get("ip") or node["target"],
            "expected": node["capacity"],
        }
        reports.append(report)
        if state["visible"] != node["capacity"]:
            reasons.append(
                f"{node_name}: exporter sees {state['visible']} NPU(s), "
                f"Kubernetes reports {node['capacity']}"
            )
        if state["health_samples"] != state["visible"]:
            reasons.append(f"{node_name}: incomplete NPU health information")
        if state["unhealthy"]:
            reasons.append(
                f"{node_name}: unhealthy NPU(s): "
                f"{','.join(state['unhealthy'])}"
            )

    return {
        "passed": (
            not reasons
            and len(reports) == len(node_reports)
            and all(report["process_count"] == 0 for report in reports)
        ),
        "nodes": reports,
        "reasons": reasons,
    }


def print_actual_npu_result(
    result: Mapping[str, Any], *, per_card: bool = False
) -> None:
    print("=== Environment check: actual NPU state ===")
    for node in result["nodes"]:
        address = node.get("ip") or node.get("target", "unknown")
        print(
            f"{node['name']} ({address}): "
            f"visible={node['visible']}, "
            f"healthy={node['healthy']}/{node['visible']}, "
            f"processes={node['process_count']}"
        )
        if per_card:
            for npu in range(node["visible"]):
                npu_id = str(npu)
                process_count = node["occupied"].get(npu_id, 0)
                health = node["health"].get(npu_id)
                if health is True:
                    health_text = "healthy"
                elif health is False:
                    health_text = "unhealthy"
                else:
                    health_text = "unknown"
                state = "occupied" if process_count else "idle"
                print(
                    f"  NPU {npu_id}: health={health_text}, "
                    f"state={state}, processes={process_count}"
                )
        for process in node["processes"]:
            owner = (
                f"{process['namespace']}/{process['pod']}"
                if process["pod"]
                else "host"
            )
            print(
                f"  WARNING: npu={process['npu']} pid={process['pid']} "
                f"owner={owner} container={process['container'] or '-'} "
                f"memory={process['memory_mb']}MB"
            )
        detailed_cards = {process["npu"] for process in node["processes"]}
        for npu, count in node["occupied"].items():
            if npu not in detailed_cards:
                print(f"  WARNING: npu={npu} processes={count}")
    for reason in result["reasons"]:
        print(f"WARNING: {reason}")
    if result["passed"]:
        print("PASS: all NPUs are healthy and no NPU process was found.")
    else:
        print("STOP: actual NPU state is not ready for training.")


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--node",
        action="append",
        help=(
            "target Kubernetes node name or InternalIP; repeat as needed "
            "(defaults to activeNodes + spareNodes from config/cluster.yaml)"
        ),
    )
    parser.add_argument(
        "--kubectl-command",
        help="kubectl command prefix, for example '/usr/local/bin/k3s kubectl'",
    )
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--npu-resource")
    parser.add_argument("--expected-devices-per-node", type=int)
    parser.add_argument("--npu-exporter-app")
    parser.add_argument("--npu-exporter-port", type=int)
    return parser


def apply_config_defaults(args: argparse.Namespace) -> None:
    if (
        args.node is not None
        and args.kubectl_command is not None
        and args.kubeconfig is not None
        and args.npu_resource is not None
        and args.expected_devices_per_node is not None
        and args.npu_exporter_app is not None
        and args.npu_exporter_port is not None
    ):
        return
    defaults = cluster_config.load_cluster_config()
    if args.node is None:
        args.node = list(defaults.all_nodes)
    if args.kubectl_command is None:
        args.kubectl_command = defaults.kubernetes.kubectl_command
    if args.kubeconfig is None:
        args.kubeconfig = defaults.kubernetes.kubeconfig
    if args.npu_resource is None:
        args.npu_resource = defaults.accelerator.resource_name
    if args.expected_devices_per_node is None:
        args.expected_devices_per_node = defaults.accelerator.devices_per_node
    if args.npu_exporter_app is None:
        args.npu_exporter_app = defaults.npu_check.exporter_app
    if args.npu_exporter_port is None:
        args.npu_exporter_port = defaults.npu_check.exporter_port


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        apply_config_defaults(args)
        if not 1 <= args.npu_exporter_port <= 65535:
            raise CheckError("NPU exporter port must be within 1..65535")
        if not 1 <= args.expected_devices_per_node <= 64:
            raise CheckError("expected devices per node must be within 1..64")
        command = shlex.split(args.kubectl_command)
        if not command:
            raise CheckError("kubectl command is empty")
        nodes = kubectl_json(command, args.kubeconfig, "nodes")
        pods = kubectl_json(command, args.kubeconfig, "pods")
        targets = tuple(args.node)
        single_node = len(targets) == 1
        result = check_kubernetes_npu_occupancy(
            nodes,
            pods,
            targets,
            args.npu_resource,
            args.expected_devices_per_node,
        )
        print_occupancy_result(result)
        inspect_single_node = single_node and len(result["nodes"]) == 1
        if not result["passed"] and not inspect_single_node:
            return 1

        actual = check_actual_npu_state(
            command,
            args.kubeconfig,
            pods,
            result["nodes"],
            args.npu_exporter_app,
            args.npu_exporter_port,
        )
        print_actual_npu_result(actual, per_card=single_node)
        return 0 if result["passed"] and actual["passed"] else 1
    except (CheckError, cluster_config.ClusterConfigError) as error:
        print(f"STOP: environment information could not be read: {error}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
