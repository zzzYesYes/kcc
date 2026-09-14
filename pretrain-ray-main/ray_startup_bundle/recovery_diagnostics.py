#!/usr/bin/env python3
"""Fail-closed node diagnosis for whole-world training recovery.

Hardware status and process occupancy are kept separate: occupancy never makes
a node look broken, but the next world is allowed to start only after every
survivor and selected spare is idle.  This uses the same Kubernetes/exporter
signals as the existing environment check.  Single-snapshot diagnosis remains
available; callers can also require bounded, consecutive confirmation.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import sys
import time
from typing import Any, Callable, Mapping, Sequence

try:
    from . import cluster_config
except ImportError:
    import cluster_config

try:  # Support both direct CLI execution and package-style imports in tests.
    from .environment_check import (
        CheckError,
        DEFAULT_EXPORTER_APP,
        DEFAULT_EXPORTER_PORT,
        DEFAULT_NPU_RESOURCE,
        TERMINAL_PHASES,
        kubectl_json,
        kubectl_raw,
        metric_samples,
        parse_npu_exporter_metrics,
        pod_npu_count,
    )
except ImportError:
    from environment_check import (
        CheckError,
        DEFAULT_EXPORTER_APP,
        DEFAULT_EXPORTER_PORT,
        DEFAULT_NPU_RESOURCE,
        TERMINAL_PHASES,
        kubectl_json,
        kubectl_raw,
        metric_samples,
        parse_npu_exporter_metrics,
        pod_npu_count,
    )


SCHEMA_VERSION = 1
NODE_STATUSES = {"healthy", "unhealthy", "unknown"}


def _node_matches(
    nodes_document: Mapping[str, Any], target: str
) -> list[Mapping[str, Any]]:
    matches: list[Mapping[str, Any]] = []
    for node in nodes_document.get("items", []):
        metadata = node.get("metadata", {})
        identities = {metadata.get("name")}
        identities.update(
            address.get("address")
            for address in node.get("status", {}).get("addresses", [])
            if address.get("type") == "InternalIP"
        )
        if target in identities:
            matches.append(node)
    return matches


def _ready_status(node: Mapping[str, Any]) -> str | None:
    for condition in node.get("status", {}).get("conditions", []):
        if condition.get("type") == "Ready":
            value = condition.get("status")
            return str(value) if value is not None else None
    return None


def _resource_value(node: Mapping[str, Any], field: str, resource: str) -> int | None:
    raw_value = node.get("status", {}).get(field, {}).get(resource)
    try:
        return int(raw_value)
    except (TypeError, ValueError):
        return None


def _exporter_candidates(
    pods_document: Mapping[str, Any], node_name: str, exporter_app: str
) -> list[Mapping[str, Any]]:
    candidates: list[Mapping[str, Any]] = []
    for pod in pods_document.get("items", []):
        metadata = pod.get("metadata", {})
        labels = metadata.get("labels", {})
        name = str(metadata.get("name", ""))
        if (
            pod.get("spec", {}).get("nodeName") == node_name
            and pod.get("status", {}).get("phase") == "Running"
            and (
                labels.get("app") == exporter_app
                or name.startswith(f"{exporter_app}-")
            )
        ):
            candidates.append(pod)
    return candidates


def _npu_owners(
    pods_document: Mapping[str, Any], node_name: str, resource_name: str
) -> tuple[list[dict[str, Any]], str | None]:
    owners: list[dict[str, Any]] = []
    try:
        for pod in pods_document.get("items", []):
            if pod.get("spec", {}).get("nodeName") != node_name:
                continue
            phase = pod.get("status", {}).get("phase", "Unknown")
            if phase in TERMINAL_PHASES:
                continue
            count = pod_npu_count(pod, resource_name)
            if count <= 0:
                continue
            metadata = pod.get("metadata", {})
            owners.append(
                {
                    "namespace": metadata.get("namespace", "default"),
                    "pod": metadata.get("name", "unknown"),
                    "phase": phase,
                    "npu": count,
                }
            )
    except CheckError as error:
        return [], str(error)
    owners.sort(key=lambda item: (item["namespace"], item["pod"]))
    return owners, None


def _base_report(target: str, *, role: str) -> dict[str, Any]:
    return {
        "target": target,
        "role": role,
        "nodeName": None,
        "ready": None,
        "capacity": None,
        "allocatable": None,
        "visible": None,
        "healthSamples": None,
        "unhealthy": [],
        "processCount": None,
        "owners": [],
        "idle": None,
        "eligible": False,
        "status": "unknown",
        "reasons": [],
    }


def assess_nodes(
    nodes_document: Mapping[str, Any],
    pods_document: Mapping[str, Any],
    targets: Sequence[str],
    exporter_metrics: Mapping[str, str],
    exporter_errors: Mapping[str, str] | None = None,
    *,
    expected_npus: int = 8,
    role: str = "active",
    npu_resource: str = DEFAULT_NPU_RESOURCE,
    exporter_app: str = DEFAULT_EXPORTER_APP,
) -> list[dict[str, Any]]:
    """Purely assess active nodes or spares from one Kubernetes snapshot.

    ``exporter_metrics`` and ``exporter_errors`` are keyed by canonical
    Kubernetes node name.  Active-node hardware status never depends on
    process ownership.  Spare eligibility additionally requires no Kubernetes
    NPU owner and an exporter process count of zero.
    """

    if role not in {"active", "spare"}:
        raise ValueError("role must be 'active' or 'spare'")
    if expected_npus <= 0:
        raise ValueError("expected_npus must be positive")

    errors = exporter_errors or {}
    reports: list[dict[str, Any]] = []
    for target in targets:
        report = _base_report(str(target), role=role)
        matches = _node_matches(nodes_document, str(target))
        if not matches:
            # A previously selected active node disappearing from the API is a
            # usable whole-machine failure signal.  A missing spare is simply
            # unavailable, but has the same hardware status for selection.
            report["status"] = "unhealthy"
            report["ready"] = False
            report["reasons"].append("node not found")
            reports.append(report)
            continue
        if len(matches) != 1:
            report["reasons"].append("target matches multiple Kubernetes nodes")
            reports.append(report)
            continue

        node = matches[0]
        node_name = str(node.get("metadata", {}).get("name", target))
        ready_status = _ready_status(node)
        report.update(
            {
                "nodeName": node_name,
                "ready": ready_status == "True",
                "capacity": _resource_value(node, "capacity", npu_resource),
                "allocatable": _resource_value(node, "allocatable", npu_resource),
            }
        )
        if ready_status != "True":
            report["status"] = "unhealthy"
            report["reasons"].append(
                f"node is not Ready (Ready={ready_status or 'missing'})"
            )
            reports.append(report)
            continue

        candidates = _exporter_candidates(pods_document, node_name, exporter_app)
        if len(candidates) != 1:
            report["reasons"].append(
                f"expected one running NPU exporter, found {len(candidates)}"
            )
            reports.append(report)
            continue
        if node_name in errors:
            report["reasons"].append(f"NPU exporter read failed: {errors[node_name]}")
            reports.append(report)
            continue
        metrics_text = exporter_metrics.get(node_name)
        if metrics_text is None:
            report["reasons"].append("NPU exporter metrics were not collected")
            reports.append(report)
            continue

        machine_samples = metric_samples(metrics_text, "machine_npu_nums")
        health_samples = metric_samples(
            metrics_text, "npu_chip_info_health_status"
        )
        if len(machine_samples) != 1:
            report["reasons"].append(
                "NPU exporter has missing or ambiguous machine_npu_nums"
            )
            reports.append(report)
            continue
        raw_visible = machine_samples[0]["value"]
        if raw_visible < 0 or not float(raw_visible).is_integer():
            report["reasons"].append("NPU exporter returned an invalid visible count")
            reports.append(report)
            continue
        if not health_samples:
            report["reasons"].append("NPU exporter has no health samples")
            reports.append(report)
            continue

        state = parse_npu_exporter_metrics(metrics_text)
        report.update(
            {
                "visible": state["visible"],
                "healthSamples": state["health_samples"],
                "unhealthy": state["unhealthy"],
                "processCount": state["process_count"],
            }
        )
        hardware_reasons: list[str] = []
        if report["capacity"] < expected_npus:
            hardware_reasons.append(
                f"Kubernetes NPU capacity is {report['capacity']}, "
                f"requires at least {expected_npus}"
            )
        if report["allocatable"] < expected_npus:
            hardware_reasons.append(
                f"Kubernetes allocatable NPU is {report['allocatable']}, "
                f"requires at least {expected_npus}"
            )
        if state["visible"] < expected_npus:
            hardware_reasons.append(
                f"exporter sees {state['visible']} NPU(s), "
                f"requires at least {expected_npus}"
            )
        if state["health_samples"] != state["visible"]:
            hardware_reasons.append(
                f"exporter has {state['health_samples']} health sample(s), "
                f"expected {state['visible']} for all visible devices"
            )
        health_ids = [sample["labels"].get("id") for sample in health_samples]
        if any(identifier is None for identifier in health_ids) or len(
            set(health_ids)
        ) != len(health_ids):
            hardware_reasons.append("exporter health sample IDs are incomplete")
        if state["unhealthy"]:
            hardware_reasons.append(
                "unhealthy NPU(s): " + ",".join(state["unhealthy"])
            )
        if hardware_reasons:
            report["status"] = "unhealthy"
            report["reasons"].extend(hardware_reasons)
            reports.append(report)
            continue

        report["status"] = "healthy"
        owners, owner_error = _npu_owners(pods_document, node_name, npu_resource)
        report["owners"] = owners
        if owner_error is not None:
            report["reasons"].append(f"cannot determine NPU ownership: {owner_error}")
        report["idle"] = (
            not owners
            and owner_error is None
            and state["process_count"] == 0
        )
        if role == "active":
            # Occupancy is evidence only for active nodes and must not turn a
            # healthy survivor into a failure candidate.
            reports.append(report)
            continue

        if owner_error is not None:
            report["idle"] = None
            reports.append(report)
            continue

        report["idle"] = not owners and state["process_count"] == 0
        if owners:
            report["reasons"].append("spare has an active Kubernetes NPU owner")
        if state["process_count"] != 0:
            report["reasons"].append(
                f"spare has {state['process_count']} NPU process(es)"
            )
        report["eligible"] = bool(report["idle"])
        reports.append(report)

    # Referencing one physical node more than once is an input ambiguity, not
    # evidence of multiple hardware failures.
    identities: dict[str, list[dict[str, Any]]] = {}
    for report in reports:
        identity = report["nodeName"] or f"target:{report['target']}"
        identities.setdefault(str(identity), []).append(report)
    for duplicates in identities.values():
        if len(duplicates) <= 1:
            continue
        for report in duplicates:
            report["status"] = "unknown"
            report["eligible"] = False
            report["reasons"].append("physical node is listed more than once")

    assert all(report["status"] in NODE_STATUSES for report in reports)
    return reports


def select_replacement(
    active: Sequence[Mapping[str, Any]],
    spares: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Pair all failed active nodes with healthy idle spares, or select none."""

    failed = [node for node in active if node.get("status") == "unhealthy"]
    ambiguous = [node for node in active if node.get("status") == "unknown"]
    healthy_spares = [
        node
        for node in spares
        if node.get("status") == "healthy" and node.get("eligible") is True
    ]
    reasons: list[str] = []
    non_idle_survivors = [
        node
        for node in active
        if node.get("status") == "healthy" and node.get("idle") is not True
    ]

    active_names = {
        str(node.get("nodeName") or node.get("target")) for node in active
    }
    overlapping_spares = [
        node
        for node in spares
        if str(node.get("nodeName") or node.get("target")) in active_names
    ]
    has_overlap = bool(overlapping_spares)
    if overlapping_spares:
        reasons.append("a spare overlaps the active node set")
        healthy_spares = [
            node for node in healthy_spares if node not in overlapping_spares
        ]

    if not failed:
        reasons.append("no failed active node was identified")
    if ambiguous:
        reasons.append(
            "active node diagnosis is ambiguous: "
            + ",".join(str(node.get("target")) for node in ambiguous)
        )
    if not healthy_spares:
        reasons.append("no healthy and idle spare is available")
    elif len(healthy_spares) < len(failed):
        reasons.append(
            f"{len(failed)} failed active node(s) require replacement, but only "
            f"{len(healthy_spares)} healthy idle spare(s) are available"
        )
    if non_idle_survivors:
        reasons.append(
            "healthy active survivors are not yet idle: "
            + ",".join(str(node.get("target")) for node in non_idle_survivors)
        )

    replacements: list[dict[str, Any]] = []
    selection_allowed = (
        bool(failed)
        and not ambiguous
        and not non_idle_survivors
        and not has_overlap
        and len(healthy_spares) >= len(failed)
    )
    if selection_allowed:
        for bad, spare in zip(failed, healthy_spares):
            replacements.append(
                {
                    "failedNode": bad.get("target"),
                    "failedNodeName": bad.get("nodeName"),
                    "spareNode": spare.get("target"),
                    "spareNodeName": spare.get("nodeName"),
                }
            )

    return {
        "schemaVersion": SCHEMA_VERSION,
        "outcome": "replacements_selected" if replacements else "inconclusive",
        "replacementAllowed": bool(replacements),
        "restartReady": bool(replacements),
        "active": list(active),
        "spares": list(spares),
        "failedActiveNodes": [node.get("target") for node in failed],
        "failedActiveCount": len(failed),
        "ambiguousActiveNodes": [node.get("target") for node in ambiguous],
        "healthySpareNodes": [node.get("target") for node in healthy_spares],
        "availableSpareCount": len(healthy_spares),
        "nonIdleActiveNodes": [
            node.get("target") for node in non_idle_survivors
        ],
        "replacements": replacements,
        "replacementCount": len(replacements),
        # Compatibility for a caller that only handles the original one-node
        # recovery case.  Multi-node recovery must consume ``replacements``.
        "replacement": replacements[0] if len(replacements) == 1 else None,
        "reasons": reasons,
    }


def _unknown_result(
    active_nodes: Sequence[str], spare_nodes: Sequence[str], reason: str
) -> dict[str, Any]:
    active = [_base_report(str(target), role="active") for target in active_nodes]
    spares = [_base_report(str(target), role="spare") for target in spare_nodes]
    result = select_replacement(active, spares)
    result["reasons"].insert(0, reason)
    return result


def diagnose_replacement(
    *,
    kubectl_command: str | Sequence[str],
    kubeconfig: Path | None,
    active_nodes: Sequence[str],
    spare_nodes: Sequence[str],
    expected_npus: int = 8,
    npu_resource: str = DEFAULT_NPU_RESOURCE,
    exporter_app: str = DEFAULT_EXPORTER_APP,
    exporter_port: int = DEFAULT_EXPORTER_PORT,
) -> dict[str, Any]:
    """Read Kubernetes once, assess all nodes, and return a JSON-safe result."""

    if expected_npus <= 0:
        return _unknown_result(
            active_nodes, spare_nodes, "expected_npus must be positive"
        )
    command = (
        shlex.split(kubectl_command)
        if isinstance(kubectl_command, str)
        else list(kubectl_command)
    )
    if not command:
        return _unknown_result(active_nodes, spare_nodes, "kubectl command is empty")
    try:
        nodes_document = kubectl_json(command, kubeconfig, "nodes")
        pods_document = kubectl_json(command, kubeconfig, "pods")
    except CheckError as error:
        return _unknown_result(
            active_nodes,
            spare_nodes,
            f"Kubernetes snapshot could not be read: {error}",
        )

    exporter_metrics: dict[str, str] = {}
    exporter_errors: dict[str, str] = {}
    all_targets = tuple(active_nodes) + tuple(spare_nodes)
    for target in all_targets:
        matches = _node_matches(nodes_document, str(target))
        if len(matches) != 1:
            continue
        node = matches[0]
        if _ready_status(node) != "True":
            continue
        node_name = str(node.get("metadata", {}).get("name", target))
        if node_name in exporter_metrics or node_name in exporter_errors:
            continue
        candidates = _exporter_candidates(pods_document, node_name, exporter_app)
        if len(candidates) != 1:
            continue
        metadata = candidates[0].get("metadata", {})
        namespace = metadata.get("namespace", "default")
        pod_name = metadata.get("name")
        path = (
            f"/api/v1/namespaces/{namespace}/pods/"
            f"{pod_name}:{exporter_port}/proxy/metrics"
        )
        try:
            exporter_metrics[node_name] = kubectl_raw(command, kubeconfig, path)
        except CheckError as error:
            exporter_errors[node_name] = str(error)

    active = assess_nodes(
        nodes_document,
        pods_document,
        active_nodes,
        exporter_metrics,
        exporter_errors,
        expected_npus=expected_npus,
        role="active",
        npu_resource=npu_resource,
        exporter_app=exporter_app,
    )
    spares = assess_nodes(
        nodes_document,
        pods_document,
        spare_nodes,
        exporter_metrics,
        exporter_errors,
        expected_npus=expected_npus,
        role="spare",
        npu_resource=npu_resource,
        exporter_app=exporter_app,
    )
    return select_replacement(active, spares)


def _failed_nodes(diagnosis: Mapping[str, Any]) -> tuple[str, ...]:
    """Return a stable node identity tuple from one diagnosis result."""

    raw_nodes = diagnosis.get("failedActiveNodes")
    if raw_nodes is None:
        replacements = diagnosis.get("replacements", [])
        if isinstance(replacements, Sequence) and not isinstance(
            replacements, (str, bytes)
        ):
            raw_nodes = [
                item.get("failedNode")
                for item in replacements
                if isinstance(item, Mapping)
            ]
    if not isinstance(raw_nodes, Sequence) or isinstance(raw_nodes, (str, bytes)):
        return ()
    return tuple(sorted({str(node) for node in raw_nodes if node is not None}))


def diagnose_stable_replacement(
    diagnose_once: Callable[[], Mapping[str, Any]],
    *,
    stable_samples: int,
    max_samples: int,
    poll_seconds: float = 0,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Confirm a hardware diagnosis using bounded consecutive samples.

    The caller owns the overall time window by choosing ``max_samples`` and
    ``poll_seconds``.  Only the latest diagnosis is retained.  A sampling
    exception is converted to an inconclusive sample and resets the streak.
    """

    if stable_samples < 1:
        raise ValueError("stable_samples must be at least 1")
    if max_samples < 1:
        raise ValueError("max_samples must be at least 1")
    if poll_seconds < 0:
        raise ValueError("poll_seconds must not be negative")

    previous_nodes: tuple[str, ...] = ()
    consecutive = 0
    latest: dict[str, Any] | None = None
    last_issue = "diagnosis did not identify a failed active node"

    for sample_count in range(1, max_samples + 1):
        try:
            sampled = diagnose_once()
            if not isinstance(sampled, Mapping):
                raise TypeError("diagnosis result is not a mapping")
            latest = dict(sampled)
            nodes = _failed_nodes(sampled)
            if nodes and nodes == previous_nodes:
                consecutive += 1
            elif nodes:
                previous_nodes = nodes
                consecutive = 1
            else:
                previous_nodes = ()
                consecutive = 0

            replacement_allowed = sampled.get(
                "replacementAllowed", sampled.get("allowReplacement")
            ) is True
            restart_ready = sampled.get("restartReady") is True
            if (
                nodes
                and consecutive >= stable_samples
                and replacement_allowed
                and restart_ready
            ):
                return {
                    "stable": True,
                    "sampleCount": sample_count,
                    "confirmedFailedNodes": list(nodes),
                    "latestDiagnosis": latest,
                    "reason": "stable hardware failure confirmed",
                }

            if not nodes:
                last_issue = "latest diagnosis did not identify a failed active node"
            elif not replacement_allowed or not restart_ready:
                last_issue = "latest diagnosis is not ready for replacement"
            else:
                last_issue = (
                    f"failure was observed for {consecutive} consecutive sample(s); "
                    f"{stable_samples} required"
                )
        except Exception as error:
            previous_nodes = ()
            consecutive = 0
            message = str(error).replace("\n", " ")[:500]
            last_issue = f"diagnosis sample failed: {message or type(error).__name__}"
            latest = {
                "schemaVersion": SCHEMA_VERSION,
                "outcome": "inconclusive",
                "replacementAllowed": False,
                "restartReady": False,
                "failedActiveNodes": [],
                "reasons": [last_issue],
            }

        if sample_count < max_samples and poll_seconds:
            sleep_fn(poll_seconds)

    return {
        "stable": False,
        "sampleCount": max_samples,
        "confirmedFailedNodes": [],
        "latestDiagnosis": latest,
        "reason": last_issue,
    }


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--active-node", action="append", required=True)
    parser.add_argument("--spare-node", action="append", required=True)
    parser.add_argument("--expected-npus", type=int, default=8)
    parser.add_argument("--kubectl-command")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--npu-resource")
    parser.add_argument("--npu-exporter-app")
    parser.add_argument("--npu-exporter-port", type=int)
    parser.add_argument("--compact", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        defaults = cluster_config.apply_kubernetes_defaults(args)
        if (
            args.npu_resource is None
            or args.npu_exporter_app is None
            or args.npu_exporter_port is None
        ):
            defaults = defaults or cluster_config.load_cluster_config()
        if args.npu_resource is None:
            args.npu_resource = defaults.accelerator.resource_name
        if args.npu_exporter_app is None:
            args.npu_exporter_app = defaults.npu_check.exporter_app
        if args.npu_exporter_port is None:
            args.npu_exporter_port = defaults.npu_check.exporter_port
    except cluster_config.ClusterConfigError as error:
        print(f"STOP: recovery configuration could not be read: {error}", file=sys.stderr)
        return 2
    result = diagnose_replacement(
        kubectl_command=args.kubectl_command,
        kubeconfig=args.kubeconfig,
        active_nodes=tuple(args.active_node),
        spare_nodes=tuple(args.spare_node),
        expected_npus=args.expected_npus,
        npu_resource=args.npu_resource,
        exporter_app=args.npu_exporter_app,
        exporter_port=args.npu_exporter_port,
    )
    print(
        json.dumps(
            result,
            ensure_ascii=False,
            indent=None if args.compact else 2,
            sort_keys=True,
        )
    )
    return 0 if result["replacementAllowed"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
