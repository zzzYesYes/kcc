"""Kubernetes scheduling evidence for NPU readiness and occupancy."""

from __future__ import annotations

from typing import Any, Mapping, Protocol, Sequence


class KubernetesListPort(Protocol):
    def list_json(
        self,
        kind: str,
        *,
        namespace: str | None = None,
        all_namespaces: bool = False,
        labels: str | None = None,
    ) -> Mapping[str, Any]: ...


def _npu_count(pod: Mapping[str, Any], resource_name: str) -> int:
    def count(container: Mapping[str, Any]) -> int:
        resources = container.get("resources")
        if not isinstance(resources, Mapping):
            return 0
        requests = resources.get("requests")
        limits = resources.get("limits")
        request = requests.get(resource_name) if isinstance(requests, Mapping) else None
        limit = limits.get(resource_name) if isinstance(limits, Mapping) else None
        try:
            return int(request if request is not None else (limit or 0))
        except (TypeError, ValueError):
            return 0

    spec = pod.get("spec")
    if not isinstance(spec, Mapping):
        return 0
    application = sum(
        count(item) for item in spec.get("containers", []) if isinstance(item, Mapping)
    )
    init = max(
        (count(item) for item in spec.get("initContainers", []) if isinstance(item, Mapping)),
        default=0,
    )
    return max(application, init)


class KubernetesDeviceHealthAdapter:
    def __init__(self, kubernetes: KubernetesListPort, resource_name: str) -> None:
        self._kubernetes = kubernetes
        self._resource_name = resource_name

    def observe(self, nodes: Sequence[str]) -> Mapping[str, Any]:
        node_document = self._kubernetes.list_json("nodes")
        pod_document = self._kubernetes.list_json("pods", all_namespaces=True)
        wanted = set(nodes)
        reports: dict[str, dict[str, Any]] = {}
        for node in node_document.get("items", []):
            if not isinstance(node, Mapping):
                continue
            metadata = node.get("metadata")
            status = node.get("status")
            if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
                continue
            name = metadata.get("name")
            addresses = status.get("addresses", [])
            identities = {name}
            identities.update(
                address.get("address")
                for address in addresses
                if isinstance(address, Mapping) and address.get("type") == "InternalIP"
            )
            targets = sorted(wanted & identities)
            if not targets:
                continue
            conditions = status.get("conditions", [])
            ready = any(
                isinstance(condition, Mapping)
                and condition.get("type") == "Ready"
                and condition.get("status") == "True"
                for condition in conditions
            )
            allocatable = status.get("allocatable")
            try:
                capacity = int(allocatable.get(self._resource_name, 0))
            except (AttributeError, TypeError, ValueError):
                capacity = 0
            for target in targets:
                reports[target] = {
                    "nodeName": name,
                    "ready": ready,
                    "allocatable": capacity,
                    "owners": [],
                }
        for pod in pod_document.get("items", []):
            if not isinstance(pod, Mapping):
                continue
            spec = pod.get("spec")
            status = pod.get("status")
            if not isinstance(spec, Mapping) or not isinstance(status, Mapping):
                continue
            if status.get("phase") in {"Succeeded", "Failed"}:
                continue
            node_name = spec.get("nodeName")
            count = _npu_count(pod, self._resource_name)
            if not count:
                continue
            metadata = pod.get("metadata")
            for report in reports.values():
                if report["nodeName"] == node_name:
                    report["owners"].append(
                        {
                            "namespace": metadata.get("namespace", "default")
                            if isinstance(metadata, Mapping)
                            else "default",
                            "name": metadata.get("name", "unknown")
                            if isinstance(metadata, Mapping)
                            else "unknown",
                            "npu": count,
                        }
                    )
        for target in nodes:
            reports.setdefault(
                target,
                {"nodeName": None, "ready": False, "allocatable": 0, "owners": []},
            )
        return {
            "nodes": reports,
            "healthy": all(
                report["ready"] and report["allocatable"] > 0
                for report in reports.values()
            ),
            "idle": all(not report["owners"] for report in reports.values()),
        }

