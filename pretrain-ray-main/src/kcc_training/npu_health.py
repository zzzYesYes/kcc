"""Fail-closed npu-exporter health provider for automatic replacement."""

from __future__ import annotations

import re
from pathlib import Path
import ssl
from typing import Any, Callable, Mapping, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from .kube_api import KubernetesApi, KubernetesApiError, SERVICE_ACCOUNT_ROOT


_LABEL = re.compile(r'([A-Za-z_][A-Za-z0-9_]*)="([^"\\]*(?:\\.[^"\\]*)*)"')


def metric_samples(text: str, metric: str) -> list[tuple[dict[str, str], float]]:
    result: list[tuple[dict[str, str], float]] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not (line.startswith(f"{metric}{{") or line.startswith(f"{metric} ")):
            continue
        fields = line.split()
        if len(fields) < 2:
            continue
        try:
            value = float(fields[1])
        except ValueError:
            continue
        labels = {key: bytes(item, "utf-8").decode("unicode_escape") for key, item in _LABEL.findall(fields[0])}
        result.append((labels, value))
    return result


def parse_metrics(text: str) -> dict[str, Any]:
    machine = metric_samples(text, "machine_npu_nums")
    health = metric_samples(text, "npu_chip_info_health_status")
    counts = metric_samples(text, "npu_chip_info_process_info_num")
    visible = int(machine[0][1]) if len(machine) == 1 else 0
    health_by_id: dict[str, bool] = {}
    process_by_id: dict[str, int] = {}
    duplicate_health_ids: set[str] = set()
    duplicate_process_ids: set[str] = set()
    invalid_health_ids = 0
    invalid_process_ids = 0
    for labels, value in health:
        identifier = labels.get("id", "").strip()
        if not identifier:
            invalid_health_ids += 1
            continue
        if identifier in health_by_id:
            duplicate_health_ids.add(identifier)
            health_by_id[identifier] = health_by_id[identifier] and value == 1
        else:
            health_by_id[identifier] = value == 1
    for labels, value in counts:
        identifier = labels.get("id", "").strip()
        if not identifier:
            invalid_process_ids += 1
            continue
        process_count = max(0, int(value))
        if identifier in process_by_id:
            duplicate_process_ids.add(identifier)
            process_by_id[identifier] = max(process_by_id[identifier], process_count)
        else:
            process_by_id[identifier] = process_count
    return {
        "visible": visible,
        "healthSamples": len(health),
        "healthDeviceCount": len(health_by_id),
        "processDeviceCount": len(process_by_id),
        "duplicateHealthIds": sorted(duplicate_health_ids),
        "duplicateProcessIds": sorted(duplicate_process_ids),
        "invalidHealthIds": invalid_health_ids,
        "invalidProcessIds": invalid_process_ids,
        "unhealthy": sorted(identifier for identifier, healthy in health_by_id.items() if not healthy),
        "processCount": sum(process_by_id.values()),
    }


class InClusterRawGet:
    def __init__(
        self,
        *,
        server: str = "https://kubernetes.default.svc",
        token_path: Path = SERVICE_ACCOUNT_ROOT / "token",
        ca_path: Path = SERVICE_ACCOUNT_ROOT / "ca.crt",
    ) -> None:
        try:
            self._token = token_path.read_text(encoding="utf-8").strip()
            self._context = ssl.create_default_context(cafile=str(ca_path))
        except (OSError, UnicodeError) as error:
            raise KubernetesApiError(None, f"cannot initialize exporter API transport: {error}") from error
        self._server = server.rstrip("/")

    def __call__(self, path: str) -> str:
        request = Request(
            f"{self._server}{path}",
            headers={"Authorization": f"Bearer {self._token}", "Accept": "text/plain"},
        )
        try:
            with urlopen(request, timeout=30, context=self._context) as response:
                return response.read().decode("utf-8")
        except HTTPError as error:
            raise KubernetesApiError(error.code, f"exporter proxy returned {error.code}") from error
        except (OSError, URLError, UnicodeError) as error:
            raise KubernetesApiError(None, f"exporter proxy failed: {error}") from error


class NpuExporterHealthProvider:
    def __init__(
        self,
        api: KubernetesApi,
        *,
        exporter_namespace: str,
        exporter_app: str = "npu-exporter",
        exporter_port: int = 8082,
        resource_name: str = "huawei.com/Ascend910",
        expected_devices: int = 8,
        raw_get: Callable[[str], str] | None = None,
    ) -> None:
        self.api = api
        self.namespace = exporter_namespace
        self.app = exporter_app
        self.port = exporter_port
        self.resource = resource_name
        self.expected = expected_devices
        self.raw_get = raw_get or InClusterRawGet()

    def observe(self, targets: Sequence[str]) -> Mapping[str, Any]:
        nodes_document = self.api.list("/api/v1/nodes")
        pods_document = self.api.list(f"/api/v1/namespaces/{quote(self.namespace, safe='')}/pods")
        resolved: dict[str, Mapping[str, Any]] = {}
        for node in nodes_document["items"]:
            if not isinstance(node, Mapping):
                continue
            metadata = node.get("metadata")
            status = node.get("status")
            if not isinstance(metadata, Mapping) or not isinstance(status, Mapping):
                continue
            identities = {metadata.get("name")}
            identities.update(
                item.get("address")
                for item in status.get("addresses", [])
                if isinstance(item, Mapping) and item.get("type") == "InternalIP"
            )
            for target in targets:
                if target in identities:
                    if target in resolved:
                        resolved[target] = {}
                    else:
                        resolved[target] = node
        exporter_by_node: dict[str, list[Mapping[str, Any]]] = {}
        for pod in pods_document["items"]:
            if not isinstance(pod, Mapping) or pod.get("status", {}).get("phase") != "Running":
                continue
            metadata = pod.get("metadata", {})
            name = metadata.get("name", "")
            labels = metadata.get("labels", {})
            if labels.get("app") != self.app and not str(name).startswith(f"{self.app}-"):
                continue
            node_name = pod.get("spec", {}).get("nodeName")
            if isinstance(node_name, str):
                exporter_by_node.setdefault(node_name, []).append(pod)
        reports: dict[str, dict[str, Any]] = {}
        for target in targets:
            node = resolved.get(target)
            if not node:
                reports[target] = {
                    "complete": False,
                    "hardwareHealthy": None,
                    "idle": False,
                    "reason": "node identity is missing or ambiguous",
                }
                continue
            metadata = node["metadata"]
            status = node["status"]
            node_name = metadata["name"]
            ready = any(
                isinstance(item, Mapping) and item.get("type") == "Ready" and item.get("status") == "True"
                for item in status.get("conditions", [])
            )
            exporters = exporter_by_node.get(node_name, [])
            if not ready or len(exporters) != 1:
                reports[target] = {
                    "complete": False,
                    "hardwareHealthy": None,
                    "idle": False,
                    "reason": "node not Ready or exporter is not unique",
                }
                continue
            pod_name = exporters[0]["metadata"]["name"]
            path = (
                f"/api/v1/namespaces/{quote(self.namespace, safe='')}/pods/"
                f"{quote(str(pod_name), safe='')}:{self.port}/proxy/metrics"
            )
            try:
                metrics = parse_metrics(self.raw_get(path))
                allocatable = int(status.get("allocatable", {}).get(self.resource, 0))
            except (KubernetesApiError, TypeError, ValueError) as error:
                reports[target] = {
                    "complete": False,
                    "hardwareHealthy": None,
                    "idle": False,
                    "reason": str(error),
                }
                continue
            complete = (
                metrics["visible"] == self.expected == allocatable
                and metrics["healthDeviceCount"] == self.expected
                and metrics["processDeviceCount"] == self.expected
                and not metrics["duplicateHealthIds"]
                and not metrics["duplicateProcessIds"]
                and metrics["invalidHealthIds"] == 0
                and metrics["invalidProcessIds"] == 0
            )
            reports[target] = {
                "complete": complete,
                "hardwareHealthy": not metrics["unhealthy"] if complete else None,
                "idle": complete and metrics["processCount"] == 0,
                "visible": metrics["visible"],
                "healthDeviceCount": metrics["healthDeviceCount"],
                "processDeviceCount": metrics["processDeviceCount"],
                "processCount": metrics["processCount"],
                "unhealthy": metrics["unhealthy"],
            }
        return {
            "nodes": reports,
            "complete": len(reports) == len(targets) and all(item["complete"] for item in reports.values()),
        }

