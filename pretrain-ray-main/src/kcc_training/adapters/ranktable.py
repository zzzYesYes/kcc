"""RankTable adapters with strict ConfigMap ownership and digest checks."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from typing import Any, Mapping, Sequence

from kcc_training.ports import KubernetesPort


class RankTableError(RuntimeError):
    pass


class ConfigMapRankTableAdapter:
    """Read the sanitized ConfigMap produced from ClusterD job-summary data."""

    def __init__(self, kubernetes: KubernetesPort, namespace: str) -> None:
        self._kubernetes = kubernetes
        self._namespace = namespace

    @staticmethod
    def _payload(document: Mapping[str, Any]) -> bytes:
        data = document.get("data")
        if isinstance(data, Mapping) and isinstance(data.get("hccl.json"), str):
            return data["hccl.json"].encode("utf-8")
        binary = document.get("binaryData")
        encoded = binary.get("hccl.json") if isinstance(binary, Mapping) else None
        if not isinstance(encoded, str):
            raise RankTableError("RankTable ConfigMap has no hccl.json key")
        try:
            return base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as error:
            raise RankTableError("RankTable binaryData is invalid base64") from error

    def resolve(self, cluster_name: str, nodes: Sequence[str]) -> tuple[bytes, str]:
        name = f"hccl-sanitized-{cluster_name}"
        document = self._kubernetes.get("configmap", name, self._namespace)
        if document is None:
            raise RankTableError(f"RankTable ConfigMap is missing: {name}")
        metadata = document.get("metadata")
        if not isinstance(metadata, Mapping):
            raise RankTableError("RankTable ConfigMap metadata is invalid")
        labels = metadata.get("labels")
        annotations = metadata.get("annotations")
        if (
            not isinstance(labels, Mapping)
            or labels.get("app.kubernetes.io/managed-by")
            != "hccl-ranktable-sanitizer"
            or not isinstance(annotations, Mapping)
            or annotations.get("ranktable.hccl-check.local/source-configmap")
            != f"job-summary-{cluster_name}"
        ):
            raise RankTableError("RankTable ConfigMap ownership is invalid")
        payload = self._payload(document)
        try:
            value = json.loads(payload)
        except (UnicodeError, json.JSONDecodeError) as error:
            raise RankTableError(f"RankTable is not valid JSON: {error}") from error
        if not isinstance(value, Mapping):
            raise RankTableError("RankTable must be a JSON object")
        server_list = value.get("server_list")
        if not isinstance(server_list, list) or len(server_list) != len(nodes):
            raise RankTableError("RankTable server count differs from selected nodes")
        server_ids = {
            item.get("server_id")
            for item in server_list
            if isinstance(item, Mapping) and isinstance(item.get("server_id"), str)
        }
        if len(server_ids) != len(nodes):
            raise RankTableError("RankTable server identities are missing or duplicated")
        return payload, hashlib.sha256(payload).hexdigest()

