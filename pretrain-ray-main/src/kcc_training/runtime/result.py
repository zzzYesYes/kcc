from __future__ import annotations

import json
from typing import Any, Mapping

from kcc_training.kube_api import KubernetesApi, core_namespaced_path


def publish(api: KubernetesApi, result: Mapping[str, Any]) -> Mapping[str, Any]:
    namespace = str(result["namespace"])
    name = f"{result['runName']}-a{int(result['attempt']):02d}-result"
    document = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {"training.kcc.io/run": str(result["runName"])},
            "annotations": {
                "training.kcc.io/run-uid": str(result["runUid"]),
                "training.kcc.io/attempt": str(result["attempt"]),
            },
        },
        "data": {"result.json": json.dumps(dict(result), ensure_ascii=False, sort_keys=True)},
    }
    collection = core_namespaced_path(namespace, "configmaps")
    item = core_namespaced_path(namespace, "configmaps", name)
    return api.upsert(collection, item, document)

