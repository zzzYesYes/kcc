#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

import yaml


def fail(message: str) -> None:
    raise SystemExit(f"render validation failed: {message}")


path = Path(sys.argv[1])
documents = [item for item in yaml.safe_load_all(path.read_text()) if item]
allowed = {"ConfigMap", "Deployment", "Service", "ServiceAccount", "Role", "RoleBinding"}
for document in documents:
    if document.get("kind") not in allowed:
        fail(f"unexpected kind {document.get('kind')}")

deployments = [item for item in documents if item["kind"] == "Deployment"]
workers = [
    item for item in deployments
    if item["metadata"].get("labels", {}).get("app.kubernetes.io/component", "").startswith("qwen-worker-")
]
if len(workers) != 2:
    fail(f"expected two worker profiles, found {len(workers)}")
if any(item["spec"].get("replicas") != 0 for item in workers):
    fail("all Qwen workers must render replicas=0")
if any(
    item["spec"]["template"]["spec"].get("automountServiceAccountToken") is not False
    for item in workers
):
    fail("Qwen workers must not mount a Kubernetes ServiceAccount token")

roles = [item for item in documents if item["kind"] == "Role"]
if len(roles) != 1:
    fail("expected exactly one namespace Role")
rules = roles[0].get("rules", [])
allowed_verbs = {
    (("apps",), ("deployments",)): {"get", "list", "watch", "patch"},
    (("",), ("pods",)): {"get", "list", "watch"},
    (("",), ("services",)): {"get", "list"},
}
patch_is_name_scoped = False
for rule in rules:
    if "*" in rule.get("verbs", []) or "*" in rule.get("resources", []):
        fail("wildcard RBAC is forbidden")
    key = (tuple(rule.get("apiGroups", [])), tuple(rule.get("resources", [])))
    if key not in allowed_verbs:
        fail(f"unexpected RBAC resource rule: {key}")
    if not set(rule.get("verbs", [])).issubset(allowed_verbs[key]):
        fail(f"excess RBAC verbs for {key}: {rule.get('verbs', [])}")
    if "patch" in rule.get("verbs", []):
        names = set(rule.get("resourceNames", []))
        expected_names = {item["metadata"]["name"] for item in workers}
        if names != expected_names:
            fail(f"Deployment patch must be restricted to workers: {names}")
        patch_is_name_scoped = True
if not patch_is_name_scoped:
    fail("no name-scoped Deployment patch rule found")
if any(item["kind"].startswith("ClusterRole") for item in documents):
    fail("cluster-scoped RBAC is forbidden")

text = path.read_text()
for forbidden in ("RayCluster", "MinIO", "MinerU", "k12-cleaned", "huawei.com/Ascend910: 0"):
    if forbidden in text:
        fail(f"forbidden coupled resource/text found: {forbidden}")
print(f"validated {len(documents)} rendered resources; workers remain scale-to-zero")
