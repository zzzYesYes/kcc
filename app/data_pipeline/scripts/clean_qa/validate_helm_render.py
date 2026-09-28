#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import yaml


def labels_match(selector: dict, labels: dict) -> bool:
    return all(labels.get(key) == value for key, value in selector.items())


def validate_pod_spec(name: str, spec: dict) -> list[str]:
    errors: list[str] = []
    volumes = {volume["name"] for volume in spec.get("volumes", [])}
    for container in spec.get("containers", []):
        for mount in container.get("volumeMounts", []):
            if mount["name"] not in volumes:
                errors.append(
                    f"{name}/{container['name']}: missing volume {mount['name']}"
                )
        resources = container.get("resources", {})
        requests = resources.get("requests", {})
        limits = resources.get("limits", {})
        for resource_name, request in requests.items():
            if "Ascend" in resource_name and limits.get(resource_name) != request:
                errors.append(
                    f"{name}/{container['name']}: NPU request/limit mismatch"
                )
    return errors


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("rendered_yaml", type=Path)
    args = parser.parse_args()
    documents = [
        document
        for document in yaml.safe_load_all(args.rendered_yaml.read_text())
        if document
    ]
    errors: list[str] = []
    workloads: list[tuple[str, dict, dict]] = []

    for document in documents:
        kind = document.get("kind")
        name = document.get("metadata", {}).get("name", "<unnamed>")
        if kind == "Secret":
            errors.append(f"{name}: chart must not render credentials Secret")
        if kind == "Deployment":
            selector = document["spec"]["selector"]["matchLabels"]
            template = document["spec"]["template"]
            labels = template["metadata"].get("labels", {})
            if not labels_match(selector, labels):
                errors.append(f"{name}: Deployment selector does not match Pod labels")
            workloads.append((name, labels, template["spec"]))
            errors.extend(validate_pod_spec(name, template["spec"]))
            component = labels.get("app.kubernetes.io/component", "")
            if component in {"mineru-worker", "qwen-worker"}:
                if document["spec"].get("replicas") != 0:
                    errors.append(f"{name}: standalone NPU Deployment is not scale-to-zero")
        elif kind == "RayCluster":
            head = document["spec"]["headGroupSpec"]["template"]
            workloads.append((f"{name}/head", head["metadata"].get("labels", {}), head["spec"]))
            errors.extend(validate_pod_spec(f"{name}/head", head["spec"]))
            for group in document["spec"].get("workerGroupSpecs", []):
                template = group["template"]
                group_name = group["groupName"]
                workloads.append(
                    (
                        f"{name}/{group_name}",
                        template["metadata"].get("labels", {}),
                        template["spec"],
                    )
                )
                errors.extend(validate_pod_spec(f"{name}/{group_name}", template["spec"]))
                if group_name.startswith(("mineru", "qa-")):
                    if group.get("replicas") != 0 or group.get("minReplicas") != 0:
                        errors.append(f"{name}/{group_name}: NPU worker group is not scale-to-zero")
        elif kind == "Role":
            for rule in document.get("rules", []):
                if "*" in rule.get("verbs", []) or "*" in rule.get("resources", []):
                    errors.append(f"{name}: RBAC wildcard is not permitted")
        elif kind in {"ClusterRole", "ClusterRoleBinding"}:
            errors.append(f"{name}: cluster-scoped RBAC is not permitted")
        elif kind == "ConfigMap" and name.endswith("-config"):
            data = document.get("data", {})
            for config_name in ("stage1.yaml", "stage2.yaml"):
                run_config = yaml.safe_load(data.get(config_name, ""))
                if not isinstance(run_config, dict) or "ops" not in run_config:
                    errors.append(f"{name}/{config_name}: missing Dagster ops root")
            contract = yaml.safe_load(data.get("data-contract.yaml", ""))
            if contract.get("stage1", {}).get("canonical_record") != "blocks.jsonl":
                errors.append(f"{name}: Stage 1 canonical record changed")
            if not contract.get("stage2", {}).get("require_judge_for_verified"):
                errors.append(f"{name}: verified output no longer requires Judge")

    for document in documents:
        if document.get("kind") != "Service":
            continue
        name = document["metadata"]["name"]
        selector = document["spec"].get("selector", {})
        matched = [item for item in workloads if labels_match(selector, item[1])]
        if not matched:
            errors.append(f"{name}: Service selector matches no rendered workload")
            continue
        for port in document["spec"].get("ports", []):
            target = port.get("targetPort", port["port"])
            if isinstance(target, int):
                continue
            if not any(
                target
                in {
                    item.get("name")
                    for container in pod_spec.get("containers", [])
                    for item in container.get("ports", [])
                }
                for _, _, pod_spec in matched
            ):
                errors.append(f"{name}: targetPort {target!r} is not declared")

    if errors:
        raise SystemExit("\n".join(f"ERROR: {error}" for error in errors))
    print(f"Validated {len(documents)} objects from {args.rendered_yaml}")


if __name__ == "__main__":
    main()
