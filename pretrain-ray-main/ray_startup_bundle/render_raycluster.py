#!/usr/bin/env python3
"""Render one create-only RayCluster manifest for the selected worker nodes."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys
from typing import Any, Mapping, Sequence

import yaml
import cluster_config


CLUSTER_TOKEN = "__KCC_RAY_CLUSTER__"
NAMESPACE_TOKEN = "__KCC_RAY_NAMESPACE__"
HEAD_NODE_TOKEN = "__KCC_RAY_HEAD_NODE__"
NPU_RESOURCE_TOKEN = "__KCC_RAY_NPU_RESOURCE__"
DEVICES_PER_NODE_TOKEN = "__KCC_RAY_DEVICES_PER_NODE__"
WORKSPACE_HOST_PATH_TOKEN = "__KCC_RAY_WORKSPACE_HOST_PATH__"
RAY_HEAD_IMAGE_TOKEN = "__KCC_RAY_HEAD_IMAGE__"
RAY_WORKER_IMAGE_TOKEN = "__KCC_RAY_WORKER_IMAGE__"
RAY_IMAGE_PULL_POLICY_TOKEN = "__KCC_RAY_IMAGE_PULL_POLICY__"


class RenderError(RuntimeError):
    pass


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise RenderError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def load_cluster_nodes(
    kubectl: Sequence[str],
) -> tuple[Mapping[str, Any], ...]:
    try:
        result = subprocess.run(
            [*kubectl, "get", "nodes", "-o", "json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise RenderError(f"cannot read Kubernetes nodes: {error}") from error
    if result.returncode != 0:
        raise RenderError(
            "cannot read Kubernetes nodes: "
            f"{result.stderr.strip() or result.stdout.strip()}"
        )
    try:
        items = json.loads(result.stdout).get("items", [])
    except (AttributeError, json.JSONDecodeError) as error:
        raise RenderError(f"kubectl returned invalid node JSON: {error}") from error
    if not isinstance(items, list):
        raise RenderError("kubectl node response has no item list")
    return tuple(item for item in items if isinstance(item, dict))


def resolve_node_names(
    targets: Sequence[str],
    cluster_nodes: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    if not targets:
        raise RenderError("at least one worker node is required")
    resolved: list[str] = []
    for target in targets:
        matches: list[str] = []
        for node in cluster_nodes:
            name = node.get("metadata", {}).get("name")
            addresses = node.get("status", {}).get("addresses", [])
            internal_ips = {
                address.get("address")
                for address in addresses
                if address.get("type") == "InternalIP"
            }
            if isinstance(name, str) and (
                target == name or target in internal_ips
            ):
                matches.append(name)
        if len(matches) != 1:
            raise RenderError(
                f"target {target!r} resolves to {len(matches)} Kubernetes nodes"
            )
        resolved.append(matches[0])
    if len(set(resolved)) != len(resolved):
        raise RenderError("worker node targets resolve to duplicates")
    return tuple(resolved)


def require_mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise RenderError(f"{label} must be an object")
    return value


def parse_selectors(values: Sequence[str], label: str) -> dict[str, str]:
    selectors: dict[str, str] = {}
    for value in values:
        key, separator, label_value = value.partition("=")
        if not separator or not key or not label_value:
            raise RenderError(f"{label} must use KEY=VALUE")
        if key in selectors:
            raise RenderError(f"{label} contains duplicate key: {key}")
        selectors[key] = label_value
    return selectors


def replace_template_tokens(value: Any, replacements: Mapping[str, str]) -> Any:
    """Replace explicit template tokens in values and mapping keys."""
    if isinstance(value, str):
        result = value
        for token, replacement in replacements.items():
            result = result.replace(token, replacement)
        return result
    if isinstance(value, list):
        return [replace_template_tokens(item, replacements) for item in value]
    if isinstance(value, dict):
        result: dict[Any, Any] = {}
        for key, item in value.items():
            replaced_key = replace_template_tokens(key, replacements)
            if replaced_key in result:
                raise RenderError("template token replacement produced a duplicate key")
            result[replaced_key] = replace_template_tokens(item, replacements)
        return result
    return value


def update_runtime_configmap(
    pod_spec: dict[str, Any],
    runtime_configmap: str,
) -> None:
    volumes = pod_spec.get("volumes")
    if not isinstance(volumes, list):
        raise RenderError("Ray Pod template has no volume list")
    matches = [
        volume
        for volume in volumes
        if isinstance(volume, dict) and volume.get("name") == "runtime-source"
    ]
    if len(matches) != 1:
        raise RenderError("Ray Pod template must have one runtime-source volume")
    configmap = require_mapping(
        matches[0].get("configMap"),
        "runtime-source ConfigMap",
    )
    configmap["name"] = runtime_configmap


def render_manifest(
    *,
    base_manifest: Path,
    output_manifest: Path,
    node_names: Sequence[str],
    namespace: str,
    cluster: str,
    head_node: str,
    npu_resource: str,
    devices_per_node: int,
    workspace_host_path: Path,
    ray_head_image: str,
    ray_worker_image: str,
    ray_image_pull_policy: str,
    runtime_class_name: str | None,
    head_selector: Mapping[str, str],
    worker_selector: Mapping[str, str],
    runtime_configmap: str,
    run_id: str,
) -> None:
    if not 1 <= devices_per_node <= 64:
        raise RenderError("devices per node must be within 1..64")
    if not workspace_host_path.is_absolute():
        raise RenderError("workspace host path must be absolute")
    try:
        cluster_config.validate_profile_image(ray_head_image, "Ray head image")
        cluster_config.validate_profile_image(ray_worker_image, "Ray worker image")
        cluster_config.validate_image_pull_policy(
            ray_image_pull_policy,
            "Ray image pull policy",
        )
    except cluster_config.ClusterConfigError as error:
        raise RenderError(str(error)) from error
    if not base_manifest.is_file() or base_manifest.is_symlink():
        raise RenderError(f"base manifest is not a regular file: {base_manifest}")
    try:
        raw_documents = list(
            yaml.safe_load_all(base_manifest.read_text(encoding="utf-8"))
        )
    except (OSError, UnicodeError, yaml.YAMLError) as error:
        raise RenderError(f"cannot parse base RayCluster manifest: {error}") from error
    replacements = {
        CLUSTER_TOKEN: cluster,
        NAMESPACE_TOKEN: namespace,
        HEAD_NODE_TOKEN: head_node,
        NPU_RESOURCE_TOKEN: npu_resource,
        DEVICES_PER_NODE_TOKEN: str(devices_per_node),
        WORKSPACE_HOST_PATH_TOKEN: str(workspace_host_path),
        RAY_HEAD_IMAGE_TOKEN: ray_head_image,
        RAY_WORKER_IMAGE_TOKEN: ray_worker_image,
        RAY_IMAGE_PULL_POLICY_TOKEN: ray_image_pull_policy,
    }
    documents = [
        replace_template_tokens(document, replacements) for document in raw_documents
    ]
    rayclusters = [
        document
        for document in documents
        if isinstance(document, dict) and document.get("kind") == "RayCluster"
    ]
    if len(rayclusters) != 1:
        raise RenderError("base manifest must contain exactly one RayCluster")
    raycluster = rayclusters[0]
    metadata = require_mapping(raycluster.get("metadata"), "RayCluster metadata")
    if metadata.get("name") != cluster or metadata.get("namespace") != namespace:
        raise RenderError(
            "base RayCluster name/namespace differs from the requested values"
        )
    annotations = metadata.setdefault("annotations", {})
    annotations = require_mapping(annotations, "RayCluster annotations")
    annotations["trainctl.io/allowed-nodes"] = ",".join(node_names)
    annotations["trainctl.io/run-id"] = run_id

    spec = require_mapping(raycluster.get("spec"), "RayCluster spec")
    worker_groups = spec.get("workerGroupSpecs")
    if not isinstance(worker_groups, list) or len(worker_groups) != 1:
        raise RenderError("base manifest must contain exactly one worker group")
    worker_group = require_mapping(worker_groups[0], "Ray worker group")
    worker_count = len(node_names)
    worker_group["replicas"] = worker_count
    worker_group["minReplicas"] = worker_count
    worker_group["maxReplicas"] = worker_count

    worker_template = require_mapping(
        worker_group.get("template"),
        "Ray worker template",
    )
    worker_spec = require_mapping(
        worker_template.get("spec"),
        "Ray worker Pod spec",
    )
    if worker_selector:
        worker_spec["nodeSelector"] = dict(worker_selector)
    else:
        worker_spec.pop("nodeSelector", None)
    if runtime_class_name is None:
        worker_spec.pop("runtimeClassName", None)
    else:
        worker_spec["runtimeClassName"] = runtime_class_name
    ray_start_params = require_mapping(
        worker_group.get("rayStartParams"), "Ray worker start parameters"
    )
    ray_resources = json.dumps(
        {"NPU": devices_per_node, "trainctl_worker": 1},
        separators=(",", ":"),
    )
    ray_start_params["resources"] = json.dumps(ray_resources)
    containers = worker_spec.get("containers")
    if not isinstance(containers, list):
        raise RenderError("Ray worker Pod spec has no container list")
    worker_containers = [
        container
        for container in containers
        if isinstance(container, dict) and container.get("name") == "ray-worker"
    ]
    if len(worker_containers) != 1:
        raise RenderError("Ray worker Pod spec must have one ray-worker container")
    resources = require_mapping(
        worker_containers[0].get("resources"), "Ray worker resources"
    )
    for field in ("requests", "limits"):
        quantities = require_mapping(resources.get(field), f"Ray worker {field}")
        quantities[npu_resource] = str(devices_per_node)
    affinity = require_mapping(worker_spec.get("affinity"), "worker affinity")
    node_affinity = require_mapping(
        affinity.get("nodeAffinity"),
        "worker node affinity",
    )
    required = require_mapping(
        node_affinity.get("requiredDuringSchedulingIgnoredDuringExecution"),
        "required worker node affinity",
    )
    terms = required.get("nodeSelectorTerms")
    if not isinstance(terms, list) or len(terms) != 1:
        raise RenderError("worker affinity must contain one selector term")
    expressions = require_mapping(terms[0], "worker selector term").get(
        "matchExpressions"
    )
    if not isinstance(expressions, list):
        raise RenderError("worker selector term has no matchExpressions")
    hostname_rules = [
        expression
        for expression in expressions
        if isinstance(expression, dict)
        and expression.get("key") == "kubernetes.io/hostname"
        and expression.get("operator") == "In"
    ]
    if len(hostname_rules) != 1:
        raise RenderError("worker affinity must have one hostname In rule")
    hostname_rules[0]["values"] = list(node_names)
    update_runtime_configmap(worker_spec, runtime_configmap)

    head_group = require_mapping(spec.get("headGroupSpec"), "Ray head group")
    head_template = require_mapping(
        head_group.get("template"),
        "Ray head template",
    )
    head_spec = require_mapping(
        head_template.get("spec"),
        "Ray head Pod spec",
    )
    effective_head_selector = dict(head_selector)
    effective_head_selector["kubernetes.io/hostname"] = head_node
    head_spec["nodeSelector"] = effective_head_selector
    update_runtime_configmap(head_spec, runtime_configmap)

    if output_manifest.exists() or output_manifest.is_symlink():
        raise RenderError(f"refusing to overwrite manifest: {output_manifest}")
    output_manifest.parent.mkdir(parents=True, exist_ok=True)
    try:
        with output_manifest.open("x", encoding="utf-8") as stream:
            yaml.safe_dump_all(
                documents,
                stream,
                allow_unicode=True,
                sort_keys=False,
                explicit_start=True,
            )
    except (OSError, yaml.YAMLError) as error:
        raise RenderError(f"cannot write rendered manifest: {error}") from error


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-manifest", type=Path, required=True)
    parser.add_argument("--output-manifest", type=Path, required=True)
    parser.add_argument("--node", action="append", required=True)
    parser.add_argument("--kubectl-command", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--head-node", required=True)
    parser.add_argument("--npu-resource", required=True)
    parser.add_argument("--devices-per-node", type=int, required=True)
    parser.add_argument("--workspace-host-path", type=Path, required=True)
    parser.add_argument("--ray-head-image", required=True)
    parser.add_argument("--ray-worker-image", required=True)
    parser.add_argument(
        "--ray-image-pull-policy",
        choices=("Always", "IfNotPresent", "Never"),
        required=True,
    )
    parser.add_argument("--runtime-class-name")
    parser.add_argument("--head-selector", action="append", default=[])
    parser.add_argument("--worker-selector", action="append", default=[])
    parser.add_argument("--runtime-configmap", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        kubectl = kubectl_prefix(args.kubectl_command, args.kubeconfig)
        node_names = resolve_node_names(
            tuple(args.node),
            load_cluster_nodes(kubectl),
        )
        render_manifest(
            base_manifest=args.base_manifest.resolve(),
            output_manifest=args.output_manifest.resolve(),
            node_names=node_names,
            namespace=args.namespace,
            cluster=args.cluster,
            head_node=args.head_node,
            npu_resource=args.npu_resource,
            devices_per_node=args.devices_per_node,
            workspace_host_path=args.workspace_host_path,
            ray_head_image=args.ray_head_image,
            ray_worker_image=args.ray_worker_image,
            ray_image_pull_policy=args.ray_image_pull_policy,
            runtime_class_name=args.runtime_class_name,
            head_selector=parse_selectors(args.head_selector, "head selector"),
            worker_selector=parse_selectors(args.worker_selector, "worker selector"),
            runtime_configmap=args.runtime_configmap,
            run_id=args.run_id,
        )
    except (RenderError, OSError, UnicodeError) as error:
        print(f"STOP: RayCluster rendering failed: {error}", file=sys.stderr)
        return 1
    print(
        "PASS: rendered RayCluster for "
        f"{len(node_names)} worker(s): {args.output_manifest.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
