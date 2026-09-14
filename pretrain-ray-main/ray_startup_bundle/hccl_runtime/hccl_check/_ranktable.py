#!/usr/bin/env python3
"""Convert ClusterD hccl.json to AI Server v1.0 and verify its worker mount."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Sequence

from .ranktable.cleaner import (
    HcclCleanError,
    HcclNotReadyError,
    RANKTABLE_PROFILE,
    RANKTABLE_VERSION,
    atomic_write,
    clean_clusterd_hccl,
    hccl_json_text,
    parse_hccl_text,
)


ANNOTATION_PREFIX = "ranktable.hccl-check.local"
CONFIGMAP_SIZE_LIMIT = 950_000
DEFAULT_MOUNT_PATH = "/user/serverid/devindex/config/hccl.json"


class KubectlError(RuntimeError):
    """Raised when a kubectl operation fails."""


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be greater than zero")
    return parsed


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    file_parser = subparsers.add_parser(
        "file",
        help="convert a local ClusterD hccl.json without accessing Kubernetes",
    )
    file_parser.add_argument("--input", type=Path, required=True)
    file_parser.add_argument("--output", type=Path, required=True)

    publish_parser = subparsers.add_parser(
        "publish", help="read ClusterD and create a separate immutable ConfigMap"
    )
    publish_parser.add_argument("--namespace", "-n", required=True)
    publish_parser.add_argument("--source-configmap", required=True)
    publish_parser.add_argument("--target-configmap", required=True)
    publish_parser.add_argument(
        "--kubectl-command",
        default="kubectl",
        help="kubectl argv prefix, for example 'k3s kubectl' or 'kubectl'",
    )
    publish_parser.add_argument("--kubeconfig", type=Path)
    publish_parser.add_argument("--wait-timeout", type=_positive_int, default=300)
    publish_parser.add_argument("--poll-interval", type=_positive_int, default=2)
    publish_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the derived ConfigMap instead of creating it",
    )

    prepare_parser = subparsers.add_parser(
        "prepare",
        help="wait for ClusterD, publish the cleaned ConfigMap, and verify mounts",
    )
    prepare_parser.add_argument("--namespace", "-n", required=True)
    prepare_parser.add_argument(
        "--raycluster",
        required=True,
        help="RayCluster name; source and target ConfigMap names are derived from it",
    )
    prepare_parser.add_argument(
        "--kubectl-command",
        default="kubectl",
        help="kubectl argv prefix, for example 'k3s kubectl' or 'kubectl'",
    )
    prepare_parser.add_argument("--kubeconfig", type=Path)
    prepare_parser.add_argument("--wait-timeout", type=_positive_int, default=300)
    prepare_parser.add_argument("--poll-interval", type=_positive_int, default=2)
    prepare_parser.add_argument("--mount-timeout", type=_positive_int, default=120)
    prepare_parser.add_argument("--container", default="ray-worker")
    prepare_parser.add_argument("--mount-path", default=DEFAULT_MOUNT_PATH)
    return parser.parse_args(argv)


@dataclass(frozen=True)
class PublishResult:
    clean_result: Any
    cleaned_text: str
    status: str
    target: str
    manifest: dict[str, object]


def _summary(result: Any) -> dict[str, object]:
    return {
        "status": "validated",
        "ranktable_profile": result.profile,
        "ranktable_version": result.ranktable_version,
        "rank_offset_removed": result.rank_offset,
        "server_count": result.server_count,
        "world_size": result.world_size,
        "pod_names": list(result.pod_names),
    }


def clean_text(text: str) -> tuple[Any, str]:
    hccl = parse_hccl_text(text)
    result = clean_clusterd_hccl(hccl)
    return result, hccl_json_text(result.hccl)


class Kubectl:
    def __init__(self, command: str, kubeconfig: Path | None) -> None:
        self.argv = shlex.split(command)
        if not self.argv:
            raise KubectlError("kubectl command must not be empty")
        self.env = os.environ.copy()
        if kubeconfig is not None:
            self.env["KUBECONFIG"] = str(kubeconfig)

    def run(
        self, arguments: list[str], *, input_text: str | None = None, check: bool = True
    ) -> subprocess.CompletedProcess[str]:
        process = subprocess.run(
            [*self.argv, *arguments],
            input=input_text,
            text=True,
            capture_output=True,
            env=self.env,
            check=False,
        )
        if check and process.returncode != 0:
            detail = process.stderr.strip() or process.stdout.strip()
            raise KubectlError(f"kubectl {' '.join(arguments)} failed: {detail}")
        return process

    def get_configmap(self, namespace: str, name: str) -> dict[str, object] | None:
        return self.get_object("configmap", namespace, name)

    def get_object(
        self, resource: str, namespace: str, name: str
    ) -> dict[str, object] | None:
        process = self.run(
            [
                "get",
                resource,
                name,
                "--namespace",
                namespace,
                "--output",
                "json",
                "--ignore-not-found",
            ]
        )
        if not process.stdout.strip():
            return None
        try:
            value = json.loads(process.stdout)
        except json.JSONDecodeError as error:
            raise KubectlError(f"kubectl returned invalid JSON: {error}") from error
        if not isinstance(value, dict):
            raise KubectlError(f"kubectl {resource} response is not an object")
        return value

    def create(self, manifest: dict[str, object]) -> subprocess.CompletedProcess[str]:
        return self.run(
            ["create", "--filename", "-"],
            input_text=json.dumps(manifest, ensure_ascii=False),
            check=False,
        )

    def read_pod_file(
        self, namespace: str, pod: str, container: str, path: str
    ) -> subprocess.CompletedProcess[str]:
        return self.run(
            [
                "exec",
                "--namespace",
                namespace,
                pod,
                "--container",
                container,
                "--",
                "cat",
                path,
            ],
            check=False,
        )


def _metadata(configmap: dict[str, object]) -> dict[str, object]:
    value = configmap.get("metadata")
    if not isinstance(value, dict):
        raise HcclCleanError("source ConfigMap metadata is missing")
    return value


def _source_text(configmap: dict[str, object]) -> str:
    if configmap.get("kind") != "ConfigMap":
        raise HcclCleanError("source object is not a ConfigMap")
    data = configmap.get("data")
    if not isinstance(data, dict):
        raise HcclNotReadyError("source ConfigMap data is not ready")
    text = data.get("hccl.json")
    if not isinstance(text, str) or not text:
        raise HcclNotReadyError("source ConfigMap does not contain data['hccl.json']")
    return text


def _identity(configmap: dict[str, object], source_text: str) -> dict[str, str]:
    metadata = _metadata(configmap)
    identity: dict[str, str] = {}
    for key in ("name", "namespace", "uid", "resourceVersion"):
        value = metadata.get(key)
        if not isinstance(value, str) or not value:
            raise HcclCleanError(f"source ConfigMap metadata.{key} is missing")
        identity[key] = value
    identity["sha256"] = hashlib.sha256(source_text.encode("utf-8")).hexdigest()
    return identity


def _target_manifest(
    *,
    namespace: str,
    name: str,
    source_identity: dict[str, str],
    cleaned_text: str,
) -> dict[str, object]:
    output_sha256 = hashlib.sha256(cleaned_text.encode("utf-8")).hexdigest()
    manifest: dict[str, object] = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": name,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "hccl-ranktable-sanitizer",
                "app.kubernetes.io/part-of": "hccl-check",
            },
            "annotations": {
                f"{ANNOTATION_PREFIX}/source-configmap": source_identity["name"],
                f"{ANNOTATION_PREFIX}/source-uid": source_identity["uid"],
                f"{ANNOTATION_PREFIX}/source-resource-version": source_identity[
                    "resourceVersion"
                ],
                f"{ANNOTATION_PREFIX}/source-sha256": source_identity["sha256"],
                f"{ANNOTATION_PREFIX}/output-sha256": output_sha256,
                f"{ANNOTATION_PREFIX}/output-profile": RANKTABLE_PROFILE,
                f"{ANNOTATION_PREFIX}/ranktable-version": RANKTABLE_VERSION,
            },
            "ownerReferences": [
                {
                    "apiVersion": "v1",
                    "kind": "ConfigMap",
                    "name": source_identity["name"],
                    "uid": source_identity["uid"],
                    "controller": False,
                    "blockOwnerDeletion": False,
                }
            ],
        },
        "immutable": True,
        "data": {"hccl.json": cleaned_text},
    }
    size = len(json.dumps(manifest, ensure_ascii=False).encode("utf-8"))
    if size > CONFIGMAP_SIZE_LIMIT:
        raise HcclCleanError(
            f"derived ConfigMap is {size} bytes, exceeding the safety limit"
        )
    return manifest


def _same_target(existing: dict[str, object], desired: dict[str, object]) -> bool:
    existing_data = existing.get("data")
    desired_data = desired.get("data")
    existing_metadata = existing.get("metadata")
    desired_metadata = desired.get("metadata")
    if not isinstance(existing_metadata, dict) or not isinstance(
        desired_metadata, dict
    ):
        return False
    existing_annotations = existing_metadata.get("annotations")
    desired_annotations = desired_metadata.get("annotations")
    if not isinstance(existing_annotations, dict) or not isinstance(
        desired_annotations, dict
    ):
        return False
    keys = {
        f"{ANNOTATION_PREFIX}/source-configmap",
        f"{ANNOTATION_PREFIX}/source-uid",
        f"{ANNOTATION_PREFIX}/source-resource-version",
        f"{ANNOTATION_PREFIX}/source-sha256",
        f"{ANNOTATION_PREFIX}/output-sha256",
        f"{ANNOTATION_PREFIX}/output-profile",
        f"{ANNOTATION_PREFIX}/ranktable-version",
    }
    return existing_data == desired_data and all(
        existing_annotations.get(key) == desired_annotations.get(key) for key in keys
    )


def _same_output(existing: dict[str, object], desired: dict[str, object]) -> bool:
    """Return whether a managed target contains the same HCCL input bytes.

    ClusterD may update source-only metadata (for example Worker Pod names and
    the ConfigMap resourceVersion) when a Ray Worker is recreated.  Those
    fields are deliberately removed by the cleaner.  An immutable target from
    an earlier publication therefore remains valid when the newly cleaned
    ``hccl.json`` is byte-identical, even though the strict source audit
    annotations no longer match.

    This weaker comparison is used only by the unified ``prepare`` lifecycle.
    It still requires a target created by this tool for the same source name
    and output profile.  A genuinely different rank table remains an error.
    """

    if existing.get("kind") != "ConfigMap" or existing.get("immutable") is not True:
        return False
    if existing.get("data") != desired.get("data"):
        return False

    existing_metadata = existing.get("metadata")
    desired_metadata = desired.get("metadata")
    if not isinstance(existing_metadata, dict) or not isinstance(
        desired_metadata, dict
    ):
        return False
    existing_labels = existing_metadata.get("labels")
    desired_labels = desired_metadata.get("labels")
    existing_annotations = existing_metadata.get("annotations")
    desired_annotations = desired_metadata.get("annotations")
    if not isinstance(existing_labels, dict) or not isinstance(desired_labels, dict):
        return False
    if not isinstance(existing_annotations, dict) or not isinstance(
        desired_annotations, dict
    ):
        return False

    label_keys = {
        "app.kubernetes.io/managed-by",
        "app.kubernetes.io/part-of",
    }
    annotation_keys = {
        f"{ANNOTATION_PREFIX}/source-configmap",
        f"{ANNOTATION_PREFIX}/output-sha256",
        f"{ANNOTATION_PREFIX}/output-profile",
        f"{ANNOTATION_PREFIX}/ranktable-version",
    }
    return all(
        existing_labels.get(key) == desired_labels.get(key) for key in label_keys
    ) and all(
        existing_annotations.get(key) == desired_annotations.get(key)
        for key in annotation_keys
    )


def run_file(args: argparse.Namespace) -> None:
    source = args.input.resolve()
    target = args.output.resolve()
    if source == target:
        raise HcclCleanError("input and output paths must be different")
    result, cleaned_text = clean_text(source.read_text(encoding="utf-8"))
    atomic_write(target, cleaned_text)
    print(json.dumps(_summary(result), ensure_ascii=False, indent=2))


def _publish(
    *,
    kubectl: Kubectl,
    namespace: str,
    source_configmap: str,
    target_configmap: str,
    wait_timeout: int,
    poll_interval: int,
    dry_run: bool = False,
    allow_equivalent_output: bool = False,
) -> PublishResult:
    if source_configmap == target_configmap:
        raise HcclCleanError("source and target ConfigMap names must be different")

    deadline = time.monotonic() + wait_timeout
    last_wait_reason = "source ConfigMap does not exist"

    while time.monotonic() < deadline:
        source = kubectl.get_configmap(namespace, source_configmap)
        if source is None:
            time.sleep(poll_interval)
            continue
        try:
            source_text = _source_text(source)
            result, cleaned_text = clean_text(source_text)
        except HcclNotReadyError as error:
            last_wait_reason = str(error)
            time.sleep(poll_interval)
            continue

        source_identity = _identity(source, source_text)
        desired = _target_manifest(
            namespace=namespace,
            name=target_configmap,
            source_identity=source_identity,
            cleaned_text=cleaned_text,
        )

        # Re-read immediately before publication so an updating ClusterD object
        # cannot result in an already-stale derived ConfigMap.
        latest = kubectl.get_configmap(namespace, source_configmap)
        if latest is None:
            last_wait_reason = "source ConfigMap disappeared before publication"
            time.sleep(poll_interval)
            continue
        latest_text = _source_text(latest)
        if _identity(latest, latest_text) != source_identity:
            last_wait_reason = "source ConfigMap changed during validation"
            time.sleep(poll_interval)
            continue

        if dry_run:
            return PublishResult(
                clean_result=result,
                cleaned_text=cleaned_text,
                status="validated",
                target=target_configmap,
                manifest=desired,
            )

        existing = kubectl.get_configmap(namespace, target_configmap)
        if existing is not None:
            if _same_target(existing, desired):
                return PublishResult(
                    clean_result=result,
                    cleaned_text=cleaned_text,
                    status="already-published",
                    target=target_configmap,
                    manifest=desired,
                )
            if allow_equivalent_output and _same_output(existing, desired):
                return PublishResult(
                    clean_result=result,
                    cleaned_text=cleaned_text,
                    status="already-published-equivalent",
                    target=target_configmap,
                    manifest=desired,
                )
            raise HcclCleanError(
                f"target ConfigMap {target_configmap!r} already exists with "
                "different data or source identity; use a unique target name "
                "for each validation run"
            )

        create = kubectl.create(desired)
        if create.returncode == 0:
            return PublishResult(
                clean_result=result,
                cleaned_text=cleaned_text,
                status="published",
                target=target_configmap,
                manifest=desired,
            )

        # Another publisher may have won the create race.  Apply the same
        # strict-or-output-equivalent policy used by the pre-create check.
        existing = kubectl.get_configmap(namespace, target_configmap)
        if existing is not None:
            if _same_target(existing, desired):
                return PublishResult(
                    clean_result=result,
                    cleaned_text=cleaned_text,
                    status="already-published",
                    target=target_configmap,
                    manifest=desired,
                )
            if allow_equivalent_output and _same_output(existing, desired):
                return PublishResult(
                    clean_result=result,
                    cleaned_text=cleaned_text,
                    status="already-published-equivalent",
                    target=target_configmap,
                    manifest=desired,
                )
        detail = create.stderr.strip() or create.stdout.strip()
        raise KubectlError(f"failed to create target ConfigMap: {detail}")

    raise HcclNotReadyError(
        f"timed out after {wait_timeout}s waiting for ClusterD: {last_wait_reason}"
    )


def run_publish(args: argparse.Namespace) -> None:
    kubectl = Kubectl(args.kubectl_command, args.kubeconfig)
    published = _publish(
        kubectl=kubectl,
        namespace=args.namespace,
        source_configmap=args.source_configmap,
        target_configmap=args.target_configmap,
        wait_timeout=args.wait_timeout,
        poll_interval=args.poll_interval,
        dry_run=args.dry_run,
    )
    if args.dry_run:
        print(json.dumps(published.manifest, ensure_ascii=False, indent=2))
        print(
            json.dumps(_summary(published.clean_result), ensure_ascii=False),
            file=sys.stderr,
        )
        return
    summary = _summary(published.clean_result)
    summary.update({"status": published.status, "target": published.target})
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def _configmap_names(raycluster: str) -> tuple[str, str]:
    return f"job-summary-{raycluster}", f"hccl-sanitized-{raycluster}"


def _declared_mount_paths(
    raycluster: dict[str, object], target_configmap: str, container_name: str
) -> set[str]:
    """Return worker paths backed by target_configmap's hccl.json key."""

    paths: set[str] = set()
    spec = raycluster.get("spec")
    if not isinstance(spec, dict):
        return paths
    groups = spec.get("workerGroupSpecs")
    if not isinstance(groups, list):
        return paths

    for group in groups:
        if not isinstance(group, dict):
            continue
        template = group.get("template")
        pod_spec = template.get("spec") if isinstance(template, dict) else None
        if not isinstance(pod_spec, dict):
            continue
        volumes = pod_spec.get("volumes")
        containers = pod_spec.get("containers")
        if not isinstance(volumes, list) or not isinstance(containers, list):
            continue

        volume_items: dict[str, set[str]] = {}
        for volume in volumes:
            if not isinstance(volume, dict):
                continue
            configmap = volume.get("configMap")
            name = volume.get("name")
            if not isinstance(configmap, dict) or not isinstance(name, str):
                continue
            if configmap.get("name") != target_configmap:
                continue
            items = configmap.get("items")
            if not isinstance(items, list):
                volume_items[name] = {"hccl.json"}
                continue
            item_paths = {
                item["path"]
                for item in items
                if isinstance(item, dict)
                and item.get("key") == "hccl.json"
                and isinstance(item.get("path"), str)
            }
            if item_paths:
                volume_items[name] = item_paths

        for container in containers:
            if (
                not isinstance(container, dict)
                or container.get("name") != container_name
            ):
                continue
            mounts = container.get("volumeMounts")
            if not isinstance(mounts, list):
                continue
            for mount in mounts:
                if not isinstance(mount, dict):
                    continue
                item_paths = volume_items.get(mount.get("name"))
                mount_path = mount.get("mountPath")
                if not item_paths or not isinstance(mount_path, str):
                    continue
                sub_path = mount.get("subPath")
                if sub_path == "hccl.json":
                    paths.add(str(PurePosixPath(mount_path)))
                elif sub_path is None:
                    base = PurePosixPath(mount_path)
                    paths.update(str(base / item_path) for item_path in item_paths)
    return paths


def _verify_worker_mounts(
    *,
    kubectl: Kubectl,
    namespace: str,
    pod_names: tuple[str, ...],
    container: str,
    mount_path: str,
    expected_text: str,
    timeout: int,
    poll_interval: int,
) -> None:
    deadline = time.monotonic() + timeout
    last_reasons: dict[str, str] = {}
    expected_sha = hashlib.sha256(expected_text.encode("utf-8")).hexdigest()

    while time.monotonic() < deadline:
        pending: list[str] = []
        for pod in pod_names:
            process = kubectl.read_pod_file(namespace, pod, container, mount_path)
            if process.returncode != 0:
                pending.append(pod)
                last_reasons[pod] = process.stderr.strip() or process.stdout.strip()
                continue
            if process.stdout != expected_text:
                pending.append(pod)
                actual_sha = hashlib.sha256(process.stdout.encode("utf-8")).hexdigest()
                last_reasons[pod] = (
                    f"mounted content sha256 {actual_sha} != expected {expected_sha}"
                )
        if not pending:
            return
        time.sleep(poll_interval)

    detail = "; ".join(
        f"{pod}: {last_reasons.get(pod, 'not ready')}" for pod in pod_names
    )
    raise HcclNotReadyError(
        f"timed out after {timeout}s waiting for worker mounts: {detail}"
    )


def run_prepare(args: argparse.Namespace) -> None:
    if not args.mount_path.startswith("/"):
        raise HcclCleanError("mount path must be absolute")

    source_configmap, target_configmap = _configmap_names(args.raycluster)
    kubectl = Kubectl(args.kubectl_command, args.kubeconfig)
    raycluster = kubectl.get_object("raycluster", args.namespace, args.raycluster)
    if raycluster is None:
        raise HcclNotReadyError(
            f"RayCluster {args.namespace}/{args.raycluster} does not exist"
        )
    declared_paths = _declared_mount_paths(
        raycluster, target_configmap, args.container
    )
    if args.mount_path not in declared_paths:
        shown = ", ".join(sorted(declared_paths)) or "none"
        raise HcclCleanError(
            f"RayCluster does not mount {target_configmap}/hccl.json at "
            f"{args.mount_path} in container {args.container}; declared paths: {shown}"
        )

    published = _publish(
        kubectl=kubectl,
        namespace=args.namespace,
        source_configmap=source_configmap,
        target_configmap=target_configmap,
        wait_timeout=args.wait_timeout,
        poll_interval=args.poll_interval,
        allow_equivalent_output=True,
    )
    _verify_worker_mounts(
        kubectl=kubectl,
        namespace=args.namespace,
        pod_names=published.clean_result.pod_names,
        container=args.container,
        mount_path=args.mount_path,
        expected_text=published.cleaned_text,
        timeout=args.mount_timeout,
        poll_interval=args.poll_interval,
    )

    summary = _summary(published.clean_result)
    summary.update(
        {
            "status": "ready",
            "publication": published.status,
            "source": source_configmap,
            "target": target_configmap,
            "mount_path": args.mount_path,
            "mounted_pods": list(published.clean_result.pod_names),
            "ranktable_sha256": hashlib.sha256(
                published.cleaned_text.encode("utf-8")
            ).hexdigest(),
        }
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        if args.command == "file":
            run_file(args)
        elif args.command == "publish":
            run_publish(args)
        else:
            run_prepare(args)
    except (HcclCleanError, KubectlError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
