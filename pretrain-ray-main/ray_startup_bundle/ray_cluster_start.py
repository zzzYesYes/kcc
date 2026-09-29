#!/usr/bin/env python3
"""Apply one RayCluster manifest and wait for its Pods to become ready."""

from __future__ import annotations

import argparse
import base64
import gzip
import hashlib
import io
import json
from pathlib import Path
import shlex
import subprocess
import sys
import tarfile
import time
from typing import Any, Mapping, Sequence


RUNTIME_ARCHIVE_KEY = "hccl-check-src.tgz"
MAX_RUNTIME_ARCHIVE_BYTES = 900_000
REQUIRED_RUNTIME_FILES = (
    "hccl_check/__init__.py",
    "hccl_check/__main__.py",
    "hccl_check/pipeline.py",
    "hccl_check/_ping.py",
    "hccl_check/_ranktable.py",
    "hccl_check/_hccl.py",
    "hccl_check/ranktable/__init__.py",
    "hccl_check/ranktable/cleaner.py",
    "native/Makefile",
    "native/src/ranktable_allreduce_probe.cc",
)

FATAL_WAITING_REASONS = {
    "CreateContainerConfigError",
    "CreateContainerError",
    "CrashLoopBackOff",
    "ErrImagePull",
    "ImagePullBackOff",
    "InvalidImageName",
    "RunContainerError",
}


class StartError(RuntimeError):
    pass


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise StartError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def run_command(
    command: Sequence[str],
    *,
    timeout: int,
    print_output: bool = True,
    input_text: str | None = None,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            input=input_text,
            text=True,
            timeout=timeout,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise StartError(f"cannot execute {shlex.join(command)}: {error}") from error
    if print_output:
        if result.stdout.strip():
            print(result.stdout.rstrip())
        if result.stderr.strip():
            print(result.stderr.rstrip(), file=sys.stderr)
    return result


def build_runtime_archive(source_dir: Path) -> tuple[bytes, str]:
    """Build a deterministic archive containing only the reviewed HCCL files."""

    if not source_dir.is_dir() or source_dir.is_symlink():
        raise StartError(f"HCCL runtime source is not a regular directory: {source_dir}")

    files: list[tuple[str, bytes]] = []
    for relative_name in REQUIRED_RUNTIME_FILES:
        path = source_dir / relative_name
        if not path.is_file() or path.is_symlink():
            raise StartError(
                f"required HCCL runtime file is missing or unsafe: {path}"
            )
        try:
            payload = path.read_bytes()
        except OSError as error:
            raise StartError(f"cannot read HCCL runtime file {path}: {error}") from error
        files.append((relative_name, payload))

    compressed = io.BytesIO()
    with gzip.GzipFile(
        filename="",
        mode="wb",
        fileobj=compressed,
        mtime=0,
    ) as gzip_stream:
        with tarfile.open(
            fileobj=gzip_stream,
            mode="w|",
            format=tarfile.USTAR_FORMAT,
        ) as archive:
            for relative_name, payload in files:
                info = tarfile.TarInfo(relative_name)
                info.size = len(payload)
                info.mode = 0o644
                info.mtime = 0
                info.uid = 0
                info.gid = 0
                info.uname = ""
                info.gname = ""
                archive.addfile(info, io.BytesIO(payload))

    archive_bytes = compressed.getvalue()
    if not archive_bytes or len(archive_bytes) > MAX_RUNTIME_ARCHIVE_BYTES:
        raise StartError(
            "HCCL runtime archive size is outside the accepted range: "
            f"{len(archive_bytes)} bytes"
        )
    return archive_bytes, hashlib.sha256(archive_bytes).hexdigest()


def apply_json_manifest(
    kubectl: Sequence[str],
    manifest: Mapping[str, Any],
    *,
    description: str,
) -> None:
    result = run_command(
        [*kubectl, "apply", "-f", "-"],
        timeout=120,
        input_text=json.dumps(manifest, ensure_ascii=False),
    )
    if result.returncode != 0:
        raise StartError(f"cannot apply {description}")


def prepare_runtime_configmap(
    kubectl: Sequence[str],
    *,
    namespace: str,
    configmap: str,
    source_dir: Path,
) -> str:
    """Create/update the small source ConfigMap before Ray Pods are scheduled."""

    archive, digest = build_runtime_archive(source_dir)
    namespace_manifest = {
        "apiVersion": "v1",
        "kind": "Namespace",
        "metadata": {
            "name": namespace,
            "labels": {"app.kubernetes.io/part-of": "pretrain-ray-platform"},
        },
    }
    apply_json_manifest(
        kubectl,
        namespace_manifest,
        description=f"Namespace {namespace}",
    )

    configmap_manifest = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": configmap,
            "namespace": namespace,
            "labels": {
                "app.kubernetes.io/managed-by": "pretrain-ray-startup",
                "app.kubernetes.io/part-of": "pretrain-ray-platform",
            },
            "annotations": {
                "trainctl.io/archive-layout": "hccl_check/,native/",
                "trainctl.io/archive-sha256": digest,
            },
        },
        "binaryData": {
            RUNTIME_ARCHIVE_KEY: base64.b64encode(archive).decode("ascii")
        },
    }
    apply_json_manifest(
        kubectl,
        configmap_manifest,
        description=f"runtime ConfigMap {namespace}/{configmap}",
    )
    print(
        "PASS: HCCL/RankTable runtime prepared "
        f"(sha256={digest}, bytes={len(archive)})."
    )
    return digest


def pod_ready(pod: Mapping[str, Any]) -> bool:
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def pod_problem(pod: Mapping[str, Any]) -> str | None:
    metadata = pod.get("metadata", {})
    name = metadata.get("name", "unknown")
    status = pod.get("status", {})
    if status.get("phase") == "Failed":
        return f"{name}: Pod entered Failed phase"
    statuses = [
        *status.get("initContainerStatuses", []),
        *status.get("containerStatuses", []),
    ]
    for container in statuses:
        waiting = container.get("state", {}).get("waiting", {})
        reason = waiting.get("reason")
        if reason in FATAL_WAITING_REASONS:
            message = waiting.get("message", "")
            suffix = f": {message}" if message else ""
            return f"{name}/{container.get('name', 'container')}: {reason}{suffix}"
    return None


def pod_status_line(pod: Mapping[str, Any]) -> str:
    metadata = pod.get("metadata", {})
    labels = metadata.get("labels", {})
    status = pod.get("status", {})
    return (
        f"{metadata.get('name', 'unknown')} "
        f"type={labels.get('ray.io/node-type', '?')} "
        f"node={pod.get('spec', {}).get('nodeName') or '-'} "
        f"phase={status.get('phase', 'Pending')} "
        f"ready={pod_ready(pod)}"
    )


def wait_for_ray_pods(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    expected_workers: int,
    timeout_seconds: int,
    poll_seconds: float = 5.0,
) -> str:
    deadline = time.monotonic() + timeout_seconds
    previous: tuple[str, ...] | None = None
    selector = f"ray.io/cluster={cluster}"

    while time.monotonic() < deadline:
        command = [
            *kubectl,
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            selector,
            "-o",
            "json",
        ]
        result = run_command(command, timeout=30, print_output=False)
        if result.returncode != 0:
            raise StartError(
                f"cannot read Ray Pods: {result.stderr.strip() or result.stdout.strip()}"
            )
        try:
            pods = json.loads(result.stdout).get("items", [])
        except json.JSONDecodeError as error:
            raise StartError(f"kubectl returned invalid Pod JSON: {error}") from error

        lines = tuple(sorted(pod_status_line(pod) for pod in pods))
        if lines != previous:
            print("=== Ray Pod status ===")
            if lines:
                for line in lines:
                    print(line)
            else:
                print("waiting for KubeRay to create Pods")
            previous = lines

        for pod in pods:
            problem = pod_problem(pod)
            if problem is not None:
                raise StartError(problem)

        heads = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("labels", {}).get("ray.io/node-type")
            == "head"
        ]
        workers = [
            pod
            for pod in pods
            if pod.get("metadata", {}).get("labels", {}).get("ray.io/node-type")
            == "worker"
        ]
        if (
            len(heads) == 1
            and len(workers) == expected_workers
            and all(pod_ready(pod) for pod in [*heads, *workers])
        ):
            return str(heads[0]["metadata"]["name"])
        time.sleep(poll_seconds)

    raise StartError(
        f"Ray Pods did not become ready within {timeout_seconds} seconds; "
        "resources were left in place for inspection"
    )


def retain_then_delete_failed_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    retention_seconds: int,
) -> None:
    if retention_seconds < 0:
        print(
            "FAILED RESOURCE RETENTION: RayCluster is retained indefinitely."
        )
        return
    print(
        "FAILED RESOURCE RETENTION: "
        f"keeping RayCluster and Pods for {retention_seconds} seconds."
    )
    deadline = time.monotonic() + retention_seconds
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            break
        time.sleep(min(60.0, remaining))

    print(
        "FAILED RESOURCE CLEANUP: deleting RayCluster "
        f"{namespace}/{cluster}; Namespace is retained."
    )
    result = run_command(
        [
            *kubectl,
            "delete",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "--ignore-not-found=true",
            "--wait=false",
        ],
        timeout=60,
    )
    if result.returncode != 0:
        print(
            "WARNING: automatic failed-resource cleanup did not complete.",
            file=sys.stderr,
        )


def start_ray_cluster(
    *,
    manifest: Path,
    runtime_source_dir: Path,
    runtime_configmap: str,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    expected_workers: int,
    timeout_seconds: int,
    failure_retention_seconds: int,
    defer_failure_cleanup: bool = False,
) -> None:
    if not manifest.is_file() or manifest.is_symlink():
        raise StartError(f"manifest is not a regular file: {manifest}")
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)

    applied = False
    try:
        print("=== Prepare HCCL/RankTable runtime ===")
        prepare_runtime_configmap(
            kubectl,
            namespace=namespace,
            configmap=runtime_configmap,
            source_dir=runtime_source_dir,
        )

        print("=== Apply RayCluster manifest ===")
        result = run_command(
            [*kubectl, "apply", "-f", str(manifest)],
            timeout=120,
        )
        if result.returncode != 0:
            raise StartError("kubectl apply failed")
        applied = True

        head_pod = wait_for_ray_pods(
            kubectl,
            namespace=namespace,
            cluster=cluster,
            expected_workers=expected_workers,
            timeout_seconds=timeout_seconds,
        )

        print("=== Ray status ===")
        result = run_command(
            [
                *kubectl,
                "exec",
                "-n",
                namespace,
                head_pod,
                "-c",
                "ray-head",
                "--",
                "ray",
                "status",
            ],
            timeout=60,
        )
        if result.returncode != 0:
            raise StartError("Ray Pods are ready but `ray status` failed")
        print("PASS: Ray head and workers are ready.")
    except StartError:
        if applied:
            if defer_failure_cleanup:
                print(
                    "FAILED RESOURCE RETENTION: delegated to workflow caller."
                )
            else:
                retain_then_delete_failed_cluster(
                    kubectl,
                    namespace=namespace,
                    cluster=cluster,
                    retention_seconds=failure_retention_seconds,
                )
        raise


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--runtime-source-dir", type=Path, required=True)
    parser.add_argument("--runtime-configmap")
    parser.add_argument("--kubectl-command", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--expected-workers", type=int, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=1800)
    parser.add_argument(
        "--failure-retention-seconds",
        type=int,
        default=1800,
        help="seconds to retain a failed RayCluster; -1 keeps it indefinitely",
    )
    parser.add_argument(
        "--defer-failure-cleanup",
        action="store_true",
        help="leave failure retention and cleanup to a parent workflow",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    if (
        args.expected_workers <= 0
        or args.timeout_seconds <= 0
        or args.failure_retention_seconds < -1
    ):
        print(
            "STOP: expected workers/timeout must be positive and "
            "failure retention must be -1 or non-negative",
            file=sys.stderr,
        )
        return 1
    try:
        runtime_configmap = (
            args.runtime_configmap or f"{args.cluster}-hccl-runtime"
        )
        start_ray_cluster(
            manifest=args.manifest.resolve(),
            runtime_source_dir=args.runtime_source_dir.resolve(),
            runtime_configmap=runtime_configmap,
            kubectl_command=args.kubectl_command,
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            cluster=args.cluster,
            expected_workers=args.expected_workers,
            timeout_seconds=args.timeout_seconds,
            failure_retention_seconds=args.failure_retention_seconds,
            defer_failure_cleanup=args.defer_failure_cleanup,
        )
        return 0
    except StartError as error:
        print(f"STOP: Ray cluster startup failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("STOP: interrupted; existing resources were left in place.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
