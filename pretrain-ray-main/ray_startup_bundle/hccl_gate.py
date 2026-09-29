#!/usr/bin/env python3
"""Run topology discovery, RankTable preparation, and HCCL on a ready RayCluster."""

from __future__ import annotations

import argparse
import datetime as dt
import json
from pathlib import Path
import re
import secrets
import shlex
import subprocess
import sys
import time
from typing import Any, Mapping, Sequence


BUNDLE_DIR = Path(__file__).resolve().parent
DEFAULT_EVIDENCE_ROOT = BUNDLE_DIR.parent / "log" / "hccl-startup"
EVIDENCE_FILES = (
    "plan.json",
    "01-ping.json",
    "02-ranktable.json",
    "03-hccl.json",
    "result.json",
)
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")


class HcclGateError(RuntimeError):
    pass


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise HcclGateError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def run_command(
    command: Sequence[str],
    *,
    timeout: int,
    print_output: bool = True,
) -> subprocess.CompletedProcess[str]:
    try:
        result = subprocess.run(
            list(command),
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout,
            shell=False,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise HcclGateError(f"cannot execute {shlex.join(command)}: {error}") from error
    if print_output:
        if result.stdout.strip():
            print(result.stdout.rstrip())
        if result.stderr.strip():
            print(result.stderr.rstrip(), file=sys.stderr)
    return result


def pod_ready(pod: Mapping[str, Any]) -> bool:
    return any(
        condition.get("type") == "Ready" and condition.get("status") == "True"
        for condition in pod.get("status", {}).get("conditions", [])
    )


def find_ready_head(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
) -> str:
    selector = f"ray.io/cluster={cluster},ray.io/node-type=head"
    result = run_command(
        [
            *kubectl,
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            selector,
            "-o",
            "json",
        ],
        timeout=30,
        print_output=False,
    )
    if result.returncode != 0:
        raise HcclGateError(
            f"cannot read Ray head Pod: {result.stderr.strip() or result.stdout.strip()}"
        )
    try:
        pods = json.loads(result.stdout).get("items", [])
    except json.JSONDecodeError as error:
        raise HcclGateError(f"kubectl returned invalid Pod JSON: {error}") from error
    if len(pods) != 1:
        raise HcclGateError(
            f"expected exactly one Ray head Pod, discovered {len(pods)}"
        )
    if not pod_ready(pods[0]):
        raise HcclGateError("the discovered Ray head Pod is not Ready")
    return str(pods[0].get("metadata", {}).get("name", ""))


def new_run_id() -> str:
    timestamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"startup-{timestamp}-{secrets.token_hex(4)}"


def pipeline_exec_command(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    head_pod: str,
    run_id: str,
    expected_workers: int,
    expected_world_size: int | None,
) -> list[str]:
    output_dir = f"/evidence/{run_id}"
    command = [
        *kubectl,
        "exec",
        "-n",
        namespace,
        head_pod,
        "-c",
        "ray-head",
        "--",
        "python",
        "-m",
        "hccl_check",
        "--execute",
        "--namespace",
        namespace,
        "--raycluster",
        cluster,
        "--ray-address",
        "auto",
        "--resource",
        "NPU",
        "--rank-table-path",
        "/user/serverid/devindex/config/hccl.json",
        "--output-dir",
        output_dir,
        "--expected-workers",
        str(expected_workers),
        "--probe-binary",
        "/opt/trainctl-runtime/bin/ranktable_allreduce_probe",
        "--kubectl-command",
        "/usr/local/bin/kubectl",
        "--kubeconfig",
        "/etc/trainctl/kubeconfig",
        "--container",
        "ray-worker",
        "--server-id-env",
        "HOST_IP",
        "--cann-env-script",
        "/usr/local/Ascend/cann/ascend-toolkit/set_env.sh",
        "--run-id",
        run_id,
    ]
    if expected_world_size is not None:
        command.extend(("--expected-world-size", str(expected_world_size)))
    return command


def export_evidence(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    run_id: str,
    evidence_root: Path,
    pipeline_result: subprocess.CompletedProcess[str] | None,
) -> tuple[Path, dict[str, Any] | None, tuple[str, ...]]:
    destination = evidence_root.resolve() / run_id
    try:
        destination.mkdir(parents=True, exist_ok=False)
    except OSError as error:
        raise HcclGateError(
            f"cannot create local HCCL evidence directory {destination}: {error}"
        ) from error

    if pipeline_result is not None:
        (destination / "pipeline.stdout.log").write_text(
            pipeline_result.stdout,
            encoding="utf-8",
        )
        (destination / "pipeline.stderr.log").write_text(
            pipeline_result.stderr,
            encoding="utf-8",
        )

    copied: list[str] = []
    for name in EVIDENCE_FILES:
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
                "cat",
                f"/evidence/{run_id}/{name}",
            ],
            timeout=60,
            print_output=False,
        )
        if result.returncode != 0:
            continue
        (destination / name).write_text(result.stdout, encoding="utf-8")
        copied.append(name)

    result_payload: dict[str, Any] | None = None
    result_path = destination / "result.json"
    if result_path.is_file():
        try:
            parsed = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise HcclGateError(f"exported result.json is invalid: {error}") from error
        if isinstance(parsed, dict):
            result_payload = parsed
    return destination, result_payload, tuple(copied)


def retain_then_delete_failed_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    retention_seconds: int,
) -> None:
    if retention_seconds < 0:
        print("FAILED RESOURCE RETENTION: RayCluster is retained indefinitely.")
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
        f"{namespace}/{cluster}; Namespace and evidence are retained."
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


def run_hccl_gate(
    *,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    expected_workers: int,
    expected_world_size: int | None,
    timeout_seconds: int,
    evidence_root: Path,
    failure_retention_seconds: int,
    run_id: str | None = None,
    defer_failure_cleanup: bool = False,
) -> Path:
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    head_pod = find_ready_head(
        kubectl,
        namespace=namespace,
        cluster=cluster,
    )
    run_id = run_id or new_run_id()
    if RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise HcclGateError("run ID contains unsupported characters")
    command = pipeline_exec_command(
        kubectl,
        namespace=namespace,
        cluster=cluster,
        head_pod=head_pod,
        run_id=run_id,
        expected_workers=expected_workers,
        expected_world_size=expected_world_size,
    )
    print(
        "=== HCCL gate: worker discovery -> RankTable -> HCCL AllReduce ==="
    )
    print("$ " + shlex.join(command))

    pipeline_result: subprocess.CompletedProcess[str] | None = None
    failure: HcclGateError | None = None
    try:
        pipeline_result = run_command(command, timeout=timeout_seconds)
        if pipeline_result.returncode != 0:
            failure = HcclGateError(
                f"HCCL pipeline exited with status {pipeline_result.returncode}"
            )
    except HcclGateError as error:
        failure = error

    destination, result_payload, copied = export_evidence(
        kubectl,
        namespace=namespace,
        head_pod=head_pod,
        run_id=run_id,
        evidence_root=evidence_root,
        pipeline_result=pipeline_result,
    )
    print(f"HCCL evidence exported to {destination}")

    if failure is None:
        missing = sorted(set(EVIDENCE_FILES) - set(copied))
        if missing:
            failure = HcclGateError(
                "HCCL reported success but evidence is incomplete: "
                + ", ".join(missing)
            )
        elif (
            result_payload is None
            or result_payload.get("status") != "PASS"
            or result_payload.get("mode") != "execute"
        ):
            failure = HcclGateError("result.json did not report execute/PASS")

    if failure is not None:
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
        raise failure

    assert result_payload is not None
    print(
        "PASS: topology discovery, RankTable validation, and HCCL passed "
        f"(ranktable_sha256={result_payload.get('ranktable_sha256')})."
    )
    return destination


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kubectl-command", default="kubectl")
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--namespace", required=True)
    parser.add_argument("--cluster", required=True)
    parser.add_argument("--expected-workers", type=int, required=True)
    parser.add_argument(
        "--expected-world-size",
        type=int,
        help="optional strict rank count; omit to discover it from this run's RankTable",
    )
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--evidence-root", type=Path, default=DEFAULT_EVIDENCE_ROOT)
    parser.add_argument(
        "--run-id",
        help="optional caller-owned run ID; omitted generates a unique ID",
    )
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
        or (
            args.expected_world_size is not None
            and args.expected_world_size <= 0
        )
        or args.failure_retention_seconds < -1
    ):
        print(
            "STOP: workers/timeout/world size must be positive and "
            "failure retention must be -1 or non-negative",
            file=sys.stderr,
        )
        return 1
    try:
        run_hccl_gate(
            kubectl_command=args.kubectl_command,
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            cluster=args.cluster,
            expected_workers=args.expected_workers,
            expected_world_size=args.expected_world_size,
            timeout_seconds=args.timeout_seconds,
            evidence_root=args.evidence_root,
            failure_retention_seconds=args.failure_retention_seconds,
            run_id=args.run_id,
            defer_failure_cleanup=args.defer_failure_cleanup,
        )
        return 0
    except HcclGateError as error:
        print(f"STOP: HCCL gate failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(
            "STOP: interrupted; existing resources were left in place.",
            file=sys.stderr,
        )
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
