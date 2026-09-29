#!/usr/bin/env python3
"""Submit injected formal training scripts to the ready Ray cluster."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
from typing import Any, Mapping, Sequence

import cluster_config


BUNDLE_DIR = Path(__file__).resolve().parent
DRIVER_PATH = BUNDLE_DIR / "ray_training_driver.py"
REMOTE_HELPER_PATH = BUNDLE_DIR / "ray_job_remote.py"
RUN_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
REMOTE_PYTHON = "/home/ray/anaconda3/bin/python"
JOB_RESPONSE_SCHEMA = "kcc-ray-job-api/v1"
SUBMISSION_RECORD_SCHEMA = "kcc-ray-job-submission/v1"
SUBMISSION_RECORD_FILENAME = "ray-job-submission.json"
TERMINAL_JOB_STATUSES = frozenset({"SUCCEEDED", "FAILED", "STOPPED"})
DEFAULT_POLL_SECONDS = 30.0
DEFAULT_STATUS_RETRY_SECONDS = 300.0
STATUS_RETRY_BACKOFF_SECONDS = (5.0, 10.0, 20.0, 30.0)


class SubmitError(RuntimeError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def require_file(path: Path, label: str) -> None:
    if not path.is_file() or path.is_symlink():
        raise SubmitError(f"{label} is not a regular file: {path}")


def load_injection(
    injection_dir: Path,
) -> tuple[Mapping[str, Any], tuple[Path, ...]]:
    manifest_path = injection_dir / "injection.json"
    require_file(manifest_path, "injection manifest")
    try:
        injection = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SubmitError(f"cannot parse injection manifest: {error}") from error
    if (
        not isinstance(injection, dict)
        or injection.get("schemaVersion") != "training-injection/v1"
    ):
        raise SubmitError("unsupported injection manifest")
    run_id = injection.get("runId")
    if not isinstance(run_id, str) or RUN_ID_PATTERN.fullmatch(run_id) is None:
        raise SubmitError("injection runId is invalid")
    topology = injection.get("topology")
    nodes = injection.get("nodes")
    if not isinstance(topology, dict) or not isinstance(nodes, list):
        raise SubmitError("injection topology or nodes are invalid")
    worker_count = topology.get("workers")
    if not isinstance(worker_count, int) or worker_count <= 0:
        raise SubmitError("injection worker count is invalid")
    if len(nodes) != worker_count:
        raise SubmitError("injection node count differs from topology")

    scripts_dir = injection_dir / "scripts"
    scripts: list[Path] = []
    ranks: set[int] = set()
    for node in nodes:
        if not isinstance(node, dict):
            raise SubmitError("injection node must be an object")
        node_rank = node.get("nodeRank")
        if (
            not isinstance(node_rank, int)
            or not 0 <= node_rank < worker_count
            or node_rank in ranks
        ):
            raise SubmitError("injection node ranks are invalid or duplicated")
        ranks.add(node_rank)
        script_name = node.get("script")
        if not isinstance(script_name, str) or Path(script_name).name != script_name:
            raise SubmitError("injection script name is invalid")
        script_path = scripts_dir / script_name
        require_file(script_path, f"node rank {node_rank} script")
        expected_sha256 = node.get("scriptSha256")
        if (
            not isinstance(expected_sha256, str)
            or SHA256_PATTERN.fullmatch(expected_sha256) is None
            or sha256_file(script_path) != expected_sha256
        ):
            raise SubmitError(f"node rank {node_rank} script digest differs")
        scripts.append(script_path)
    if ranks != set(range(worker_count)):
        raise SubmitError("injection node ranks are not contiguous")
    require_file(DRIVER_PATH, "Ray training driver")
    require_file(REMOTE_HELPER_PATH, "Ray Jobs API helper")
    return injection, tuple(scripts)


def kubectl_prefix(command_text: str, kubeconfig: Path | None) -> list[str]:
    command = shlex.split(command_text)
    if not command:
        raise SubmitError("kubectl command is empty")
    if kubeconfig is not None:
        command.extend(("--kubeconfig", str(kubeconfig)))
    return command


def run_command(
    command: Sequence[str],
    *,
    timeout: int | None,
    check: bool = True,
    print_output: bool = True,
    print_command: bool = True,
) -> subprocess.CompletedProcess[str]:
    if print_command:
        print("$ " + shlex.join(command), flush=True)
    try:
        result = subprocess.run(
            list(command),
            check=False,
            shell=False,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
    except (OSError, subprocess.TimeoutExpired) as error:
        raise SubmitError(f"cannot run {shlex.join(command)}: {error}") from error
    if print_output and result.stdout:
        print(result.stdout.rstrip())
    if print_output and result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)
    if check and result.returncode != 0:
        raise SubmitError(
            f"command failed with exit code {result.returncode}: "
            f"{shlex.join(command)}"
        )
    return result


def find_ready_head(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
) -> str:
    result = run_command(
        [
            *kubectl,
            "get",
            "pods",
            "-n",
            namespace,
            "-l",
            f"ray.io/cluster={cluster},ray.io/node-type=head",
            "-o",
            "json",
        ],
        timeout=30,
        print_output=False,
    )
    try:
        pods = json.loads(result.stdout).get("items", [])
    except (AttributeError, json.JSONDecodeError) as error:
        raise SubmitError(f"cannot parse Ray head Pod response: {error}") from error
    ready = [
        pod
        for pod in pods
        if any(
            condition.get("type") == "Ready"
            and condition.get("status") == "True"
            for condition in pod.get("status", {}).get("conditions", [])
        )
    ]
    if len(ready) != 1:
        raise SubmitError(f"expected one ready Ray head Pod, found {len(ready)}")
    pod_name = ready[0].get("metadata", {}).get("name")
    if not isinstance(pod_name, str) or not pod_name:
        raise SubmitError("Ray head Pod name is missing")
    return pod_name


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def write_json_create_only(
    path: Path,
    payload: Mapping[str, Any],
    *,
    label: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary_path: Path | None = None
    linked = False
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as stream:
            temporary_path = Path(stream.name)
            json.dump(payload, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        try:
            os.link(temporary_path, path)
        except FileExistsError as error:
            raise SubmitError(f"refusing to overwrite {label}: {path}") from error
        linked = True
    finally:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except FileNotFoundError:
                pass
        if linked:
            directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)


def write_result_create_only(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_create_only(path, payload, label="result")


def load_submission_record(
    path: Path,
    *,
    expected_run_id: str,
    expected_namespace: str,
    expected_cluster: str,
) -> dict[str, Any]:
    """Load the create-only handoff needed to reattach without resubmitting."""
    if not path.is_file() or path.is_symlink():
        raise SubmitError(f"Ray Job submission record is not a regular file: {path}")
    try:
        if path.stat().st_size > 64 * 1024:
            raise SubmitError(f"Ray Job submission record is unexpectedly large: {path}")
        record = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise SubmitError(f"cannot read Ray Job submission record: {error}") from error

    expected_remote_dir = f"/tmp/pretrain-ray-submit/{expected_run_id}"
    if (
        not isinstance(record, dict)
        or record.get("schemaVersion") != SUBMISSION_RECORD_SCHEMA
        or record.get("runId") != expected_run_id
        or record.get("submissionId") != expected_run_id
        or record.get("namespace") != expected_namespace
        or record.get("cluster") != expected_cluster
        or not isinstance(record.get("headPod"), str)
        or not record.get("headPod")
        or record.get("remoteDir") != expected_remote_dir
        or record.get("remoteHelper")
        != f"{expected_remote_dir}/{REMOTE_HELPER_PATH.name}"
        or record.get("remoteResult")
        != f"{expected_remote_dir}/execution-result.json"
        or not isinstance(record.get("submittedAt"), str)
        or not record.get("submittedAt")
    ):
        raise SubmitError("Ray Job submission record ownership is invalid")
    return record


def export_result(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_result: str,
    local_result: Path,
    expected_run_id: str,
    expected_status: str,
) -> Mapping[str, Any]:
    exported = run_command(
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
            remote_result,
        ],
        timeout=120,
        print_output=False,
    )
    try:
        payload = json.loads(exported.stdout)
    except json.JSONDecodeError as error:
        raise SubmitError(f"remote training result is invalid JSON: {error}") from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != "ray-training-result/v1"
    ):
        raise SubmitError("remote training result has an unsupported schema")
    if payload.get("runId") != expected_run_id:
        raise SubmitError("remote training result belongs to another run")
    if payload.get("status") != expected_status:
        raise SubmitError(
            "remote training result status differs from Ray Job status: "
            f"expected {expected_status}, got {payload.get('status')!r}"
        )
    write_result_create_only(local_result, payload)
    return payload


def delete_ray_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
) -> None:
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
        check=False,
    )
    if result.returncode != 0:
        raise SubmitError(f"cannot delete RayCluster {namespace}/{cluster}")


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
        "FAILED RESOURCE CLEANUP: "
        f"deleting RayCluster {namespace}/{cluster}; checkpoints are untouched."
    )
    delete_ray_cluster(kubectl, namespace=namespace, cluster=cluster)


def prepare_remote_submission(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    run_id: str,
    injection_dir: Path,
    scripts: Sequence[Path],
) -> tuple[str, str]:
    head_pod = find_ready_head(kubectl, namespace=namespace, cluster=cluster)
    remote_parent = "/tmp/pretrain-ray-submit"
    remote_dir = f"{remote_parent}/{run_id}"
    remote_target = f"{namespace}/{head_pod}:{remote_dir}"
    run_command(
        [
            *kubectl,
            "exec",
            "-n",
            namespace,
            head_pod,
            "-c",
            "ray-head",
            "--",
            "mkdir",
            "-p",
            "--",
            remote_parent,
        ],
        timeout=30,
    )
    run_command(
        [
            *kubectl,
            "exec",
            "-n",
            namespace,
            head_pod,
            "-c",
            "ray-head",
            "--",
            "mkdir",
            "--",
            remote_dir,
        ],
        timeout=30,
    )
    transfer_files = [
        injection_dir / "injection.json",
        *scripts,
        DRIVER_PATH,
        REMOTE_HELPER_PATH,
    ]
    for source in transfer_files:
        run_command(
            [
                *kubectl,
                "cp",
                str(source),
                f"{remote_target}/{source.name}",
                "-c",
                "ray-head",
            ],
            timeout=120,
        )
    return head_pod, remote_dir


def remote_helper_command(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_dir: str,
    operation: str,
    arguments: Sequence[str],
) -> list[str]:
    return [
        *kubectl,
        "exec",
        "-n",
        namespace,
        head_pod,
        "-c",
        "ray-head",
        "--",
        REMOTE_PYTHON,
        f"{remote_dir}/{REMOTE_HELPER_PATH.name}",
        operation,
        *arguments,
    ]


def parse_job_response(
    output: str,
    *,
    operation: str,
    submission_id: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise SubmitError(
            f"Ray Jobs {operation} returned invalid JSON: {error}"
        ) from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != JOB_RESPONSE_SCHEMA
        or payload.get("operation") != operation
        or payload.get("submissionId") != submission_id
    ):
        raise SubmitError(f"Ray Jobs {operation} returned invalid ownership data")
    return payload


def call_job_api(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_dir: str,
    operation: str,
    submission_id: str,
    extra_arguments: Sequence[str] = (),
) -> dict[str, Any]:
    result = run_command(
        remote_helper_command(
            kubectl,
            namespace=namespace,
            head_pod=head_pod,
            remote_dir=remote_dir,
            operation=operation,
            arguments=(
                "--submission-id",
                submission_id,
                *extra_arguments,
            ),
        ),
        timeout=60,
        print_output=False,
        print_command=False,
    )
    return parse_job_response(
        result.stdout,
        operation=operation,
        submission_id=submission_id,
    )


def wait_for_ray_job(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_dir: str,
    submission_id: str,
    poll_seconds: float,
    status_retry_seconds: float = DEFAULT_STATUS_RETRY_SECONDS,
) -> str:
    previous: str | None = None
    failure_deadline: float | None = None
    retry_index = 0
    while True:
        try:
            payload = call_job_api(
                kubectl,
                namespace=namespace,
                head_pod=head_pod,
                remote_dir=remote_dir,
                operation="status",
                submission_id=submission_id,
            )
        except SubmitError as error:
            now = time.monotonic()
            if failure_deadline is None:
                failure_deadline = now + status_retry_seconds
            remaining = failure_deadline - now
            if remaining <= 0:
                raise SubmitError(
                    "Ray Job status query failed continuously for "
                    f"{status_retry_seconds:g} seconds: {error}"
                ) from error
            delay = min(
                STATUS_RETRY_BACKOFF_SECONDS[
                    min(retry_index, len(STATUS_RETRY_BACKOFF_SECONDS) - 1)
                ],
                remaining,
            )
            retry_index += 1
            print(
                "WARNING: Ray Job status query failed; "
                f"retrying in {delay:g}s: {error}",
                file=sys.stderr,
                flush=True,
            )
            time.sleep(delay)
            continue

        if failure_deadline is not None:
            print("RAY JOB: status query recovered", flush=True)
            failure_deadline = None
            retry_index = 0
        status = payload.get("status")
        if not isinstance(status, str):
            raise SubmitError("Ray Jobs status response omitted status")
        status = status.upper()
        if status != previous:
            print(f"RAY JOB: submission={submission_id} status={status}", flush=True)
            previous = status
        if status in TERMINAL_JOB_STATUSES:
            return status
        if status not in {"PENDING", "RUNNING"}:
            raise SubmitError(f"Ray Job returned unsupported status: {status}")
        time.sleep(poll_seconds)


def print_job_logs(
    kubectl: Sequence[str],
    *,
    namespace: str,
    head_pod: str,
    remote_dir: str,
    submission_id: str,
) -> None:
    command = remote_helper_command(
        kubectl,
        namespace=namespace,
        head_pod=head_pod,
        remote_dir=remote_dir,
        operation="logs",
        arguments=("--submission-id", submission_id),
    )
    try:
        result = run_command(command, timeout=120, print_output=False)
    except SubmitError as error:
        print(f"WARNING: cannot fetch Ray Job logs: {error}", file=sys.stderr)
        return
    if result.stdout:
        print("=== Ray Job driver logs ===")
        print(result.stdout.rstrip())


def require_owned_ray_cluster(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    expected_run_id: str,
) -> None:
    result = run_command(
        [
            *kubectl,
            "get",
            "raycluster",
            cluster,
            "-n",
            namespace,
            "-o",
            "json",
        ],
        timeout=30,
        print_output=False,
        print_command=False,
    )
    try:
        document = json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise SubmitError(f"cannot parse RayCluster ownership: {error}") from error
    metadata = document.get("metadata") if isinstance(document, Mapping) else None
    annotations = metadata.get("annotations") if isinstance(metadata, Mapping) else None
    if (
        not isinstance(document, Mapping)
        or document.get("kind") != "RayCluster"
        or not isinstance(metadata, Mapping)
        or metadata.get("name") != cluster
        or metadata.get("namespace") != namespace
        or not isinstance(annotations, Mapping)
        or annotations.get("trainctl.io/run-id") != expected_run_id
    ):
        raise SubmitError("RayCluster ownership differs from the resumed attempt")


def finalize_submitted_job(
    kubectl: Sequence[str],
    *,
    namespace: str,
    cluster: str,
    run_id: str,
    head_pod: str,
    remote_dir: str,
    remote_result: str,
    result_path: Path,
    job_status: str,
    failure_retention_seconds: int,
    keep_success_resources: bool,
) -> None:
    print_job_logs(
        kubectl,
        namespace=namespace,
        head_pod=head_pod,
        remote_dir=remote_dir,
        submission_id=run_id,
    )

    if job_status == "STOPPED":
        print(
            "STOPPED RESOURCE RETENTION: automatic cleanup and result export "
            "are disabled; use kcc_ray cancel for owned cleanup.",
            file=sys.stderr,
        )
        raise SubmitError(f"Ray Job {run_id} was stopped; no result was trusted")

    export_error: SubmitError | None = None
    try:
        export_result(
            kubectl,
            namespace=namespace,
            head_pod=head_pod,
            remote_result=remote_result,
            local_result=result_path,
            expected_run_id=run_id,
            expected_status="PASS" if job_status == "SUCCEEDED" else "FAIL",
        )
    except SubmitError as error:
        export_error = error

    if job_status != "SUCCEEDED":
        suffix = (
            f"; failure result exported to {result_path}"
            if export_error is None
            else f"; result export failed: {export_error}"
        )
        if export_error is None:
            retain_then_delete_failed_cluster(
                kubectl,
                namespace=namespace,
                cluster=cluster,
                retention_seconds=failure_retention_seconds,
            )
        else:
            print(
                "FAILED RESOURCE RETENTION: result export failed; "
                "RayCluster is retained indefinitely.",
                file=sys.stderr,
            )
        raise SubmitError(f"Ray Job {run_id} ended with {job_status}{suffix}")
    if export_error is not None:
        print(
            "FAILED RESOURCE RETENTION: successful result could not be exported; "
            "RayCluster is retained indefinitely.",
            file=sys.stderr,
        )
        raise export_error
    if keep_success_resources:
        print("SUCCESS RESOURCE RETENTION: RayCluster was left in place.")
    else:
        print(
            "SUCCESS RESOURCE CLEANUP: deleting the RayCluster; "
            "all checkpoints and training logs are retained."
        )
        delete_ray_cluster(kubectl, namespace=namespace, cluster=cluster)
    print(f"PASS: training result exported to {result_path}")


def resume_existing_submission(
    *,
    expected_run_id: str,
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    result_path: Path,
    failure_retention_seconds: int,
    poll_seconds: float,
    keep_success_resources: bool,
) -> None:
    """Reattach to a recorded Ray Job; this path never submits a new job."""
    if result_path.exists() or result_path.is_symlink():
        raise SubmitError(f"refusing to overwrite result: {result_path}")
    record = load_submission_record(
        result_path.with_name(SUBMISSION_RECORD_FILENAME),
        expected_run_id=expected_run_id,
        expected_namespace=namespace,
        expected_cluster=cluster,
    )
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    require_owned_ray_cluster(
        kubectl,
        namespace=namespace,
        cluster=cluster,
        expected_run_id=expected_run_id,
    )
    current_head = find_ready_head(kubectl, namespace=namespace, cluster=cluster)
    if current_head != record["headPod"]:
        raise SubmitError(
            "ready Ray head differs from the recorded submission head; "
            "Ray Job ownership is uncertain"
        )

    print(f"RAY JOB: reattached {expected_run_id}", flush=True)
    job_status = wait_for_ray_job(
        kubectl,
        namespace=namespace,
        head_pod=current_head,
        remote_dir=str(record["remoteDir"]),
        submission_id=expected_run_id,
        poll_seconds=poll_seconds,
    )
    finalize_submitted_job(
        kubectl,
        namespace=namespace,
        cluster=cluster,
        run_id=expected_run_id,
        head_pod=current_head,
        remote_dir=str(record["remoteDir"]),
        remote_result=str(record["remoteResult"]),
        result_path=result_path,
        job_status=job_status,
        failure_retention_seconds=failure_retention_seconds,
        keep_success_resources=keep_success_resources,
    )


def submit(
    *,
    injection_dir: Path,
    injection: Mapping[str, Any],
    scripts: Sequence[Path],
    kubectl_command: str,
    kubeconfig: Path | None,
    namespace: str,
    cluster: str,
    result_path: Path,
    timeout_seconds: int,
    failure_retention_seconds: int,
    poll_seconds: float,
    keep_success_resources: bool,
) -> None:
    submission_record_path = result_path.with_name(SUBMISSION_RECORD_FILENAME)
    if result_path.exists() or result_path.is_symlink():
        raise SubmitError(f"refusing to overwrite result: {result_path}")
    if submission_record_path.exists() or submission_record_path.is_symlink():
        raise SubmitError(
            "existing Ray Job submission record requires explicit resume: "
            f"{submission_record_path}"
        )
    kubectl = kubectl_prefix(kubectl_command, kubeconfig)
    run_id = str(injection["runId"])
    try:
        head_pod, remote_dir = prepare_remote_submission(
            kubectl,
            namespace=namespace,
            cluster=cluster,
            run_id=run_id,
            injection_dir=injection_dir,
            scripts=scripts,
        )
    except SubmitError:
        retain_then_delete_failed_cluster(
            kubectl,
            namespace=namespace,
            cluster=cluster,
            retention_seconds=failure_retention_seconds,
        )
        raise

    remote_result = f"{remote_dir}/execution-result.json"
    driver_entrypoint = shlex.join(
        [
            REMOTE_PYTHON,
            f"{remote_dir}/{DRIVER_PATH.name}",
            "--injection",
            f"{remote_dir}/injection.json",
            "--scripts-dir",
            remote_dir,
            "--result",
            remote_result,
            "--timeout-seconds",
            str(timeout_seconds),
        ]
    )
    submission_id = run_id
    metadata = json.dumps(
        {
            "kccRunId": run_id,
            "kccNamespace": namespace,
            "kccRayCluster": cluster,
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    try:
        submitted = call_job_api(
            kubectl,
            namespace=namespace,
            head_pod=head_pod,
            remote_dir=remote_dir,
            operation="submit",
            submission_id=submission_id,
            extra_arguments=(
                "--entrypoint",
                driver_entrypoint,
                "--metadata-json",
                metadata,
            ),
        )
        if submitted.get("submissionId") != submission_id:
            raise SubmitError("Ray Jobs API changed the requested submission ID")
        write_json_create_only(
            submission_record_path,
            {
                "schemaVersion": SUBMISSION_RECORD_SCHEMA,
                "runId": run_id,
                "submissionId": submission_id,
                "namespace": namespace,
                "cluster": cluster,
                "headPod": head_pod,
                "remoteDir": remote_dir,
                "remoteHelper": f"{remote_dir}/{REMOTE_HELPER_PATH.name}",
                "remoteResult": remote_result,
                "submittedAt": utc_now(),
            },
            label="Ray Job submission record",
        )
        print(f"RAY JOB: submitted {submission_id}", flush=True)
        job_status = wait_for_ray_job(
            kubectl,
            namespace=namespace,
            head_pod=head_pod,
            remote_dir=remote_dir,
            submission_id=submission_id,
            poll_seconds=poll_seconds,
        )
    except SubmitError as error:
        print(
            "FAILED RESOURCE RETENTION: Ray Job state is uncertain; "
            "RayCluster is retained indefinitely.",
            file=sys.stderr,
        )
        raise SubmitError(f"Ray Job submission or status failed: {error}") from error

    finalize_submitted_job(
        kubectl,
        namespace=namespace,
        cluster=cluster,
        run_id=run_id,
        head_pod=head_pod,
        remote_dir=remote_dir,
        remote_result=remote_result,
        result_path=result_path,
        job_status=job_status,
        failure_retention_seconds=failure_retention_seconds,
        keep_success_resources=keep_success_resources,
    )


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--injection-dir", type=Path, required=True)
    parser.add_argument(
        "--kubectl-command",
    )
    parser.add_argument("--kubeconfig", type=Path)
    parser.add_argument("--namespace")
    parser.add_argument("--cluster")
    parser.add_argument("--result", type=Path, required=True)
    parser.add_argument(
        "--resume-existing",
        action="store_true",
        help="reattach to the recorded Ray Job without submitting a new one",
    )
    parser.add_argument(
        "--poll-seconds",
        type=float,
        default=DEFAULT_POLL_SECONDS,
        help="Ray Job status poll interval",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        help="per-worker training timeout; 0 means no timeout",
    )
    parser.add_argument(
        "--failure-retention-seconds",
        type=int,
        help="failed training retention; -1 keeps the RayCluster indefinitely",
    )
    parser.add_argument(
        "--keep-success-resources",
        action="store_true",
        help="do not delete the RayCluster after successful training",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        defaults = cluster_config.apply_kubernetes_defaults(args)
        if args.timeout_seconds is None or args.failure_retention_seconds is None:
            defaults = defaults or cluster_config.load_cluster_config()
        if args.timeout_seconds is None:
            args.timeout_seconds = defaults.timeouts.training_seconds
        if args.failure_retention_seconds is None:
            args.failure_retention_seconds = (
                defaults.timeouts.failed_resource_retention_seconds
            )
        if (
            args.timeout_seconds < 0
            or args.failure_retention_seconds < -1
            or args.poll_seconds <= 0
        ):
            raise SubmitError(
                "timeout must be non-negative, retention at least -1, "
                "and poll interval positive"
            )
        injection_dir = args.injection_dir.resolve()
        injection, scripts = load_injection(injection_dir)
        if args.resume_existing:
            resume_existing_submission(
                expected_run_id=str(injection["runId"]),
                kubectl_command=args.kubectl_command,
                kubeconfig=args.kubeconfig.resolve(),
                namespace=args.namespace,
                cluster=args.cluster,
                result_path=args.result.resolve(),
                failure_retention_seconds=args.failure_retention_seconds,
                poll_seconds=args.poll_seconds,
                keep_success_resources=args.keep_success_resources,
            )
        else:
            submit(
                injection_dir=injection_dir,
                injection=injection,
                scripts=scripts,
                kubectl_command=args.kubectl_command,
                kubeconfig=args.kubeconfig.resolve(),
                namespace=args.namespace,
                cluster=args.cluster,
                result_path=args.result.resolve(),
                timeout_seconds=args.timeout_seconds,
                failure_retention_seconds=args.failure_retention_seconds,
                poll_seconds=args.poll_seconds,
                keep_success_resources=args.keep_success_resources,
            )
    except (
        SubmitError,
        OSError,
        UnicodeError,
        cluster_config.ClusterConfigError,
    ) as error:
        print(f"STOP: Ray training submission failed: {error}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(
            "STOP: interrupted; Ray resources and checkpoints were left in place.",
            file=sys.stderr,
        )
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
