#!/usr/bin/env python3
"""Inspect or cancel the Ray Job owned by a kcc_ray run."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import supervisor_job
import training_control as legacy
import cluster_config


RUN_STATUS_SCHEMA = "kcc-ray-run-status/v1"
SUBMISSION_RECORD_SCHEMA = "kcc-ray-job-submission/v1"
JOB_RESPONSE_SCHEMA = "kcc-ray-job-api/v1"
SUBMISSION_RECORD_FILENAME = "ray-job-submission.json"
REMOTE_PYTHON = "/home/ray/anaconda3/bin/python"


class JobControlError(RuntimeError):
    pass


def load_submission_record(
    args: argparse.Namespace,
    attempt: str,
) -> dict[str, Any]:
    path = (
        args.training_artifact_root.resolve()
        / attempt
        / SUBMISSION_RECORD_FILENAME
    )
    try:
        record = legacy.read_json(path, "Ray Job submission record")
    except legacy.ControlError as error:
        raise JobControlError(str(error)) from error
    expected_helper = f"/tmp/pretrain-ray-submit/{attempt}/ray_job_remote.py"
    if (
        record.get("schemaVersion") != SUBMISSION_RECORD_SCHEMA
        or record.get("runId") != attempt
        or record.get("submissionId") != attempt
        or record.get("namespace") != args.namespace
        or record.get("cluster") != args.cluster
        or record.get("remoteHelper") != expected_helper
        or not isinstance(record.get("headPod"), str)
    ):
        raise JobControlError("Ray Job submission record ownership is invalid")
    return record


def run_job_operation(
    args: argparse.Namespace,
    record: Mapping[str, Any],
    operation: str,
) -> str:
    result = legacy.run(
        args,
        "exec",
        "-n",
        args.namespace,
        str(record["headPod"]),
        "-c",
        "ray-head",
        "--",
        REMOTE_PYTHON,
        str(record["remoteHelper"]),
        operation,
        "--submission-id",
        str(record["submissionId"]),
    )
    return result.stdout


def parse_job_response(
    output: str,
    *,
    operation: str,
    submission_id: str,
) -> dict[str, Any]:
    try:
        payload = json.loads(output)
    except json.JSONDecodeError as error:
        raise JobControlError(
            f"Ray Jobs {operation} returned invalid JSON: {error}"
        ) from error
    if (
        not isinstance(payload, dict)
        or payload.get("schemaVersion") != JOB_RESPONSE_SCHEMA
        or payload.get("operation") != operation
        or payload.get("submissionId") != submission_id
    ):
        raise JobControlError(f"Ray Jobs {operation} returned invalid ownership data")
    return payload


def recovery_state_summary(args: argparse.Namespace) -> dict[str, Any] | None:
    path = args.recovery_state_root.resolve() / args.run_id / "state.json"
    if not path.exists() and not path.is_symlink():
        return None
    try:
        state = legacy.read_json(
            path,
            "recovery state",
            max_bytes=legacy.RECOVERY_STATE_MAX_BYTES,
        )
    except legacy.ControlError as error:
        raise JobControlError(str(error)) from error
    attempts = state.get("attempts")
    status = state.get("status")
    if (
        state.get("schemaVersion") != legacy.RECOVERY_STATE_SCHEMA
        or state.get("jobId") != args.run_id
        or not isinstance(status, str)
        or not isinstance(attempts, list)
    ):
        raise JobControlError("recovery state ownership or schema is invalid")
    current_attempt = None
    if attempts:
        latest = attempts[-1]
        if not isinstance(latest, Mapping) or not isinstance(
            latest.get("runId"), str
        ):
            raise JobControlError("recovery state current attempt is invalid")
        current_attempt = latest["runId"]
    return {
        "status": status,
        "currentAttempt": current_attempt,
        "activeNodes": state.get("activeNodes"),
        "spareNodes": state.get("spareNodes"),
        "replacementCount": state.get("replacementCount"),
        "updatedAt": state.get("updatedAt"),
    }


def inspect(args: argparse.Namespace) -> int:
    if args.command == "logs" and getattr(args, "supervisor", False):
        try:
            output = supervisor_job.supervisor_job_logs(
                kubectl_command=args.kubectl_command,
                kubeconfig=args.kubeconfig,
                namespace=args.namespace,
                cluster=args.cluster,
                run_id=args.run_id,
            )
        except supervisor_job.SupervisorJobError as error:
            raise JobControlError(str(error)) from error
        if output:
            print(output, end="" if output.endswith("\n") else "\n")
        return 0

    if args.command == "logs":
        _uid, attempt = legacy.cluster_identity(args)
        legacy.owned_state_dir(args, attempt)
        record = load_submission_record(args, attempt)
        output = run_job_operation(args, record, "logs")
        if output:
            print(output, end="" if output.endswith("\n") else "\n")
        return 0

    try:
        outer = supervisor_job.supervisor_job_status(
            kubectl_command=args.kubectl_command,
            kubeconfig=args.kubeconfig,
            namespace=args.namespace,
            cluster=args.cluster,
            run_id=args.run_id,
        )
    except supervisor_job.SupervisorJobError as error:
        raise JobControlError(str(error)) from error
    recovery = recovery_state_summary(args)
    ray_job: dict[str, Any] | None = None
    ray_error: str | None = None
    try:
        _uid, attempt = legacy.cluster_identity(args)
        legacy.owned_state_dir(args, attempt)
        record = load_submission_record(args, attempt)
        output = run_job_operation(args, record, "status")
        ray_job = parse_job_response(
            output,
            operation="status",
            submission_id=attempt,
        )
    except (legacy.ControlError, JobControlError) as error:
        ray_error = str(error)
    payload: dict[str, Any] = {
        "schemaVersion": RUN_STATUS_SCHEMA,
        "runId": args.run_id,
        "supervisorJob": outer,
        "recovery": recovery,
        "rayJob": ray_job,
    }
    if ray_error is not None:
        payload["rayJobUnavailable"] = ray_error
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def cancel(args: argparse.Namespace) -> int:
    try:
        uid, attempt = legacy.cluster_identity(args)
    except legacy.ControlError as error:
        no_cluster = f"no active RayCluster {args.namespace}/{args.cluster}"
        if str(error) == no_cluster:
            return legacy.request_stop_without_cluster(args)
        raise

    state_dir = legacy.owned_state_dir(args, attempt)
    record: dict[str, Any] | None = None
    record_error: JobControlError | None = None
    try:
        record = load_submission_record(args, attempt)
    except JobControlError as error:
        record_error = error

    try:
        legacy.mark_stop(
            args,
            state_dir,
            uid,
            attempt,
            "IMMEDIATE",
            None,
        )
    except legacy.ControlError as error:
        raise JobControlError(str(error)) from error

    if record is not None:
        try:
            output = run_job_operation(args, record, "stop")
            payload = parse_job_response(
                output,
                operation="stop",
                submission_id=attempt,
            )
            print(json.dumps(payload, ensure_ascii=False, indent=2))
        except (legacy.ControlError, JobControlError) as error:
            print(
                f"WARNING: Ray Job stop request failed; "
                f"falling back to RayCluster cleanup: {error}",
                file=sys.stderr,
            )
    else:
        print(
            f"WARNING: {record_error}; falling back to RayCluster cleanup.",
            file=sys.stderr,
        )

    try:
        legacy.delete_cluster(args, uid, attempt)
    except legacy.ControlError as error:
        raise JobControlError(str(error)) from error
    print("PASS: cancellation requested and owned RayCluster removed.")
    return 0


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-id", required=True)
    parser.add_argument(
        "--kubectl-command",
    )
    parser.add_argument(
        "--kubeconfig",
        type=Path,
    )
    parser.add_argument("--namespace")
    parser.add_argument("--cluster")
    parser.add_argument(
        "--recovery-state-root",
        type=Path,
        default=legacy.DEFAULT_STATE_ROOT,
    )
    parser.add_argument(
        "--training-artifact-root",
        type=Path,
        default=legacy.DEFAULT_ARTIFACT_ROOT,
    )
    parser.add_argument("--cleanup-timeout-seconds", type=int)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="kcc_ray")
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("status", "logs", "cancel"):
        command = commands.add_parser(name)
        add_common(command)
        if name == "logs":
            command.add_argument(
                "--supervisor",
                action="store_true",
                help="print outer Supervisor Job logs instead of Ray driver logs",
            )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        defaults = cluster_config.apply_kubernetes_defaults(args)
        cluster_config.apply_cleanup_timeout_default(args, defaults)
        if args.cleanup_timeout_seconds <= 0:
            raise JobControlError("cleanup timeout must be positive")
        if args.command == "cancel":
            return cancel(args)
        return inspect(args)
    except (
        JobControlError,
        legacy.ControlError,
        cluster_config.ClusterConfigError,
    ) as error:
        print(f"STOP: Ray Job control failed: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("STOP: Ray Job control was interrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
