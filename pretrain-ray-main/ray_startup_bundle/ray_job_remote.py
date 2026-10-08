#!/usr/bin/env python3
"""Small in-cluster adapter for the Ray Jobs Python API.

The launcher copies this file to the Ray head and invokes it through short
``kubectl exec`` calls. Keeping the SDK import on the head avoids adding Ray
as a dependency of the external launcher and guarantees client/server version
alignment.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any, Mapping, Sequence


DEFAULT_ADDRESS = "http://127.0.0.1:8265"
RESPONSE_SCHEMA = "kcc-ray-job-api/v1"


class JobApiError(RuntimeError):
    pass


def status_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    text = str(raw)
    return text.rsplit(".", 1)[-1].upper()


def emit(payload: Mapping[str, Any]) -> None:
    print(json.dumps(dict(payload), ensure_ascii=False, sort_keys=True))


def load_metadata(text: str) -> dict[str, str]:
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise JobApiError(f"metadata is not valid JSON: {error}") from error
    if not isinstance(value, dict) or not all(
        isinstance(key, str) and isinstance(item, str)
        for key, item in value.items()
    ):
        raise JobApiError("metadata must be a JSON object of string values")
    return value


def make_client(address: str) -> Any:
    try:
        from ray.job_submission import JobSubmissionClient
    except ImportError as error:
        raise JobApiError(f"Ray Jobs SDK is unavailable: {error}") from error
    try:
        return JobSubmissionClient(address)
    except Exception as error:
        raise JobApiError(
            f"cannot connect to Ray Jobs API at {address}: {error}"
        ) from error


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default=DEFAULT_ADDRESS)
    commands = parser.add_subparsers(dest="command", required=True)

    submit = commands.add_parser("submit")
    submit.add_argument("--submission-id", required=True)
    submit.add_argument("--entrypoint", required=True)
    submit.add_argument("--metadata-json", default="{}")

    for name in ("status", "logs", "stop"):
        command = commands.add_parser(name)
        command.add_argument("--submission-id", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    try:
        client = make_client(args.address)
        if args.command == "submit":
            metadata = load_metadata(args.metadata_json)
            returned_id = client.submit_job(
                entrypoint=args.entrypoint,
                submission_id=args.submission_id,
                metadata=metadata,
            )
            emit(
                {
                    "schemaVersion": RESPONSE_SCHEMA,
                    "operation": "submit",
                    "submissionId": returned_id,
                    "status": status_text(client.get_job_status(returned_id)),
                }
            )
        elif args.command == "status":
            emit(
                {
                    "schemaVersion": RESPONSE_SCHEMA,
                    "operation": "status",
                    "submissionId": args.submission_id,
                    "status": status_text(client.get_job_status(args.submission_id)),
                }
            )
        elif args.command == "logs":
            logs = client.get_job_logs(args.submission_id)
            sys.stdout.write(logs)
            if logs and not logs.endswith("\n"):
                sys.stdout.write("\n")
        else:
            accepted = client.stop_job(args.submission_id)
            emit(
                {
                    "schemaVersion": RESPONSE_SCHEMA,
                    "operation": "stop",
                    "submissionId": args.submission_id,
                    "accepted": bool(accepted),
                    "status": status_text(client.get_job_status(args.submission_id)),
                }
            )
    except Exception as error:  # Ray SDK exception types vary by release.
        print(f"STOP: Ray Jobs API operation failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
