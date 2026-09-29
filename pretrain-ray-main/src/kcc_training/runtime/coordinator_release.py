"""Release runtime: train, publish immutable output, then attest the result."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sys
import time
from typing import Any, Callable, Mapping, Sequence

from kcc_training.artifact_publish import publish_directory
from kcc_training.artifacts import ArtifactError, ArtifactGateway

from .checkpoints import CheckpointError, CheckpointUnavailable, snapshot
from .coordinator import _publish, execute, failure_result, publish_progress
from .spec import RuntimeSpec


def _token(path: str | None) -> str | None:
    if not path:
        return None
    value = Path(path).read_text(encoding="utf-8").strip()
    if not value:
        raise ArtifactError("artifact token file is empty")
    return value


COMPLETION_RECEIPT = ".kcc-training-complete.json"
MAX_RECEIPT_BYTES = 1024 * 1024
UPLOAD_ATTEMPTS = 3


def _training_binding(spec: RuntimeSpec) -> str:
    document = {
        "schemaVersion": "kcc-training-binding/v1",
        "runName": spec.run_name,
        "namespace": spec.namespace,
        "runUid": spec.run_uid,
        "workers": spec.workers,
        "devicesPerNode": spec.devices_per_node,
        "command": list(spec.command),
        "workingDirectory": str(spec.working_directory),
        "environment": dict(spec.environment),
        "outputRoot": str(spec.output_root),
        "checkpointRoot": str(spec.checkpoint_root),
        "artifactProvider": getattr(spec, "artifact_provider", "gateway"),
    }
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(payload).hexdigest()


def _checkpoint_summary(value: Mapping[str, Any]) -> dict[str, Any]:
    summary = {
        key: value[key]
        for key in (
            "available",
            "iteration",
            "trackerSha256",
            "hashMode",
            "sampleBytesPerFile",
            "snapshotSha256",
            "selectedDir",
            "fileCount",
            "totalBytes",
        )
        if key in value
    }
    files = value.get("files")
    if isinstance(files, list):
        summary["fileCount"] = len(files)
        summary["totalBytes"] = sum(
            item[1]
            for item in files
            if isinstance(item, (list, tuple))
            and len(item) >= 2
            and isinstance(item[1], int)
        )
    return summary


def _receipt_result(result: Mapping[str, Any]) -> dict[str, Any]:
    checkpoint = result.get("checkpoint")
    if isinstance(checkpoint, Mapping):
        checkpoint = _checkpoint_summary(checkpoint)
    receipt = {
        key: result[key]
        for key in (
            "schemaVersion",
            "runName",
            "namespace",
            "runUid",
            "attempt",
            "status",
            "checkpointConsistent",
            "checkpointAvailable",
            "failureScope",
            "failedNodes",
            "rankTableSha256",
        )
        if key in result
    } | {"checkpoint": checkpoint, "trainingAttempt": int(result["attempt"])}
    evaluation = result.get("evaluation")
    if isinstance(evaluation, Mapping):
        receipt["evaluation"] = dict(evaluation)
    return receipt


def _receipt_path(spec: RuntimeSpec) -> Path:
    return spec.output_root / COMPLETION_RECEIPT


def _load_completion(spec: RuntimeSpec) -> dict[str, Any] | None:
    path = _receipt_path(spec)
    if not path.exists():
        return None
    try:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > MAX_RECEIPT_BYTES:
            raise ArtifactError("training completion receipt is not a bounded regular file")
        document = json.loads(path.read_text(encoding="utf-8"))
    except ArtifactError:
        raise
    except (OSError, UnicodeError, json.JSONDecodeError) as error:
        raise ArtifactError(f"cannot read training completion receipt: {error}") from error
    if not isinstance(document, Mapping) or document.get("schemaVersion") != "kcc-training-completion/v1":
        raise ArtifactError("training completion receipt schema is invalid")
    if document.get("bindingSha256") != _training_binding(spec):
        raise ArtifactError("training completion receipt belongs to a different training plan")
    stored = document.get("result")
    if not isinstance(stored, Mapping) or stored.get("status") != "PASS":
        raise ArtifactError("training completion receipt does not attest PASS")
    if stored.get("runUid") != spec.run_uid or stored.get("checkpointConsistent") is not True:
        raise ArtifactError("training completion receipt identity or checkpoint evidence is invalid")
    checkpoint = stored.get("checkpoint")
    if stored.get("checkpointAvailable") is True:
        try:
            current = snapshot(spec.checkpoint_root)
        except (CheckpointError, CheckpointUnavailable) as error:
            raise ArtifactError(f"completed checkpoint is no longer readable: {error}") from error
        if (
            not isinstance(checkpoint, Mapping)
            or dict(checkpoint) != _checkpoint_summary(current)
        ):
            raise ArtifactError("completed checkpoint changed after training")
    result = dict(stored)
    result["attempt"] = spec.attempt
    result["reusedTrainingReceipt"] = True
    return result


def _write_completion(spec: RuntimeSpec, result: Mapping[str, Any]) -> None:
    path = _receipt_path(spec)
    path.parent.mkdir(parents=True, exist_ok=True)
    existing = _load_completion(spec)
    if existing is not None:
        return
    document = {
        "schemaVersion": "kcc-training-completion/v1",
        "bindingSha256": _training_binding(spec),
        "result": _receipt_result(result),
    }
    payload = json.dumps(document, ensure_ascii=False, sort_keys=True).encode("utf-8")
    if len(payload) > MAX_RECEIPT_BYTES:
        raise ArtifactError("training completion receipt exceeds its size limit")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def run(
    spec: RuntimeSpec,
    gateway: ArtifactGateway,
    *,
    execute_fn: Callable[[RuntimeSpec], Mapping[str, Any]] = execute,
    publish_fn: Callable[[ArtifactGateway, str, str, Path, int], str] = publish_directory,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Mapping[str, Any]:
    result = _load_completion(spec)
    if result is None:
        result = dict(execute_fn(spec))
        if result.get("status") != "PASS":
            return result
        result.setdefault("trainingAttempt", spec.attempt)
        try:
            _write_completion(spec, result)
        except Exception as error:
            failed = failure_result(spec, error, base=result, scope="artifact")
            failed.update(trainingStatus="PASS", publicationRetryable=True)
            return failed
    result["trainingStatus"] = "PASS"

    publication_attempt = spec.attempt
    publish_error: Exception | None = None
    attempts_used = 0
    for upload_attempt in range(UPLOAD_ATTEMPTS):
        attempts_used = upload_attempt + 1
        try:
            result["outputArtifact"] = publish_fn(
                gateway,
                spec.namespace,
                f"{spec.run_name}-output",
                spec.output_root,
                publication_attempt,
            )
            publish_error = None
            break
        except (ArtifactError, ConnectionError, OSError, TimeoutError) as error:
            publish_error = error
            if attempts_used < UPLOAD_ATTEMPTS:
                sleep_fn(float(2**upload_attempt))
        except Exception as error:
            publish_error = error
            break
    if publish_error is not None:
        failed = failure_result(spec, publish_error, base=result, scope="artifact")
        failed.update(
            trainingStatus="PASS",
            publicationRetryable=True,
            publicationAttempts=attempts_used,
        )
        return failed
    result["publicationRetryable"] = False
    result["publicationAttempts"] = attempts_used
    return result

def run_workspace(
    spec: RuntimeSpec,
    *,
    execute_fn: Callable[[RuntimeSpec], Mapping[str, Any]] = execute,
) -> Mapping[str, Any]:
    result = _load_completion(spec)
    if result is None:
        result = dict(execute_fn(spec))
        if result.get("status") != "PASS":
            return result
        result.setdefault("trainingAttempt", spec.attempt)
        try:
            _write_completion(spec, result)
        except Exception as error:
            failed = failure_result(spec, error, base=result, scope="artifact")
            failed.update(trainingStatus="PASS", publicationRetryable=True)
            return failed
    result["trainingStatus"] = "PASS"
    receipt_digest = hashlib.sha256(_receipt_path(spec).read_bytes()).hexdigest()[:16]
    result.update(
        outputProvider="workspace",
        outputPath=str(spec.output_root),
        outputArtifact=(
            f"artifact://{spec.namespace}/{spec.run_name}-output/"
            f"attempt-{spec.attempt:02d}-workspace-{receipt_digest}"
        ),
        publicationRetryable=False,
        publicationAttempts=0,
    )
    return result


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--gateway", default=os.environ.get("KCC_ARTIFACT_GATEWAY"))
    parser.add_argument("--token-file", default=os.environ.get("KCC_ARTIFACT_TOKEN_FILE"))
    return parser


def main(
    argv: Sequence[str] | None = None,
    *,
    execute_fn: Callable[[RuntimeSpec], Mapping[str, Any]] = execute,
    publish_fn: Callable[[ArtifactGateway, str, str, Path, int], str] = publish_directory,
) -> int:
    args = make_parser().parse_args(argv)
    spec: RuntimeSpec | None = None
    try:
        spec = RuntimeSpec.load(args.spec)
        if getattr(spec, "artifact_provider", "gateway") == "workspace":
            result = run_workspace(spec, execute_fn=execute_fn)
        else:
            if not args.gateway:
                raise ArtifactError("artifact gateway is required")
            gateway = ArtifactGateway(args.gateway, token=_token(args.token_file))
            result = run(spec, gateway, execute_fn=execute_fn, publish_fn=publish_fn)
    except Exception as error:
        if spec is None:
            print(f"runtime refused invalid spec: {error}", file=sys.stderr)
            return 2
        result = failure_result(
            spec,
            error,
            scope="artifact" if isinstance(error, ArtifactError) else None,
        )
    if spec is not None:
        final_status = str(result.get("status", "FAIL"))
        final_details: dict[str, Any] = {
            "outputArtifact": result.get("outputArtifact"),
            "checkpointIteration": (
                result.get("checkpoint", {}).get("iteration")
                if isinstance(result.get("checkpoint"), Mapping)
                else None
            ),
        }
        if isinstance(result.get("evaluation"), Mapping):
            final_details["evaluation"] = result["evaluation"]
        publish_progress(
            spec,
            "Completed" if final_status == "PASS" else "Failed",
            "Passed" if final_status == "PASS" else final_status.title(),
            (
                "training output is ready"
                if final_status == "PASS"
                else str(result.get("failure") or f"runtime finished with {final_status}")
            ),
            **final_details,
        )
    try:
        _publish(result)
    except Exception as error:
        print(f"runtime result publication failed: {error}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0 if result["status"] == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
