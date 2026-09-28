from __future__ import annotations

import hashlib
import json
import time
from datetime import datetime, timezone

import daft
from dagster import DynamicOut, DynamicOutput, Field, In, Out, op
from ray.job_submission import JobStatus

from ..resources.ray_job_resource import RayJobResource
from ..resources.s3_resource import S3Resource


RUN_SCHEMA = {
    "batch_id": str,
    "mode": str,
    "count": int,
    "input_bucket": str,
    "input_prefix": str,
    "output_bucket": str,
    "output_prefix": str,
    "mineru_service_count": Field(
        int,
        default_value=2,
        description="Number of existing single-NPU MinerU services to use: 1 or 2.",
    ),
    "inference_slots": Field(int, default_value=4),
    "document_inflight_per_service": Field(int, default_value=5),
    "window_prefetch": Field(int, default_value=1),
    "download_workers": Field(int, default_value=2),
    "upload_workers": Field(int, default_value=4),
    "block_prepare_workers": Field(int, default_value=12),
    "render_workers": Field(int, default_value=6),
    "finalize_workers": Field(int, default_value=3),
    "archive_workers": Field(int, default_value=2),
    "multipart_chunksize_mib": Field(int, default_value=16),
    "multipart_max_concurrency": Field(int, default_value=4),
    "sample_size": Field(int, default_value=5),
    "document_ui_detail_limit": Field(
        int,
        default_value=30,
        description="Maximum per-document Dagster status nodes; 0 disables them.",
    ),
}


def emit(context, event: str, **payload) -> None:
    context.log.info(
        "DAGSTER_EVENT %s",
        json.dumps(
            {
                "event": event,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                **payload,
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ),
    )


def stable_id(bucket: str, key: str, etag: str) -> str:
    digest = hashlib.sha1(f"{bucket}/{key}/{etag}".encode()).hexdigest()[:20]
    return f"pdf-{digest}"


def parse_ray_events(logs: str) -> list[dict]:
    events = []
    marker = "DAGSTER_EVENT "
    decoder = json.JSONDecoder()
    cursor = 0
    while True:
        cursor = logs.find(marker, cursor)
        if cursor < 0:
            return events
        cursor += len(marker)
        try:
            event, consumed = decoder.raw_decode(logs[cursor:].lstrip())
        except json.JSONDecodeError:
            continue
        events.append(event)
        cursor += consumed


def count_ray_events(logs: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for event in parse_ray_events(logs):
        counts[event["event"]] = counts.get(event["event"], 0) + 1
    return counts


@op(config_schema=RUN_SCHEMA, out=Out(dict))
def validate_run_config(context) -> dict:
    state = dict(context.op_config)
    if state["mode"] not in {"count", "all"}:
        raise ValueError("mode must be count or all")
    if state["mode"] == "count" and state["count"] < 1:
        raise ValueError("count mode requires a positive count")
    if state["mineru_service_count"] not in {1, 2}:
        raise ValueError("mineru_service_count must be 1 or 2")
    for name in (
        "inference_slots",
        "document_inflight_per_service",
        "window_prefetch",
        "download_workers",
        "upload_workers",
        "block_prepare_workers",
        "render_workers",
        "finalize_workers",
        "archive_workers",
        "multipart_chunksize_mib",
        "multipart_max_concurrency",
    ):
        if state[name] < 1:
            raise ValueError(f"{name} must be positive")
    if state["sample_size"] < 1:
        raise ValueError("sample_size must be positive")
    if state["document_ui_detail_limit"] < 0 or state["document_ui_detail_limit"] > 100:
        raise ValueError("document_ui_detail_limit must be between 0 and 100")
    if state["inference_slots"] > 8:
        raise ValueError("inference_slots above 8 is intentionally blocked")
    if state["document_inflight_per_service"] > 10:
        raise ValueError("document_inflight_per_service above 10 is intentionally blocked")
    state["output_prefix"] = state["output_prefix"].strip("/")
    state["input_prefix"] = state["input_prefix"].strip("/")
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def scan_s3_with_daft(context, state: dict) -> dict:
    emit(
        context,
        "SCAN_STARTED",
        batch_id=state["batch_id"],
        input_bucket=state["input_bucket"],
        input_prefix=state["input_prefix"],
    )
    s3: S3Resource = context.resources.s3
    rows = []
    for item in s3.list_prefix(state["input_bucket"], state["input_prefix"]):
        key = item["Key"]
        if not key.lower().endswith(".pdf"):
            continue
        etag = item.get("ETag", "").strip('"')
        rows.append(
            {
                "document_id": stable_id(state["input_bucket"], key, etag),
                "object_key": key,
                "etag": etag,
                "size_bytes": int(item["Size"]),
                "estimated_page_count": max(1, int(item["Size"] + 458751) // 458752),
                "last_modified": item["LastModified"].astimezone(timezone.utc).isoformat(),
            }
        )
    rows.sort(key=lambda row: row["object_key"])
    frame = daft.from_pylist(rows)
    if state["mode"] == "count":
        frame = frame.limit(state["count"])
    documents = frame.collect().to_pylist()
    if not documents:
        raise ValueError("Daft scan produced no PDF documents")
    state.update(
        {
            "source_manifest": {"documents": documents},
            "pdf_count": len(documents),
            "total_bytes": sum(row["size_bytes"] for row in documents),
            "scan_timestamp": datetime.now(timezone.utc).isoformat(),
            "daft_version": daft.__version__,
        }
    )
    context.log.info("Daft selected %d PDF metadata rows", len(documents))
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def write_pdf_manifest(context, state: dict) -> dict:
    manifest_key = f"{state['output_prefix']}/_INPUT_MANIFEST.json"
    manifest = {
        "source": {"bucket": state["input_bucket"], "prefix": state["input_prefix"]},
        "selection": {
            "mode": state["mode"],
            "requested_count": state["count"] if state["mode"] == "count" else None,
            "pdf_count": state["pdf_count"],
            "scanner": f"daft-{state['daft_version']}",
        },
        "documents": state["source_manifest"]["documents"],
    }
    s3: S3Resource = context.resources.s3
    s3.write_json(state["output_bucket"], manifest_key, manifest)
    state.update(
        {
            "source_manifest_key": manifest_key,
            "manifest_key": manifest_key,
            "manifest_count": len(manifest["documents"]),
            "manifest_uri": f"s3://{state['output_bucket']}/{manifest_key}",
        }
    )
    emit(
        context,
        "MANIFEST_WRITTEN",
        batch_id=state["batch_id"],
        manifest_uri=state["manifest_uri"],
        document_count=state["manifest_count"],
    )
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def check_ray_cluster(context, state: dict) -> dict:
    client = context.resources.ray_jobs.client()
    version = client.get_version()
    state["ray_version"] = version
    context.log.info("Ray Jobs API healthy: %s", version)
    return state


def check_service(context, state: dict, service: str) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    probe_id = f"dagster-probe-{service.lower()}-{int(time.time())}"
    ray_jobs.submit(
        probe_id,
        f"python3 -m runtime.dual_npu.service_probe --service {service}",
        {"PYTHONPATH": "."},
    )
    status = ray_jobs.wait(probe_id, timeout_seconds=180)
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"MinerU service {service} probe failed: {ray_jobs.logs(probe_id)}")
    context.log.info("MinerU service %s healthy: %s", service, ray_jobs.logs(probe_id).strip())
    state[f"service_{service.lower()}_healthy"] = True
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def check_mineru_service_a(context, state: dict) -> dict:
    return check_service(context, state, "A")


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def check_mineru_service_b(context, state: dict) -> dict:
    if state["mineru_service_count"] == 1:
        context.log.info("MinerU service B disabled by run config")
        state["service_b_healthy"] = None
        return state
    return check_service(context, state, "B")


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def submit_ray_job(context, state: dict) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    ray_job_id = f"mineru-dagster-{state['batch_id']}-{int(time.time())}"
    entrypoint = " ".join(
        [
            "python3 -m runtime.dual_npu.dual_ray_job",
            f"--manifest-key {state['manifest_key']}",
            f"--output-prefix {state['output_prefix']}",
            f"--count {state['manifest_count']}",
            f"--run-dir /tmp/{ray_job_id}",
            "--mapping-file /tmp/mineru-dual/npu-mapping.json",
            f"--batch-id {state['batch_id']}",
            f"--ray-job-id {ray_job_id}",
            f"--service-count {state['mineru_service_count']}",
            f"--inference-slots {state['inference_slots']}",
            f"--document-inflight {state['document_inflight_per_service']}",
            f"--window-prefetch {state['window_prefetch']}",
            f"--download-workers {state['download_workers']}",
            f"--upload-workers {state['upload_workers']}",
            f"--block-prepare-workers {state['block_prepare_workers']}",
            f"--render-workers {state['render_workers']}",
            f"--finalize-workers {state['finalize_workers']}",
            f"--archive-workers {state['archive_workers']}",
            f"--multipart-chunksize-mib {state['multipart_chunksize_mib']}",
            f"--multipart-max-concurrency {state['multipart_max_concurrency']}",
        ]
    )
    ray_jobs.submit(
        ray_job_id,
        entrypoint,
        {
            "PYTHONPATH": ".",
            "MINERU_INPUT_BUCKET": state["input_bucket"],
            "MINERU_OUTPUT_BUCKET": state["output_bucket"],
        },
    )
    state["ray_job_id"] = ray_job_id
    state["submission_mode"] = (
        "synchronous" if context.job_name == "mineru_smoke_10_job" else "asynchronous"
    )
    state["submission_timestamp"] = datetime.now(timezone.utc).isoformat()
    context.resources.s3.write_json(
        state["output_bucket"],
        f"_control/dagster/submissions/{state['batch_id']}.json",
        {key: value for key, value in state.items() if key != "source_manifest"},
    )
    emit(
        context,
        "RAY_JOB_SUBMITTED",
        batch_id=state["batch_id"],
        ray_job_id=ray_job_id,
        document_count=state["manifest_count"],
    )
    return state


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def monitor_ray_job(context, state: dict) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    event_counts: dict[str, int] = {}
    for line in ray_jobs.tail_logs(state["ray_job_id"]):
        marker = "DAGSTER_EVENT "
        if marker not in line:
            continue
        parsed = parse_ray_events(line)
        if not parsed:
            context.log.warning("Could not parse Ray event: %s", line[:500])
            continue
        for event in parsed:
            event_counts[event["event"]] = event_counts.get(event["event"], 0) + 1
            context.log.info("DAGSTER_EVENT %s", json.dumps(event, ensure_ascii=False))
    status = ray_jobs.status(state["ray_job_id"])
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(
            f"Ray job {state['ray_job_id']} ended as {status}: {ray_jobs.logs(state['ray_job_id'])[-4000:]}"
        )
    expected = state["manifest_count"]
    s3: S3Resource = context.resources.s3
    audits = []
    for document in state["source_manifest"]["documents"]:
        key = f"_control/{state['batch_id']}/documents/{document['document_id']}.json"
        if not s3.exists(state["output_bucket"], key):
            continue
        audit = s3.read_json(state["output_bucket"], key)
        if audit.get("status") != "success":
            continue
        audits.append(audit)
    if len(audits) != expected:
        raise RuntimeError(f"expected {expected} successful S3 document audits, got {len(audits)}")
    # Actor stdout is best-effort in Ray. Durable S3 audit records determine
    # final completion while streamed events remain available for live display.
    for event_name in (
        "DOCUMENT_ASSIGNED",
        "DOCUMENT_STARTED",
        "DOCUMENT_PARSED",
        "DOCUMENT_UPLOADED",
        "DOCUMENT_SUCCEEDED",
    ):
        event_counts[event_name] = expected
    state["event_counts"] = event_counts
    state["audited_document_count"] = len(audits)
    return state


@op(ins={"state": In(dict)}, out=DynamicOut(dict))
def fan_out_document_monitors(context, state: dict):
    limit = min(state["manifest_count"], state["document_ui_detail_limit"])
    for index, document in enumerate(
        state["source_manifest"]["documents"][:limit], start=1
    ):
        document_id = document["document_id"]
        yield DynamicOutput(
            {
                "batch_id": state["batch_id"],
                "output_bucket": state["output_bucket"],
                "document_id": document_id,
                "document_index": index,
                "document_total": state["manifest_count"],
                "audit_key": f"_control/{state['batch_id']}/documents/{document_id}.json",
            },
            mapping_key=f"doc_{index:04d}_{document_id.replace('-', '_')}",
        )


@op(ins={"document": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def monitor_document_status(context, document: dict) -> dict:
    deadline = time.time() + 7200
    s3: S3Resource = context.resources.s3
    context.log.info(
        "Waiting for document %d/%d: %s",
        document["document_index"],
        document["document_total"],
        document["document_id"],
    )
    while time.time() < deadline:
        if s3.exists(document["output_bucket"], document["audit_key"]):
            audit = s3.read_json(document["output_bucket"], document["audit_key"])
            if audit.get("status") == "failed":
                raise RuntimeError(
                    f"document {document['document_id']} failed: {audit.get('error')}"
                )
            if audit.get("status") == "success":
                result = {**document, **audit}
                break
        time.sleep(2)
    else:
        raise TimeoutError(f"document {document['document_id']} did not finish within 7200s")
    context.log.info(
        "Document %d/%d succeeded: %s, service=%s, pages=%s",
        result["document_index"],
        result["document_total"],
        result["document_id"],
        result.get("service"),
        result.get("page_count"),
    )
    context.add_output_metadata(
        {
            "document_index": result["document_index"],
            "document_total": result["document_total"],
            "document_id": result["document_id"],
            "service": result.get("service", "unknown"),
            "page_count": int(result.get("page_count", 0)),
            "parse_seconds": float(
                result.get("parse_seconds")
                or result.get("timings", {}).get("parse_and_output_seconds", 0)
            ),
        }
    )
    return result


@op(ins={"state": In(dict), "documents": In(list)}, out=Out(dict))
def join_document_progress(context, state: dict, documents: list[dict]) -> dict:
    expected = min(state["manifest_count"], state["document_ui_detail_limit"])
    if len(documents) != expected:
        raise RuntimeError(
            f"expected {expected} document progress nodes, got {len(documents)}"
        )
    state["document_progress_count"] = len(documents)
    return state


@op(
    config_schema={"batch_id": str, "output_bucket": str},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def load_submission_state(context) -> dict:
    s3: S3Resource = context.resources.s3
    batch_id = context.op_config["batch_id"]
    output_bucket = context.op_config["output_bucket"]
    state = s3.read_json(
        output_bucket,
        f"_control/dagster/submissions/{batch_id}.json",
    )
    state.setdefault("sample_size", 5)
    status = context.resources.ray_jobs.status(state["ray_job_id"])
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Ray job {state['ray_job_id']} is {status}, not SUCCEEDED")
    state["source_manifest"] = s3.read_json(output_bucket, state["manifest_key"])
    event_counts = count_ray_events(context.resources.ray_jobs.logs(state["ray_job_id"]))
    audit_count = 0
    for document in state["source_manifest"]["documents"]:
        key = f"_control/{batch_id}/documents/{document['document_id']}.json"
        if not s3.exists(output_bucket, key):
            continue
        audit = s3.read_json(output_bucket, key)
        if audit.get("status") != "success":
            continue
        audit_count += 1
        context.log.info(
            "DAGSTER_EVENT %s",
            json.dumps(
                {
                    "event": "DOCUMENT_AUDITED",
                    "document_id": audit["document_id"],
                    "input_key": audit.get("input_key"),
                    "worker_ip": audit.get("worker_ip"),
                    "service": audit.get("service"),
                    "actor_pid": audit.get("actor_pid"),
                    "npu_logical_id": audit.get("npu_logical_id"),
                    "page_count": audit.get("page_count"),
                    "timings": audit.get("timings"),
                    "artifact_count": audit.get("artifacts", {}).get("artifact_count"),
                    "status": audit.get("status"),
                },
                ensure_ascii=False,
            ),
        )
    # Ray does not reliably forward every actor stdout line to the driver log. The
    # per-document S3 audit records are the durable source of completion truth.
    if audit_count:
        for name in (
            "DOCUMENT_STARTED",
            "DOCUMENT_PARSED",
            "DOCUMENT_UPLOADED",
            "DOCUMENT_SUCCEEDED",
        ):
            event_counts[name] = audit_count
    state["event_counts"] = event_counts
    state["audited_document_count"] = audit_count
    return state
