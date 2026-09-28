from __future__ import annotations

import math
import os
import re
import shlex
import time
from datetime import timezone
from pathlib import Path

import yaml
from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    In,
    MetadataValue,
    Out,
    Output,
    job,
    multiprocess_executor,
    op,
)
from ray.job_submission import JobStatus

from clean_qa.k12_clean_qa_pipeline.dagster_defs.e2e_demo_job import stable_document_id
from clean_qa.mineru_dagster.resources import RayJobResource, S3Resource


PROFILE_CONFIG = (
    Path(__file__).parents[1]
    / "configs"
    / "textbook_mineru_clean_qa_10_profile.yaml"
)
DEFAULT_INPUT_KEYS = yaml.safe_load(PROFILE_CONFIG.read_text())["ops"][
    "resolve_e2e_books"
]["config"]["input_object_keys"]
AUTOSCALE_DASHBOARD = os.environ.get(
    "AUTOSCALE_RAY_DASHBOARD_ADDRESS",
    "http://raycluster-k12-autoscale-nojudge-head-svc.k12.svc.cluster.local:8265",
)
PIPELINE_WORKING_DIR = os.environ.get("PIPELINE_WORKING_DIR", "/opt/data-pipeline/src")
PIPELINE_NO_PROXY = os.environ.get(
    "PIPELINE_NO_PROXY", "localhost,127.0.0.1,.svc,.svc.cluster.local"
)
SCHEMA = {
    "input_bucket": Field(str, default_value="k12-textbook-raw"),
    "input_object_keys": Field([str], default_value=DEFAULT_INPUT_KEYS),
    "count": Field(int, default_value=10),
    "mineru_bucket": Field(str, default_value="k12-mineru-output"),
    "output_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "output_root": Field(
        str,
        default_value=(
            "stage2/training-jsonl-collection-nojudge-autoscale-v1/runs"
        ),
    ),
    "mineru_batch_size": Field(int, default_value=4),
    "inference_slots": Field(int, default_value=4),
    "queue_size": Field(int, default_value=8),
    "qa_max_pods": Field(int, default_value=3),
    "qa_actors_per_pod": Field(int, default_value=2),
    "generation_max_inflight_per_actor": Field(int, default_value=8),
    "block_inflight": Field(int, default_value=8),
    "microbatch_size": Field(int, default_value=2),
    "max_blocks_per_document": Field(int, default_value=0),
    "qwen_model": Field(str, default_value="qwen3.6-35b-a3b"),
    "resume": Field(bool, default_value=True),
}


def default_config() -> dict:
    return {
        "ops": {
            "prepare_autoscale_nojudge_run": {
                "config": {
                    key: field.default_value for key, field in SCHEMA.items()
                }
            }
        }
    }


@op(config_schema=SCHEMA, out=Out(dict))
def prepare_autoscale_nojudge_run(context) -> dict:
    config = dict(context.op_config)
    if config["count"] != 10:
        raise ValueError("the reviewed formal smoke requires exactly 10 books")
    if config["mineru_batch_size"] != 4:
        raise ValueError("mineru_batch_size must be 4")
    if config["qa_max_pods"] > 3:
        raise ValueError("QA pod limit must not exceed 3")
    run_key = context.run_id[:12]
    run_root = f"{config['output_root'].rstrip('/')}/{run_key}"
    state = {
        **config,
        "run_key": run_key,
        "run_root": run_root,
        "manifest_key": f"{run_root}/manifests/source.json",
        "mineru_prefix": f"autoscale-nojudge/{run_key}/mineru",
        "stage1_prefix": f"{run_root}/stage1",
        "stage2_prefix": f"{run_root}/stage2",
        "ray_job_id": f"k12-autoscale-nojudge-{run_key}",
    }
    context.add_output_metadata(
        {
            "pipeline": "MinerU -> Cleaning -> Qwen schema-valid no-Judge",
            "mineru_batch_size": config["mineru_batch_size"],
            "judge_enabled": False,
            "output_root": MetadataValue.path(
                f"s3://{config['output_bucket']}/{run_root}"
            ),
        }
    )
    return state


@op(ins={"state": In(dict)}, out=Out(dict))
def audit_npu_and_worker_groups(context, state: dict) -> dict:
    audit = {
        "ray_cluster": "raycluster-k12-autoscale-nojudge",
        "mineru_physical_devices": [14, 15],
        "qa_physical_device_pairs": [[8, 9], [10, 11], [12, 13]],
        "qa_tensor_parallel_size": 1,
        "qa_actors_per_pod": 2,
        "worker_group_min_replicas": 0,
        "exact_device_binding": "Ascend Device Plugin annotation + resource limit",
        "judge_enabled": False,
    }
    context.add_output_metadata({"audit": MetadataValue.json(audit)})
    return {**state, "audit": audit}


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3"},
)
def inventory_source_documents(context, state: dict) -> dict:
    import daft

    client = context.resources.s3.client()
    rows = []
    for key in state["input_object_keys"][: state["count"]]:
        head = client.head_object(Bucket=state["input_bucket"], Key=key)
        etag = head["ETag"].strip('"')
        rows.append(
            {
                "document_id": stable_document_id(
                    state["input_bucket"], key, etag
                ),
                "object_key": key,
                "etag": etag,
                "size_bytes": int(head["ContentLength"]),
                "estimated_page_count": max(
                    1, math.ceil(int(head["ContentLength"]) / 458752)
                ),
                "last_modified": head["LastModified"]
                .astimezone(timezone.utc)
                .isoformat(),
            }
        )
    frame = daft.from_pydict(
        {key: [row[key] for row in rows] for key in rows[0]}
    )
    documents = [
        dict(zip(frame.column_names, values))
        for values in zip(
            *[
                frame.collect().to_pydict()[name]
                for name in frame.column_names
            ]
        )
    ]
    context.resources.s3.write_json(
        state["mineru_bucket"],
        state["manifest_key"],
        {
            "created_by_job": True,
            "dagster_run_id": context.run_id,
            "pipeline_name": "k12-e2e-autoscale-nojudge",
            "documents": documents,
        },
    )
    context.add_output_metadata(
        {
            "document_count": len(documents),
            "daft_rows": len(documents),
            "manifest": MetadataValue.path(
                f"s3://{state['mineru_bucket']}/{state['manifest_key']}"
            ),
        }
    )
    return {**state, "documents": documents}


def entrypoint(state: dict) -> str:
    parts = [
        "python3",
        "-m",
        "clean_qa.k12_clean_qa_pipeline.autoscale_nojudge.driver",
        "--manifest-bucket",
        state["mineru_bucket"],
        "--manifest-key",
        state["manifest_key"],
        "--input-bucket",
        state["input_bucket"],
        "--mineru-bucket",
        state["mineru_bucket"],
        "--mineru-prefix",
        state["mineru_prefix"],
        "--output-bucket",
        state["output_bucket"],
        "--stage1-prefix",
        state["stage1_prefix"],
        "--output-prefix",
        state["stage2_prefix"],
        "--ray-job-id",
        state["ray_job_id"],
        "--mineru-batch-size",
        str(state["mineru_batch_size"]),
        "--inference-slots",
        str(state["inference_slots"]),
        "--queue-size",
        str(state["queue_size"]),
        "--qa-max-pods",
        str(state["qa_max_pods"]),
        "--qa-actors-per-pod",
        str(state["qa_actors_per_pod"]),
        "--generation-max-inflight-per-actor",
        str(state["generation_max_inflight_per_actor"]),
        "--block-inflight",
        str(state["block_inflight"]),
        "--microbatch-size",
        str(state["microbatch_size"]),
        "--max-blocks-per-document",
        str(state["max_blocks_per_document"]),
        "--qwen-model",
        state["qwen_model"],
    ]
    if state["resume"]:
        parts.append("--resume")
    return " ".join(shlex.quote(value) for value in parts)


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs"},
)
def submit_ray_pipeline_job(context, state: dict) -> dict:
    command = entrypoint(state)
    context.resources.ray_jobs.submit(
        state["ray_job_id"],
        command,
        env_vars={
            "PYTHONPATH": f".:{PIPELINE_WORKING_DIR}",
            "NO_PROXY": PIPELINE_NO_PROXY,
            "no_proxy": PIPELINE_NO_PROXY,
        },
    )
    context.add_output_metadata(
        {
            "ray_job_id": state["ray_job_id"],
            "entrypoint": MetadataValue.text(command),
        }
    )
    return state


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def monitor_autoscale_pipeline(context, state: dict) -> dict:
    deadline = time.monotonic() + 12 * 3600
    last_progress = None
    while time.monotonic() < deadline:
        status = context.resources.ray_jobs.status(state["ray_job_id"])
        progress_key = f"{state['stage2_prefix']}/_PROGRESS.json"
        if context.resources.s3.exists(state["output_bucket"], progress_key):
            progress = context.resources.s3.read_json(
                state["output_bucket"], progress_key
            )
            counts: dict[str, int] = {}
            for document in progress["documents"].values():
                stage = document["status"]
                counts[stage] = counts.get(stage, 0) + 1
            snapshot = {
                "counts": counts,
                "qa_worker_pods_ready": progress["qa_worker_pods_ready"],
                "qa_vllm_actors_ready": progress["qa_vllm_actors_ready"],
            }
            if snapshot != last_progress:
                context.log.info("Pipeline progress: %s", snapshot)
                last_progress = snapshot
        if status in {
            JobStatus.SUCCEEDED,
            JobStatus.FAILED,
            JobStatus.STOPPED,
        }:
            if status != JobStatus.SUCCEEDED:
                raise RuntimeError(
                    context.resources.ray_jobs.logs(state["ray_job_id"])[
                        -20000:
                    ]
                )
            return state
        time.sleep(5)
    raise TimeoutError("autoscale no-Judge Ray Job exceeded 12 hours")


def _events(context, state: dict) -> list[dict]:
    key = f"{state['stage2_prefix']}/_AUTOSCALING_EVENTS.json"
    if not context.resources.s3.exists(state["output_bucket"], key):
        return []
    return context.resources.s3.read_json(state["output_bucket"], key)


def _progress(context, state: dict) -> dict:
    key = f"{state['stage2_prefix']}/_PROGRESS.json"
    if not context.resources.s3.exists(state["output_bucket"], key):
        return {}
    return context.resources.s3.read_json(state["output_bucket"], key)


def _check_ray_running(context, state: dict, waiting_for: str) -> None:
    status = context.resources.ray_jobs.status(state["ray_job_id"])
    if status in {JobStatus.FAILED, JobStatus.STOPPED}:
        raise RuntimeError(
            f"Ray job became {status} while waiting for {waiting_for}: "
            f"{context.resources.ray_jobs.logs(state['ray_job_id'])[-12000:]}"
        )


def _mapping_key(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value)


def _wait_for_event(
    context,
    state: dict,
    component: str,
    event: str,
    identity: dict | None = None,
    timeout_seconds: int = 3600,
) -> dict:
    identity = identity or {}
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        for row in _events(context, state):
            if row.get("component") != component or row.get("event") != event:
                continue
            if all(row.get(key) == value for key, value in identity.items()):
                return row
        _check_ray_running(context, state, f"{component}:{event}:{identity}")
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {component}:{event}:{identity}")


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def mineru_worker_pod_ready(context, state: dict) -> dict:
    event = _wait_for_event(
        context,
        state,
        "mineru_resource_manager",
        "pod_ready",
        timeout_seconds=2400,
    )
    context.add_output_metadata(
        {
            "pod_name": event.get("pod_name", "unknown"),
            "lifecycle": "ready",
            "actors": MetadataValue.json(event.get("actors", [])),
        }
    )
    return {**state, "mineru_pod": event}


def _mineru_serve_ready(service: str):
    serve_id = f"mineru-serve-{service.lower()}"

    @op(
        name=f"{serve_id.replace('-', '_')}_ready",
        ins={"state": In(dict)},
        out=Out(dict),
        required_resource_keys={"s3", "ray_jobs"},
    )
    def wait_ready(context, state: dict) -> dict:
        event = _wait_for_event(
            context,
            state,
            "mineru_serve",
            "ready",
            {"serve_id": serve_id},
            timeout_seconds=2400,
        )
        context.add_output_metadata(
            {
                "pod_name": event.get("pod_name", "unknown"),
                "serve_id": serve_id,
                "actor_id": event.get("actor_id", "unknown"),
                "chip_id": event.get("chip_id", -1),
                "lifecycle": "ready",
            }
        )
        return {**state, "mineru_service": service, "mineru_serve": event}

    return wait_ready


def _fan_out_mineru_books(service: str):
    @op(
        name=f"mineru_serve_{service.lower()}_books",
        ins={"state": In(dict)},
        out=DynamicOut(dict),
        required_resource_keys={"s3", "ray_jobs"},
    )
    def fan_out(context, state: dict):
        key = f"{state['stage2_prefix']}/_MINERU_ROUTING_PLAN.json"
        deadline = time.monotonic() + 2400
        while not context.resources.s3.exists(state["output_bucket"], key):
            _check_ray_running(context, state, f"MinerU routing plan {service}")
            if time.monotonic() >= deadline:
                raise TimeoutError(f"missing MinerU routing plan: {key}")
            time.sleep(2)
        plan = context.resources.s3.read_json(state["output_bucket"], key)
        documents = {
            row["document_id"]: row for row in state["documents"]
        }
        rows = [
            row
            for row in plan.get("documents", [])
            if row.get("service") == service
        ]
        for index, route in enumerate(rows, start=1):
            document = documents[route["document_id"]]
            yield DynamicOutput(
                {
                    **state,
                    **document,
                    "book_index": index,
                    "mineru_route": route,
                },
                mapping_key=_mapping_key(
                    f"book_{index:02d}_{route['document_id']}"
                ),
            )

    return fan_out


@op(ins={"state": In(dict)}, out=DynamicOut(dict))
def fan_out_autoscale_books(context, state: dict):
    for index, document in enumerate(state["documents"], start=1):
        document_id = document["document_id"]
        mapping_key = re.sub(
            r"[^A-Za-z0-9_]",
            "_",
            f"book_{index:02d}_{document_id}",
        )
        yield DynamicOutput(
            {**state, **document, "book_index": index},
            mapping_key=mapping_key,
        )


def _wait_for_progress_stage(
    context,
    book: dict,
    stage: str,
    timeout_seconds: int,
) -> dict:
    deadline = time.monotonic() + timeout_seconds
    next_log = 0.0
    while time.monotonic() < deadline:
        progress = _progress(context, book)
        document = progress.get("documents", {}).get(book["document_id"], {})
        details = document.get("stages", {}).get(stage, {})
        if details.get("status") == "completed":
            return details
        if details.get("status") == "failed" or document.get("status") == "failed":
            raise RuntimeError(
                f"{book['document_id']} failed in {stage}: "
                f"{details.get('error') or document.get('error')}"
            )
        _check_ray_running(context, book, f"{book['document_id']}:{stage}")
        if time.monotonic() >= next_log:
            context.log.info(
                "%s waiting for %s; current=%s",
                book["document_id"],
                stage,
                details.get("status", document.get("status", "pending")),
            )
            next_log = time.monotonic() + 15
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {book['document_id']}:{stage}")


def _wait_for_observation_stage(
    context,
    book: dict,
    stage: str,
    timeout_seconds: int,
) -> tuple[dict, dict]:
    key = (
        f"{book['stage2_prefix'].rstrip('/')}/{book['document_id']}"
        "/_OBSERVABILITY.json"
    )
    deadline = time.monotonic() + timeout_seconds
    next_log = 0.0
    while time.monotonic() < deadline:
        if context.resources.s3.exists(book["output_bucket"], key):
            observation = context.resources.s3.read_json(
                book["output_bucket"], key
            )
            details = observation.get("stages", {}).get(stage, {})
            if details.get("status") == "completed":
                return details, observation
            if details.get("status") == "failed":
                raise RuntimeError(
                    f"{book['document_id']} failed in {stage}: {details}"
                )
        _check_ray_running(context, book, f"{book['document_id']}:{stage}")
        if time.monotonic() >= next_log:
            context.log.info(
                "%s waiting for observable stage %s",
                book["document_id"],
                stage,
            )
            next_log = time.monotonic() + 15
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {book['document_id']}:{stage}")


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def book_mineru(context, book: dict) -> dict:
    details = _wait_for_progress_stage(context, book, "mineru", 4 * 3600)
    context.add_output_metadata(
        {
            "book_id": book["document_id"],
            "pod_name": details.get("pod_name", "unknown"),
            "serve_id": details.get("serve_id", "unknown"),
            "actor_id": details.get("actor_id", "unknown"),
            "chip_id": details.get("chip_id", -1),
            "queue_wait": details.get("queue_wait", 0),
            "processing_time": details.get("processing_time", 0),
            "page_count": details.get("page_count", 0),
        }
    )
    return {**book, "mineru_observation": details}


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def book_cleaning(context, book: dict) -> dict:
    details = _wait_for_progress_stage(context, book, "cleaning", 2 * 3600)
    report_key = (
        f"{book['stage1_prefix'].rstrip('/')}/{book['document_id']}"
        "/cleaning_report.json"
    )
    report = context.resources.s3.read_json(
        book["output_bucket"], report_key
    )
    context.add_output_metadata(
        {
            "book_id": book["document_id"],
            "processing_time": details.get("processing_time", 0),
            "kept_blocks": int(report.get("kept_block_count", 0)),
            "exercises": int(report.get("exercise_count", 0)),
            "output": MetadataValue.path(
                f"s3://{book['output_bucket']}/"
                f"{book['stage1_prefix']}/{book['document_id']}"
            ),
        }
    )
    return {**book, "cleaning_observation": details}


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def book_qa_mcq(context, book: dict) -> dict:
    details, observation = _wait_for_observation_stage(
        context, book, "qa_mcq", 8 * 3600
    )
    assignments = observation.get("qwen_assignments", [])
    serves = sorted(
        {
            str(row.get("serve_id") or row.get("endpoint_id"))
            for row in assignments
        }
    )
    context.add_output_metadata(
        {
            "book_id": book["document_id"],
            "processing_time": details.get("processing_time", 0),
            "generation_batches": details.get("generation_batches", 0),
            "generated_results": details.get("generated_results", 0),
            "serve_ids": MetadataValue.json(serves),
            "block_assignments": MetadataValue.json(assignments),
        }
    )
    return {**book, "qa_mcq_observation": details}


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def book_schema_validate(context, book: dict) -> dict:
    details, _ = _wait_for_observation_stage(
        context, book, "schema_validate", 2 * 3600
    )
    context.add_output_metadata(
        {
            "book_id": book["document_id"],
            "processing_time": details.get("processing_time", 0),
            "qa_candidates": details.get("qa_candidates", 0),
            "mcq_candidates": details.get("mcq_candidates", 0),
            "qa_schema_valid_unjudged": details.get(
                "qa_schema_valid_unjudged", 0
            ),
            "mcq_schema_valid_unjudged": details.get(
                "mcq_schema_valid_unjudged", 0
            ),
        }
    )
    return {**book, "schema_observation": details}


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def book_minio_write(context, book: dict) -> dict:
    _wait_for_observation_stage(context, book, "minio_write", 2 * 3600)
    marker_key = (
        f"{book['stage2_prefix'].rstrip('/')}/{book['document_id']}"
        "/_SUCCESS.json"
    )
    deadline = time.monotonic() + 120
    while not context.resources.s3.exists(book["output_bucket"], marker_key):
        _check_ray_running(context, book, f"{book['document_id']}:MinIO")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"missing final marker: {marker_key}")
        time.sleep(1)
    marker = context.resources.s3.read_json(
        book["output_bucket"], marker_key
    )
    context.add_output_metadata(
        {
            "book_id": book["document_id"],
            "validation_status": marker["validation_status"],
            "artifact_count": len(marker.get("artifact_sha256", {})),
            "output": MetadataValue.path(
                f"s3://{book['output_bucket']}/"
                f"{book['stage2_prefix']}/{book['document_id']}"
            ),
        }
    )
    return {
        "document_id": book["document_id"],
        "status": "success",
        "metrics": marker.get("metrics", {}),
    }


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def first_cleaning_output_ready(context, state: dict) -> dict:
    deadline = time.monotonic() + 6 * 3600
    while time.monotonic() < deadline:
        progress = _progress(context, state)
        completed = [
            document_id
            for document_id, document in progress.get("documents", {}).items()
            if document.get("stages", {})
            .get("cleaning", {})
            .get("status")
            == "completed"
        ]
        if completed:
            context.add_output_metadata(
                {
                    "first_document_id": completed[0],
                    "trigger": "first Stage 1 cleaning output",
                }
            )
            return {**state, "first_cleaned_document": completed[0]}
        _check_ray_running(context, state, "first cleaning output")
        time.sleep(2)
    raise TimeoutError("timed out waiting for first cleaning output")


def _qa_pod_gate(worker_group: str):
    suffix = worker_group.replace("-", "_")

    @op(
        name=f"{suffix}_worker_pod_ready",
        ins={"state": In(dict)},
        out=Out(dict, is_required=False),
        required_resource_keys={"s3", "ray_jobs"},
    )
    def gate(context, state: dict):
        seen = 0
        while True:
            events = _events(context, state)
            matches = [
                row
                for row in events
                if row.get("component") == "qa_scaling_controller"
                and row.get("event") == "pod_ready"
                and row.get("worker_group") == worker_group
            ]
            if matches:
                event = matches[-1]
                context.add_output_metadata(
                    {
                        "worker_group": worker_group,
                        "pod_name": event.get("pod_name", "unknown"),
                        "chip_ids": MetadataValue.json(
                            event.get("chip_ids", [])
                        ),
                        "startup_seconds": event.get("startup_seconds", 0),
                        "lifecycle": "ready",
                    }
                )
                yield Output(
                    {
                        **state,
                        "qa_worker_group": worker_group,
                        "qa_pod": event,
                    }
                )
                return
            if len(events) != seen:
                seen = len(events)
                context.log.info(
                    "QA worker group %s not ready yet; events=%s",
                    worker_group,
                    seen,
                )
            status = context.resources.ray_jobs.status(state["ray_job_id"])
            if status in {
                JobStatus.SUCCEEDED,
                JobStatus.FAILED,
                JobStatus.STOPPED,
            }:
                context.log.info(
                    "QA worker group %s was not requested; downstream "
                    "lifecycle "
                    "nodes will be skipped",
                    worker_group,
                )
                return
            time.sleep(2)

    return gate


def _resource_lifecycle_op(
    name: str,
    component: str,
    released_event: str,
    identity: dict,
):
    @op(
        name=name,
        ins={"state": In(dict)},
        out=Out(dict),
        required_resource_keys={"s3", "ray_jobs"},
    )
    def observe(context, state: dict) -> dict:
        seen: set[tuple] = set()
        lifecycle: list[dict] = []
        last_busy: tuple = ()
        observed_assignments: dict[str, dict] = {}
        while True:
            for row in _events(context, state):
                if row.get("component") != component:
                    continue
                if any(row.get(key) != value for key, value in identity.items()):
                    continue
                signature = (
                    row.get("event"),
                    row.get("timestamp"),
                    row.get("serve_id"),
                )
                if signature not in seen:
                    seen.add(signature)
                    lifecycle.append(row)
                    context.log.info(
                        "%s lifecycle=%s details=%s",
                        name,
                        row.get("event"),
                        row,
                    )
            progress = _progress(context, state)
            resources = progress.get("resources", {})
            active: list[dict] = []
            if component == "qa_serve":
                serve_id = identity.get("serve_id")
                for endpoint in resources.get("qa", {}).get("endpoints", []):
                    if endpoint.get("serve_id") == serve_id:
                        active.extend(
                            endpoint.get("active_assignments", [])
                        )
                        break
                known = {
                    row.get("assignment_id") for row in active
                }
                active.extend(
                    row
                    for row in resources.get("qa", {}).get(
                        "active_assignments", []
                    )
                    if row.get("endpoint_id") == serve_id
                    and row.get("assignment_id") not in known
                )
            elif component == "qa_scaling_controller":
                pod_serve_ids = {
                    endpoint.get("serve_id")
                    for endpoint in resources.get("qa", {}).get(
                        "endpoints", []
                    )
                    if endpoint.get("pod_index")
                    == identity.get("pod_index")
                }
                for endpoint in resources.get("qa", {}).get("endpoints", []):
                    if endpoint.get("pod_index") == identity.get("pod_index"):
                        active.extend(
                            endpoint.get("active_assignments", [])
                        )
                known = {
                    row.get("assignment_id") for row in active
                }
                active.extend(
                    row
                    for row in resources.get("qa", {}).get(
                        "active_assignments", []
                    )
                    if row.get("endpoint_id") in pod_serve_ids
                    and row.get("assignment_id") not in known
                )
            elif component == "mineru_serve":
                for serve in resources.get("mineru", []):
                    if serve.get("serve_id") == identity.get("serve_id"):
                        active = [
                            {"document_id": document_id}
                            for document_id in serve.get(
                                "active_documents", []
                            )
                        ]
                        break
            elif component == "mineru_resource_manager":
                for serve in resources.get("mineru", []):
                    active.extend(
                        {
                            "document_id": document_id,
                            "serve_id": serve.get("serve_id"),
                        }
                        for document_id in serve.get(
                            "active_documents", []
                        )
                    )
            busy_signature = tuple(
                sorted(
                    (
                        str(row.get("document_id")),
                        tuple(row.get("block_ids", [])),
                    )
                    for row in active
                )
            )
            for row in active:
                assignment_key = str(
                    row.get("assignment_id")
                    or (
                        f"{row.get('document_id')}|"
                        f"{','.join(row.get('block_ids', []))}|"
                        f"{row.get('serve_id', '')}"
                    )
                )
                observed_assignments[assignment_key] = row
            if busy_signature != last_busy:
                context.log.info(
                    "%s lifecycle=%s assignments=%s",
                    name,
                    "busy" if active else "ready",
                    active,
                )
                last_busy = busy_signature
            released = next(
                (
                    row
                    for row in lifecycle
                    if row.get("event") == released_event
                ),
                None,
            )
            if released is not None:
                ready = next(
                    (
                        row
                        for row in lifecycle
                        if row.get("event") in {"ready", "pod_ready"}
                    ),
                    {},
                )
                ready_pod_name = ready.get("pod_name")
                if not ready_pod_name and ready.get("endpoints"):
                    ready_pod_name = ready["endpoints"][0].get("pod_name")
                context.add_output_metadata(
                    {
                        "lifecycle": MetadataValue.json(lifecycle),
                        "pod_name": (
                            ready_pod_name
                            or released.get("pod_name", "unknown")
                        ),
                        "serve_id": identity.get("serve_id", "pod"),
                        "chip_id": ready.get("chip_id", -1),
                        "final_status": "released",
                        "observed_assignments": MetadataValue.json(
                            list(observed_assignments.values())
                        ),
                    }
                )
                return state
            _check_ray_running(context, state, name)
            time.sleep(2)

    return observe


qa_8_9_worker_pod_ready = _qa_pod_gate("qa-8-9")
qa_10_11_worker_pod_ready = _qa_pod_gate("qa-10-11")
qa_12_13_worker_pod_ready = _qa_pod_gate("qa-12-13")


def _qa_serve_ready(worker_group: str, chip_id: int):
    serve_id = f"{worker_group}-vllm-{chip_id}"

    @op(
        name=f"{worker_group.replace('-', '_')}_vllm_{chip_id}_ready",
        ins={"state": In(dict)},
        out=Out(dict),
        required_resource_keys={"s3", "ray_jobs"},
    )
    def wait_ready(context, state: dict) -> dict:
        event = _wait_for_event(
            context,
            state,
            "qa_serve",
            "ready",
            {"serve_id": serve_id},
            timeout_seconds=2400,
        )
        context.add_output_metadata(
            {
                "worker_group": worker_group,
                "pod_name": event.get("pod_name", "unknown"),
                "serve_id": serve_id,
                "actor_id": event.get("actor_id", "unknown"),
                "chip_id": event.get("chip_id", chip_id),
                "port": event.get("port", 0),
                "lifecycle": "ready",
            }
        )
        return {
            **state,
            "qa_worker_group": worker_group,
            "qa_serve_id": serve_id,
            "qa_chip_id": chip_id,
        }

    return wait_ready


def _qwen_assignment_fanout(worker_group: str, chip_id: int):
    serve_id = f"{worker_group}-vllm-{chip_id}"

    @op(
        name=f"{worker_group.replace('-', '_')}_vllm_{chip_id}_assignments",
        ins={"state": In(dict), "summary": In(dict)},
        out=DynamicOut(dict),
    )
    def fan_out(context, state: dict, summary: dict):
        grouped: dict[str, list[dict]] = {}
        for assignment in summary.get("qwen", {}).get(
            "recent_assignments", []
        ):
            if assignment.get("endpoint_id") != serve_id:
                continue
            document_id = str(assignment.get("document_id", "unknown"))
            grouped.setdefault(document_id, []).append(assignment)
        for document_id, assignments in sorted(grouped.items()):
            yield DynamicOutput(
                {
                    **state,
                    "document_id": document_id,
                    "qa_serve_id": serve_id,
                    "qwen_assignments": assignments,
                },
                mapping_key=_mapping_key(document_id),
            )

    return fan_out


def _qwen_book_assignment(worker_group: str, chip_id: int):
    @op(
        name=f"{worker_group.replace('-', '_')}_vllm_{chip_id}_book",
        ins={"assignment": In(dict)},
        out=Out(dict),
    )
    def observe(context, assignment: dict) -> dict:
        rows = assignment["qwen_assignments"]
        block_ids = sorted(
            {
                block_id
                for row in rows
                for block_id in row.get("block_ids", [])
            }
        )
        context.add_output_metadata(
            {
                "book_id": assignment["document_id"],
                "serve_id": assignment["qa_serve_id"],
                "request_count": len(rows),
                "block_count": len(block_ids),
                "block_ids": MetadataValue.json(block_ids),
                "queue_wait_seconds": round(
                    sum(float(row.get("queue_wait", 0)) for row in rows), 3
                ),
                "processing_time_seconds": round(
                    sum(float(row.get("processing_time", 0)) for row in rows),
                    3,
                ),
            }
        )
        return {
            "document_id": assignment["document_id"],
            "serve_id": assignment["qa_serve_id"],
            "request_count": len(rows),
            "block_count": len(block_ids),
        }

    return observe


observe_mineru_worker_pod = _resource_lifecycle_op(
    "mineru_worker_pod",
    "mineru_resource_manager",
    "pod_released",
    {},
)
observe_mineru_serve_a = _resource_lifecycle_op(
    "mineru_serve_a",
    "mineru_serve",
    "released",
    {"serve_id": "mineru-serve-a"},
)
observe_mineru_serve_b = _resource_lifecycle_op(
    "mineru_serve_b",
    "mineru_serve",
    "released",
    {"serve_id": "mineru-serve-b"},
)

mineru_serve_a_ready = _mineru_serve_ready("A")
mineru_serve_b_ready = _mineru_serve_ready("B")
mineru_serve_a_books = _fan_out_mineru_books("A")
mineru_serve_b_books = _fan_out_mineru_books("B")

QA_GROUPS = {
    "qa-8-9": (8, 9),
    "qa-10-11": (10, 11),
    "qa-12-13": (12, 13),
}
qa_serve_ready_ops = {
    (worker_group, chip_id): _qa_serve_ready(worker_group, chip_id)
    for worker_group, chips in QA_GROUPS.items()
    for chip_id in chips
}
qwen_assignment_fanout_ops = {
    (worker_group, chip_id): _qwen_assignment_fanout(
        worker_group, chip_id
    )
    for worker_group, chips in QA_GROUPS.items()
    for chip_id in chips
}
qwen_book_assignment_ops = {
    (worker_group, chip_id): _qwen_book_assignment(
        worker_group, chip_id
    )
    for worker_group, chips in QA_GROUPS.items()
    for chip_id in chips
}


@op(
    ins={
        "service_a_books": In(list),
        "service_b_books": In(list),
        "state": In(dict),
    },
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def merge_observed_books(
    context,
    service_a_books: list[dict],
    service_b_books: list[dict],
    state: dict,
) -> dict:
    books = service_a_books + service_b_books
    status = context.resources.ray_jobs.wait(
        state["ray_job_id"], timeout_seconds=12 * 3600
    )
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(
            context.resources.ray_jobs.logs(state["ray_job_id"])[-12000:]
        )
    summary = context.resources.s3.read_json(
        state["output_bucket"],
        f"{state['stage2_prefix']}/_SUMMARY.json",
    )
    context.add_output_metadata(
        {
            "ray_job_id": state["ray_job_id"],
            "books": len(books),
            "success_documents": summary["success_documents"],
            "failed_documents": summary["failed_documents"],
            "elapsed_seconds": summary["elapsed_seconds"],
            "output": MetadataValue.path(
                f"s3://{state['output_bucket']}/{state['stage2_prefix']}"
            ),
        }
    )
    return summary


@op(ins={"state": In(dict), "summary": In(dict)}, out=Out(dict))
def prepare_resource_observation(
    context,
    state: dict,
    summary: dict,
) -> dict:
    context.add_output_metadata(
        {
            "ray_job_id": state["ray_job_id"],
            "ray_status": summary["status"],
            "resource_observation_mode": "post-run event replay",
        }
    )
    return state


def summary_probe(name: str):
    @op(
        name=name,
        ins={"state": In(dict)},
        out=Out(dict),
        required_resource_keys={"s3"},
    )
    def probe(context, state: dict) -> dict:
        key = f"{state['stage2_prefix']}/_SUMMARY.json"
        summary = context.resources.s3.read_json(state["output_bucket"], key)
        context.add_output_metadata(
            {
                "status": summary["status"],
                "success_documents": summary["success_documents"],
                "failed_documents": summary["failed_documents"],
                "judge_enabled": summary["judge_enabled"],
            }
        )
        return state

    return probe


drain_and_release_mineru_workers = summary_probe(
    "drain_and_release_mineru_workers"
)
drain_and_release_qa_workers = summary_probe("drain_and_release_qa_workers")
collect_autoscaling_profile = summary_probe("collect_autoscaling_profile")
finalize_run_manifest = summary_probe("finalize_run_manifest")


AUTOSCALE_RAY_RESOURCE = RayJobResource(
    dashboard_address=AUTOSCALE_DASHBOARD
)


@job(
    resource_defs={
        "s3": S3Resource(),
        "ray_jobs": AUTOSCALE_RAY_RESOURCE,
    },
    config=default_config(),
    executor_def=multiprocess_executor.configured({"max_concurrent": 6}),
)
def k12_e2e_autoscale_nojudge_job():
    state = prepare_autoscale_nojudge_run()
    state = audit_npu_and_worker_groups(state)
    state = inventory_source_documents(state)
    state = submit_ray_pipeline_job(state)

    mineru_pod = mineru_worker_pod_ready(state)
    serve_a = mineru_serve_a_ready(mineru_pod)
    serve_b = mineru_serve_b_ready(mineru_pod)

    books_a = mineru_serve_a_books(serve_a)
    parsed_a = books_a.map(book_mineru.alias("mineru_a_book"))
    cleaned_a = parsed_a.map(book_cleaning.alias("cleaning_a_book"))
    generated_a = cleaned_a.map(book_qa_mcq.alias("qa_mcq_a_book"))
    validated_a = generated_a.map(
        book_schema_validate.alias("schema_validate_a_book")
    )
    written_a = validated_a.map(
        book_minio_write.alias("minio_write_a_book")
    )

    books_b = mineru_serve_b_books(serve_b)
    parsed_b = books_b.map(book_mineru.alias("mineru_b_book"))
    cleaned_b = parsed_b.map(book_cleaning.alias("cleaning_b_book"))
    generated_b = cleaned_b.map(book_qa_mcq.alias("qa_mcq_b_book"))
    validated_b = generated_b.map(
        book_schema_validate.alias("schema_validate_b_book")
    )
    written_b = validated_b.map(
        book_minio_write.alias("minio_write_b_book")
    )
    summary = merge_observed_books(
        written_a.collect(),
        written_b.collect(),
        state,
    )

    first_cleaned = first_cleaning_output_ready(state)
    qa_pods = {
        "qa-8-9": qa_8_9_worker_pod_ready(first_cleaned),
        "qa-10-11": qa_10_11_worker_pod_ready(first_cleaned),
        "qa-12-13": qa_12_13_worker_pod_ready(first_cleaned),
    }
    for worker_group, chips in QA_GROUPS.items():
        for chip_id in chips:
            serve = qa_serve_ready_ops[(worker_group, chip_id)](
                qa_pods[worker_group]
            )
            assignments = qwen_assignment_fanout_ops[
                (worker_group, chip_id)
            ](serve, summary)
            assignments.map(
                qwen_book_assignment_ops[(worker_group, chip_id)]
            )

    resource_state = prepare_resource_observation(state, summary)
    collect_autoscaling_profile(resource_state)
    finalize_run_manifest(resource_state)


AUTOSCALE_NOJUDGE_JOBS = [k12_e2e_autoscale_nojudge_job]
