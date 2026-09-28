from __future__ import annotations

import json
import re
import shlex
import time

from dagster import AssetMaterialization, DynamicOut, DynamicOutput, Field, In, MetadataValue, Out, op
from ray.job_submission import JobStatus

from legacy.k12_cleaner import CLEANER_VERSION

from ..resources.ray_job_resource import RayJobResource
from ..resources.s3_resource import S3Resource


SCAN_SCHEMA = {
    "batch_id": str,
    "count": Field(int, default_value=10),
    "parsed_bucket": Field(str, default_value="k12-mineru-output"),
    "parsed_prefix": str,
    "output_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "output_prefix": str,
    "markdown_glob": Field(str, default_value="**/*.md"),
    "document_ui_detail_limit": Field(int, default_value=10),
}

RAY_STAGE_SCHEMA = {
    "parallelism": Field(int, default_value=10),
    "cpus_per_task": Field(float, default_value=2.0),
    "max_retries": Field(int, default_value=1),
    "task_timeout_seconds": Field(float, default_value=900.0),
}

STEP1_SCHEMA = {
    **RAY_STAGE_SCHEMA,
    "remove_image_markdown": Field(bool, default_value=True),
    "remove_natural_image_details": Field(bool, default_value=True),
    "text_image_policy": Field(str, default_value="classify"),
    "text_image_min_chars": Field(int, default_value=4),
    "repeated_line_min_occurrences": Field(int, default_value=3),
    "repeated_line_max_chars": Field(int, default_value=80),
    "watermark_regex": Field(str, default_value=r"(?:仅供|内部|试读|样书|水印|www\.)"),
    "remove_romanized_cover": Field(bool, default_value=True),
    "cover_scan_lines": Field(int, default_value=120),
    "isolated_text_max_chars": Field(int, default_value=1),
}

STEP2_SCHEMA = {
    **RAY_STAGE_SCHEMA,
    "preserve_latex": Field(bool, default_value=True),
    "convert_html_tables": Field(bool, default_value=True),
    "unicode_form": Field(str, default_value="NFKC"),
    "max_consecutive_blank_lines": Field(int, default_value=1),
    "ocr_max_repeated_char_run": Field(int, default_value=8),
    "drop_severe_ocr_lines": Field(bool, default_value=False),
    "ocr_warn_line_ratio": Field(float, default_value=0.02),
}

STEP3_SCHEMA = {
    **RAY_STAGE_SCHEMA,
    "chapter_heading_max_level": Field(int, default_value=3),
    "sample_min_chars": Field(int, default_value=200),
    "sample_max_chars": Field(int, default_value=8000),
    "sample_overlap_chars": Field(int, default_value=0),
    "include_source_uri": Field(bool, default_value=True),
}


def _validate_parallel_config(config: dict) -> None:
    if not 1 <= int(config["parallelism"]) <= 64:
        raise ValueError("parallelism must be in [1, 64]")
    if not 0.25 <= float(config["cpus_per_task"]) <= 16:
        raise ValueError("cpus_per_task must be in [0.25, 16]")
    if float(config["task_timeout_seconds"]) < 30:
        raise ValueError("task_timeout_seconds must be at least 30")


def _submit_and_monitor(context, state: dict, name: str, entrypoint: str) -> tuple[dict, dict]:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    job_id = f"cleaning-{name}-{state['batch_id']}-{int(time.time())}"
    ray_jobs.submit(
        job_id,
        entrypoint,
        {"PYTHONPATH": ".", "RAY_DEDUP_LOGS": "0"},
    )
    context.log.info("submitted Ray job %s: %s", job_id, entrypoint)
    final_event = None
    for line in ray_jobs.tail_logs(job_id):
        context.log.info("[ray:%s] %s", job_id, line)
        if "DAGSTER_EVENT " not in line:
            continue
        try:
            event = json.loads(line.split("DAGSTER_EVENT ", 1)[1].strip())
        except json.JSONDecodeError:
            continue
        if event.get("event") in {"MANIFEST_SCAN_SUCCEEDED", "STAGE_SUCCEEDED", "STAGE_FAILED"}:
            final_event = event
    status = ray_jobs.status(job_id)
    if status != JobStatus.SUCCEEDED or not final_event or final_event.get("event") == "STAGE_FAILED":
        logs = ray_jobs.logs(job_id)
        raise RuntimeError(f"Ray job {job_id} ended as {status}; final_event={final_event}; logs={logs[-4000:]}")
    state[f"{name}_ray_job_id"] = job_id
    state[f"{name}_event"] = final_event
    return state, final_event


@op(config_schema=SCAN_SCHEMA, out=Out(dict), required_resource_keys={"ray_jobs"})
def scan_cleaning_manifest(context) -> dict:
    state = dict(context.op_config)
    state["parsed_prefix"] = state["parsed_prefix"].strip("/")
    state["output_prefix"] = state["output_prefix"].strip("/")
    if context.job_name == "cleaning_smoke_10_job" and state["count"] != 10:
        raise ValueError("cleaning_smoke_10_job is intentionally limited to exactly 10 documents")
    if context.job_name == "cleaning_full_job" and state["count"] != 0:
        raise ValueError("cleaning_full_job requires count=0 so Daft scans the complete prefix")
    if not 0 <= state["document_ui_detail_limit"] <= 100:
        raise ValueError("document_ui_detail_limit must be in [0, 100]")
    state["manifest_key"] = f"{state['output_prefix']}/_control/{state['batch_id']}/cleaning_manifest.json"
    state["manifest_uri"] = f"s3://{state['output_bucket']}/{state['manifest_key']}"
    command = " ".join(
        shlex.quote(part)
        for part in (
            "python3", "-m", "legacy.k12_cleaner.ray_driver", "scan",
            "--batch-id", state["batch_id"], "--count", str(state["count"]),
            "--parsed-bucket", state["parsed_bucket"], "--parsed-prefix", state["parsed_prefix"],
            "--output-bucket", state["output_bucket"], "--output-prefix", state["output_prefix"],
            "--markdown-glob", state["markdown_glob"],
            "--manifest-uri", state["manifest_uri"],
        )
    )
    state, event = _submit_and_monitor(context, state, "manifest", command)
    state["count"] = int(event["document_count"])
    state["document_ui_count"] = min(state["count"], state["document_ui_detail_limit"])
    context.add_output_metadata(
        {
            "document_count": event["document_count"],
            "input_bytes": event["input_bytes"],
            "elapsed_seconds": event["elapsed_seconds"],
            "manifest_uri": MetadataValue.url(state["manifest_uri"]),
            "ray_job_id": state["manifest_ray_job_id"],
        }
    )
    return state


def _submit_stage(context, state: dict, stage: str) -> dict:
    config = dict(context.op_config)
    _validate_parallel_config(config)
    runtime_keys = {"parallelism", "cpus_per_task", "max_retries", "task_timeout_seconds"}
    cleaning_config = {key: value for key, value in config.items() if key not in runtime_keys}
    job_id = f"cleaning-{stage}-{state['batch_id']}-{time.time_ns()}"
    command = " ".join(
        shlex.quote(part)
        for part in (
            "python3", "-m", "legacy.k12_cleaner.ray_driver", "stage",
            "--stage", stage,
            "--manifest-uri", state["manifest_uri"],
            "--run-id", job_id,
            "--parallelism", str(config["parallelism"]),
            "--cpus-per-task", str(config["cpus_per_task"]),
            "--max-retries", str(config["max_retries"]),
            "--task-timeout-seconds", str(config["task_timeout_seconds"]),
            "--stage-config-json", json.dumps(cleaning_config, ensure_ascii=False, separators=(",", ":")),
        )
    )
    context.resources.ray_jobs.submit(
        job_id,
        command,
        {
            "PYTHONPATH": ".",
            "RAY_DEDUP_LOGS": "1" if state["count"] > 100 else "0",
        },
    )
    state[f"{stage}_ray_job_id"] = job_id
    state[f"{stage}_timeout_seconds"] = float(config["task_timeout_seconds"])
    state[f"{stage}_parallelism"] = int(config["parallelism"])
    context.log.info("submitted %s as Ray job %s", stage, job_id)
    context.add_output_metadata(
        {
            "document_count": state["count"],
            "parallelism": config["parallelism"],
            "cpus_per_task": config["cpus_per_task"],
            "max_retries": config["max_retries"],
            "task_timeout_seconds": config["task_timeout_seconds"],
            "ray_job_id": job_id,
        }
    )
    return state


def _fan_out_stage_documents(context, state: dict, stage: str):
    manifest = context.resources.s3.read_json(state["output_bucket"], state["manifest_key"])
    visible_documents = manifest["documents"][: state["document_ui_detail_limit"]]
    for index, row in enumerate(visible_documents, start=1):
        document_id = row["document_id"]
        mapping_key = re.sub(r"[^A-Za-z0-9_]", "_", f"doc_{index:04d}_{document_id}")
        yield DynamicOutput(
            {
                "stage": stage,
                "document_id": document_id,
                "document_index": index,
                "document_total": len(manifest["documents"]),
                "output_bucket": state["output_bucket"],
                "audit_key": f"{state['output_prefix']}/_control/{state['batch_id']}/stages/{stage}/{document_id}.json",
                "ray_job_id": state[f"{stage}_ray_job_id"],
                "timeout_seconds": state[f"{stage}_timeout_seconds"],
            },
            mapping_key=mapping_key,
        )


def _monitor_document(context, descriptor: dict) -> dict:
    s3: S3Resource = context.resources.s3
    ray_jobs: RayJobResource = context.resources.ray_jobs
    deadline = time.time() + descriptor["timeout_seconds"]
    context.log.info(
        "waiting for %s document %d/%d: %s",
        descriptor["stage"],
        descriptor["document_index"],
        descriptor["document_total"],
        descriptor["document_id"],
    )
    while time.time() < deadline:
        if s3.exists(descriptor["output_bucket"], descriptor["audit_key"]):
            audit = s3.read_json(descriptor["output_bucket"], descriptor["audit_key"])
            if audit.get("run_id") == descriptor["ray_job_id"]:
                metadata = {
                    "document": f"{descriptor['document_index']}/{descriptor['document_total']}",
                    "document_id": descriptor["document_id"],
                    "stage": descriptor["stage"],
                    "status": audit.get("status", "unknown"),
                    "input_count": int(audit.get("input_count", 0)),
                    "output_count": int(audit.get("output_count", 0)),
                    "elapsed_seconds": float(audit.get("elapsed_seconds", 0)),
                    "worker_run_id": descriptor["ray_job_id"],
                }
                context.add_output_metadata(metadata)
                if audit.get("status") != "success":
                    raise RuntimeError(
                        f"{descriptor['stage']} failed for {descriptor['document_id']}: {audit.get('error')}"
                    )
                context.log.info(
                    "%s succeeded for %s: input=%s output=%s elapsed=%ss",
                    descriptor["stage"], descriptor["document_id"],
                    audit.get("input_count"), audit.get("output_count"), audit.get("elapsed_seconds"),
                )
                return audit
        status = ray_jobs.status(descriptor["ray_job_id"])
        if status in {JobStatus.FAILED, JobStatus.STOPPED}:
            raise RuntimeError(
                f"Ray job {descriptor['ray_job_id']} ended as {status} before a successful audit for "
                f"{descriptor['document_id']}"
            )
        time.sleep(1.0)
    raise TimeoutError(
        f"timed out waiting for {descriptor['stage']} document {descriptor['document_id']}"
    )


def _monitor_stage(context, state: dict, stage: str) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    s3: S3Resource = context.resources.s3
    job_id = state[f"{stage}_ray_job_id"]
    final_event = None
    for line in ray_jobs.tail_logs(job_id):
        context.log.info("[ray:%s] %s", job_id, line)
        if "DAGSTER_EVENT " not in line:
            continue
        try:
            event = json.loads(line.split("DAGSTER_EVENT ", 1)[1].strip())
        except json.JSONDecodeError:
            continue
        if event.get("event") in {"STAGE_SUCCEEDED", "STAGE_FAILED"}:
            final_event = event
    status = ray_jobs.status(job_id)
    summary_key = f"{state['output_prefix']}/_control/{state['batch_id']}/stages/{stage}/_SUMMARY.json"
    summary = s3.read_json(state["output_bucket"], summary_key)
    if (
        status != JobStatus.SUCCEEDED
        or summary.get("status") != "success"
        or summary.get("run_id") != job_id
    ):
        raise RuntimeError(
            f"Ray job {job_id} ended as {status}; final_event={final_event}; "
            f"durable_summary={summary}; logs={ray_jobs.logs(job_id)[-4000:]}"
        )
    final_event = {
        "event": "STAGE_SUCCEEDED",
        "stage": stage,
        "document_count": summary["document_count"],
        "success_count": summary["success_count"],
        "input_count": summary["input_count"],
        "output_count": summary["output_count"],
        "elapsed_seconds": summary["elapsed_seconds"],
        "summary_uri": f"s3://{state['output_bucket']}/{summary_key}",
    }
    state.setdefault("stage_results", {})[stage] = final_event
    context.add_output_metadata(
        {
            "input_document_count": final_event["document_count"],
            "success_document_count": final_event["success_count"],
            "input_count": final_event["input_count"],
            "output_count": final_event["output_count"],
            "elapsed_seconds": final_event["elapsed_seconds"],
            "ray_job_id": job_id,
            "summary_uri": MetadataValue.url(final_event["summary_uri"]),
        }
    )
    return state


def _join_stage(context, state: dict, documents: list[dict], stage: str) -> dict:
    if len(documents) != state["document_ui_count"]:
        raise RuntimeError(
            f"{stage} expected {state['document_ui_count']} UI document statuses, got {len(documents)}"
        )
    failed = [row["document_id"] for row in documents if row.get("status") != "success"]
    if failed:
        raise RuntimeError(f"{stage} failed documents: {failed}")
    context.add_output_metadata(
        {
            "document_count": len(documents),
            "success_count": len(documents),
            "total_stage_document_count": state["count"],
            "input_count": sum(int(row.get("input_count", 0)) for row in documents),
            "output_count": sum(int(row.get("output_count", 0)) for row in documents),
        }
    )
    return state


@op(config_schema=STEP1_SCHEMA, ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def step_job1(context, state: dict) -> dict:
    return _submit_stage(context, state, "step_job1")


@op(ins={"state": In(dict)}, out=DynamicOut(dict), required_resource_keys={"s3"})
def fan_out_step_job1_documents(context, state: dict):
    yield from _fan_out_stage_documents(context, state, "step_job1")


@op(ins={"descriptor": In(dict)}, out=Out(dict), required_resource_keys={"s3", "ray_jobs"})
def step_job1_document_status(context, descriptor: dict) -> dict:
    return _monitor_document(context, descriptor)


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs", "s3"})
def monitor_step_job1(context, state: dict) -> dict:
    return _monitor_stage(context, state, "step_job1")


@op(ins={"state": In(dict), "documents": In(list)}, out=Out(dict))
def join_step_job1(context, state: dict, documents: list[dict]) -> dict:
    return _join_stage(context, state, documents, "step_job1")


@op(config_schema=STEP2_SCHEMA, ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def step_job2(context, state: dict) -> dict:
    return _submit_stage(context, state, "step_job2")


@op(ins={"state": In(dict)}, out=DynamicOut(dict), required_resource_keys={"s3"})
def fan_out_step_job2_documents(context, state: dict):
    yield from _fan_out_stage_documents(context, state, "step_job2")


@op(ins={"descriptor": In(dict)}, out=Out(dict), required_resource_keys={"s3", "ray_jobs"})
def step_job2_document_status(context, descriptor: dict) -> dict:
    return _monitor_document(context, descriptor)


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs", "s3"})
def monitor_step_job2(context, state: dict) -> dict:
    return _monitor_stage(context, state, "step_job2")


@op(ins={"state": In(dict), "documents": In(list)}, out=Out(dict))
def join_step_job2(context, state: dict, documents: list[dict]) -> dict:
    return _join_stage(context, state, documents, "step_job2")


@op(config_schema=STEP3_SCHEMA, ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def step_job3(context, state: dict) -> dict:
    return _submit_stage(context, state, "step_job3")


@op(ins={"state": In(dict)}, out=DynamicOut(dict), required_resource_keys={"s3"})
def fan_out_step_job3_documents(context, state: dict):
    yield from _fan_out_stage_documents(context, state, "step_job3")


@op(ins={"descriptor": In(dict)}, out=Out(dict), required_resource_keys={"s3", "ray_jobs"})
def step_job3_document_status(context, descriptor: dict) -> dict:
    return _monitor_document(context, descriptor)


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs", "s3"})
def monitor_step_job3(context, state: dict) -> dict:
    return _monitor_stage(context, state, "step_job3")


@op(ins={"state": In(dict), "documents": In(list)}, out=Out(dict))
def join_step_job3(context, state: dict, documents: list[dict]) -> dict:
    return _join_stage(context, state, documents, "step_job3")


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def validate_cleaned_outputs(context, state: dict) -> dict:
    s3: S3Resource = context.resources.s3
    summary_key = f"{state['output_prefix']}/_SUMMARY.json"
    summary = s3.read_json(state["output_bucket"], summary_key)
    manifest = s3.read_json(state["output_bucket"], state["manifest_key"])
    required = ("cleaned.md", "pretrain.jsonl", "quality_report.json", "provenance.json", "_SUCCESS.json")
    existing_keys = {
        item["Key"]
        for item in s3.list_prefix(state["output_bucket"], f"{state['output_prefix']}/")
    }
    checks = {
        "summary_status_success": summary.get("status") == "success",
        "document_count_matches": summary.get("document_count") == state["count"] == len(manifest["documents"]),
        "failed_document_count_zero": summary.get("failed_count") == 0,
        "all_success_markers_exist": all(
            f"{state['output_prefix']}/{row['document_id']}/_SUCCESS.json" in existing_keys
            for row in manifest["documents"]
        ),
        "all_required_outputs_exist": all(
            all(f"{state['output_prefix']}/{row['document_id']}/{name}" in existing_keys for name in required)
            for row in manifest["documents"]
        ),
    }
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError(f"cleaning validation failed: {failed}")
    state["cleaning_summary"] = summary
    state["cleaning_summary_uri"] = f"s3://{state['output_bucket']}/{summary_key}"
    state["cleaning_checks"] = checks
    context.add_output_metadata({**checks, "document_count": summary["document_count"], "summary_uri": MetadataValue.url(state["cleaning_summary_uri"])})
    return state


@op(ins={"state": In(dict)}, required_resource_keys={"s3"})
def materialize_cleaned_documents(context, state: dict) -> None:
    summary = state["cleaning_summary"]
    elapsed = sum(float(value.get("elapsed_seconds", 0)) for value in state.get("stage_results", {}).values())
    plain_metadata = {
        "input_document_count": summary["document_count"],
        "success_document_count": summary["success_count"],
        "failed_document_count": summary["failed_count"],
        "training_sample_count": summary.get("output_count", 0),
        "pipeline_stage_count": 3,
        "pipeline_elapsed_seconds": round(elapsed, 3),
        "output_bucket": state["output_bucket"],
        "output_prefix": state["output_prefix"],
        "summary_uri": state["cleaning_summary_uri"],
        "cleaning_code_version": CLEANER_VERSION,
    }
    metadata = {
        **plain_metadata,
        "output_prefix": MetadataValue.path(state["output_prefix"]),
        "summary_uri": MetadataValue.url(state["cleaning_summary_uri"]),
        "stage_metrics": MetadataValue.json(state.get("stage_results", {})),
    }
    context.instance.add_dynamic_partitions("batch_id", [state["batch_id"]])
    context.log_event(AssetMaterialization(asset_key="cleaned_documents", partition=state["batch_id"], metadata=metadata))
    context.resources.s3.write_json(
        state["output_bucket"],
        f"_control/dagster/cleaning-registrations/{state['batch_id']}.json",
        {"batch_id": state["batch_id"], "checks": state["cleaning_checks"], "metadata": plain_metadata},
    )
