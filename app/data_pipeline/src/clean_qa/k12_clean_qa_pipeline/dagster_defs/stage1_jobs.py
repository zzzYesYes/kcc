from __future__ import annotations

import json
import os
import shlex
import time
from pathlib import Path

from dagster import Field, In, MetadataValue, Out, job, op
from ray.job_submission import JobStatus

from clean_qa.k12_clean_qa_pipeline.stage1_clean import STAGE1_VERSION
from clean_qa.mineru_dagster.resources import RayJobResource, S3Resource


SOURCE_PREFIX = "full-output/mineru34-hybrid-a3-full-20260722T104600Z"
TEST_OUTPUT_PREFIX = "stage1/test-10/stage1-v1.0.2-20260724"
PIPELINE_WORKING_DIR = os.environ.get("PIPELINE_WORKING_DIR", "/opt/data-pipeline/src")
PIPELINE_NO_PROXY = os.environ.get(
    "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
)

STAGE1_SCHEMA = {
    "source_bucket": Field(str, default_value="k12-mineru-output"),
    "source_prefix": Field(str, default_value=SOURCE_PREFIX),
    "output_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "output_prefix": Field(str, default_value=TEST_OUTPUT_PREFIX),
    "selection_manifest_key": Field(str, default_value="manifests/stage1_test_10.json"),
    "count": Field(int, default_value=10),
    "resume": Field(bool, default_value=True),
    "cpu_workers": Field(int, default_value=16),
    "max_document_inflight": Field(int, default_value=8),
    "stage1_version": Field(str, default_value=STAGE1_VERSION),
    "automated_validation": Field(bool, default_value=True),
    "dry_run": Field(bool, default_value=False),
}


def stage1_default_config(full: bool) -> dict:
    return {
        "ops": {
            "resolve_source_manifest": {
                "config": {
                    "source_bucket": "k12-mineru-output",
                    "source_prefix": SOURCE_PREFIX,
                    "output_bucket": "k12-cleaned-corpus",
                    "output_prefix": (
                        "stage1/full/stage1-v1.0.2"
                        if full
                        else TEST_OUTPUT_PREFIX
                    ),
                    "selection_manifest_key": (
                        "" if full else "manifests/stage1_test_10.json"
                    ),
                    "count": 0 if full else 10,
                    "resume": True,
                    "cpu_workers": 16,
                    "max_document_inflight": 12 if full else 8,
                    "stage1_version": STAGE1_VERSION,
                    "automated_validation": not full,
                    "dry_run": False,
                }
            }
        }
    }


@op(
    config_schema=STAGE1_SCHEMA,
    out=Out(dict),
    required_resource_keys={"s3"},
)
def resolve_source_manifest(context) -> dict:
    config = dict(context.op_config)
    if config["stage1_version"] != STAGE1_VERSION:
        raise ValueError(
            f"stage1_version must be {STAGE1_VERSION}, got {config['stage1_version']}"
        )
    if config["source_bucket"] == config["output_bucket"] and config[
        "output_prefix"
    ].startswith(config["source_prefix"].rstrip("/") + "/"):
        raise ValueError("output prefix overlaps the read-only MinerU source")
    s3: S3Resource = context.resources.s3
    summary = s3.read_json(
        config["source_bucket"],
        f"{config['source_prefix'].rstrip('/')}/_SUMMARY.json",
    )
    if summary.get("status") != "success" or summary.get("failed_count") != 0:
        raise RuntimeError(f"MinerU source is not complete: {summary}")
    if config["selection_manifest_key"]:
        if not s3.exists(config["output_bucket"], config["selection_manifest_key"]):
            local = (
                Path(PIPELINE_WORKING_DIR)
                / "clean_qa/k12_clean_qa_pipeline/manifests/stage1_test_10.json"
            )
            s3.write_json(
                config["output_bucket"],
                config["selection_manifest_key"],
                json.loads(local.read_text(encoding="utf-8")),
            )
        selection = s3.read_json(
            config["output_bucket"], config["selection_manifest_key"]
        )
        if len(selection.get("documents", [])) != config["count"]:
            raise ValueError("selection manifest count does not match requested count")
    state = {
        **config,
        "dagster_run_id": context.run_id,
        "mineru_summary": {
            key: summary.get(key)
            for key in ("pdf_count", "success_count", "skipped_count", "failed_count")
        },
    }
    context.add_output_metadata(
        {
            "source": MetadataValue.path(
                f"s3://{config['source_bucket']}/{config['source_prefix']}"
            ),
            "available_documents": int(summary["pdf_count"]),
            "selected_documents": int(config["count"] or summary["pdf_count"]),
            "dry_run": config["dry_run"],
        }
    )
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def select_documents(context, state: dict) -> dict:
    if state["count"] == 10 and not state["selection_manifest_key"]:
        raise ValueError("10-document job requires a fixed selection manifest")
    context.add_output_metadata(
        {
            "document_count": state["count"] or state["mineru_summary"]["pdf_count"],
            "selection_manifest_key": state["selection_manifest_key"] or "(all successful)",
        }
    )
    return state


def stage1_entrypoint(state: dict) -> str:
    parts = [
        "python3",
        "-m",
        "clean_qa.k12_clean_qa_pipeline.stage1_clean.driver",
        "--source-bucket",
        state["source_bucket"],
        "--source-prefix",
        state["source_prefix"],
        "--output-bucket",
        state["output_bucket"],
        "--output-prefix",
        state["output_prefix"],
        "--max-document-inflight",
        str(state["max_document_inflight"]),
    ]
    if state["selection_manifest_key"]:
        parts += ["--selection-manifest-key", state["selection_manifest_key"]]
    if state["count"]:
        parts += ["--limit", str(state["count"])]
    if state["resume"]:
        parts.append("--resume")
    if state["automated_validation"]:
        parts.append("--automated-validation")
    if state["dry_run"]:
        parts.append("--dry-run")
    return " ".join(shlex.quote(part) for part in parts)


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs"},
)
def submit_ray_clean_job(context, state: dict) -> dict:
    entrypoint = stage1_entrypoint(state)
    state["ray_entrypoint"] = entrypoint
    state["stage1_ray_job_id"] = (
        f"stage1-clean-{'dryrun' if state['dry_run'] else 'run'}-{context.run_id[:8]}"
    )
    if not state["dry_run"]:
        ray_jobs: RayJobResource = context.resources.ray_jobs
        ray_jobs.submit(
            state["stage1_ray_job_id"],
            entrypoint,
            {
                "PYTHONPATH": f".:{PIPELINE_WORKING_DIR}",
                "NO_PROXY": PIPELINE_NO_PROXY,
                "no_proxy": PIPELINE_NO_PROXY,
            },
        )
    context.add_output_metadata(
        {
            "ray_job_id": state["stage1_ray_job_id"],
            "entrypoint": MetadataValue.text(entrypoint),
            "submitted": not state["dry_run"],
        }
    )
    return state


def stage_probe(name: str):
    @op(name=name, ins={"state": In(dict)}, out=Out(dict))
    def _probe(context, state: dict) -> dict:
        context.add_output_metadata(
            {
                "stage": name,
                "document_count": state["count"] or state["mineru_summary"]["pdf_count"],
                "ray_job_id": state["stage1_ray_job_id"],
            }
        )
        return state

    return _probe


read_mineru_markdown = stage_probe("read_mineru_markdown")
parse_document_structure = stage_probe("parse_document_structure")
filter_noise = stage_probe("filter_noise")
normalize_math_content = stage_probe("normalize_math_content")
build_structured_blocks = stage_probe("build_structured_blocks")
render_clean_markdown = stage_probe("render_clean_markdown")
write_document_outputs = stage_probe("write_document_outputs")


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def validate_outputs(context, state: dict) -> dict:
    if state["dry_run"]:
        context.add_output_metadata(
            {
                "dry_run": True,
                "manifest_valid": True,
                "ray_entrypoint_valid": bool(state["ray_entrypoint"]),
                "submitted": False,
            }
        )
        return state
    ray_jobs: RayJobResource = context.resources.ray_jobs
    status = ray_jobs.wait(
        state["stage1_ray_job_id"],
        timeout_seconds=24 * 3600,
    )
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(
            f"Stage 1 Ray job failed: {ray_jobs.logs(state['stage1_ray_job_id'])[-8000:]}"
        )
    s3: S3Resource = context.resources.s3
    summary = s3.read_json(
        state["output_bucket"],
        f"{state['output_prefix'].rstrip('/')}/_SUMMARY.json",
    )
    validation = summary.get("validation")
    if summary.get("status") != "success":
        raise RuntimeError(f"Stage 1 summary failed: {summary}")
    if state["automated_validation"] and (
        not validation or validation.get("status") != "pass"
    ):
        raise RuntimeError(f"Stage 1 automated validation failed: {validation}")
    state["stage1_summary"] = summary
    context.add_output_metadata(
        {
            "status": summary["status"],
            "total_documents": summary["total_documents"],
            "failed_documents": summary["failed_documents"],
            "kept_blocks": summary["metrics"]["kept_blocks"],
            "quarantine_blocks": summary["metrics"]["quarantine_blocks"],
            "formula_repairs": summary["metrics"]["formula_repairs"],
            "validation": MetadataValue.json(validation or {}),
        }
    )
    return state


@op(ins={"state": In(dict)}, out=Out(dict))
def write_summary(context, state: dict) -> dict:
    context.add_output_metadata(
        {
            "dagster_run_id": context.run_id,
            "ray_job_id": state["stage1_ray_job_id"],
            "output": MetadataValue.path(
                f"s3://{state['output_bucket']}/{state['output_prefix']}"
            ),
            "full_job_executed": not state["dry_run"],
        }
    )
    return state


def stage1_graph():
    state = select_documents(resolve_source_manifest())
    state = submit_ray_clean_job(state)
    state = read_mineru_markdown(state)
    state = parse_document_structure(state)
    state = filter_noise(state)
    state = normalize_math_content(state)
    state = build_structured_blocks(state)
    state = render_clean_markdown(state)
    state = write_document_outputs(state)
    state = validate_outputs(state)
    write_summary(state)


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage1_default_config(False),
)
def cleanjopbstage1_10():
    stage1_graph()


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage1_default_config(True),
)
def cleanjopbstage1_ful():
    stage1_graph()


STAGE1_JOBS = [cleanjopbstage1_10, cleanjopbstage1_ful]
