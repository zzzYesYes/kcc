from __future__ import annotations

import hashlib
import math
import os
import re
import shlex
import time
from datetime import timezone

from dagster import DynamicOut, DynamicOutput, Field, In, MetadataValue, Out, job, op
from ray.job_submission import JobStatus

from clean_qa.k12_clean_qa_pipeline.stage1_clean import STAGE1_VERSION
from clean_qa.k12_clean_qa_pipeline.stage2_qa import PROMPT_VERSION, STAGE2_VERSION
from clean_qa.mineru_dagster.resources import RayJobResource, S3Resource


DEFAULT_INPUT_PREFIX = os.environ.get(
    "K12_SOURCE_PREFIX",
    "source=tchMaterial-parser/batch_id=k12_pdf_full_20260713T062427Z",
).rstrip("/") + "/"
DEFAULT_INPUT_KEY = os.environ.get("K12_SAMPLE_OBJECT_KEY") or (
    DEFAULT_INPUT_PREFIX
    + "content_id=12ef79ba-6a49-47e6-987f-5b0df8c1130b/pdf/"
    "义务教育教科书·科学·学生活动手册三年级下册.pdf"
)

E2E_SCHEMA = {
    "input_bucket": Field(str, default_value="k12-textbook-raw"),
    "input_prefix": Field(
        str,
        default_value=DEFAULT_INPUT_PREFIX,
    ),
    "input_object_key": Field(
        str,
        default_value=DEFAULT_INPUT_KEY,
        description="Optional exact PDF key. Clear it to select count PDFs from input_prefix.",
    ),
    "input_object_keys": Field(
        [str],
        default_value=[],
        description="Optional reproducible PDF key list; takes precedence over input_object_key.",
    ),
    "count": Field(int, default_value=1),
    "mineru_bucket": Field(str, default_value="k12-mineru-output"),
    "output_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "output_root": Field(str, default_value="e2e-demo"),
    "resume": Field(bool, default_value=True),
    "mineru_batch_size": Field(int, default_value=1),
    "inference_slots": Field(int, default_value=4),
    "queue_size": Field(int, default_value=6),
    "block_inflight": Field(int, default_value=8),
    "generation_max_inflight": Field(int, default_value=8),
    "judge_max_inflight": Field(int, default_value=4),
    "http_pool_size": Field(int, default_value=16),
    "microbatch_size": Field(int, default_value=2),
    "max_blocks_per_document": Field(
        int,
        default_value=0,
        description="0 keeps the Stage 2 v1.1 chapter/document quota policy.",
    ),
    "judge_batch_size": Field(int, default_value=8),
    "qwen_model": Field(str, default_value="qwen3.6-35b-a3b"),
}


def default_config() -> dict:
    return {"ops": {"resolve_e2e_books": {"config": {
        key: field.default_value for key, field in E2E_SCHEMA.items()
    }}}}


def stable_document_id(bucket: str, key: str, etag: str) -> str:
    digest = hashlib.sha1(f"{bucket}/{key}/{etag}".encode()).hexdigest()[:20]
    return f"pdf-{digest}"


@op(config_schema=E2E_SCHEMA, out=Out(dict), required_resource_keys={"s3"})
def resolve_e2e_books(context) -> dict:
    config = dict(context.op_config)
    if config["count"] < 1:
        raise ValueError("count must be positive")
    if config["queue_size"] < config["inference_slots"]:
        raise ValueError("queue_size must be at least inference_slots")
    s3: S3Resource = context.resources.s3
    client = s3.client()
    objects = []
    exact_keys = [value.strip() for value in config["input_object_keys"] if value.strip()]
    exact_key = config["input_object_key"].strip()
    if exact_keys:
        for key in exact_keys:
            head = client.head_object(Bucket=config["input_bucket"], Key=key)
            objects.append({
                "Key": key,
                "ETag": head["ETag"],
                "Size": head["ContentLength"],
                "LastModified": head["LastModified"],
            })
    elif exact_key:
        head = client.head_object(Bucket=config["input_bucket"], Key=exact_key)
        objects = [{
            "Key": exact_key,
            "ETag": head["ETag"],
            "Size": head["ContentLength"],
            "LastModified": head["LastModified"],
        }]
    else:
        objects = [
            row
            for row in s3.list_prefix(config["input_bucket"], config["input_prefix"])
            if row["Key"].lower().endswith(".pdf")
        ]
        objects.sort(key=lambda row: row["Key"])
        objects = objects[: config["count"]]
    if not objects:
        raise RuntimeError("no PDF objects matched the E2E input selection")

    documents = []
    for row in objects[: config["count"]]:
        etag = row["ETag"].strip('"')
        documents.append({
            "document_id": stable_document_id(
                config["input_bucket"], row["Key"], etag
            ),
            "object_key": row["Key"],
            "etag": etag,
            "size_bytes": int(row["Size"]),
            "estimated_page_count": max(1, math.ceil(int(row["Size"]) / 458752)),
            "last_modified": row["LastModified"].astimezone(timezone.utc).isoformat(),
        })

    run_key = context.run_id[:12]
    run_root = f"{config['output_root'].rstrip('/')}/{run_key}"
    state = {
        **config,
        "run_key": run_key,
        "documents": documents,
        "manifest_key": f"{run_root}/manifest.json",
        "mineru_prefix": f"{run_root}/mineru",
        "stage1_prefix": f"{run_root}/stage1",
        "stage2_prefix": f"{run_root}/stage2",
    }
    s3.write_json(
        config["mineru_bucket"],
        state["manifest_key"],
        {
            "source": {
                "bucket": config["input_bucket"],
                "prefix": config["input_prefix"],
            },
            "documents": documents,
        },
    )
    context.add_output_metadata({
        "document_count": len(documents),
        "documents": MetadataValue.json(documents),
        "manifest": MetadataValue.path(
            f"s3://{config['mineru_bucket']}/{state['manifest_key']}"
        ),
        "mineru_devices": "physical 12,13",
        "qwen_devices": "physical 14,15",
        "stage1_version": STAGE1_VERSION,
        "stage2_version": STAGE2_VERSION,
        "prompt_version": PROMPT_VERSION,
    })
    return state


def e2e_entrypoint(state: dict) -> str:
    parts = [
        "python3", "-m", "clean_qa.k12_clean_qa_pipeline.e2e_demo.driver",
        "--manifest-bucket", state["mineru_bucket"],
        "--manifest-key", state["manifest_key"],
        "--input-bucket", state["input_bucket"],
        "--mineru-bucket", state["mineru_bucket"],
        "--mineru-prefix", state["mineru_prefix"],
        "--output-bucket", state["output_bucket"],
        "--stage1-prefix", state["stage1_prefix"],
        "--stage2-prefix", state["stage2_prefix"],
        "--service-a-logical-id", "12",
        "--service-b-logical-id", "13",
        "--mineru-batch-size", str(state["mineru_batch_size"]),
        "--inference-slots", str(state["inference_slots"]),
        "--queue-size", str(state["queue_size"]),
        "--block-inflight", str(state["block_inflight"]),
        "--generation-max-inflight", str(state["generation_max_inflight"]),
        "--judge-max-inflight", str(state["judge_max_inflight"]),
        "--http-pool-size", str(state["http_pool_size"]),
        "--microbatch-size", str(state["microbatch_size"]),
        "--max-blocks-per-document", str(state["max_blocks_per_document"]),
        "--judge-batch-size", str(state["judge_batch_size"]),
        "--qwen-model", state["qwen_model"],
    ]
    if state["resume"]:
        parts.append("--resume")
    return " ".join(shlex.quote(value) for value in parts)


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs"},
)
def submit_e2e_ray_pipeline(context, state: dict) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    job_id = f"k12-e2e-{state['run_key']}"
    ray_jobs.submit(job_id, e2e_entrypoint(state))
    context.add_output_metadata({
        "ray_job_id": job_id,
        "entrypoint": MetadataValue.text(e2e_entrypoint(state)),
        "pipeline": "MinerU -> deterministic clean -> Qwen QA/MCQ",
    })
    return {**state, "ray_job_id": job_id}


@op(ins={"state": In(dict)}, out=DynamicOut(dict))
def fan_out_e2e_books(context, state: dict):
    for index, document in enumerate(state["documents"], start=1):
        document_id = document["document_id"]
        mapping_key = re.sub(r"[^A-Za-z0-9_]", "_", f"book_{index:02d}_{document_id}")
        context.log.info("Book %s/%s: %s", index, len(state["documents"]), document_id)
        yield DynamicOutput(
            {**state, **document, "book_index": index},
            mapping_key=mapping_key,
        )


def wait_for_marker(
    context,
    book: dict,
    bucket: str,
    marker_key: str,
    stage: str,
    timeout_seconds: int,
) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    s3: S3Resource = context.resources.s3
    deadline = time.monotonic() + timeout_seconds
    next_log = 0.0
    while time.monotonic() < deadline:
        if s3.exists(bucket, marker_key):
            marker = s3.read_json(bucket, marker_key)
            context.add_output_metadata({
                "document_id": book["document_id"],
                "stage": stage,
                "status": "success",
                "marker": MetadataValue.path(f"s3://{bucket}/{marker_key}"),
                "details": MetadataValue.json(marker),
            })
            return {**book, f"{stage}_marker": marker}
        status = ray_jobs.status(book["ray_job_id"])
        if status in {JobStatus.FAILED, JobStatus.STOPPED, JobStatus.SUCCEEDED}:
            raise RuntimeError(
                f"Ray job became {status} before {stage} completed for "
                f"{book['document_id']}"
            )
        if time.monotonic() >= next_log:
            context.log.info(
                "Book %s is waiting in stage %s", book["document_id"], stage
            )
            next_log = time.monotonic() + 15
        time.sleep(2)
    raise TimeoutError(f"timed out waiting for {stage}: {book['document_id']}")


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def mineru_parse_book(context, book: dict) -> dict:
    return wait_for_marker(
        context,
        book,
        book["mineru_bucket"],
        f"{book['mineru_prefix']}/{book['document_id']}/_SUCCESS.json",
        "mineru",
        4 * 3600,
    )


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def clean_book(context, book: dict) -> dict:
    return wait_for_marker(
        context,
        book,
        book["output_bucket"],
        f"{book['stage1_prefix']}/{book['document_id']}/_SUCCESS.json",
        "cleaning",
        2 * 3600,
    )


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def generate_qa_book(context, book: dict) -> dict:
    return wait_for_marker(
        context,
        book,
        book["output_bucket"],
        f"{book['stage2_prefix']}/{book['document_id']}/_SUCCESS.json",
        "qa",
        8 * 3600,
    )


@op(ins={"book": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def validate_e2e_book_outputs(context, book: dict) -> dict:
    s3: S3Resource = context.resources.s3
    document_id = book["document_id"]
    clean_report = s3.read_json(
        book["output_bucket"],
        f"{book['stage1_prefix']}/{document_id}/cleaning_report.json",
    )
    generation_report = s3.read_json(
        book["output_bucket"],
        f"{book['stage2_prefix']}/{document_id}/generation_report.json",
    )
    metrics = generation_report["metrics"]
    verified_total = int(metrics.get("qa_verified", 0)) + int(
        metrics.get("mcq_verified", 0)
    )
    if verified_total < 1:
        raise RuntimeError(
            f"{document_id} produced no verified QA or MCQ training items"
        )
    context.add_output_metadata({
        "document_id": document_id,
        "clean_blocks": int(clean_report.get("kept_block_count", 0)),
        "exercises": int(clean_report.get("exercise_count", 0)),
        "qa_verified": int(metrics.get("qa_verified", 0)),
        "mcq_verified": int(metrics.get("mcq_verified", 0)),
        "stage2_output": MetadataValue.path(
            f"s3://{book['output_bucket']}/{book['stage2_prefix']}/{document_id}"
        ),
    })
    return {
        "document_id": document_id,
        "status": "success",
        "clean_blocks": int(clean_report.get("kept_block_count", 0)),
        "qa_verified": int(metrics.get("qa_verified", 0)),
        "mcq_verified": int(metrics.get("mcq_verified", 0)),
    }


@op(
    ins={"books": In(list), "state": In(dict)},
    out=Out(dict),
    required_resource_keys={"s3", "ray_jobs"},
)
def merge_e2e_books(context, books: list[dict], state: dict) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    status = ray_jobs.wait(state["ray_job_id"], timeout_seconds=12 * 3600)
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(
            f"E2E Ray job failed: {ray_jobs.logs(state['ray_job_id'])[-12000:]}"
        )
    summary = context.resources.s3.read_json(
        state["output_bucket"],
        f"{state['stage2_prefix']}/_E2E_SUMMARY.json",
    )
    context.add_output_metadata({
        "ray_job_id": state["ray_job_id"],
        "documents": len(books),
        "success_documents": summary["success_documents"],
        "elapsed_seconds": summary["elapsed_seconds"],
        "qa_verified": sum(row["qa_verified"] for row in books),
        "mcq_verified": sum(row["mcq_verified"] for row in books),
        "output": MetadataValue.path(
            f"s3://{state['output_bucket']}/{state['stage2_prefix']}"
        ),
    })
    return summary


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=default_config(),
)
def textbook_mineru_clean_qa_demo_job():
    state = submit_e2e_ray_pipeline(resolve_e2e_books())
    books = fan_out_e2e_books(state)
    parsed = books.map(mineru_parse_book)
    cleaned = parsed.map(clean_book)
    generated = cleaned.map(generate_qa_book)
    validated = generated.map(validate_e2e_book_outputs)
    merge_e2e_books(validated.collect(), state)


E2E_DEMO_JOBS = [textbook_mineru_clean_qa_demo_job]
