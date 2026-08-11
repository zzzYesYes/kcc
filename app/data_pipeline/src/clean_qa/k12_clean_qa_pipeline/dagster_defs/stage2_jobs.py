from __future__ import annotations

import os
import re
import shlex
import time

from dagster import (
    DynamicOut,
    DynamicOutput,
    Field,
    In,
    MetadataValue,
    Out,
    graph,
    job,
    multiprocess_executor,
    op,
)
from ray.job_submission import JobStatus

from clean_qa.k12_clean_qa_pipeline.stage2_qa import PROMPT_VERSION, STAGE2_VERSION
from clean_qa.mineru_dagster.resources import RayJobResource, S3Resource


TEST_STAGE1_PREFIX = "stage1/full/stage1-v1.0.2"
TEST_OUTPUT_PREFIX = "stage2/demo-10/stage2-v1.1.0-k12-qa-v1.2-20260728"
PIPELINE_WORKING_DIR = os.environ.get("PIPELINE_WORKING_DIR", "/opt/data-pipeline/src")
PIPELINE_NO_PROXY = os.environ.get(
    "PIPELINE_NO_PROXY", "127.0.0.1,localhost,.svc,.svc.cluster.local"
)

STAGE2_SCHEMA = {
    "stage1_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "stage1_source_prefix": Field(str, default_value=TEST_STAGE1_PREFIX),
    "output_bucket": Field(str, default_value="k12-cleaned-corpus"),
    "output_prefix": Field(str, default_value=TEST_OUTPUT_PREFIX),
    "count": Field(int, default_value=10),
    "resume": Field(bool, default_value=True),
    "document_inflight": Field(int, default_value=3),
    "block_inflight": Field(int, default_value=8),
    "generation_max_inflight": Field(int, default_value=8),
    "judge_max_inflight": Field(int, default_value=4),
    "http_pool_size": Field(int, default_value=16),
    "microbatch_size": Field(int, default_value=2),
    "max_blocks_per_document": Field(int, default_value=6),
    "merge_max_chars": Field(int, default_value=3200),
    "merge_max_blocks": Field(int, default_value=8),
    "chapter_max_units": Field(int, default_value=12),
    "document_max_units": Field(int, default_value=48),
    "judge_batch_size": Field(int, default_value=8),
    "stage2_version": Field(str, default_value=STAGE2_VERSION),
    "prompt_version": Field(str, default_value=PROMPT_VERSION),
    "qwen_api_base": Field(str, default_value="http://127.0.0.1:8000"),
    "qwen_api_bases": Field(
        str,
        default_value="",
        description="Comma-separated Qwen API endpoints; empty uses qwen_api_base.",
    ),
    "qwen_model": Field(str, default_value="qwen3.6-35b-a3b"),
    "benchmark": Field(bool, default_value=True),
    "automated_validation": Field(bool, default_value=True),
    "dry_run": Field(bool, default_value=False),
}


def stage2_8_default_config(full: bool) -> dict:
    return {
        "ops": {
            "resolve_stage1_test_manifest": {
                "config": {
                    "stage1_bucket": "k12-cleaned-corpus",
                    "stage1_source_prefix": "stage1/full/stage1-v1.0.2",
                    "output_bucket": "k12-cleaned-corpus",
                    "output_prefix": (
                        "stage2/full/stage2-v1.1.0-8npu"
                        if full
                        else "stage2/test-16/stage2-v1.1.0-8npu-20260724"
                    ),
                    "count": 0 if full else 16,
                    "resume": True,
                    "document_inflight": 8,
                    "block_inflight": 4,
                    "generation_max_inflight": 8,
                    "judge_max_inflight": 8,
                    "http_pool_size": 16,
                    "microbatch_size": 2,
                    "max_blocks_per_document": 0,
                    "merge_max_chars": 3200,
                    "merge_max_blocks": 8,
                    "chapter_max_units": 12,
                    "document_max_units": 48,
                    "judge_batch_size": 8,
                    "stage2_version": STAGE2_VERSION,
                    "prompt_version": PROMPT_VERSION,
                    "qwen_api_base": "http://127.0.0.1:8000",
                    "qwen_api_bases": (
                        "http://127.0.0.1:8000,http://127.0.0.1:8001,"
                        "http://127.0.0.1:8002,http://127.0.0.1:8003"
                    ),
                    "qwen_model": "qwen3.6-35b-a3b",
                    "benchmark": False,
                    "automated_validation": not full,
                    "dry_run": False,
                }
            }
        }
    }


def stage2_default_config(full: bool) -> dict:
    return {
        "ops": {
            "resolve_stage1_test_manifest": {
                "config": {
                    "stage1_bucket": "k12-cleaned-corpus",
                    "stage1_source_prefix": (
                        "stage1/full/stage1-v1.0.2"
                        if full
                        else TEST_STAGE1_PREFIX
                    ),
                    "output_bucket": "k12-cleaned-corpus",
                    "output_prefix": (
                        "stage2/full/stage2-v1.1.0"
                        if full
                        else TEST_OUTPUT_PREFIX
                    ),
                    "count": 0 if full else 10,
                    "resume": True,
                    "document_inflight": 3,
                    "block_inflight": 8,
                    "generation_max_inflight": 8,
                    "judge_max_inflight": 4,
                    "http_pool_size": 16,
                    "microbatch_size": 2,
                    "max_blocks_per_document": 0,
                    "merge_max_chars": 3200,
                    "merge_max_blocks": 8,
                    "chapter_max_units": 12,
                    "document_max_units": 48,
                    "judge_batch_size": 8,
                    "stage2_version": STAGE2_VERSION,
                    "prompt_version": PROMPT_VERSION,
                    "qwen_api_base": "http://127.0.0.1:8000",
                    "qwen_api_bases": "",
                    "qwen_model": "qwen3.6-35b-a3b",
                    "benchmark": False,
                    "automated_validation": not full,
                    "dry_run": full,
                }
            }
        }
    }


@op(config_schema=STAGE2_SCHEMA, out=Out(dict), required_resource_keys={"s3"})
def resolve_stage1_test_manifest(context) -> dict:
    config = dict(context.op_config)
    if config["stage2_version"] != STAGE2_VERSION:
        raise ValueError(f"stage2_version must be {STAGE2_VERSION}")
    if config["prompt_version"] != PROMPT_VERSION:
        raise ValueError(f"prompt_version must be {PROMPT_VERSION}")
    if config["stage1_bucket"] == config["output_bucket"] and config[
        "output_prefix"
    ].startswith(config["stage1_source_prefix"].rstrip("/") + "/"):
        raise ValueError("Stage 2 output overlaps Stage 1 read-only source")
    s3: S3Resource = context.resources.s3
    summary = s3.read_json(
        config["stage1_bucket"],
        f"{config['stage1_source_prefix'].rstrip('/')}/_SUMMARY.json",
    )
    if summary.get("status") != "success":
        raise RuntimeError("Stage 1 source has not passed its quality gate")
    if config["count"] and int(summary.get("total_documents", 0)) < config["count"]:
        raise ValueError("Stage 1 source has fewer documents than requested")
    stage1_manifest = s3.read_json(
        config["stage1_bucket"],
        f"{config['stage1_source_prefix'].rstrip('/')}/_RUN_MANIFEST.json",
    )
    document_ids = [
        row["document_id"] if isinstance(row, dict) else row
        for row in stage1_manifest.get("documents", [])
    ][: config["count"] or None]
    if len(document_ids) != int(config["count"] or summary["total_documents"]):
        raise ValueError("Stage 1 run manifest does not contain the requested documents")
    state = {
        **config,
        "dagster_run_id": context.run_id,
        "document_ids": document_ids,
    }
    context.add_output_metadata(
        {
            "stage1_source": MetadataValue.path(
                f"s3://{config['stage1_bucket']}/{config['stage1_source_prefix']}"
            ),
            "document_count": int(config["count"] or summary["total_documents"]),
            "document_ids": MetadataValue.json(document_ids),
            "dry_run": config["dry_run"],
        }
    )
    return state


def stage2_entrypoint(state: dict) -> str:
    parts = [
        "python3", "-m", "clean_qa.k12_clean_qa_pipeline.stage2_qa.driver",
        "--stage1-bucket", state["stage1_bucket"],
        "--stage1-prefix", state["stage1_source_prefix"],
        "--output-bucket", state["output_bucket"],
        "--output-prefix", state["output_prefix"],
        "--qwen-api-base", state["qwen_api_base"],
        "--qwen-api-bases", state["qwen_api_bases"],
        "--qwen-model", state["qwen_model"],
        "--limit", str(state["count"]),
        "--document-inflight", str(state["document_inflight"]),
        "--block-inflight", str(state["block_inflight"]),
        "--generation-max-inflight", str(state["generation_max_inflight"]),
        "--judge-max-inflight", str(state["judge_max_inflight"]),
        "--http-pool-size", str(state["http_pool_size"]),
        "--microbatch-size", str(state["microbatch_size"]),
        "--max-blocks-per-document", str(state["max_blocks_per_document"]),
        "--merge-max-chars", str(state["merge_max_chars"]),
        "--merge-max-blocks", str(state["merge_max_blocks"]),
        "--chapter-max-units", str(state["chapter_max_units"]),
        "--document-max-units", str(state["document_max_units"]),
        "--judge-batch-size", str(state["judge_batch_size"]),
    ]
    for enabled, flag in (
        (state["resume"], "--resume"),
        (state["benchmark"], "--benchmark"),
        (state["automated_validation"], "--automated-validation"),
        (state["dry_run"], "--dry-run"),
    ):
        if enabled:
            parts.append(flag)
    return " ".join(shlex.quote(value) for value in parts)


def probe(name: str):
    @op(name=name, ins={"state": In(dict)}, out=Out(dict))
    def _probe(context, state: dict) -> dict:
        context.add_output_metadata(
            {
                "stage": name,
                "document_count": state["count"],
                "ray_job_id": state.get("stage2_ray_job_id", "not-submitted"),
            }
        )
        return state
    return _probe


validate_stage1_outputs = probe("validate_stage1_outputs")
load_structured_blocks = probe("load_structured_blocks")
rule_prefilter = probe("rule_prefilter")
classify_eligibility = probe("classify_eligibility")
extract_facts_and_skills = probe("extract_facts_and_skills")
generate_qa_candidates = probe("generate_qa_candidates")
solve_textbook_exercises = probe("solve_textbook_exercises")
generate_mcq_candidates = probe("generate_mcq_candidates")
run_structure_validation = probe("run_structure_validation")
run_math_validation = probe("run_math_validation")
run_qwen_judge = probe("run_qwen_judge")
deduplicate_items = probe("deduplicate_items")
export_training_formats = probe("export_training_formats")
validate_document_outputs = probe("validate_document_outputs")


def _read_bytes(s3: S3Resource, bucket: str, key: str) -> bytes:
    return s3.client().get_object(Bucket=bucket, Key=key)["Body"].read()


def _jsonl_count(body: bytes) -> int:
    return sum(bool(line.strip()) for line in body.splitlines())


@op(ins={"state": In(dict)}, out=DynamicOut(dict))
def fan_out_stage2_books(context, state: dict):
    for index, document_id in enumerate(state["document_ids"], start=1):
        mapping_key = re.sub(r"[^A-Za-z0-9_]", "_", f"book_{index:02d}_{document_id}")
        context.log.info("Scheduling book %s/%s: %s", index, len(state["document_ids"]), document_id)
        yield DynamicOutput(
            {**state, "document_id": document_id, "book_index": index},
            mapping_key=mapping_key,
        )


@op(ins={"book": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def load_book_clean_data(context, book: dict) -> dict:
    document_id = book["document_id"]
    prefix = f"{book['stage1_source_prefix'].rstrip('/')}/{document_id}"
    s3: S3Resource = context.resources.s3
    metadata = s3.read_json(book["stage1_bucket"], f"{prefix}/book_metadata.json")
    counts = {
        name: _jsonl_count(
            _read_bytes(s3, book["stage1_bucket"], f"{prefix}/{name}.jsonl")
        )
        for name in ("blocks", "exercises", "image_manifest", "quarantine")
    }
    state = {
        **book,
        "book_title": metadata.get("title") or document_id,
        "page_count": int(metadata.get("page_count") or 0),
        "stage1_counts": counts,
    }
    context.add_output_metadata(
        {
            "book": state["book_title"],
            "document_id": document_id,
            "pages": state["page_count"],
            "clean_blocks": counts["blocks"],
            "exercises": counts["exercises"],
            "images": counts["image_manifest"],
            "quarantine": counts["quarantine"],
            "clean_data": MetadataValue.path(
                f"s3://{book['stage1_bucket']}/{prefix}"
            ),
        }
    )
    return state


@op(
    ins={"book": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def wait_for_book_stage2(context, book: dict) -> dict:
    if book["dry_run"]:
        context.add_output_metadata({"dry_run": True, "document_id": book["document_id"]})
        return book
    marker_key = (
        f"{book['output_prefix'].rstrip('/')}/{book['document_id']}/_SUCCESS.json"
    )
    ray_jobs: RayJobResource = context.resources.ray_jobs
    s3: S3Resource = context.resources.s3
    deadline = time.monotonic() + 12 * 3600
    next_log = 0.0
    while time.monotonic() < deadline:
        if s3.exists(book["output_bucket"], marker_key):
            marker = s3.read_json(book["output_bucket"], marker_key)
            if marker.get("stage2_version") != STAGE2_VERSION:
                raise RuntimeError(f"Unexpected Stage 2 version for {book['document_id']}")
            if marker.get("prompt_version") != PROMPT_VERSION:
                raise RuntimeError(f"Unexpected prompt version for {book['document_id']}")
            context.add_output_metadata(
                {
                    "book": book["book_title"],
                    "document_id": book["document_id"],
                    "state": "completed",
                    "stage2_version": marker["stage2_version"],
                    "prompt_version": marker["prompt_version"],
                    "model": marker.get("model", book["qwen_model"]),
                }
            )
            return {**book, "stage2_marker": marker}
        status = ray_jobs.status(book["stage2_ray_job_id"])
        if status in {JobStatus.FAILED, JobStatus.STOPPED, JobStatus.SUCCEEDED}:
            raise RuntimeError(
                f"Ray job became {status} before {book['document_id']} produced _SUCCESS"
            )
        now = time.monotonic()
        if now >= next_log:
            progress_key = f"{book['output_prefix'].rstrip('/')}/_PROGRESS.json"
            phase = "waiting"
            try:
                progress = s3.read_json(book["output_bucket"], progress_key)
                if book["document_id"] in progress.get("active_documents", []):
                    phase = "processing"
            except Exception:
                pass
            context.log.info(
                "Book %s (%s): %s in Ray Stage 2",
                book["book_title"],
                book["document_id"],
                phase,
            )
            next_log = now + 15
        time.sleep(2)
    raise TimeoutError(f"Timed out waiting for {book['document_id']}")


@op(ins={"book": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def show_book_prefilter_and_merge(context, book: dict) -> dict:
    if book["dry_run"]:
        return book
    report_key = (
        f"{book['output_prefix'].rstrip('/')}/{book['document_id']}/generation_report.json"
    )
    report = context.resources.s3.read_json(book["output_bucket"], report_key)
    metrics = report["metrics"]
    eligible = int(metrics["eligible_block_count"])
    blocks = int(metrics["block_count"])
    context.add_output_metadata(
        {
            "book": book["book_title"],
            "input_blocks": blocks,
            "eligible_blocks": eligible,
            "filtered_blocks": blocks - eligible,
            "eligibility_rate": round(eligible / blocks, 4) if blocks else 0,
            "generation_units": int(metrics["generation_unit_count"]),
            "merged_source_blocks": int(metrics["merged_source_block_count"]),
            "rejected_items": int(metrics["rejected"]),
            "processing_seconds": float(metrics["elapsed_seconds"]),
        }
    )
    return {**book, "stage2_metrics": metrics}


@op(ins={"book": In(dict)}, out=Out(dict))
def show_book_qa_extraction(context, book: dict) -> dict:
    if book["dry_run"]:
        return book
    metrics = book["stage2_metrics"]
    candidates = int(metrics["qa_candidates"])
    verified = int(metrics["qa_verified"])
    context.add_output_metadata(
        {
            "book": book["book_title"],
            "qa_candidates": candidates,
            "qa_verified": verified,
            "qa_acceptance_rate": round(verified / candidates, 4) if candidates else 0,
            "textbook_exercise_solutions": int(metrics["textbook_exercise_solutions"]),
            "judge_candidates": int(metrics["judge_candidate_count"]),
            "judge_batches": int(metrics["judge_batch_count"]),
        }
    )
    return book


@op(ins={"book": In(dict)}, out=Out(dict))
def show_book_mcq_extraction(context, book: dict) -> dict:
    if book["dry_run"]:
        return book
    metrics = book["stage2_metrics"]
    candidates = int(metrics["mcq_candidates"])
    verified = int(metrics["mcq_verified"])
    context.add_output_metadata(
        {
            "book": book["book_title"],
            "mcq_candidates": candidates,
            "mcq_verified": verified,
            "mcq_acceptance_rate": round(verified / candidates, 4) if candidates else 0,
        }
    )
    return book


@op(ins={"book": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def validate_book_outputs(context, book: dict) -> dict:
    if book["dry_run"]:
        return book
    base = f"{book['output_prefix'].rstrip('/')}/{book['document_id']}"
    s3: S3Resource = context.resources.s3
    expected = {
        "qa_candidates.jsonl": int(book["stage2_metrics"]["qa_candidates"]),
        "qa_verified.jsonl": int(book["stage2_metrics"]["qa_verified"]),
        "mcq_candidates.jsonl": int(book["stage2_metrics"]["mcq_candidates"]),
        "mcq_verified.jsonl": int(book["stage2_metrics"]["mcq_verified"]),
        "textbook_exercise_solutions.jsonl": int(
            book["stage2_metrics"]["textbook_exercise_solutions"]
        ),
    }
    actual = {
        name: _jsonl_count(_read_bytes(s3, book["output_bucket"], f"{base}/{name}"))
        for name in expected
    }
    if actual != expected:
        raise RuntimeError(
            f"Output counts differ for {book['document_id']}: expected={expected}, actual={actual}"
        )
    result = {
        "document_id": book["document_id"],
        "book_title": book["book_title"],
        "page_count": book["page_count"],
        "stage1_counts": book["stage1_counts"],
        "metrics": book["stage2_metrics"],
        "output_prefix": base,
        "output_bucket": book["output_bucket"],
        "run_output_prefix": book["output_prefix"],
        "stage2_ray_job_id": book["stage2_ray_job_id"],
        "dry_run": book["dry_run"],
    }
    context.add_output_metadata(
        {
            "book": book["book_title"],
            "status": "validated",
            "qa_verified": actual["qa_verified.jsonl"],
            "mcq_verified": actual["mcq_verified.jsonl"],
            "exercise_solutions": actual["textbook_exercise_solutions.jsonl"],
            "output": MetadataValue.path(f"s3://{book['output_bucket']}/{base}"),
        }
    )
    return result


@graph
def process_each_book(book):
    loaded = load_book_clean_data(book)
    completed = wait_for_book_stage2(loaded)
    filtered = show_book_prefilter_and_merge(completed)
    qa = show_book_qa_extraction(filtered)
    mcq = show_book_mcq_extraction(qa)
    return validate_book_outputs(mcq)


@op(
    ins={"books": In(list)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def merge_all_books(context, books: list[dict]) -> dict:
    if not books:
        raise RuntimeError("No book results were collected")
    first = books[0]
    if first.get("dry_run"):
        context.add_output_metadata({"dry_run": True, "books": len(books)})
        return {"status": "dry_run", "books": len(books)}
    state = first
    status = context.resources.ray_jobs.wait(
        state["stage2_ray_job_id"], timeout_seconds=72 * 3600
    )
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Stage 2 Ray job ended with {status}")
    summary = context.resources.s3.read_json(
        state["output_bucket"],
        f"{state['run_output_prefix'].rstrip('/')}/_SUMMARY.json",
    )
    if summary.get("status") != "success":
        raise RuntimeError(f"Stage 2 summary failed: {summary}")
    qa_verified = sum(int(book["metrics"]["qa_verified"]) for book in books)
    mcq_verified = sum(int(book["metrics"]["mcq_verified"]) for book in books)
    exercises = sum(
        int(book["metrics"]["textbook_exercise_solutions"]) for book in books
    )
    context.add_output_metadata(
        {
            "status": "success",
            "books": len(books),
            "total_pages": sum(int(book["page_count"]) for book in books),
            "qa_verified": qa_verified,
            "mcq_verified": mcq_verified,
            "textbook_exercise_solutions": exercises,
            "stage2_version": STAGE2_VERSION,
            "prompt_version": PROMPT_VERSION,
            "output": MetadataValue.path(
                f"s3://{state['output_bucket']}/{state['run_output_prefix']}"
            ),
        }
    )
    return {
        "status": "success",
        "books": len(books),
        "qa_verified": qa_verified,
        "mcq_verified": mcq_verified,
        "textbook_exercise_solutions": exercises,
    }


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def submit_ray_stage2_job(context, state: dict) -> dict:
    entrypoint = stage2_entrypoint(state)
    state["stage2_ray_job_id"] = (
        f"stage2-qa-{'dryrun' if state['dry_run'] else 'run'}-{context.run_id[:8]}"
    )
    state["ray_entrypoint"] = entrypoint
    if not state["dry_run"]:
        context.resources.ray_jobs.submit(
            state["stage2_ray_job_id"],
            entrypoint,
            {
                "PYTHONPATH": f".:{PIPELINE_WORKING_DIR}",
                "NO_PROXY": PIPELINE_NO_PROXY,
                "no_proxy": PIPELINE_NO_PROXY,
            },
        )
    context.add_output_metadata(
        {
            "ray_job_id": state["stage2_ray_job_id"],
            "entrypoint": MetadataValue.text(entrypoint),
            "submitted": not state["dry_run"],
        }
    )
    return state


@op(
    ins={"state": In(dict)},
    out=Out(dict),
    required_resource_keys={"ray_jobs", "s3"},
)
def write_stage2_summary(context, state: dict) -> dict:
    if state["dry_run"]:
        context.add_output_metadata(
            {"dry_run": True, "submitted": False, "entrypoint_valid": True}
        )
        return state
    ray_jobs: RayJobResource = context.resources.ray_jobs
    status = ray_jobs.wait(state["stage2_ray_job_id"], timeout_seconds=72 * 3600)
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(
            f"Stage 2 Ray job failed: {ray_jobs.logs(state['stage2_ray_job_id'])[-8000:]}"
        )
    summary = context.resources.s3.read_json(
        state["output_bucket"],
        f"{state['output_prefix'].rstrip('/')}/_SUMMARY.json",
    )
    if summary.get("status") != "success":
        raise RuntimeError(f"Stage 2 automated validation failed: {summary}")
    state["stage2_summary"] = summary
    optimized = summary["experiments"][-1]
    context.add_output_metadata(
        {
            "status": summary["status"],
            "qa_verified": optimized["metrics"]["qa_verified"],
            "mcq_verified": optimized["metrics"]["mcq_verified"],
            "documents_per_hour": optimized["documents_per_hour"],
            "selected_default": summary["selected_default"],
            "validation": MetadataValue.json(summary["validation"]),
            "output": MetadataValue.path(
                f"s3://{state['output_bucket']}/{state['output_prefix']}"
            ),
        }
    )
    return state


def stage2_graph():
    state = resolve_stage1_test_manifest()
    state = validate_stage1_outputs(state)
    state = submit_ray_stage2_job(state)
    state = load_structured_blocks(state)
    state = rule_prefilter(state)
    state = classify_eligibility(state)
    state = extract_facts_and_skills(state)
    state = generate_qa_candidates(state)
    state = solve_textbook_exercises(state)
    state = generate_mcq_candidates(state)
    state = run_structure_validation(state)
    state = run_math_validation(state)
    state = run_qwen_judge(state)
    state = deduplicate_items(state)
    state = export_training_formats(state)
    state = validate_document_outputs(state)
    write_stage2_summary(state)


def stage2_demo_graph():
    state = resolve_stage1_test_manifest()
    state = validate_stage1_outputs(state)
    state = submit_ray_stage2_job(state)
    books = fan_out_stage2_books(state)
    processed = books.map(process_each_book)
    merge_all_books(processed.collect())


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage2_default_config(False),
    executor_def=multiprocess_executor.configured({"max_concurrent": 10}),
)
def qajobstage2_10():
    stage2_demo_graph()


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage2_default_config(True),
)
def qajobstage2_ful():
    stage2_graph()


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage2_8_default_config(False),
)
def qa_stage2_8_16():
    stage2_graph()


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=stage2_8_default_config(True),
)
def qa_stage2_8_ful():
    stage2_graph()


STAGE2_JOBS = [
    qajobstage2_10,
    qajobstage2_ful,
    qa_stage2_8_16,
    qa_stage2_8_ful,
]
