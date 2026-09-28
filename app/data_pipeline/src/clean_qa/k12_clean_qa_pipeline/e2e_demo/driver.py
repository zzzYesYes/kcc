from __future__ import annotations

import argparse
import json
import statistics
import time
import uuid
from collections import deque
from typing import Any

import ray
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import atomic_write_json
from clean_qa.k12_clean_qa_pipeline.common.manifests import document_artifacts
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import utc_now
from clean_qa.k12_clean_qa_pipeline.stage1_clean import STAGE1_VERSION
from clean_qa.k12_clean_qa_pipeline.stage1_clean.driver import process_document_remote
from clean_qa.k12_clean_qa_pipeline.stage2_qa import PROMPT_VERSION, STAGE2_VERSION
from clean_qa.k12_clean_qa_pipeline.stage2_qa.core import process_document as process_stage2
from clean_qa.k12_clean_qa_pipeline.stage2_qa.qwen import QwenRequestCoordinator
from runtime.mineru34_hybrid_lake.hybrid_lake_ray_job import (
    HybridLakeActor,
    dual_worker_node_id,
)


def stage2_config(args: argparse.Namespace) -> dict[str, int]:
    return {
        "block_inflight": args.block_inflight,
        "microbatch_size": args.microbatch_size,
        "max_blocks_per_document": args.max_blocks_per_document,
        "merge_max_chars": args.merge_max_chars,
        "merge_max_blocks": args.merge_max_blocks,
        "chapter_max_units": args.chapter_max_units,
        "document_max_units": args.document_max_units,
        "judge_batch_size": args.judge_batch_size,
    }


def write_progress(
    store: ObjectStore,
    args: argparse.Namespace,
    documents: list[dict[str, Any]],
    states: dict[str, dict[str, Any]],
) -> None:
    counts = {
        status: sum(row["status"] == status for row in states.values())
        for status in (
            "pending",
            "mineru",
            "cleaning",
            "qa",
            "success",
            "failed",
        )
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.stage2_prefix.rstrip('/')}/_E2E_PROGRESS.json",
        {
            "updated_at": utc_now(),
            "total_documents": len(documents),
            "counts": counts,
            "documents": states,
        },
    )


def percentile(values: list[float], ratio: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    return round(ordered[min(len(ordered) - 1, int(len(ordered) * ratio))], 3)


def build_profile_summary(
    samples: list[dict[str, Any]],
    states: dict[str, dict[str, Any]],
    qwen_stats: dict[str, Any],
) -> dict[str, Any]:
    running = [float(row["vllm"].get("running", 0)) for row in samples]
    waiting = [float(row["vllm"].get("waiting", 0)) for row in samples]
    overlap_seconds = {
        "mineru_and_qa": 0.0,
        "cleaning_and_qa": 0.0,
        "all_three_stages": 0.0,
    }
    longest_busy = 0.0
    current_busy = 0.0
    for current, following in zip(samples, samples[1:]):
        seconds = max(
            0.0,
            float(following["elapsed_seconds"]) - float(current["elapsed_seconds"]),
        )
        stages = current["pipeline"]["stage_counts"]
        mineru = int(stages.get("mineru", 0)) > 0
        cleaning = int(stages.get("cleaning", 0)) > 0
        qa = int(stages.get("qa", 0)) > 0
        if mineru and qa:
            overlap_seconds["mineru_and_qa"] += seconds
        if cleaning and qa:
            overlap_seconds["cleaning_and_qa"] += seconds
        if mineru and cleaning and qa:
            overlap_seconds["all_three_stages"] += seconds
        if float(current["vllm"].get("running", 0)) > 0:
            current_busy += seconds
            longest_busy = max(longest_busy, current_busy)
        else:
            current_busy = 0.0

    token_rates = {}
    if len(samples) >= 2:
        elapsed = max(
            0.001,
            float(samples[-1]["elapsed_seconds"])
            - float(samples[0]["elapsed_seconds"]),
        )
        for key in ("prompt_tokens_total", "generation_tokens_total"):
            delta = float(samples[-1]["vllm"].get(key, 0)) - float(
                samples[0]["vllm"].get(key, 0)
            )
            token_rates[f"{key}_per_second"] = round(delta / elapsed, 3)

    return {
        "sample_count": len(samples),
        "vllm": {
            "running_avg": round(statistics.mean(running), 3) if running else 0,
            "running_p50": percentile(running, 0.5),
            "running_p90": percentile(running, 0.9),
            "running_max": max(running, default=0),
            "waiting_avg": round(statistics.mean(waiting), 3) if waiting else 0,
            "waiting_p50": percentile(waiting, 0.5),
            "waiting_p90": percentile(waiting, 0.9),
            "waiting_max": max(waiting, default=0),
            "waiting_positive_sample_ratio": round(
                sum(value > 0 for value in waiting) / len(waiting), 4
            )
            if waiting
            else 0,
            "running_positive_sample_ratio": round(
                sum(value > 0 for value in running) / len(running), 4
            )
            if running
            else 0,
            "longest_continuous_busy_seconds": round(longest_busy, 3),
            **token_rates,
        },
        "coordinator": {
            "max_waiting": qwen_stats.get("max_waiting", {}),
            "max_active": qwen_stats.get("max_active", {}),
            "queue_wait_seconds": qwen_stats.get("queue_wait_seconds", {}),
            "submitted": qwen_stats.get("submitted", {}),
            "completed": qwen_stats.get("completed", {}),
        },
        "pipeline_overlap_seconds": {
            key: round(value, 3) for key, value in overlap_seconds.items()
        },
        "book_stage_timelines": {
            document_id: {
                "object_key": row["object_key"],
                "stage_timings": row.get("stage_timings", {}),
                "history": row.get("history", []),
            }
            for document_id, row in states.items()
        },
    }


def create_mineru_actors(args: argparse.Namespace) -> dict[str, Any]:
    node_id = dual_worker_node_id()
    affinity = NodeAffinitySchedulingStrategy(node_id=node_id, soft=False)
    actors = {
        "A": HybridLakeActor.options(scheduling_strategy=affinity).remote(
            "A", args.service_a_logical_id, "http://127.0.0.1:30001", list(range(0, 32))
        ),
        "B": HybridLakeActor.options(scheduling_strategy=affinity).remote(
            "B", args.service_b_logical_id, "http://127.0.0.1:30002", list(range(32, 64))
        ),
    }
    health = ray.get([actor.health.remote() for actor in actors.values()], timeout=30)
    print("MINERU_HEALTH " + json.dumps(health, ensure_ascii=False), flush=True)
    return actors


def create_qwen_coordinator(args: argparse.Namespace) -> Any:
    coordinator = QwenRequestCoordinator.options(
        name=f"e2e-qwen-{uuid.uuid4().hex[:10]}",
        lifetime="non_detached",
    ).remote(
        args.qwen_api_base,
        args.qwen_model,
        args.generation_max_inflight,
        args.judge_max_inflight,
        args.http_pool_size,
        args.qwen_timeout_seconds,
        args.qwen_max_retries,
    )
    health = ray.get(coordinator.health.remote(), timeout=30)
    if not health.get("healthy"):
        raise RuntimeError(f"Qwen service is unavailable: {health}")
    print("QWEN_HEALTH " + json.dumps(health, ensure_ascii=False), flush=True)
    return coordinator


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Pipeline books through MinerU, deterministic cleaning, and Qwen QA."
    )
    parser.add_argument("--manifest-bucket", required=True)
    parser.add_argument("--manifest-key", required=True)
    parser.add_argument("--input-bucket", required=True)
    parser.add_argument("--mineru-bucket", default="k12-mineru-output")
    parser.add_argument("--mineru-prefix", required=True)
    parser.add_argument("--output-bucket", default="k12-cleaned-corpus")
    parser.add_argument("--stage1-prefix", required=True)
    parser.add_argument("--stage2-prefix", required=True)
    parser.add_argument("--service-a-logical-id", type=int, default=12)
    parser.add_argument("--service-b-logical-id", type=int, default=13)
    parser.add_argument("--mineru-batch-size", type=int, default=1)
    parser.add_argument("--inference-slots", type=int, default=4)
    parser.add_argument("--queue-size", type=int, default=6)
    parser.add_argument("--pair-timeout-seconds", type=int, default=7200)
    parser.add_argument("--stage1-inflight", type=int, default=4)
    parser.add_argument("--qwen-api-base", default="http://127.0.0.1:8000")
    parser.add_argument("--qwen-model", default="qwen3.6-35b-a3b")
    parser.add_argument("--generation-max-inflight", type=int, default=8)
    parser.add_argument("--judge-max-inflight", type=int, default=4)
    parser.add_argument("--http-pool-size", type=int, default=16)
    parser.add_argument("--qwen-timeout-seconds", type=int, default=180)
    parser.add_argument("--qwen-max-retries", type=int, default=3)
    parser.add_argument("--block-inflight", type=int, default=8)
    parser.add_argument("--microbatch-size", type=int, default=2)
    parser.add_argument("--max-blocks-per-document", type=int, default=0)
    parser.add_argument("--merge-max-chars", type=int, default=3200)
    parser.add_argument("--merge-max-blocks", type=int, default=8)
    parser.add_argument("--chapter-max-units", type=int, default=12)
    parser.add_argument("--document-max-units", type=int, default=48)
    parser.add_argument("--judge-batch-size", type=int, default=8)
    parser.add_argument("--resume", action="store_true")
    args = parser.parse_args()

    if args.mineru_batch_size < 1:
        parser.error("mineru-batch-size must be positive")
    if args.queue_size < args.inference_slots:
        parser.error("queue-size must be at least inference-slots")

    store = ObjectStore()
    manifest = store.read_json(args.manifest_bucket, args.manifest_key)
    documents = manifest["documents"]
    if not documents:
        raise RuntimeError("the E2E manifest contains no documents")

    run_manifest = {
        "pipeline": "mineru-clean-qa-e2e-v1",
        "created_at": utc_now(),
        "versions": {
            "stage1": STAGE1_VERSION,
            "stage2": STAGE2_VERSION,
            "prompt": PROMPT_VERSION,
        },
        "input_bucket": args.input_bucket,
        "mineru_bucket": args.mineru_bucket,
        "mineru_prefix": args.mineru_prefix,
        "output_bucket": args.output_bucket,
        "stage1_prefix": args.stage1_prefix,
        "stage2_prefix": args.stage2_prefix,
        "documents": documents,
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.stage2_prefix.rstrip('/')}/_RUN_MANIFEST.json",
        run_manifest,
    )
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.stage1_prefix.rstrip('/')}/_RUN_MANIFEST.json",
        run_manifest,
    )

    ray.init(address="auto", log_to_driver=True)
    mineru_actors = create_mineru_actors(args)
    qwen = create_qwen_coordinator(args)
    started = time.monotonic()
    started_epoch = time.time()
    states = {
        row["document_id"]: {
            "document_id": row["document_id"],
            "object_key": row["object_key"],
            "status": "pending",
            "updated_at": utc_now(),
            "stage_timings": {
                "pending": {
                    "started_at_epoch": started_epoch,
                    "start_offset_seconds": 0.0,
                }
            },
            "history": [
                {
                    "stage": "pending",
                    "at_epoch": started_epoch,
                    "offset_seconds": 0.0,
                }
            ],
        }
        for row in documents
    }
    service_queues = {"A": deque(), "B": deque()}
    estimated = {"A": 0, "B": 0}
    for row in sorted(
        documents,
        key=lambda item: item.get("estimated_page_count", 1),
        reverse=True,
    ):
        service = min(estimated, key=estimated.get)
        service_queues[service].append(row)
        estimated[service] += int(row.get("estimated_page_count", 1))

    active: dict[Any, dict[str, Any]] = {}
    service_busy = {"A": False, "B": False}
    results: dict[str, dict[str, Any]] = {}
    profile_samples: list[dict[str, Any]] = []
    last_profile_at = 0.0

    def transition(document_id: str, status: str, **metadata: Any) -> None:
        state = states[document_id]
        now_epoch = time.time()
        offset = round(time.monotonic() - started, 3)
        previous = state["status"]
        previous_timing = state["stage_timings"].setdefault(previous, {})
        previous_timing.setdefault("ended_at_epoch", now_epoch)
        previous_timing.setdefault("end_offset_seconds", offset)
        timing = state["stage_timings"].setdefault(status, {})
        timing.setdefault("started_at_epoch", now_epoch)
        timing.setdefault("start_offset_seconds", offset)
        state.update(status=status, updated_at=utc_now(), **metadata)
        state["history"].append(
            {"stage": status, "at_epoch": now_epoch, "offset_seconds": offset}
        )

    def write_profile() -> None:
        body = (
            "\n".join(
                json.dumps(row, ensure_ascii=False, sort_keys=True)
                for row in profile_samples
            )
            + "\n"
        ).encode("utf-8")
        store.put_bytes(
            args.output_bucket,
            f"{args.stage2_prefix.rstrip('/')}/_QWEN_PIPELINE_PROFILE.jsonl",
            body,
            "application/x-ndjson",
        )

    def sample_profile(force: bool = False) -> None:
        nonlocal last_profile_at
        now = time.monotonic()
        if not force and now - last_profile_at < 2:
            return
        last_profile_at = now
        try:
            qwen_sample = ray.get(qwen.profile_snapshot.remote(), timeout=10)
        except Exception as exc:
            qwen_sample = {
                "sampled_at_epoch": time.time(),
                "coordinator": {},
                "vllm": {},
                "error": repr(exc),
            }
        stage_counts = {
            stage: sum(row["status"] == stage for row in states.values())
            for stage in ("pending", "mineru", "cleaning", "qa", "success", "failed")
        }
        active_by_stage = {"mineru": 0, "cleaning": 0, "qa": 0}
        for task in active.values():
            if task["stage"] == "mineru":
                active_by_stage["mineru"] += len(task["documents"])
            else:
                active_by_stage[task["stage"]] += 1
        profile_samples.append(
            {
                **qwen_sample,
                "elapsed_seconds": round(now - started, 3),
                "pipeline": {
                    "stage_counts": stage_counts,
                    "active_by_stage": active_by_stage,
                    "mineru_service_busy": dict(service_busy),
                    "mineru_queued": {
                        name: len(queue) for name, queue in service_queues.items()
                    },
                },
            }
        )
        if force or len(profile_samples) % 5 == 0:
            write_profile()

    def submit_mineru(service: str) -> None:
        if service_busy[service] or not service_queues[service]:
            return
        batch = [
            service_queues[service].popleft()
            for _ in range(min(args.mineru_batch_size, len(service_queues[service])))
        ]
        for document in batch:
            transition(document["document_id"], "mineru", service=service)
        reference = mineru_actors[service].parse_pair.remote(
            batch,
            args.input_bucket,
            args.mineru_bucket,
            args.mineru_prefix,
            args.inference_slots,
            args.queue_size,
            args.pair_timeout_seconds,
        )
        active[reference] = {"stage": "mineru", "service": service, "documents": batch}
        service_busy[service] = True

    for service in ("A", "B"):
        submit_mineru(service)
    sample_profile(force=True)
    write_progress(store, args, documents, states)

    while active:
        ready, _ = ray.wait(list(active), num_returns=1, timeout=2)
        if not ready:
            sample_profile()
            write_progress(store, args, documents, states)
            continue
        reference = ready[0]
        task = active.pop(reference)
        stage = task["stage"]
        try:
            value = ray.get(reference)
        except Exception as exc:
            value = {"status": "failed", "error": repr(exc)}

        if stage == "mineru":
            service = task["service"]
            service_busy[service] = False
            rows = value if isinstance(value, list) else [value]
            by_id = {row.get("document_id"): row for row in rows}
            for document in task["documents"]:
                document_id = document["document_id"]
                row = by_id.get(document_id, {"status": "failed", "error": "missing MinerU result"})
                if row.get("status") != "success":
                    transition(
                        document_id,
                        "failed",
                        failed_stage="mineru",
                        error=row.get("error"),
                    )
                    results[document_id] = states[document_id]
                    continue
                resolved = {
                    **document_artifacts(
                        store, args.mineru_bucket, args.mineru_prefix, document_id
                    ),
                    "document_id": document_id,
                    "source_bucket": args.mineru_bucket,
                    "source_prefix": args.mineru_prefix,
                    "source_etag": document.get("etag", ""),
                    "source_input": row.get("input", {}),
                    "page_count": row.get("page_count"),
                    "image_count": row.get("image_count", 0),
                }
                transition(document_id, "cleaning")
                clean_ref = process_document_remote.remote(
                    resolved,
                    args.output_bucket,
                    args.stage1_prefix,
                    args.resume,
                )
                active[clean_ref] = {
                    "stage": "cleaning",
                    "document": document,
                    "resolved": resolved,
                }
            submit_mineru(service)

        elif stage == "cleaning":
            document = task["document"]
            document_id = document["document_id"]
            if value.get("status") not in {"success", "skipped"}:
                transition(
                    document_id,
                    "failed",
                    failed_stage="cleaning",
                    error=value.get("error"),
                )
                results[document_id] = states[document_id]
                continue
            transition(document_id, "qa")
            config = stage2_config(args)
            qa_ref = process_stage2.remote(
                document_id,
                args.output_bucket,
                args.stage1_prefix,
                args.output_bucket,
                args.stage2_prefix,
                qwen,
                args.qwen_model,
                config["block_inflight"],
                config["microbatch_size"],
                config["max_blocks_per_document"],
                config["merge_max_chars"],
                config["merge_max_blocks"],
                config["chapter_max_units"],
                config["document_max_units"],
                config["judge_batch_size"],
                args.resume,
            )
            active[qa_ref] = {"stage": "qa", "document": document}

        elif stage == "qa":
            document_id = task["document"]["document_id"]
            if value.get("status") not in {"success", "skipped"}:
                transition(
                    document_id,
                    "failed",
                    failed_stage="qa",
                    error=value.get("error"),
                )
            else:
                transition(
                    document_id,
                    "success",
                    metrics=value.get("metrics", {}),
                )
            results[document_id] = states[document_id]
        sample_profile()
        write_progress(store, args, documents, states)

    sample_profile(force=True)
    qwen_stats = ray.get(qwen.snapshot.remote())
    profile_summary = build_profile_summary(profile_samples, states, qwen_stats)
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.stage2_prefix.rstrip('/')}/_PIPELINE_PROFILE_SUMMARY.json",
        profile_summary,
    )
    failed = [row for row in states.values() if row["status"] != "success"]
    summary = {
        "status": "success" if not failed else "partial",
        "created_at": utc_now(),
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "total_documents": len(documents),
        "success_documents": len(documents) - len(failed),
        "failed_documents": len(failed),
        "versions": run_manifest["versions"],
        "qwen": qwen_stats,
        "profiling": profile_summary,
        "results": [states[row["document_id"]] for row in documents],
    }
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.stage2_prefix.rstrip('/')}/_E2E_SUMMARY.json",
        summary,
    )
    print("E2E_SUMMARY " + json.dumps(summary, ensure_ascii=False), flush=True)
    for actor in mineru_actors.values():
        ray.kill(actor, no_restart=True)
    ray.kill(qwen, no_restart=True)
    if failed:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
