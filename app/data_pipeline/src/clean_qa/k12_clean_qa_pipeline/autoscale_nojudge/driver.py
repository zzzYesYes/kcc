from __future__ import annotations

import argparse
import json
import math
import time
from collections import deque
from pathlib import Path
from typing import Any

import ray
from ray.util.placement_group import placement_group, remove_placement_group

from clean_qa.k12_clean_qa_pipeline.autoscale_nojudge.qwen_pool import (
    QwenPoolCoordinator,
    QwenTP1Endpoint,
)
from clean_qa.k12_clean_qa_pipeline.common.atomic_writer import atomic_write_json
from clean_qa.k12_clean_qa_pipeline.common.manifests import document_artifacts
from clean_qa.k12_clean_qa_pipeline.common.minio_client import ObjectStore
from clean_qa.k12_clean_qa_pipeline.common.progress import utc_now
from clean_qa.k12_clean_qa_pipeline.stage1_clean.driver import process_document_remote
from clean_qa.k12_clean_qa_pipeline.stage2_qa.core import process_document as process_stage2
from runtime.mineru34_hybrid_lake.hybrid_lake_ray_job import HybridLakeActor


def emit_event(
    store: ObjectStore,
    args: argparse.Namespace,
    events: list[dict[str, Any]],
    component: str,
    event: str,
    **details: Any,
) -> None:
    row = {
        "timestamp": utc_now(),
        "epoch_seconds": time.time(),
        "component": component,
        "event": event,
        **details,
    }
    events.append(row)
    atomic_write_json(
        store,
        args.output_bucket,
        f"{args.output_prefix.rstrip('/')}/_AUTOSCALING_EVENTS.json",
        events,
    )
    print("AUTOSCALE_EVENT " + json.dumps(row, ensure_ascii=False), flush=True)


class QAPoolManager:
    def __init__(
        self,
        store: ObjectStore,
        args: argparse.Namespace,
        events: list[dict[str, Any]],
    ) -> None:
        self.store = store
        self.args = args
        self.events = events
        self.pool = QwenPoolCoordinator.remote()
        self.placement_groups: list[Any] = []
        self.endpoints: list[Any] = []
        self.created_at: list[float] = []
        self.pod_metadata: dict[int, dict[str, Any]] = {}

    @property
    def pod_count(self) -> int:
        return len(self.placement_groups)

    def ensure(self, desired: int, workload: int) -> None:
        desired = max(0, min(self.args.qa_max_pods, desired))
        emit_event(
            self.store,
            self.args,
            self.events,
            "qa_scaling_controller",
            "decision",
            pending_blocks=workload,
            inflight_requests=0,
            capacity_per_pod=(
                self.args.qa_actors_per_pod
                * self.args.generation_max_inflight_per_actor
            ),
            current_pods=self.pod_count,
            desired_pods=desired,
            decision=(
                "scale_up"
                if desired > self.pod_count
                else "hold"
                if desired == self.pod_count
                else "defer_scale_down"
            ),
        )
        new_groups: list[tuple[int, Any, float]] = []
        for index in range(self.pod_count, desired):
            requested_at = time.time()
            pg = placement_group(
                [
                    {"CPU": 1, "qa_vllm_npu": 1},
                    {"CPU": 1, "qa_vllm_npu": 1},
                ],
                strategy="STRICT_PACK",
                name=f"{self.args.ray_job_id}-qa-pg-{index}",
            )
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_scaling_controller",
                "pod_requested",
                pod_index=index,
                requested_at=requested_at,
            )
            new_groups.append((index, pg, requested_at))
        if not new_groups:
            return

        ray.get(
            [pg.ready() for _, pg, _ in new_groups],
            timeout=self.args.worker_start_timeout_seconds,
        )
        scheduled_at = time.time()
        new_endpoints: list[Any] = []
        endpoint_groups: list[tuple[int, Any, float, list[Any]]] = []
        for index, pg, requested_at in new_groups:
            chip_ids = [8 + index * 2, 9 + index * 2]
            endpoints = [
                QwenTP1Endpoint.options(
                    placement_group=pg,
                    placement_group_bundle_index=bundle_index,
                    name=(
                        f"{self.args.ray_job_id}-qa-{index}-"
                        f"{bundle_index}"
                    ),
                    lifetime="non_detached",
                ).remote(
                    f"qa-pod-{index}-actor-{bundle_index}",
                    8000 + bundle_index,
                    self.args.qwen_model,
                    self.args.generation_max_inflight_per_actor,
                    self.args.qwen_timeout_seconds,
                    self.args.qwen_max_retries,
                    index,
                    chip_ids[bundle_index],
                )
                for bundle_index in range(2)
            ]
            for bundle_index, endpoint in enumerate(endpoints):
                emit_event(
                    self.store,
                    self.args,
                    self.events,
                    "qa_serve",
                    "requested",
                    pod_index=index,
                    serve_id=f"qa-pod-{index}-actor-{bundle_index}",
                    actor_id=str(endpoint._actor_id),
                    chip_id=chip_ids[bundle_index],
                )
            new_endpoints.extend(endpoints)
            endpoint_groups.append((index, pg, requested_at, endpoints))

        health = ray.get(
            [endpoint.health.remote() for endpoint in new_endpoints],
            timeout=self.args.worker_start_timeout_seconds,
        )
        if not all(row.get("healthy") for row in health):
            raise RuntimeError(f"Qwen TP1 endpoints failed health: {health}")
        endpoint_ids = [row["endpoint_id"] for row in health]
        ray.get(self.pool.add_endpoints.remote(new_endpoints, endpoint_ids))

        health_by_actor = {
            str(endpoint._actor_id): row
            for endpoint, row in zip(new_endpoints, health)
        }
        for index, pg, requested_at, endpoints in endpoint_groups:
            self.placement_groups.append(pg)
            self.endpoints.extend(endpoints)
            self.created_at.append(requested_at)
            group_health = [
                health_by_actor[str(endpoint._actor_id)]
                for endpoint in endpoints
            ]
            worker_group = str(group_health[0]["worker_group"])
            pod_name = str(group_health[0]["pod_name"])
            self.pod_metadata[index] = {
                "worker_group": worker_group,
                "pod_name": pod_name,
                "chip_ids": sorted(
                    int(row["chip_id"]) for row in group_health
                ),
            }
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_scaling_controller",
                "pod_ready",
                pod_index=index,
                scheduled_at=scheduled_at,
                model_ready_at=time.time(),
                startup_seconds=round(time.time() - requested_at, 3),
                worker_group=worker_group,
                pod_name=pod_name,
                chip_ids=self.pod_metadata[index]["chip_ids"],
                endpoints=group_health,
            )
            for endpoint_health in group_health:
                emit_event(
                    self.store,
                    self.args,
                    self.events,
                    "qa_serve",
                    "ready",
                    **endpoint_health,
                )

    def snapshot(self) -> dict[str, Any]:
        return ray.get(self.pool.snapshot.remote())

    def close(self) -> None:
        try:
            endpoint_snapshots = (
                ray.get(
                    [
                        endpoint.snapshot.remote()
                        for endpoint in self.endpoints
                    ]
                )
                if self.endpoints
                else []
            )
        except Exception as exc:
            print(
                f"observability snapshot failed during QA cleanup: {exc!r}",
                flush=True,
            )
            endpoint_snapshots = [
                {
                    "pod_index": index // 2,
                    "pod_name": "unknown",
                    "serve_id": (
                        f"qa-pod-{index // 2}-actor-{index % 2}"
                    ),
                    "actor_id": str(endpoint._actor_id),
                    "chip_id": 8 + index,
                }
                for index, endpoint in enumerate(self.endpoints)
            ]
        for snapshot in endpoint_snapshots:
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_serve",
                "draining",
                **snapshot,
            )
        for index in range(len(self.placement_groups)):
            metadata = self.pod_metadata.get(index, {})
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_scaling_controller",
                "pod_draining",
                pod_index=index,
                **metadata,
            )
        for endpoint, snapshot in zip(self.endpoints, endpoint_snapshots):
            ray.kill(endpoint, no_restart=True)
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_serve",
                "released",
                pod_index=snapshot["pod_index"],
                pod_name=snapshot["pod_name"],
                worker_group=snapshot.get("worker_group", "unknown"),
                serve_id=snapshot["serve_id"],
                actor_id=snapshot["actor_id"],
                chip_id=snapshot["chip_id"],
                port=snapshot.get("port"),
            )
        for index, pg in reversed(
            list(enumerate(self.placement_groups))
        ):
            remove_placement_group(pg)
            metadata = self.pod_metadata.get(index, {})
            emit_event(
                self.store,
                self.args,
                self.events,
                "qa_scaling_controller",
                "pod_released",
                pod_index=index,
                **metadata,
            )
        emit_event(
            self.store,
            self.args,
            self.events,
            "qa_scaling_controller",
            "all_resources_released",
            stopped_at=time.time(),
            released_pods=len(self.placement_groups),
            released_actors=len(self.endpoints),
        )


def create_mineru_pool(
    args: argparse.Namespace,
    store: ObjectStore,
    events: list[dict[str, Any]],
) -> tuple[Any, dict[str, Any], dict[str, dict[str, Any]]]:
    requested_at = time.time()
    pg = placement_group(
        [
            {"CPU": 32, "NPU": 1, "MINERU_NPU": 1},
            {"CPU": 32, "NPU": 1, "MINERU_NPU": 1},
        ],
        strategy="STRICT_PACK",
        name=f"{args.ray_job_id}-mineru-pg",
    )
    emit_event(
        store,
        args,
        events,
        "mineru_resource_manager",
        "pod_requested",
        requested_at=requested_at,
    )
    ray.get(pg.ready(), timeout=args.worker_start_timeout_seconds)
    actors = {
        "A": HybridLakeActor.options(
            placement_group=pg,
            placement_group_bundle_index=0,
            name=f"{args.ray_job_id}-mineru-a",
            lifetime="non_detached",
        ).remote("A", 14, "http://127.0.0.1:30001", list(range(0, 32))),
        "B": HybridLakeActor.options(
            placement_group=pg,
            placement_group_bundle_index=1,
            name=f"{args.ray_job_id}-mineru-b",
            lifetime="non_detached",
        ).remote("B", 15, "http://127.0.0.1:30002", list(range(32, 64))),
    }
    for service, actor in actors.items():
        emit_event(
            store,
            args,
            events,
            "mineru_serve",
            "requested",
            serve_id=f"mineru-serve-{service.lower()}",
            actor_id=str(actor._actor_id),
            chip_id=14 if service == "A" else 15,
        )
    health = ray.get(
        [actor.health.remote() for actor in actors.values()],
        timeout=args.worker_start_timeout_seconds,
    )
    node_names = {
        str(node["NodeID"]): str(node.get("NodeManagerHostname", "unknown"))
        for node in ray.nodes()
    }
    resources: dict[str, dict[str, Any]] = {}
    for row in health:
        service = str(row["service"])
        resource = {
            **row,
            "pod_name": node_names.get(str(row.get("node_id")), "unknown"),
            "serve_id": f"mineru-serve-{service.lower()}",
            "actor_id": str(actors[service]._actor_id),
            "chip_id": int(row["logical_id"]),
            "lifecycle": "ready",
        }
        resources[service] = resource
    emit_event(
        store,
        args,
        events,
        "mineru_resource_manager",
        "pod_ready",
        model_ready_at=time.time(),
        startup_seconds=round(time.time() - requested_at, 3),
        pod_name=next(iter(resources.values()))["pod_name"],
        actors=list(resources.values()),
    )
    for resource in resources.values():
        emit_event(
            store,
            args,
            events,
            "mineru_serve",
            "ready",
            **resource,
        )
    return pg, actors, resources


def assign_services(documents: list[dict[str, Any]]) -> dict[str, deque]:
    queues = {"A": deque(), "B": deque()}
    load = {"A": 0, "B": 0}
    for document in sorted(
        documents,
        key=lambda row: int(row.get("estimated_page_count", 1)),
        reverse=True,
    ):
        service = min(load, key=load.get)
        queues[service].append(document)
        load[service] += int(document.get("estimated_page_count", 1))
    return queues


def count_stage1_workload(
    store: ObjectStore,
    bucket: str,
    prefix: str,
    document_id: str,
    maximum: int,
) -> int:
    body = store.read_bytes(
        bucket,
        f"{prefix.rstrip('/')}/{document_id}/blocks.jsonl",
    )
    blocks = sum(1 for line in body.splitlines() if line.strip())
    return min(maximum, max(1, blocks))


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser()
    result.add_argument("--manifest-bucket", required=True)
    result.add_argument("--manifest-key", required=True)
    result.add_argument("--input-bucket", required=True)
    result.add_argument("--mineru-bucket", required=True)
    result.add_argument("--mineru-prefix", required=True)
    result.add_argument("--output-bucket", required=True)
    result.add_argument("--stage1-prefix", required=True)
    result.add_argument("--output-prefix", required=True)
    result.add_argument("--ray-job-id", required=True)
    result.add_argument("--mineru-batch-size", type=int, default=4)
    result.add_argument("--inference-slots", type=int, default=4)
    result.add_argument("--queue-size", type=int, default=8)
    result.add_argument("--pair-timeout-seconds", type=int, default=7200)
    result.add_argument("--worker-start-timeout-seconds", type=int, default=1800)
    result.add_argument("--qa-max-pods", type=int, default=3)
    result.add_argument("--qa-actors-per-pod", type=int, default=2)
    result.add_argument(
        "--generation-max-inflight-per-actor", type=int, default=8
    )
    result.add_argument("--qwen-model", default="qwen3.6-35b-a3b")
    result.add_argument("--qwen-timeout-seconds", type=int, default=180)
    result.add_argument("--qwen-max-retries", type=int, default=3)
    result.add_argument("--block-inflight", type=int, default=8)
    result.add_argument("--microbatch-size", type=int, default=2)
    result.add_argument("--max-blocks-per-document", type=int, default=0)
    result.add_argument("--merge-max-chars", type=int, default=3200)
    result.add_argument("--merge-max-blocks", type=int, default=8)
    result.add_argument("--chapter-max-units", type=int, default=12)
    result.add_argument("--document-max-units", type=int, default=48)
    result.add_argument("--resume", action="store_true")
    return result


def main() -> None:
    args = parser().parse_args()
    if args.mineru_batch_size != 4:
        raise ValueError("this reviewed Job requires mineru_batch_size=4")
    ray.init(address="auto")
    store = ObjectStore()
    documents = store.read_json(
        args.manifest_bucket, args.manifest_key
    )["documents"]
    started = time.time()
    events: list[dict[str, Any]] = []
    queued_at = time.time()
    states = {
        row["document_id"]: {
            "document_id": row["document_id"],
            "object_key": row["object_key"],
            "status": "pending",
            "history": [{"stage": "pending", "at": utc_now()}],
            "queued_at": queued_at,
            "stages": {},
        }
        for row in documents
    }
    active: dict[Any, dict[str, Any]] = {}
    results: dict[str, dict[str, Any]] = {}
    mineru_pg = None
    mineru_actors: dict[str, Any] = {}
    mineru_resources: dict[str, dict[str, Any]] = {}
    mineru_released = False
    qa = QAPoolManager(store, args, events)
    qa_started = False
    qa_workload: dict[str, int] = {}
    qa_observation: dict[str, Any] = {}
    last_observation_write = 0.0

    def write_progress() -> None:
        atomic_write_json(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/_PROGRESS.json",
            {
                "updated_at": utc_now(),
                "documents": states,
                "qa_worker_pods_ready": qa.pod_count,
                "qa_vllm_actors_ready": len(qa.endpoints),
                "resources": {
                    "mineru": list(mineru_resources.values()),
                    "qa": qa_observation,
                },
            },
        )

    def transition(document_id: str, status: str, **details: Any) -> None:
        states[document_id].update({"status": status, **details})
        states[document_id]["history"].append(
            {"stage": status, "at": utc_now()}
        )
        write_progress()

    def update_stage(
        document_id: str,
        stage: str,
        status: str,
        **details: Any,
    ) -> None:
        now = time.time()
        previous = states[document_id]["stages"].get(stage, {})
        states[document_id]["stages"][stage] = {
            **previous,
            "status": status,
            "started_at": previous.get("started_at", now),
            "updated_at": now,
            **details,
        }
        if status in {"completed", "failed"}:
            states[document_id]["stages"][stage]["completed_at"] = now
            states[document_id]["stages"][stage]["processing_time"] = round(
                now - states[document_id]["stages"][stage]["started_at"],
                3,
            )
        write_progress()

    def release_mineru_workers(reason: str) -> None:
        nonlocal mineru_pg, mineru_actors, mineru_released
        if mineru_released:
            return
        pod_name = (
            next(iter(mineru_resources.values())).get("pod_name", "unknown")
            if mineru_resources
            else "unknown"
        )
        emit_event(
            store,
            args,
            events,
            "mineru_resource_manager",
            "pod_draining",
            pod_name=pod_name,
            reason=reason,
        )
        for service, actor in mineru_actors.items():
            resource = mineru_resources.get(service, {})
            emit_event(
                store,
                args,
                events,
                "mineru_serve",
                "draining",
                **resource,
            )
            ray.kill(actor, no_restart=True)
            emit_event(
                store,
                args,
                events,
                "mineru_serve",
                "released",
                **resource,
            )
            if resource:
                resource["lifecycle"] = "released"
        mineru_actors = {}
        if mineru_pg is not None:
            remove_placement_group(mineru_pg)
            mineru_pg = None
        emit_event(
            store,
            args,
            events,
            "mineru_resource_manager",
            "pod_released",
            pod_name=pod_name,
            reason=reason,
        )
        mineru_released = True
        emit_event(
            store,
            args,
            events,
            "mineru_resource_manager",
            "all_cleaning_resources_released",
            reason=reason,
            stopped_at=time.time(),
        )

    try:
        (
            mineru_pg,
            mineru_actors,
            mineru_resources,
        ) = create_mineru_pool(args, store, events)
        service_queues = assign_services(documents)
        routing_plan = []
        for service, queue in service_queues.items():
            resource = mineru_resources[service]
            routing_plan.extend(
                {
                    "document_id": document["document_id"],
                    "object_key": document["object_key"],
                    "estimated_page_count": int(
                        document.get("estimated_page_count", 1)
                    ),
                    "service": service,
                    "pod_name": resource["pod_name"],
                    "serve_id": resource["serve_id"],
                    "actor_id": resource["actor_id"],
                    "chip_id": resource["chip_id"],
                }
                for document in queue
            )
        atomic_write_json(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/_MINERU_ROUTING_PLAN.json",
            {
                "created_at": utc_now(),
                "documents": routing_plan,
            },
        )
        service_busy = {"A": False, "B": False}

        def submit_mineru(service: str) -> None:
            if service_busy[service] or not service_queues[service]:
                return
            batch = [
                service_queues[service].popleft()
                for _ in range(
                    min(args.mineru_batch_size, len(service_queues[service]))
                )
            ]
            assignment_started_at = time.time()
            resource = mineru_resources[service]
            resource["lifecycle"] = "busy"
            resource["active_documents"] = [
                document["document_id"] for document in batch
            ]
            for document in batch:
                assignment = {
                    "pod_name": resource["pod_name"],
                    "serve_id": resource["serve_id"],
                    "actor_id": resource["actor_id"],
                    "chip_id": resource["chip_id"],
                    "queue_wait": round(
                        assignment_started_at
                        - states[document["document_id"]]["queued_at"],
                        3,
                    ),
                    "assigned_at": assignment_started_at,
                }
                transition(
                    document["document_id"],
                    "mineru",
                    service=service,
                    mineru_assignment=assignment,
                )
                update_stage(
                    document["document_id"],
                    "mineru",
                    "busy",
                    **assignment,
                )
            reference = mineru_actors[service].parse_pair.remote(
                batch,
                args.input_bucket,
                args.mineru_bucket,
                args.mineru_prefix,
                args.inference_slots,
                args.queue_size,
                args.pair_timeout_seconds,
            )
            active[reference] = {
                "stage": "mineru",
                "service": service,
                "documents": batch,
                "started_at": assignment_started_at,
            }
            service_busy[service] = True

        submit_mineru("A")
        submit_mineru("B")
        while active:
            ready, _ = ray.wait(list(active), num_returns=1, timeout=2)
            if not ready:
                if qa_started and time.time() - last_observation_write >= 5:
                    try:
                        qa_observation = qa.snapshot()
                    except Exception as exc:
                        print(
                            "QA observability snapshot failed: "
                            f"{exc!r}",
                            flush=True,
                        )
                    last_observation_write = time.time()
                    write_progress()
                continue
            reference = ready[0]
            task = active.pop(reference)
            try:
                value = ray.get(reference)
            except Exception as exc:
                value = {"status": "failed", "error": repr(exc)}
            if task["stage"] == "mineru":
                service = task["service"]
                service_busy[service] = False
                mineru_resources[service]["lifecycle"] = "ready"
                mineru_resources[service]["active_documents"] = []
                rows = value if isinstance(value, list) else [value]
                by_id = {row.get("document_id"): row for row in rows}
                for document in task["documents"]:
                    document_id = document["document_id"]
                    row = by_id.get(document_id, {"status": "failed"})
                    if row.get("status") != "success":
                        update_stage(
                            document_id,
                            "mineru",
                            "failed",
                            error=row.get(
                                "error", "missing MinerU result"
                            ),
                        )
                        transition(
                            document_id,
                            "failed",
                            failed_stage="mineru",
                            error=row.get("error", "missing MinerU result"),
                        )
                        results[document_id] = states[document_id]
                        continue
                    update_stage(
                        document_id,
                        "mineru",
                        "completed",
                        page_count=row.get("page_count"),
                        image_count=row.get("image_count", 0),
                        service_batch_processing_time=round(
                            time.time() - task["started_at"], 3
                        ),
                    )
                    resolved = {
                        **document_artifacts(
                            store,
                            args.mineru_bucket,
                            args.mineru_prefix,
                            document_id,
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
                    update_stage(document_id, "cleaning", "busy")
                    clean_ref = process_document_remote.remote(
                        resolved,
                        args.output_bucket,
                        args.stage1_prefix,
                        args.resume,
                    )
                    active[clean_ref] = {
                        "stage": "cleaning",
                        "document": document,
                        "started_at": time.time(),
                    }
                submit_mineru(service)
            elif task["stage"] == "cleaning":
                document = task["document"]
                document_id = document["document_id"]
                if value.get("status") not in {"success", "skipped"}:
                    update_stage(
                        document_id,
                        "cleaning",
                        "failed",
                        error=value.get("error"),
                    )
                    transition(
                        document_id,
                        "failed",
                        failed_stage="cleaning",
                        error=value.get("error"),
                    )
                    results[document_id] = states[document_id]
                    continue
                update_stage(
                    document_id,
                    "cleaning",
                    "completed",
                    result_status=value.get("status"),
                    metrics=value.get("metrics", {}),
                )
                workload = count_stage1_workload(
                    store,
                    args.output_bucket,
                    args.stage1_prefix,
                    document_id,
                    args.document_max_units,
                )
                qa_workload[document_id] = workload
                total_workload = sum(qa_workload.values())
                capacity = (
                    args.qa_actors_per_pod
                    * args.generation_max_inflight_per_actor
                )
                desired = min(
                    args.qa_max_pods,
                    max(1, math.ceil(total_workload / capacity)),
                )
                qa.ensure(desired, total_workload)
                qa_started = True
                try:
                    qa_observation = qa.snapshot()
                except Exception as exc:
                    print(
                        f"QA observability snapshot failed: {exc!r}",
                        flush=True,
                    )
                last_observation_write = time.time()
                transition(document_id, "qa")
                qa_ref = process_stage2.remote(
                    document_id,
                    args.output_bucket,
                    args.stage1_prefix,
                    args.output_bucket,
                    args.output_prefix,
                    qa.pool,
                    args.qwen_model,
                    args.block_inflight,
                    args.microbatch_size,
                    args.max_blocks_per_document,
                    args.merge_max_chars,
                    args.merge_max_blocks,
                    args.chapter_max_units,
                    args.document_max_units,
                    1,
                    args.resume,
                    False,
                    True,
                )
                active[qa_ref] = {
                    "stage": "qa",
                    "document": document,
                }
            elif task["stage"] == "qa":
                document_id = task["document"]["document_id"]
                qa_workload.pop(document_id, None)
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
                        qwen_assignments=value.get(
                            "qwen_assignments", []
                        ),
                    )
                results[document_id] = states[document_id]

            if not any(
                row["status"] in {"pending", "mineru", "cleaning"}
                for row in states.values()
            ):
                release_mineru_workers("all_cleaning_outputs_emitted")

        qwen = qa.snapshot() if qa_started else {}
        failed = [
            row for row in states.values() if row["status"] != "success"
        ]
        summary = {
            "status": "success" if not failed else "partial",
            "created_at": utc_now(),
            "elapsed_seconds": round(time.time() - started, 3),
            "total_documents": len(documents),
            "success_documents": len(documents) - len(failed),
            "failed_documents": len(failed),
            "judge_enabled": False,
            "validation_status": "schema_valid_unjudged",
            "mineru_batch_size": args.mineru_batch_size,
            "qwen": qwen,
            "autoscaling_events": events,
            "results": list(states.values()),
        }
        atomic_write_json(
            store,
            args.output_bucket,
            f"{args.output_prefix.rstrip('/')}/_SUMMARY.json",
            summary,
        )
        if failed:
            raise RuntimeError(f"{len(failed)} documents failed")
    finally:
        qa.close()
        release_mineru_workers("driver_shutdown")
        emit_event(
            store,
            args,
            events,
            "mineru_resource_manager",
            "all_resources_released",
            stopped_at=time.time(),
        )


if __name__ == "__main__":
    main()
