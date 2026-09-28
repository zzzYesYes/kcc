from __future__ import annotations

import asyncio
import json
import os
import re
import statistics
import time
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any

import ray

from clean_qa.k12_clean_qa_pipeline.stage2_qa.helpers import json_from_content


def physical_endpoint_identity(
    pod_name: str,
    port: int,
    fallback_chip_id: int,
) -> dict[str, Any]:
    """Resolve the physical worker group after Ray has placed the actor."""
    match = re.search(r"qa-(\d+)-(\d+)-worker", pod_name)
    if not match:
        return {
            "worker_group": "unknown",
            "chip_id": fallback_chip_id,
            "serve_id": f"qa-unknown-vllm-{fallback_chip_id}",
        }
    chips = (int(match.group(1)), int(match.group(2)))
    chip_id = chips[0] if port == 8000 else chips[1]
    worker_group = f"qa-{chips[0]}-{chips[1]}"
    return {
        "worker_group": worker_group,
        "chip_id": chip_id,
        "serve_id": f"{worker_group}-vllm-{chip_id}",
    }


@ray.remote(
    max_concurrency=64,
    num_cpus=1,
    resources={"qa_vllm_npu": 1},
)
class QwenTP1Endpoint:
    """One Ray-owned proxy for one pod-local TP1 vLLM service."""

    def __init__(
        self,
        endpoint_id: str,
        port: int,
        model: str,
        max_inflight: int,
        timeout_seconds: int,
        max_retries: int,
        pod_index: int,
        chip_id: int,
    ) -> None:
        self.scheduling_id = endpoint_id
        self.pod_index = pod_index
        self.pod_name = os.environ.get("HOSTNAME", "unknown")
        identity = physical_endpoint_identity(self.pod_name, port, chip_id)
        self.worker_group = identity["worker_group"]
        self.chip_id = identity["chip_id"]
        self.endpoint_id = identity["serve_id"]
        self.serve_id = identity["serve_id"]
        self.port = port
        self.actor_id = str(ray.get_runtime_context().get_actor_id())
        self.api_base = f"http://127.0.0.1:{port}"
        self.model = model
        self.gate = asyncio.Semaphore(max_inflight)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.created_at = time.time()
        self.first_request_at: float | None = None
        self.last_request_at: float | None = None
        self.stats: dict[str, Any] = {
            "requests": 0,
            "retries": 0,
            "errors": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "active": 0,
            "max_active": 0,
            "latencies": [],
        }
        self.active_assignments: dict[str, dict[str, Any]] = {}

    def _post(
        self, messages: list[dict[str, str]], max_tokens: int
    ) -> dict[str, Any]:
        request = urllib.request.Request(
            f"{self.api_base}/v1/chat/completions",
            data=json.dumps(
                {
                    "model": self.model,
                    "messages": messages,
                    "temperature": 0,
                    "max_tokens": max_tokens,
                    "chat_template_kwargs": {"enable_thinking": False},
                },
                ensure_ascii=False,
            ).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read())

    async def request(
        self,
        messages: list[dict[str, str]],
        max_tokens: int,
        routing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        async with self.gate:
            routing = dict(routing or {})
            assignment_id = str(
                routing.get("assignment_id", f"{self.endpoint_id}-unknown")
            )
            processing_started_at = time.time()
            dispatch_queue_wait = float(routing.get("queue_wait", 0))
            serve_queue_wait = max(
                0.0,
                processing_started_at
                - float(routing.get("assigned_at", processing_started_at)),
            )
            active_assignment = {
                **routing,
                "assignment_id": assignment_id,
                "pod_name": self.pod_name,
                "serve_id": self.serve_id,
                "actor_id": self.actor_id,
                "chip_id": self.chip_id,
                "processing_started_at": processing_started_at,
                "dispatch_queue_wait": round(dispatch_queue_wait, 6),
                "serve_queue_wait": round(serve_queue_wait, 6),
                "queue_wait": round(
                    dispatch_queue_wait + serve_queue_wait, 6
                ),
                "status": "busy",
            }
            self.active_assignments[assignment_id] = active_assignment
            self.stats["active"] += 1
            self.stats["max_active"] = max(
                self.stats["max_active"], self.stats["active"]
            )
            self.first_request_at = self.first_request_at or time.time()
            try:
                last_error: Exception | None = None
                for attempt in range(self.max_retries + 1):
                    started = time.monotonic()
                    try:
                        payload = await asyncio.to_thread(
                            self._post, messages, max_tokens
                        )
                        latency = time.monotonic() - started
                        usage = payload.get("usage", {})
                        content = payload["choices"][0]["message"]["content"]
                        result = json_from_content(content)
                        self.stats["requests"] += 1
                        self.stats["input_tokens"] += int(
                            usage.get("prompt_tokens", 0)
                        )
                        self.stats["output_tokens"] += int(
                            usage.get("completion_tokens", 0)
                        )
                        self.stats["latencies"].append(latency)
                        self.last_request_at = time.time()
                        return {
                            "data": result,
                            "latency_seconds": round(latency, 3),
                            "usage": usage,
                            "endpoint_id": self.endpoint_id,
                            "routing": {
                                **active_assignment,
                                "status": "completed",
                                "processing_time": round(
                                    time.time() - processing_started_at, 3
                                ),
                                "completed_at": time.time(),
                            },
                        }
                    except (
                        OSError,
                        KeyError,
                        ValueError,
                        json.JSONDecodeError,
                        urllib.error.HTTPError,
                    ) as exc:
                        last_error = exc
                        self.stats["errors"] += 1
                        if attempt == self.max_retries:
                            break
                        self.stats["retries"] += 1
                        await asyncio.sleep(min(2**attempt, 8))
                raise RuntimeError(
                    f"Qwen endpoint {self.endpoint_id} exhausted retries: "
                    f"{last_error!r}"
                )
            finally:
                self.stats["active"] -= 1
                self.active_assignments.pop(assignment_id, None)

    async def health(self, timeout_seconds: int = 900) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                request = urllib.request.Request(f"{self.api_base}/health")
                response = await asyncio.to_thread(
                    urllib.request.urlopen, request, None, 10
                )
                response.close()
                return {
                    "healthy": True,
                    "endpoint_id": self.endpoint_id,
                    "api_base": self.api_base,
                    "node_id": ray.get_runtime_context().get_node_id(),
                    "pod_index": self.pod_index,
                    "pod_name": self.pod_name,
                    "worker_group": self.worker_group,
                    "serve_id": self.serve_id,
                    "scheduling_id": self.scheduling_id,
                    "actor_id": self.actor_id,
                    "chip_id": self.chip_id,
                    "port": self.port,
                    "ready_at": time.time(),
                }
            except Exception as exc:
                last_error = exc
                await asyncio.sleep(5)
        return {
            "healthy": False,
            "endpoint_id": self.endpoint_id,
            "error": repr(last_error),
        }

    def snapshot(self) -> dict[str, Any]:
        latencies = sorted(self.stats["latencies"])
        return {
            "endpoint_id": self.endpoint_id,
            "serve_id": self.serve_id,
            "pod_index": self.pod_index,
            "pod_name": self.pod_name,
            "worker_group": self.worker_group,
            "actor_id": self.actor_id,
            "chip_id": self.chip_id,
            "scheduling_id": self.scheduling_id,
            "port": self.port,
            "api_base": self.api_base,
            "created_at": self.created_at,
            "first_request_at": self.first_request_at,
            "last_request_at": self.last_request_at,
            **{
                key: value
                for key, value in self.stats.items()
                if key != "latencies"
            },
            "latency_p50": (
                round(statistics.median(latencies), 3) if latencies else 0
            ),
            "latency_p95": (
                round(latencies[min(len(latencies) - 1, int(len(latencies) * 0.95))], 3)
                if latencies
                else 0
            ),
            "active_assignments": list(self.active_assignments.values()),
            "lifecycle": "busy" if self.active_assignments else "ready",
        }


@ray.remote(max_concurrency=256, num_cpus=0)
class QwenPoolCoordinator:
    """Least-inflight dispatcher across dynamically attached TP1 endpoints."""

    def __init__(self) -> None:
        self.endpoints: list[Any] = []
        self.endpoint_ids: list[str] = []
        self.inflight: list[int] = []
        self.lock = asyncio.Lock()
        self.queue_wait_seconds: list[float] = []
        self.submitted = 0
        self.completed = 0
        self.assignment_sequence = 0
        self.recent_assignments: list[dict[str, Any]] = []
        self.active_assignments: dict[str, dict[str, Any]] = {}

    async def add_endpoints(
        self, endpoints: list[Any], endpoint_ids: list[str]
    ) -> None:
        async with self.lock:
            self.endpoints.extend(endpoints)
            self.endpoint_ids.extend(endpoint_ids)
            self.inflight.extend([0] * len(endpoints))

    async def request(
        self,
        kind: str,
        messages: list[dict[str, str]],
        max_tokens: int = 1800,
        routing: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        if kind != "generation":
            raise ValueError("Judge is disabled for the autoscale no-Judge job")
        queued_at = time.monotonic()
        while True:
            async with self.lock:
                if self.endpoints:
                    index = min(
                        range(len(self.endpoints)),
                        key=lambda value: (self.inflight[value], value),
                    )
                    endpoint = self.endpoints[index]
                    self.inflight[index] += 1
                    self.submitted += 1
                    self.assignment_sequence += 1
                    assignment_id = (
                        f"qwen-{self.assignment_sequence:08d}"
                    )
                    break
            await asyncio.sleep(0.2)
        queue_wait = time.monotonic() - queued_at
        self.queue_wait_seconds.append(queue_wait)
        assignment = {
            **dict(routing or {}),
            "assignment_id": assignment_id,
            "endpoint_id": self.endpoint_ids[index],
            "queue_wait": round(queue_wait, 6),
            "assigned_at": time.time(),
            "status": "assigned",
        }
        self.active_assignments[assignment_id] = assignment
        try:
            response = await endpoint.request.remote(
                messages, max_tokens, assignment
            )
            completed = dict(response.get("routing", assignment))
            self.recent_assignments.append(completed)
            self.recent_assignments = self.recent_assignments[-1000:]
            return response
        except Exception:
            self.recent_assignments.append(
                {
                    **assignment,
                    "status": "failed",
                    "completed_at": time.time(),
                }
            )
            self.recent_assignments = self.recent_assignments[-1000:]
            raise
        finally:
            self.active_assignments.pop(assignment_id, None)
            async with self.lock:
                self.inflight[index] -= 1
                self.completed += 1

    async def snapshot(self) -> dict[str, Any]:
        endpoint_stats = (
            await asyncio.gather(
                *[endpoint.snapshot.remote() for endpoint in self.endpoints]
            )
            if self.endpoints
            else []
        )
        waits = sorted(self.queue_wait_seconds)
        endpoint_active = {
            assignment["assignment_id"]: assignment
            for endpoint in endpoint_stats
            for assignment in endpoint.get("active_assignments", [])
        }
        active_assignments = {
            **self.active_assignments,
            **endpoint_active,
        }
        return {
            "endpoint_count": len(self.endpoints),
            "endpoint_ids": list(self.endpoint_ids),
            "inflight": list(self.inflight),
            "submitted": self.submitted,
            "completed": self.completed,
            "queue_wait_p50": (
                round(statistics.median(waits), 3) if waits else 0
            ),
            "queue_wait_p95": (
                round(waits[min(len(waits) - 1, int(len(waits) * 0.95))], 3)
                if waits
                else 0
            ),
            "endpoints": endpoint_stats,
            "active_assignments": list(active_assignments.values()),
            "recent_assignments": list(self.recent_assignments),
        }
