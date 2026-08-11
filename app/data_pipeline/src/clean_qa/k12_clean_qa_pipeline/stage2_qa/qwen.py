from __future__ import annotations

import asyncio
import json
import re
import statistics
import time
import urllib.error
import urllib.request
from collections import defaultdict
from typing import Any

import ray
from clean_qa.k12_clean_qa_pipeline.stage2_qa.helpers import json_from_content


@ray.remote(
    max_concurrency=256,
    num_cpus=0,
    resources={"QWEN36_A3B_API": 1},
)
class QwenRequestCoordinator:
    def __init__(
        self,
        api_base: str,
        model: str,
        generation_max_inflight: int,
        judge_max_inflight: int,
        http_pool_size: int,
        timeout_seconds: int = 180,
        max_retries: int = 3,
    ):
        self.api_base = api_base.rstrip("/")
        self.model = model
        self.generation_gate = asyncio.Semaphore(generation_max_inflight)
        self.judge_gate = asyncio.Semaphore(judge_max_inflight)
        self.http_gate = asyncio.Semaphore(http_pool_size)
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.stats: dict[str, Any] = {
            "requests": 0,
            "retries": 0,
            "errors": 0,
            "input_tokens": 0,
            "output_tokens": 0,
            "latencies": defaultdict(list),
            "queue_wait_seconds": defaultdict(list),
            "submitted": defaultdict(int),
            "completed": defaultdict(int),
            "waiting": defaultdict(int),
            "active": defaultdict(int),
            "max_waiting": defaultdict(int),
            "max_active": defaultdict(int),
        }

    def _post(self, messages: list[dict[str, str]], max_tokens: int) -> dict[str, Any]:
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
            ).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=self.timeout_seconds) as response:
            return json.loads(response.read())

    async def request(
        self,
        kind: str,
        messages: list[dict[str, str]],
        max_tokens: int = 1800,
    ) -> dict[str, Any]:
        gate = self.judge_gate if kind == "judge" else self.generation_gate
        queued_at = time.monotonic()
        self.stats["submitted"][kind] += 1
        self.stats["waiting"][kind] += 1
        self.stats["max_waiting"][kind] = max(
            self.stats["max_waiting"][kind],
            self.stats["waiting"][kind],
        )
        entered = False
        try:
            async with gate:
                async with self.http_gate:
                    self.stats["waiting"][kind] -= 1
                    self.stats["active"][kind] += 1
                    entered = True
                    self.stats["max_active"][kind] = max(
                        self.stats["max_active"][kind],
                        self.stats["active"][kind],
                    )
                    self.stats["queue_wait_seconds"][kind].append(
                        time.monotonic() - queued_at
                    )
                    return await self._request_with_retries(kind, messages, max_tokens)
        finally:
            if entered:
                self.stats["active"][kind] -= 1
                self.stats["completed"][kind] += 1
            elif self.stats["waiting"][kind] > 0:
                self.stats["waiting"][kind] -= 1

    async def _request_with_retries(
        self,
        kind: str,
        messages: list[dict[str, str]],
        max_tokens: int,
    ) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(self.max_retries + 1):
            started = time.monotonic()
            try:
                response = await asyncio.to_thread(
                    self._post, messages, max_tokens
                )
                latency = time.monotonic() - started
                self.stats["requests"] += 1
                self.stats["latencies"][kind].append(latency)
                usage = response.get("usage", {})
                self.stats["input_tokens"] += int(usage.get("prompt_tokens", 0))
                self.stats["output_tokens"] += int(
                    usage.get("completion_tokens", 0)
                )
                content = response["choices"][0]["message"]["content"]
                return {
                    "data": json_from_content(content),
                    "latency_seconds": round(latency, 3),
                    "usage": usage,
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
        raise RuntimeError(f"Qwen {kind} request exhausted retries: {last_error!r}")

    async def health(self) -> dict[str, Any]:
        started = time.monotonic()
        try:
            request = urllib.request.Request(f"{self.api_base}/health")
            response = await asyncio.to_thread(
                urllib.request.urlopen, request, None, 10
            )
            response.close()
            return {
                "healthy": True,
                "latency_seconds": round(time.monotonic() - started, 3),
            }
        except Exception as exc:
            return {"healthy": False, "error": repr(exc)}

    def snapshot(self) -> dict[str, Any]:
        result = dict(self.stats)
        latency_summary = {}
        for kind, values in self.stats["latencies"].items():
            ordered = sorted(values)
            latency_summary[kind] = {
                "count": len(values),
                "p50": round(statistics.median(values), 3) if values else 0,
                "p95": round(
                    ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 3
                )
                if values
                else 0,
            }
        result["latencies"] = latency_summary
        queue_summary = {}
        for kind, values in self.stats["queue_wait_seconds"].items():
            ordered = sorted(values)
            queue_summary[kind] = {
                "count": len(values),
                "p50": round(statistics.median(values), 3) if values else 0,
                "p95": round(
                    ordered[min(len(ordered) - 1, int(len(ordered) * 0.95))], 3
                )
                if values
                else 0,
                "max": round(max(values), 3) if values else 0,
            }
        result["queue_wait_seconds"] = queue_summary
        for key in (
            "submitted",
            "completed",
            "waiting",
            "active",
            "max_waiting",
            "max_active",
        ):
            result[key] = dict(self.stats[key])
        return result

    def _read_vllm_metrics(self) -> dict[str, float]:
        request = urllib.request.Request(f"{self.api_base}/metrics")
        with urllib.request.urlopen(request, timeout=10) as response:
            text = response.read().decode("utf-8", "replace")

        def total(metric: str) -> float:
            pattern = re.compile(rf"^{re.escape(metric)}(?:\{{[^}}]*\}})?\s+(\S+)$")
            values = []
            for line in text.splitlines():
                match = pattern.match(line)
                if match:
                    values.append(float(match.group(1)))
            return sum(values)

        return {
            "running": total("vllm:num_requests_running"),
            "waiting": total("vllm:num_requests_waiting"),
            "prompt_tokens_total": total("vllm:prompt_tokens_total"),
            "generation_tokens_total": total("vllm:generation_tokens_total"),
            "request_success_total": total("vllm:request_success_total"),
        }

    async def profile_snapshot(self) -> dict[str, Any]:
        return {
            "sampled_at_epoch": time.time(),
            "coordinator": self.snapshot(),
            "vllm": await asyncio.to_thread(self._read_vllm_metrics),
        }
