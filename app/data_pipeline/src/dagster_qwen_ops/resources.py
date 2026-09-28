from __future__ import annotations

import json
import os
import ssl
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

from dagster import ConfigurableResource


class QwenServingResource(ConfigurableResource):
    """Namespace-scoped Kubernetes and HTTP client for one Qwen worker profile."""

    namespace: str
    deployment_name: str
    service_name: str
    pod_label_key: str = "app.kubernetes.io/component"
    pod_label_value: str
    service_port: int = 8000
    model_name: str
    health_path: str = "/health"
    models_path: str = "/v1/models"
    chat_path: str = "/v1/chat/completions"
    api_base_url: str = ""
    api_key: str = ""
    request_timeout_seconds: int = 30

    _token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    _ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    _api_base = "https://kubernetes.default.svc"

    def base_url(self) -> str:
        if self.api_base_url.strip():
            return self.api_base_url.rstrip("/")
        return (
            f"http://{self.service_name}.{self.namespace}.svc.cluster.local:"
            f"{self.service_port}"
        )

    def _kubernetes_request(
        self, method: str, path: str, body: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        token = self._token_path.read_text(encoding="utf-8").strip()
        payload = json.dumps(body).encode() if body is not None else None
        request = Request(
            f"{self._api_base}{path}",
            data=payload,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json",
            },
            method=method,
        )
        context = ssl.create_default_context(cafile=self._ca_path)
        try:
            with urlopen(
                request, context=context, timeout=self.request_timeout_seconds
            ) as response:
                raw = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:2000]
            raise RuntimeError(
                f"Kubernetes {method} {path} failed: {exc.code} {detail}"
            ) from exc
        return json.loads(raw) if raw else {}

    def _http_json(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        timeout_seconds: int | None = None,
    ) -> tuple[int, dict[str, Any] | None, float]:
        headers = {"Accept": "application/json"}
        data = None
        if body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(body, ensure_ascii=False).encode()
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = Request(
            f"{self.base_url()}{path}", data=data, headers=headers, method=method
        )
        started = time.monotonic()
        try:
            with urlopen(
                request,
                timeout=timeout_seconds or self.request_timeout_seconds,
            ) as response:
                raw = response.read()
                status = response.status
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:4000]
            raise RuntimeError(
                f"Qwen API {method} {path} failed: {exc.code} {detail}"
            ) from exc
        except URLError as exc:
            raise RuntimeError(f"Qwen API {method} {path} unavailable: {exc}") from exc
        elapsed = time.monotonic() - started
        if not raw:
            return status, None, elapsed
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"Qwen API {method} {path} returned non-JSON: "
                f"{raw.decode(errors='replace')[:1000]}"
            ) from exc
        return status, parsed, elapsed

    def deployment(self) -> dict[str, Any]:
        return self._kubernetes_request(
            "GET",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/"
            f"{self.deployment_name}",
        )

    def pods(self) -> list[dict[str, Any]]:
        selector = (
            f"{quote(self.pod_label_key, safe='')}%3D"
            f"{quote(self.pod_label_value, safe='')}"
        )
        result = self._kubernetes_request(
            "GET", f"/api/v1/namespaces/{self.namespace}/pods?labelSelector={selector}"
        )
        return result.get("items", [])

    @staticmethod
    def _pod_summary(pod: dict[str, Any]) -> dict[str, Any]:
        statuses = pod.get("status", {}).get("containerStatuses", [])
        return {
            "name": pod.get("metadata", {}).get("name"),
            "phase": pod.get("status", {}).get("phase", "Unknown"),
            "node": pod.get("spec", {}).get("nodeName"),
            "pod_ip": pod.get("status", {}).get("podIP"),
            "ready": bool(statuses) and all(bool(item.get("ready")) for item in statuses),
            "containers": {
                item.get("name", "unknown"): {
                    "ready": bool(item.get("ready")),
                    "restart_count": int(item.get("restartCount", 0)),
                }
                for item in statuses
            },
        }

    def status(self) -> dict[str, Any]:
        deployment = self.deployment()
        spec = deployment.get("spec", {})
        state = deployment.get("status", {})
        pods = [self._pod_summary(pod) for pod in self.pods()]
        return {
            "deployment": self.deployment_name,
            "namespace": self.namespace,
            "desired_replicas": int(spec.get("replicas", 0)),
            "available_replicas": int(state.get("availableReplicas", 0)),
            "updated_replicas": int(state.get("updatedReplicas", 0)),
            "pods": pods,
            "api_base_url": self.base_url(),
            "model": self.model_name,
        }

    def scale(self, replicas: int, restart: bool = False) -> dict[str, Any]:
        if replicas not in (0, 1):
            raise ValueError("portable lifecycle demo only permits replicas 0 or 1")
        patch: dict[str, Any] = {"spec": {"replicas": replicas}}
        if restart and replicas == 1:
            patch["spec"]["template"] = {
                "metadata": {
                    "annotations": {
                        "dagster-qwen-ops/restarted-at": str(time.time_ns())
                    }
                }
            }
        self._kubernetes_request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/"
            f"{self.deployment_name}",
            patch,
        )
        return self.status()

    def wait_for_kubernetes(
        self,
        expected_replicas: int,
        timeout_seconds: int,
        poll_interval_seconds: float,
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last: dict[str, Any] = {}
        while time.monotonic() < deadline:
            last = self.status()
            if expected_replicas == 0:
                if last["desired_replicas"] == 0 and not last["pods"]:
                    return last
            elif (
                last["desired_replicas"] == 1
                and last["available_replicas"] == 1
                and any(pod["ready"] for pod in last["pods"])
            ):
                return last
            time.sleep(poll_interval_seconds)
        raise TimeoutError(
            f"Qwen Deployment did not reach replicas={expected_replicas}: {last}"
        )

    def probe(self) -> dict[str, Any]:
        health_status, health_body, health_seconds = self._http_json(
            "GET", self.health_path
        )
        models_status, models_body, models_seconds = self._http_json(
            "GET", self.models_path
        )
        models = []
        if isinstance(models_body, dict):
            models = [
                item.get("id")
                for item in models_body.get("data", [])
                if isinstance(item, dict) and item.get("id")
            ]
        if models and self.model_name not in models:
            raise RuntimeError(
                f"Configured model {self.model_name!r} not present in /v1/models: {models}"
            )
        return {
            "status": "ready",
            "endpoint": self.base_url(),
            "health": {
                "path": self.health_path,
                "http_status": health_status,
                "latency_seconds": round(health_seconds, 4),
                "body": health_body,
            },
            "models": {
                "path": self.models_path,
                "http_status": models_status,
                "latency_seconds": round(models_seconds, 4),
                "ids": models,
            },
        }

    def wait_for_vllm(
        self, timeout_seconds: int, poll_interval_seconds: float
    ) -> dict[str, Any]:
        deadline = time.monotonic() + timeout_seconds
        last_error = "not attempted"
        while time.monotonic() < deadline:
            try:
                return self.probe()
            except RuntimeError as exc:
                last_error = str(exc)
                time.sleep(poll_interval_seconds)
        raise TimeoutError(f"vLLM service did not become ready: {last_error}")

    def chat(self, payload: dict[str, Any], timeout_seconds: int) -> dict[str, Any]:
        status, body, elapsed = self._http_json(
            "POST", self.chat_path, body=payload, timeout_seconds=timeout_seconds
        )
        if not isinstance(body, dict):
            raise RuntimeError("Qwen chat response body is empty")
        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise RuntimeError(f"Qwen chat response has no choices: {body}")
        choice = choices[0]
        message = choice.get("message", {})
        return {
            "status": "success",
            "endpoint": f"{self.base_url()}{self.chat_path}",
            "model": body.get("model", self.model_name),
            "response": message.get("content", ""),
            "reasoning_content": message.get("reasoning_content"),
            "latency_seconds": round(elapsed, 4),
            "token_usage": body.get("usage", {}),
            "finish_reason": choice.get("finish_reason"),
            "http_status": status,
            "response_id": body.get("id"),
        }


def resource_from_env(prefix: str) -> QwenServingResource:
    def value(name: str, default: str = "") -> str:
        return os.environ.get(f"{prefix}_{name}", default)

    return QwenServingResource(
        namespace=value("NAMESPACE", "dagster-qwen-ops"),
        deployment_name=value("DEPLOYMENT_NAME", "dagster-qwen-ops-worker"),
        service_name=value("SERVICE_NAME", "dagster-qwen-ops-worker"),
        pod_label_key=value("POD_LABEL_KEY", "app.kubernetes.io/component"),
        pod_label_value=value("POD_LABEL_VALUE", "qwen-worker"),
        service_port=int(value("SERVICE_PORT", "8000")),
        model_name=value("MODEL_NAME", "qwen-model"),
        health_path=value("HEALTH_PATH", "/health"),
        models_path=value("MODELS_PATH", "/v1/models"),
        chat_path=value("CHAT_PATH", "/v1/chat/completions"),
        api_base_url=value("API_BASE_URL"),
        api_key=value("API_KEY"),
        request_timeout_seconds=int(value("REQUEST_TIMEOUT_SECONDS", "30")),
    )
