from __future__ import annotations

import json
from typing import Any

from dagster import Field, In, MetadataValue, Out, op

from .resources import QwenServingResource


LIFECYCLE_SCHEMA = {
    "action": Field(
        str,
        default_value="status",
        description="status/start/restart/stop; start and restart wait for Kubernetes and vLLM readiness.",
    ),
    "startup_timeout_seconds": Field(int, default_value=1800),
    "shutdown_timeout_seconds": Field(int, default_value=300),
    "poll_interval_seconds": Field(float, default_value=5.0),
}

CHAT_SCHEMA = {
    "target": Field(str, default_value="standard", description="standard or eight_npu"),
    "prompt": Field(str),
    "system_prompt": Field(str, default_value="You are a precise and helpful assistant."),
    "history_json": Field(str, default_value="[]"),
    "temperature": Field(float, default_value=0.0),
    "max_tokens": Field(int, default_value=256),
    "enable_thinking": Field(bool, default_value=False),
    "timeout_seconds": Field(int, default_value=300),
}


def _validate_lifecycle(config: dict[str, Any]) -> None:
    if config["action"] not in {"status", "start", "restart", "stop"}:
        raise ValueError("action must be status, start, restart, or stop")
    if not 60 <= config["startup_timeout_seconds"] <= 7200:
        raise ValueError("startup_timeout_seconds must be between 60 and 7200")
    if not 10 <= config["shutdown_timeout_seconds"] <= 1800:
        raise ValueError("shutdown_timeout_seconds must be between 10 and 1800")
    if not 0.5 <= config["poll_interval_seconds"] <= 60:
        raise ValueError("poll_interval_seconds must be between 0.5 and 60")


def _control(context, resource: QwenServingResource) -> dict[str, Any]:
    config = dict(context.op_config)
    _validate_lifecycle(config)
    action = config["action"]
    if action == "status":
        state = resource.status()
        probe = None
    elif action == "stop":
        resource.scale(0)
        state = resource.wait_for_kubernetes(
            0, config["shutdown_timeout_seconds"], config["poll_interval_seconds"]
        )
        probe = None
    else:
        resource.scale(1, restart=action == "restart")
        state = resource.wait_for_kubernetes(
            1, config["startup_timeout_seconds"], config["poll_interval_seconds"]
        )
        probe = resource.wait_for_vllm(
            config["startup_timeout_seconds"], config["poll_interval_seconds"]
        )
    result = {"action": action, "deployment_state": state, "vllm_probe": probe}
    context.add_output_metadata(
        {
            "action": action,
            "deployment": resource.deployment_name,
            "namespace": resource.namespace,
            "desired_replicas": state["desired_replicas"],
            "status": MetadataValue.json(result),
        }
    )
    return result


@op(config_schema=LIFECYCLE_SCHEMA, out=Out(dict), required_resource_keys={"qwen_k8s"})
def control_qwen_worker(context) -> dict[str, Any]:
    return _control(context, context.resources.qwen_k8s)


@op(config_schema=LIFECYCLE_SCHEMA, out=Out(dict), required_resource_keys={"qwen8_k8s"})
def control_qwen_8npu_worker(context) -> dict[str, Any]:
    return _control(context, context.resources.qwen8_k8s)


def _chat_resource(context, target: str) -> QwenServingResource:
    if target == "standard":
        return context.resources.qwen_k8s
    if target == "eight_npu":
        return context.resources.qwen8_k8s
    raise ValueError("target must be standard or eight_npu")


@op(
    config_schema=CHAT_SCHEMA,
    out=Out(dict),
    required_resource_keys={"qwen_k8s", "qwen8_k8s"},
)
def call_qwen_chat(context) -> dict[str, Any]:
    config = dict(context.op_config)
    prompt = config["prompt"].strip()
    if not prompt or len(prompt) > 16000:
        raise ValueError("prompt must contain 1 to 16000 characters")
    try:
        history = json.loads(config["history_json"])
    except json.JSONDecodeError as exc:
        raise ValueError("history_json must be a JSON array") from exc
    if not isinstance(history, list) or len(history) > 40:
        raise ValueError("history_json must contain at most 40 messages")
    if any(
        not isinstance(item, dict)
        or item.get("role") not in {"user", "assistant"}
        or not isinstance(item.get("content"), str)
        for item in history
    ):
        raise ValueError("history messages require user/assistant role and string content")
    if not 0 <= config["temperature"] <= 2:
        raise ValueError("temperature must be between 0 and 2")
    if not 1 <= config["max_tokens"] <= 8192:
        raise ValueError("max_tokens must be between 1 and 8192")
    if not 10 <= config["timeout_seconds"] <= 3600:
        raise ValueError("timeout_seconds must be between 10 and 3600")

    resource = _chat_resource(context, config["target"])
    state = resource.status()
    if state["available_replicas"] != 1 or not any(
        pod["ready"] for pod in state["pods"]
    ):
        raise RuntimeError(f"Qwen worker is not ready: {state}")
    probe = resource.probe()
    payload = {
        "model": resource.model_name,
        "messages": [
            {"role": "system", "content": config["system_prompt"]},
            *history,
            {"role": "user", "content": prompt},
        ],
        "temperature": config["temperature"],
        "max_tokens": config["max_tokens"],
        "chat_template_kwargs": {"enable_thinking": config["enable_thinking"]},
    }
    result = resource.chat(payload, config["timeout_seconds"])
    result["target"] = config["target"]
    result["health"] = probe
    context.log.info(
        "Qwen chat completed endpoint=%s model=%s latency=%.4fs finish_reason=%s",
        result["endpoint"], result["model"], result["latency_seconds"],
        result["finish_reason"],
    )
    context.add_output_metadata(
        {
            "status": result["status"],
            "endpoint": result["endpoint"],
            "model": result["model"],
            "response": MetadataValue.text(result["response"]),
            "latency_seconds": result["latency_seconds"],
            "token_usage": MetadataValue.json(result["token_usage"]),
            "finish_reason": str(result["finish_reason"]),
            "http_status": result["http_status"],
        }
    )
    return result
