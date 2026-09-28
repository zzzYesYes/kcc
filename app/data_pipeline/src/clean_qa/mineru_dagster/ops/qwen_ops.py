from __future__ import annotations

import base64
import json
import time
from datetime import datetime, timezone

from dagster import Field, In, MetadataValue, Out, op
from ray.job_submission import JobStatus

from ..resources.qwen_kubernetes_resource import QwenKubernetesResource
from ..resources.ray_job_resource import RayJobResource


VLLM_SCHEMA = {
    "action": Field(
        str,
        default_value="status",
        description="status is read-only; start applies parameters at replicas=1; restart applies and reloads; stop scales to zero.",
    ),
    "max_model_len": Field(int, default_value=32768),
    "max_num_seqs": Field(int, default_value=8),
    "max_num_batched_tokens": Field(int, default_value=4096),
    "gpu_memory_utilization": Field(float, default_value=0.85),
    "startup_timeout_seconds": Field(int, default_value=1800),
}

CHAT_SCHEMA = {
    "prompt": Field(str, description="The user message for this auditable single-turn Qwen run."),
    "system_prompt": Field(str, default_value="You are a precise document analysis assistant."),
    "history_json": Field(
        str,
        default_value="[]",
        description="JSON array of prior user/assistant messages for a multi-turn Launchpad run.",
    ),
    "temperature": Field(float, default_value=0.3),
    "max_tokens": Field(int, default_value=1024),
    "enable_thinking": Field(bool, default_value=False),
    "timeout_seconds": Field(int, default_value=600),
}


def _validate_vllm(config: dict) -> None:
    if config["action"] not in {"status", "start", "restart", "stop"}:
        raise ValueError("action must be status, start, restart, or stop")
    if not 1024 <= config["max_model_len"] <= 32768:
        raise ValueError("max_model_len must be between 1024 and 32768")
    if not 1 <= config["max_num_seqs"] <= 64:
        raise ValueError("max_num_seqs must be between 1 and 64")
    if not 512 <= config["max_num_batched_tokens"] <= config["max_model_len"]:
        raise ValueError("max_num_batched_tokens must be between 512 and max_model_len")
    if not 0.70 <= config["gpu_memory_utilization"] <= 0.90:
        raise ValueError("gpu_memory_utilization must be between 0.70 and 0.90")
    if not 60 <= config["startup_timeout_seconds"] <= 1800:
        raise ValueError("startup_timeout_seconds must be between 60 and 1800")


@op(config_schema=VLLM_SCHEMA, out=Out(dict), required_resource_keys={"qwen_k8s"})
def configure_qwen_vllm(context) -> dict:
    config = dict(context.op_config)
    _validate_vllm(config)
    qwen: QwenKubernetesResource = context.resources.qwen_k8s
    action = config["action"]
    if action == "status":
        state = qwen.status()
    elif action == "stop":
        state = qwen.stop()
    else:
        # A ConfigMap update is not observed by an already-running vLLM process.
        # Start therefore rolls the Deployment when it is already at one replica.
        state = qwen.configure_and_restart(config, replicas=1, restart=True)
    context.add_output_metadata({"action": action, "status": MetadataValue.json(state)})
    return {"action": action, "config": config, "status": state}


@op(config_schema=VLLM_SCHEMA, out=Out(dict), required_resource_keys={"qwen8_k8s"})
def configure_qwen_vllm_8npu(context) -> dict:
    config = dict(context.op_config)
    _validate_vllm(config)
    qwen: QwenKubernetesResource = context.resources.qwen8_k8s
    action = config["action"]
    if action == "status":
        state = qwen.status()
    elif action == "stop":
        state = qwen.stop()
    else:
        state = qwen.configure_and_restart(config, replicas=1, restart=True)
    context.add_output_metadata({"action": action, "status": MetadataValue.json(state)})
    return {"action": action, "config": config, "status": state}


@op(ins={"control": In(dict)}, out=Out(dict), required_resource_keys={"qwen_k8s"})
def wait_for_qwen_vllm(context, control: dict) -> dict:
    qwen: QwenKubernetesResource = context.resources.qwen_k8s
    if control["action"] in {"start", "restart"}:
        state = qwen.wait_until_ready(control["config"]["startup_timeout_seconds"])
    else:
        state = qwen.status()
    context.add_output_metadata({"status": MetadataValue.json(state)})
    return state


@op(ins={"control": In(dict)}, out=Out(dict), required_resource_keys={"qwen8_k8s"})
def wait_for_qwen_vllm_8npu(context, control: dict) -> dict:
    qwen: QwenKubernetesResource = context.resources.qwen8_k8s
    if control["action"] in {"start", "restart"}:
        state = qwen.wait_until_ready(control["config"]["startup_timeout_seconds"])
    else:
        state = qwen.status()
    context.add_output_metadata({"status": MetadataValue.json(state)})
    return state


@op(config_schema=CHAT_SCHEMA, out=Out(dict), required_resource_keys={"qwen_k8s", "ray_jobs"})
def submit_qwen_chat(context) -> dict:
    config = dict(context.op_config)
    if not config["prompt"].strip() or len(config["prompt"]) > 16000:
        raise ValueError("prompt must contain 1 to 16000 characters")
    try:
        history = json.loads(config["history_json"])
    except json.JSONDecodeError as exc:
        raise ValueError("history_json must be a JSON array") from exc
    if not isinstance(history, list) or len(history) > 40:
        raise ValueError("history_json must contain at most 40 messages")
    if any(
        not isinstance(message, dict)
        or message.get("role") not in {"user", "assistant"}
        or not isinstance(message.get("content"), str)
        for message in history
    ):
        raise ValueError("history_json messages require user/assistant role and string content")
    if not 0 <= config["temperature"] <= 2:
        raise ValueError("temperature must be between 0 and 2")
    if not 1 <= config["max_tokens"] <= 8192:
        raise ValueError("max_tokens must be between 1 and 8192")
    if not 30 <= config["timeout_seconds"] <= 1800:
        raise ValueError("timeout_seconds must be between 30 and 1800")

    qwen: QwenKubernetesResource = context.resources.qwen_k8s
    status = qwen.status()
    if not status["pod"]["ready"]:
        raise RuntimeError(f"Qwen service is not ready: {status}")

    encode = lambda value: base64.b64encode(value.encode()).decode()
    job_id = f"qwen-chat-{int(time.time())}"
    entrypoint = " ".join(
        [
            "python3 -m clean_qa.mineru_dagster.qwen_chat_ray_job",
            f"--prompt-b64 {encode(config['prompt'])}",
            f"--system-prompt-b64 {encode(config['system_prompt'])}",
            f"--history-b64 {encode(json.dumps(history, ensure_ascii=False))}",
            f"--temperature {config['temperature']}",
            f"--max-tokens {config['max_tokens']}",
            f"--enable-thinking {str(config['enable_thinking']).lower()}",
        ]
    )
    ray_jobs: RayJobResource = context.resources.ray_jobs
    ray_jobs.submit(job_id, entrypoint)
    state = {"ray_job_id": job_id, "timeout_seconds": config["timeout_seconds"], "submitted_at": datetime.now(timezone.utc).isoformat()}
    context.add_output_metadata({"ray_job_id": job_id, "model": "qwen3.6-35b-a3b"})
    return state


@op(ins={"submission": In(dict)}, out=Out(dict), required_resource_keys={"ray_jobs"})
def monitor_qwen_chat(context, submission: dict) -> dict:
    ray_jobs: RayJobResource = context.resources.ray_jobs
    status = ray_jobs.wait(submission["ray_job_id"], timeout_seconds=submission["timeout_seconds"])
    logs = ray_jobs.logs(submission["ray_job_id"])
    if status != JobStatus.SUCCEEDED:
        raise RuntimeError(f"Qwen Ray job {submission['ray_job_id']} ended as {status}: {logs[-4000:]}")
    marker = "QWEN_CHAT_RESULT "
    matches = [line[len(marker):] for line in logs.splitlines() if line.startswith(marker)]
    if not matches:
        raise RuntimeError(f"Qwen Ray job returned no result marker: {logs[-4000:]}")
    result = json.loads(matches[-1])
    context.add_output_metadata(
        {
            "answer": MetadataValue.text(result["answer"]),
            "elapsed_seconds": float(result["elapsed_seconds"]),
            "usage": MetadataValue.json(result.get("usage", {})),
            "ray_node_ip": result["ray_node_ip"],
        }
    )
    return result
