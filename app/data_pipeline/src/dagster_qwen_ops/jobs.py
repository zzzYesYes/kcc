from dagster import job

from .config import CHAT_CONFIG, LIFECYCLE_8NPU_CONFIG, LIFECYCLE_CONFIG
from .ops import call_qwen_chat, control_qwen_8npu_worker, control_qwen_worker
from .resources import resource_from_env


@job(
    resource_defs={"qwen_k8s": resource_from_env("QWEN")},
    config=LIFECYCLE_CONFIG,
    description="Scale, wait for, probe, inspect, or stop the standard Qwen vLLM worker.",
)
def qwen_vllm_lifecycle_job():
    control_qwen_worker()


@job(
    resource_defs={"qwen8_k8s": resource_from_env("QWEN8")},
    config=LIFECYCLE_8NPU_CONFIG,
    description="Scale, wait for, probe, inspect, or stop the eight-NPU Qwen worker.",
)
def qwen_vllm_8npulifecycle_job():
    control_qwen_8npu_worker()


@job(
    resource_defs={
        "qwen_k8s": resource_from_env("QWEN"),
        "qwen8_k8s": resource_from_env("QWEN8"),
    },
    config=CHAT_CONFIG,
    description="Call a ready Qwen OpenAI-compatible chat endpoint and record the result.",
)
def qwen_chat_job():
    call_qwen_chat()


ALL_JOBS = [qwen_vllm_lifecycle_job, qwen_vllm_8npulifecycle_job, qwen_chat_job]
