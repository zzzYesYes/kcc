from dagster import job

from ..ops.qwen_ops import (
    configure_qwen_vllm,
    configure_qwen_vllm_8npu,
    monitor_qwen_chat,
    submit_qwen_chat,
    wait_for_qwen_vllm,
    wait_for_qwen_vllm_8npu,
)
from ..resources import QwenKubernetesResource, RayJobResource
from ..run_configs import QWEN_CHAT_CONFIG, QWEN_VLLM_8NPU_CONFIG, QWEN_VLLM_CONFIG


@job(
    resource_defs={"qwen_k8s": QwenKubernetesResource()},
    config=QWEN_VLLM_CONFIG,
)
def qwen_vllm_lifecycle_job():
    wait_for_qwen_vllm(configure_qwen_vllm())


@job(
    resource_defs={
        "qwen8_k8s": QwenKubernetesResource(
            deployment_name="qwen36-35b-a3b-worker-8npu",
            config_map_name="qwen36-35b-launcher-8npu",
            service_name="qwen36-35b-a3b-8npu",
            pod_label="qwen36-35b-a3b-worker-8npu",
            service_count=4,
            device_pairs="8,9;10,11;12,13;14,15",
        )
    },
    config=QWEN_VLLM_8NPU_CONFIG,
)
def qwen_vllm_8npulifecycle_job():
    wait_for_qwen_vllm_8npu(configure_qwen_vllm_8npu())


@job(
    resource_defs={"qwen_k8s": QwenKubernetesResource(), "ray_jobs": RayJobResource()},
    config=QWEN_CHAT_CONFIG,
)
def qwen_chat_job():
    monitor_qwen_chat(submit_qwen_chat())
