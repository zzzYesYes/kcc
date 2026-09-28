from .register_existing_batch import register_existing_mineru_batch_job
from .mineru_finalize_job import mineru_finalize_job
from .mineru_smoke_job import mineru_smoke_10_job
from .mineru_submit_job import mineru_submit_job
from .cleaning_job import cleaning_smoke_10_job
from .cleaning_full_job import cleaning_full_job
from .qwen_jobs import (
    qwen_chat_job,
    qwen_vllm_8npulifecycle_job,
    qwen_vllm_lifecycle_job,
)


ALL_JOBS = [
    register_existing_mineru_batch_job,
    mineru_smoke_10_job,
    mineru_submit_job,
    mineru_finalize_job,
    cleaning_smoke_10_job,
    cleaning_full_job,
    qwen_vllm_lifecycle_job,
    qwen_vllm_8npulifecycle_job,
    qwen_chat_job,
]
