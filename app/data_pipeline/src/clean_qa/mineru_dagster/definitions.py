from dagster import Definitions

from .assets import ALL_ASSETS
from .checks import ALL_CHECKS
from .jobs import ALL_JOBS
from .resources import QwenKubernetesResource, RayJobResource, S3Resource
from .sensors import ALL_SENSORS
from clean_qa.k12_clean_qa_pipeline.dagster_defs import ALL_PIPELINE_JOBS


defs = Definitions(
    assets=ALL_ASSETS,
    asset_checks=ALL_CHECKS,
    jobs=[*ALL_JOBS, *ALL_PIPELINE_JOBS],
    sensors=ALL_SENSORS,
    resources={
        "s3": S3Resource(),
        "ray_jobs": RayJobResource(),
        "qwen_k8s": QwenKubernetesResource(),
    },
)
