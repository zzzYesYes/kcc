from dagster import job

from ..ops.ray_job_ops import (
    check_mineru_service_a,
    check_mineru_service_b,
    check_ray_cluster,
    scan_s3_with_daft,
    submit_ray_job,
    validate_run_config,
    write_pdf_manifest,
)
from ..resources import RayJobResource, S3Resource
from ..run_configs import MINERU_SUBMIT_CONFIG


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=MINERU_SUBMIT_CONFIG,
)
def mineru_submit_job():
    config = validate_run_config()
    scanned = scan_s3_with_daft(config)
    written = write_pdf_manifest(scanned)
    ray_ready = check_ray_cluster(written)
    service_a = check_mineru_service_a(ray_ready)
    service_b = check_mineru_service_b(service_a)
    submit_ray_job(service_b)
