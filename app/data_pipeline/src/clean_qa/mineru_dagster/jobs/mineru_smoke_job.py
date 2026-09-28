from dagster import job

from ..ops.asset_ops import register_asset_metadata
from ..ops.manifest_ops import load_or_rebuild_manifest
from ..ops.ray_job_ops import (
    check_mineru_service_a,
    check_mineru_service_b,
    check_ray_cluster,
    fan_out_document_monitors,
    join_document_progress,
    monitor_document_status,
    monitor_ray_job,
    scan_s3_with_daft,
    submit_ray_job,
    validate_run_config,
    write_pdf_manifest,
)
from ..ops.validation_ops import read_existing_summary, validate_existing_outputs
from ..resources import RayJobResource, S3Resource
from ..run_configs import MINERU_SMOKE_CONFIG


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=MINERU_SMOKE_CONFIG,
)
def mineru_smoke_10_job():
    config = validate_run_config()
    scanned = scan_s3_with_daft(config)
    written = write_pdf_manifest(scanned)
    ray_ready = check_ray_cluster(written)
    service_a = check_mineru_service_a(ray_ready)
    service_b = check_mineru_service_b(service_a)
    submitted = submit_ray_job(service_b)
    monitored = monitor_ray_job(submitted)
    documents = fan_out_document_monitors(submitted)
    completed = documents.map(monitor_document_status)
    joined = join_document_progress(monitored, completed.collect())
    manifest = load_or_rebuild_manifest(joined)
    summarized = read_existing_summary(manifest)
    validated = validate_existing_outputs(summarized)
    register_asset_metadata(validated)
