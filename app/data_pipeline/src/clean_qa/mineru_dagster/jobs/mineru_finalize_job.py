from dagster import job

from ..ops.asset_ops import register_asset_metadata
from ..ops.manifest_ops import load_or_rebuild_manifest
from ..ops.ray_job_ops import load_submission_state
from ..ops.validation_ops import read_existing_summary, validate_existing_outputs
from ..resources import RayJobResource, S3Resource
from ..run_configs import MINERU_FINALIZE_CONFIG


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=MINERU_FINALIZE_CONFIG,
)
def mineru_finalize_job():
    submission = load_submission_state()
    manifest = load_or_rebuild_manifest(submission)
    summarized = read_existing_summary(manifest)
    validated = validate_existing_outputs(summarized)
    register_asset_metadata(validated)
