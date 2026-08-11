from dagster import job

from ..ops.asset_ops import register_asset_metadata
from ..ops.manifest_ops import load_or_rebuild_manifest
from ..ops.s3_ops import scan_existing_input, validate_registration_config
from ..ops.validation_ops import read_existing_summary, validate_existing_outputs
from ..resources.s3_resource import S3Resource
from ..run_configs import REGISTER_BATCH_002_CONFIG


@job(resource_defs={"s3": S3Resource()}, config=REGISTER_BATCH_002_CONFIG)
def register_existing_mineru_batch_job():
    config = validate_registration_config()
    scanned = scan_existing_input(config)
    manifest = load_or_rebuild_manifest(scanned)
    summarized = read_existing_summary(manifest)
    validated = validate_existing_outputs(summarized)
    register_asset_metadata(validated)
