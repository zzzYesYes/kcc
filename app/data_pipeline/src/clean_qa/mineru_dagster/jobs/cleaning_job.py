from dagster import job

from ..ops.cleaning_ops import (
    fan_out_step_job1_documents,
    fan_out_step_job2_documents,
    fan_out_step_job3_documents,
    join_step_job1,
    join_step_job2,
    join_step_job3,
    materialize_cleaned_documents,
    monitor_step_job1,
    monitor_step_job2,
    monitor_step_job3,
    scan_cleaning_manifest,
    step_job1,
    step_job1_document_status,
    step_job2,
    step_job2_document_status,
    step_job3,
    step_job3_document_status,
    validate_cleaned_outputs,
)
from ..resources import RayJobResource, S3Resource
from ..run_configs import CLEANING_SMOKE_CONFIG


@job(
    resource_defs={"s3": S3Resource(), "ray_jobs": RayJobResource()},
    config=CLEANING_SMOKE_CONFIG,
)
def cleaning_smoke_10_job():
    manifest = scan_cleaning_manifest()
    stage1_submission = step_job1(manifest)
    stage1_documents = fan_out_step_job1_documents(stage1_submission).map(step_job1_document_status)
    stage1 = join_step_job1(monitor_step_job1(stage1_submission), stage1_documents.collect())

    stage2_submission = step_job2(stage1)
    stage2_documents = fan_out_step_job2_documents(stage2_submission).map(step_job2_document_status)
    stage2 = join_step_job2(monitor_step_job2(stage2_submission), stage2_documents.collect())

    stage3_submission = step_job3(stage2)
    stage3_documents = fan_out_step_job3_documents(stage3_submission).map(step_job3_document_status)
    stage3 = join_step_job3(monitor_step_job3(stage3_submission), stage3_documents.collect())
    validated = validate_cleaned_outputs(stage3)
    materialize_cleaned_documents(validated)
