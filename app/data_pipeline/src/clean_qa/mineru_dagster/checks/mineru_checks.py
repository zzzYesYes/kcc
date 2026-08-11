from __future__ import annotations

from dagster import AssetCheckResult, asset_check

from ..partitions import batch_partitions
from ..resources.s3_resource import S3Resource


def registration(context, s3: S3Resource) -> dict:
    return s3.read_json(
        "k12-mineru-output",
        f"_control/dagster/registrations/{context.partition_key}.json",
    )


def registration_check(name: str):
    @asset_check(
        asset="mineru_parsed_documents",
        name=name,
        partitions_def=batch_partitions,
        required_resource_keys={"s3"},
    )
    def check(context) -> AssetCheckResult:
        state = registration(context, context.resources.s3)
        passed = bool(state["checks"].get(name))
        return AssetCheckResult(
            passed=passed,
            metadata={"batch_id": context.partition_key, "value": passed},
        )

    return check


MINERU_CHECKS = [
    registration_check(name)
    for name in (
        "summary_exists",
        "summary_status_success",
        "pdf_count_matches",
        "manifest_count_matches",
        "page_count_positive",
        "throughput_positive",
        "output_prefix_exists",
        "sample_output_integrity",
    )
]
