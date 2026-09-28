from dagster import AssetCheckResult, asset_check

from ..partitions import batch_partitions


def cleaning_check(name: str):
    @asset_check(asset="cleaned_documents", name=name, partitions_def=batch_partitions, required_resource_keys={"s3"})
    def check(context) -> AssetCheckResult:
        state = context.resources.s3.read_json("k12-cleaned-corpus", f"_control/dagster/cleaning-registrations/{context.partition_key}.json")
        passed = bool(state["checks"].get(name))
        return AssetCheckResult(passed=passed, metadata={"batch_id": context.partition_key, "value": passed})

    return check


CLEANING_CHECKS = [
    cleaning_check(name)
    for name in (
        "summary_status_success",
        "document_count_matches",
        "failed_document_count_zero",
        "all_success_markers_exist",
        "all_required_outputs_exist",
    )
]
