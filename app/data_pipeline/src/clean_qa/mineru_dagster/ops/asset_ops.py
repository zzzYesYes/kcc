from __future__ import annotations

from dagster import AssetMaterialization, In, MetadataValue, op

from ..resources.s3_resource import S3Resource


@op(ins={"state": In(dict)}, required_resource_keys={"s3"})
def register_asset_metadata(context, state: dict) -> None:
    batch_id = state["batch_id"]
    summary = state["summary"]
    context.instance.add_dynamic_partitions("batch_id", [batch_id])
    context.log_event(
        AssetMaterialization(
            asset_key="raw_pdf_batch",
            partition=batch_id,
            metadata={
                "input_prefix": MetadataValue.path(state["input_prefix"]),
                "pdf_count": state["pdf_count"],
                "total_bytes": state["total_bytes"],
                "source": "s3",
                "batch_id": batch_id,
                "scan_timestamp": state["scan_timestamp"],
            },
        )
    )
    context.log_event(
        AssetMaterialization(
            asset_key="pdf_manifest",
            partition=batch_id,
            metadata={
                "record_count": state["manifest_count"],
                "manifest_uri": MetadataValue.url(state["manifest_uri"]),
                "batch_id": batch_id,
            },
        )
    )
    service_documents = {"A": 0, "B": 0}
    service_pages = {"A": 0, "B": 0}
    for row in summary.get("results", []):
        service = row.get("service")
        if service in service_documents:
            service_documents[service] += 1
            service_pages[service] += int(row.get("page_count", 0))
    context.log_event(
        AssetMaterialization(
            asset_key="mineru_parsed_documents",
            partition=batch_id,
            metadata={
                "input_pdf_count": state["manifest_count"],
                "success_pdf_count": summary["success_count"],
                "failed_pdf_count": summary["pdf_count"] - summary["success_count"],
                "total_pages": summary["page_count"],
                "service_a_documents": service_documents["A"],
                "service_b_documents": service_documents["B"],
                "service_a_pages": service_pages["A"],
                "service_b_pages": service_pages["B"],
                "elapsed_seconds": summary["elapsed_seconds"],
                "pages_per_second": summary["pages_per_second"],
                "ray_job_id": state["ray_job_id"],
                "manifest_uri": MetadataValue.url(state["manifest_uri"]),
                "summary_uri": MetadataValue.url(state["summary_uri"]),
                "output_prefix": MetadataValue.path(state["output_prefix"]),
            },
        )
    )
    registration = {
        key: value
        for key, value in state.items()
        if key not in {"summary"}
    }
    registration["summary_metrics"] = {
        key: summary.get(key)
        for key in (
            "status",
            "pdf_count",
            "success_count",
            "page_count",
            "elapsed_seconds",
            "pages_per_second",
        )
    }
    s3: S3Resource = context.resources.s3
    s3.write_json(
        state["output_bucket"],
        f"_control/dagster/registrations/{batch_id}.json",
        registration,
    )
