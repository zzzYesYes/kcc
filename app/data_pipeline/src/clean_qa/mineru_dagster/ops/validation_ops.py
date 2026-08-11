from __future__ import annotations

from dagster import In, Out, op

from ..resources.s3_resource import S3Resource


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def read_existing_summary(context, state: dict) -> dict:
    s3: S3Resource = context.resources.s3
    summary_key = f"{state['output_prefix']}/_SUMMARY.json"
    summary = s3.read_json(state["output_bucket"], summary_key)
    state.update(
        {
            "summary": summary,
            "summary_key": summary_key,
            "summary_uri": f"s3://{state['output_bucket']}/{summary_key}",
        }
    )
    return state


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def validate_existing_outputs(context, state: dict) -> dict:
    s3: S3Resource = context.resources.s3
    summary = state["summary"]
    checks = {
        "summary_exists": s3.exists(state["output_bucket"], state["summary_key"]),
        "summary_status_success": summary.get("status") == "success",
        "pdf_count_matches": summary.get("pdf_count") == summary.get("success_count"),
        "manifest_count_matches": summary.get("pdf_count") == state["manifest_count"],
        "page_count_positive": int(summary.get("page_count", 0)) > 0,
        "throughput_positive": float(summary.get("pages_per_second", 0)) > 0,
        "output_prefix_exists": any(
            s3.list_prefix(state["output_bucket"], f"{state['output_prefix']}/")
        ),
    }
    results = summary.get("results", [])
    if results:
        step = max(1, len(results) // max(1, state["sample_size"]))
        sample = results[::step][: state["sample_size"]]
    else:
        sample = []
    sample_failures = []
    for row in sample:
        document_id = row["document_id"]
        prefix = f"{state['output_prefix']}/{document_id}"
        uploaded_keys = {item["key"] for item in row.get("uploaded", [])}
        required = [
            f"{prefix}/_SUCCESS.json",
            next((key for key in uploaded_keys if key.endswith("_content_list_v2.json")), ""),
            next((key for key in uploaded_keys if key.endswith(".md")), ""),
            next((key for key in uploaded_keys if key.endswith("images.tar.zst")), ""),
        ]
        missing = [
            key for key in required if not key or not s3.exists(state["output_bucket"], key)
        ]
        if missing:
            sample_failures.append({"document_id": document_id, "missing": missing})
    checks["sample_output_integrity"] = not sample_failures and bool(sample)
    if "event_counts" in state:
        checks["all_documents_assigned"] = (
            state["event_counts"].get("DOCUMENT_ASSIGNED") == state["manifest_count"]
        )
        checks["all_documents_uploaded"] = (
            state["event_counts"].get("DOCUMENT_UPLOADED") == state["manifest_count"]
        )
        checks["all_document_events_succeeded"] = (
            state["event_counts"].get("DOCUMENT_SUCCEEDED") == state["manifest_count"]
            and not state["event_counts"].get("DOCUMENT_FAILED")
        )
    state["checks"] = checks
    state["sample_failures"] = sample_failures
    failed = [name for name, passed in checks.items() if not passed]
    if failed:
        raise ValueError(f"existing batch validation failed: {failed}; {sample_failures[:3]}")
    context.log.info("All registration checks passed: %s", sorted(checks))
    return state
