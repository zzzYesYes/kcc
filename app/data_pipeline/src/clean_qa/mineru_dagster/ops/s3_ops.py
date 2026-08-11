from __future__ import annotations

from datetime import datetime, timezone

from dagster import In, Out, op

from ..resources.s3_resource import S3Resource


REGISTRATION_SCHEMA = {
    "batch_id": str,
    "input_bucket": str,
    "input_prefix": str,
    "output_bucket": str,
    "output_prefix": str,
    "ray_job_id": str,
    "sample_size": int,
}


@op(config_schema=REGISTRATION_SCHEMA, out=Out(dict))
def validate_registration_config(context) -> dict:
    config = dict(context.op_config)
    config["output_prefix"] = config["output_prefix"].strip("/")
    config["input_prefix"] = config["input_prefix"].strip("/")
    if not config["batch_id"] or not config["output_prefix"]:
        raise ValueError("batch_id and output_prefix are required")
    context.log.info(
        "Registering %s from s3://%s/%s",
        config["batch_id"],
        config["output_bucket"],
        config["output_prefix"],
    )
    return config


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def scan_existing_input(context, state: dict) -> dict:
    s3: S3Resource = context.resources.s3
    manifest_keys = [
        f"{state['output_prefix']}/_INPUT_MANIFEST.json",
        f"{state['output_prefix']}/_HEAD_MANIFEST.json",
    ]
    manifest_key = next(
        (key for key in manifest_keys if s3.exists(state["output_bucket"], key)),
        None,
    )
    if manifest_key is None:
        raise FileNotFoundError("neither _INPUT_MANIFEST.json nor _HEAD_MANIFEST.json exists")
    manifest = s3.read_json(state["output_bucket"], manifest_key)
    documents = manifest["documents"]
    state.update(
        {
            "source_manifest_key": manifest_key,
            "source_manifest": manifest,
            "pdf_count": len(documents),
            "total_bytes": sum(int(row.get("size_bytes", 0)) for row in documents),
            "scan_timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    context.log.info("Found %d PDF records in existing manifest", len(documents))
    return state
