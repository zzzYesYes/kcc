from __future__ import annotations

import io

import pyarrow as pa
import pyarrow.parquet as pq
from dagster import In, Out, op

from ..resources.s3_resource import S3Resource


@op(ins={"state": In(dict)}, out=Out(dict), required_resource_keys={"s3"})
def load_or_rebuild_manifest(context, state: dict) -> dict:
    s3: S3Resource = context.resources.s3
    documents = state.pop("source_manifest")["documents"]
    records = [
        {
            "document_id": row["document_id"],
            "input_bucket": state["input_bucket"],
            "input_key": row["object_key"],
            "etag": row.get("etag", ""),
            "size_bytes": int(row.get("size_bytes", 0)),
            "output_prefix": f"{state['output_prefix']}/{row['document_id']}",
            "batch_id": state["batch_id"],
        }
        for row in documents
    ]
    table = pa.Table.from_pylist(records)
    sink = io.BytesIO()
    pq.write_table(table, sink, compression="zstd")
    manifest_key = f"{state['output_prefix']}/_dagster/pdf_manifest.parquet"
    s3.client().put_object(
        Bucket=state["output_bucket"],
        Key=manifest_key,
        Body=sink.getvalue(),
        ContentType="application/vnd.apache.parquet",
    )
    state.update(
        {
            "manifest_count": len(records),
            "manifest_key": manifest_key,
            "manifest_uri": f"s3://{state['output_bucket']}/{manifest_key}",
        }
    )
    context.log.info("Wrote %d-row Parquet manifest", len(records))
    return state
