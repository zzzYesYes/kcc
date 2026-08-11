from dagster import AssetSpec

from ..partitions import batch_partitions


pdf_manifest = AssetSpec(
    "pdf_manifest",
    deps=["raw_pdf_batch"],
    description=(
        "Daft scans the data lake and creates stable bucket/key/etag task manifests; "
        "only small task descriptors are sent to Ray."
    ),
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3", "parquet", "daft"},
    metadata={"execution_plane": "Ray Head / Daft", "payload": "bucket, key, etag"},
)
