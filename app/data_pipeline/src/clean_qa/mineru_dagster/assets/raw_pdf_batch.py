from dagster import AssetSpec

from ..partitions import batch_partitions


raw_pdf_batch = AssetSpec(
    "raw_pdf_batch",
    description="Raw textbook PDF objects stored in the K12 S3/MinIO data lake.",
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3"},
    metadata={"bucket": "k12-textbook-raw", "role": "read-only source"},
)
