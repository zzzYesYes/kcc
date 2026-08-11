from dagster import AssetSpec

from ..partitions import batch_partitions


mineru_parsed_documents = AssetSpec(
    "mineru_parsed_documents",
    deps=["pdf_manifest"],
    description=(
        "Dedicated Ascend MinerU workers read PDFs directly from S3, run MinerU 3.4 "
        "Hybrid parsing, then write Markdown, JSON and image archives back to S3."
    ),
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3", "ray", "mineru"},
    metadata={
        "worker": "MinerU NPU Worker",
        "accelerator": "Ascend 910C",
        "version": "MinerU 3.4 Hybrid",
        "bucket": "k12-mineru-output",
    },
)
