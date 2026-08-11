from dagster import AssetSpec

from ..partitions import batch_partitions


cleaned_documents = AssetSpec(
    "cleaned_documents",
    deps=["mineru_parsed_documents"],
    description=(
        "Stage 1 deterministic CPU cleaning reads MinerU artifacts from S3 and writes "
        "clean.md, blocks.jsonl, exercises.jsonl and quality reports back to S3."
    ),
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3", "ray", "cleaning"},
    metadata={
        "stage": "stage1-v1.0.2",
        "bucket": "k12-cleaned-corpus",
        "prefix": "stage1/full/stage1-v1.0.2",
    },
)
