from dagster import AssetSpec

from ..partitions import batch_partitions


training_jsonl_dataset = AssetSpec(
    "training_jsonl_dataset",
    deps=["qa_mcq_documents"],
    description=(
        "Verified QA and MCQ are normalized into per-document JSONL shards whose only "
        "top-level fields are id and text, then published back to the data lake."
    ),
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3", "jsonl", "training"},
    metadata={
        "schema": "strict top-level {id, text}",
        "bucket": "k12-cleaned-corpus",
        "prefix": "stage2/training-jsonl-collection-v1",
    },
)
