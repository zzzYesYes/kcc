from dagster import AssetSpec

from ..partitions import batch_partitions


qa_mcq_documents = AssetSpec(
    "qa_mcq_documents",
    deps=["cleaned_documents"],
    description=(
        "Dedicated Qwen Ray workers read Stage 1 blocks and exercises from S3, call the "
        "resident Qwen3.6 service, validate QA/MCQ items, and write verified outputs to S3."
    ),
    group_name="mineru_lake",
    partitions_def=batch_partitions,
    kinds={"s3", "ray", "qwen"},
    metadata={
        "stage": "stage2-v1.1.0",
        "prompt": "k12-qa-zh-v1.2",
        "model": "qwen3.6-35b-a3b",
        "worker": "Qwen QA Worker",
        "accelerator": "Ascend 910C",
        "bucket": "k12-cleaned-corpus",
        "prefix": "stage2/full/stage2-v1.1.0-8npu",
    },
)
