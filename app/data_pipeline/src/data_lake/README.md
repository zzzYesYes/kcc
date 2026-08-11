# Data Lake Namespace

This namespace owns data acquisition and S3/MinIO utility code:

* `data_pipeline_tools`: connectivity and object-store smoke checks;
* `k12_ingest`: source acquisition and raw-object ingestion;
* `s3_lake_batch`: manifest and progress utilities.

It does not own Dagster, Ray, MinerU, Qwen, Stage 1, or Stage 2.

