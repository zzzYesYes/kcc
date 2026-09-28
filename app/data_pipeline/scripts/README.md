# Operations Scripts

Scripts are separated by ownership boundary:

```text
scripts/data_lake/   MinIO, buckets, ingest, S3 smoke, and data retention
scripts/clean_qa/    Dagster, Ray, MinerU/Qwen lifecycle, Stage 1/2, and Helm
scripts/dagster_qwen_ops/  Independent Dagster-to-Qwen lifecycle demo
```

The three groups use independent release names and configuration helpers. Do not
source `data_lake/common.sh` from a Cleaning/QA script or use the Data Lake
cleanup command to remove pipeline resources. The `dagster_qwen_ops` scripts do
not source either existing subsystem and operate only on their own Helm release.
