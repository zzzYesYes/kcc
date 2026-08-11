# K12 Data Platform Documentation

Documentation follows three independent system boundaries:

```text
docs/data_lake/  MinIO, buckets, ingest, recovery, and data retention
docs/clean_qa/   Dagster, Ray, MinerU, Stage 1, Stage 2, and training export
docs/dagster_qwen_ops/  Portable Dagster-managed Qwen serving lifecycle
```

Shared evidence and the repository file map remain at the `docs/` root.

* `STRUCTURE_REORGANIZATION_REPORT.md`: cross-system directory and namespace migration report.
* `FILE_MANIFEST.md`: current file ownership map.
* `ACTUAL_RUN_EVIDENCE.md`: historical runtime evidence shared by both systems.
