# Legacy K12 Cleaner

This package is the pre-Stage-1 deterministic cleaner retained only for the
older `cleaning_*` Dagster jobs. It is not the production Stage 1 source of
truth.

Current cleaning code lives in:

```text
clean_qa.k12_clean_qa_pipeline.stage1_clean
```

Do not add new cleaning rules here. Existing jobs may continue importing this
package until they are retired or migrated explicitly.
