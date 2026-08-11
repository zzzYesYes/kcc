# Cleaning/QA Namespace

This is the current K12 production application namespace:

* `k12_clean_qa_pipeline`: deterministic Stage 1 cleaning, Stage 2 QA/MCQ,
  collectors, manifests, validation, and Ray drivers;
* `mineru_dagster`: Dagster assets, jobs, resources, checks, and sensors.

The canonical Stage 1 implementation is
`clean_qa.k12_clean_qa_pipeline.stage1_clean`. The compatibility cleaner under
`legacy` must not be selected for new production work.

