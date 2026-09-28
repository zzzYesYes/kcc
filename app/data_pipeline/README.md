# K12 Data Platform

This module contains three independently deployable systems with one explicit
runtime/compatibility layer:

```text
K12 Data Lake
  acquisition -> raw PDF/metadata -> MinIO buckets

K12 Cleaning/QA Production Pipeline
  S3 manifest -> Ray -> MinerU -> Stage 1 -> Stage 2 -> Training JSONL

Dagster Qwen Ops Demo
  Dagster -> scale Qwen vLLM -> health/models -> chat -> scale-to-zero
```

## Directory map

```text
src/data_lake/       acquisition, S3 smoke, and batch manifest tools
src/clean_qa/        current Stage 1/2 pipeline and Dagster orchestration
src/runtime/         MinerU 3.4, window/concurrency, and dual-NPU runtimes
src/legacy/          compatibility-only pre-Stage-1 cleaner
helm/data-lake/      MinIO, storage, services, and bucket initialization
helm/k12-clean-qa-pipeline/ Dagster, KubeRay, CPU/NPU workers, lifecycle
helm/dagster-qwen-ops/ Portable Dagster + Qwen lifecycle demonstration
scripts/data_lake/   data-lake deployment and ingest operations
scripts/clean_qa/    data-production deployment and execution operations
scripts/dagster_qwen_ops/ Qwen lifecycle operations and validation
docs/data_lake/      data-lake runbook
docs/clean_qa/       Cleaning/QA deployment and audit documents
docs/dagster_qwen_ops/ Standalone Qwen lifecycle migration guide
```

The Data Lake Chart does not install Dagster or compute resources. The
Cleaning/QA Chart treats MinIO/S3 as an external dependency and does not own
MinIO storage or credentials.

## Data Lake

```bash
cd app/data_pipeline
./scripts/data_lake/validate.sh
./scripts/data_lake/init-secrets.sh
./scripts/data_lake/deploy.sh
./scripts/data_lake/status.sh
```

Read [the Data Lake runbook](docs/data_lake/K12_DATA_LAKE_DEPLOYMENT.md).

## Cleaning/QA Pipeline

```bash
cd app/data_pipeline
./scripts/clean_qa/helm_lint.sh
cp helm/k12-clean-qa-pipeline/values-example.yaml /tmp/k12-site.yaml
PROFILE=/tmp/k12-site.yaml S3_SECRET_NAME=k12-pipeline-s3 \
  ./scripts/clean_qa/install_pipeline.sh
./scripts/clean_qa/status_pipeline.sh
```

Read [the Cleaning/QA deployment guide](docs/clean_qa/K12_CLEAN_QA_PIPELINE_DEPLOYMENT.md).

## Dagster Qwen Ops Demo

This standalone Chart contains no Ray, MinIO, MinerU, Stage 1, or Stage 2
resources. Both Qwen profiles install at `replicas=0` and use namespace-scoped
minimal RBAC.

```bash
HELM_BIN=helm PYTHON_BIN=python3 ./scripts/dagster_qwen_ops/validate.sh
```

Read [the lifecycle guide](docs/dagster_qwen_ops/DAGSTER_QWEN_LIFECYCLE_DEPLOYMENT.md).

## Validation

```bash
./scripts/data_lake/validate.sh
./scripts/clean_qa/validate.sh
```

No credential, platform token, private model path, or decoded Secret should be
committed. `.env` remains local and ignored.
