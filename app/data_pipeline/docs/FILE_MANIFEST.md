# 文件职责清单

## 顶层

| 文件 | 职责 |
| --- | --- |
| `README.md` | 三个独立系统的入口与边界 |
| `.env.example` | 非敏感本地参数示例 |
| `Dockerfile` | Cleaning/QA Dagster controller 镜像 |
| `requirements.txt` / `pyproject.toml` | Python 3.11 依赖、包发现和 CLI |
| `config/clean_qa/` | Dagster 配置和历史/当前 Job run config |

## Data Lake

```text
helm/data-lake/
scripts/data_lake/
src/data_lake/
docs/data_lake/
```

* `helm/data-lake`：MinIO、PVC、Service、Secret 引用和 bucket-init；不包含 Dagster。
* `src/data_lake/k12_ingest`：教材发现、下载、校验、上传和恢复。
* `src/data_lake/data_pipeline_tools`：S3 写读删 smoke。
* `src/data_lake/s3_lake_batch`：manifest 与进度快照。
* `scripts/data_lake`：Secret、部署、状态、采集、smoke、验证和保留式卸载。

## Cleaning/QA Pipeline

```text
helm/k12-clean-qa-pipeline/
scripts/clean_qa/
src/clean_qa/
docs/clean_qa/
```

* `src/clean_qa/k12_clean_qa_pipeline`：当前生产 Stage 1、Stage 2、collector、验证和 Job。
* `src/clean_qa/mineru_dagster`：Dagster assets/checks/jobs/resources/sensors 总入口。
* `helm/k12-clean-qa-pipeline`：Dagster、RayCluster、CPU Worker、零副本 MinerU/Qwen。
* `scripts/clean_qa`：构建、Helm、状态、扩缩容、smoke 和 Dagster Job 执行。

## Dagster Qwen Ops

```text
helm/dagster-qwen-ops/
scripts/dagster_qwen_ops/
src/dagster_qwen_ops/
docs/dagster_qwen_ops/
```

* `src/dagster_qwen_ops`：独立的 Qwen vLLM 生命周期、健康检查和 Chat Job。
* `helm/dagster-qwen-ops`：可迁移的 Dagster、零副本 Qwen Worker、Service 和最小 RBAC。
* `scripts/dagster_qwen_ops`：镜像构建、Secret、部署、观察、渲染和静态验证入口。
* `docs/dagster_qwen_ops`：部署运行手册与验证报告。

该模块不依赖 Ray、MinIO、MinerU 或 Cleaning/QA 生产逻辑。

## MinerU Runtime

```text
src/runtime/mineru34_hybrid_lake/
src/runtime/mineru_pipeline/
src/runtime/dual_npu/
```

这些模块负责 PDF 解析、官方 window/concurrency、双 NPU Serve 和 Ray 调度，不是 Stage 1
文本清洗代码。

## Legacy

`src/legacy/k12_cleaner` 是旧 `cleaning_*` Dagster Job 的兼容实现。当前 Stage 1 唯一主线是
`clean_qa.k12_clean_qa_pipeline.stage1_clean`。禁止向 legacy 包添加新清洗规则。

## 共享证据

* `docs/ACTUAL_RUN_EVIDENCE.md`：跨数据湖与生产管线的历史运行证据。
* `docs/README.md`：文档导航。
* 本文件：最终文件归属。
