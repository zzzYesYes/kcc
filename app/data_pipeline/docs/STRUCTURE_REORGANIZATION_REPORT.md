# Data Pipeline 结构整理报告

日期：2026-08-11

## 1. 整理目标

`app/data_pipeline` 明确承载两个独立系统：

```text
K12 Data Lake
  MinIO + bucket 初始化 + 教材采集入湖

K12 Cleaning/QA Pipeline
  Dagster + Ray/KubeRay + MinerU + Stage 1 + Stage 2 + Training JSONL
```

数据湖拥有存储生命周期；Cleaning/QA 只通过 endpoint、bucket、prefix 和 credential
Secret 使用外部 S3。两个 Chart、脚本和文档不再交叉承担资源所有权。

## 2. 最终目录

```text
app/data_pipeline/
├── config/clean_qa/
├── docs/
│   ├── data_lake/
│   └── clean_qa/
├── helm/
│   ├── data-lake/
│   └── k12-clean-qa-pipeline/
├── scripts/
│   ├── data_lake/
│   └── clean_qa/
└── src/
    ├── data_lake/
    ├── clean_qa/
    ├── runtime/
    └── legacy/
```

详细文件归属见 `docs/FILE_MANIFEST.md`。

## 3. Python namespace 迁移

| 原顶层包/文件 | 新模块 | 定位 |
| --- | --- | --- |
| `k12_ingest` | `data_lake.k12_ingest` | 当前数据采集 |
| `data_pipeline_tools` | `data_lake.data_pipeline_tools` | 当前 S3 工具 |
| `s3_lake_batch` | `data_lake.s3_lake_batch` | 当前 manifest 工具 |
| `k12_clean_qa_pipeline` | `clean_qa.k12_clean_qa_pipeline` | 当前 Stage 1/2 |
| `mineru_dagster` | `clean_qa.mineru_dagster` | 当前 Dagster Definitions |
| `mineru34_hybrid_lake` | `runtime.mineru34_hybrid_lake` | 当前 MinerU runtime |
| `dual_npu` | `runtime.dual_npu` | 当前双 NPU runtime |
| 根目录 MinerU runners | `runtime.mineru_pipeline` | 当前运行适配 |
| `k12_cleaner` | `legacy.k12_cleaner` | 兼容代码，不是当前 Stage 1 |

所有 Dagster/Ray 命令统一使用 `python -m <完整模块名>`。MinerU window runner 不再依赖
`/tmp/mineru_flash30`，Hybrid 子进程也不再直接执行带相对导入的 `.py` 文件。

## 4. Helm 边界

`helm/data-lake` 只渲染 Namespace、可选 Secret、MinIO StatefulSet、Service 和 bucket-init
Job，不包含 Dagster、Ray 或 NPU 权限。

`helm/k12-clean-qa-pipeline` 负责 Dagster、最小 RBAC、RayCluster、CPU Worker 和初始为零
副本的 MinerU/Qwen Worker；它不创建 MinIO、数据湖 PVC 或真实 credential Secret。

## 5. 配置与镜像入口

* Dagster 配置移动到 `config/clean_qa`。
* Docker 镜像安装整个 `src` 包，并以
  `clean_qa.mineru_dagster.definitions` 启动 Dagster。
* `pyproject.toml` 从 `src` 自动发现四个 namespace，CLI 指向 `data_lake.*`。
* Ray runtime_env 的 working directory 保持 `/opt/data-pipeline/src`，入口均使用完整包名。

## 6. 验证结果

```text
Shell syntax                         PASS
Python compileall                    PASS
pyproject.toml parse                 PASS
common tests                  3 / 3  PASS
Stage 1 tests                13 / 13 PASS
Stage 2 tests                17 / 17 PASS
helm lint data-lake                  PASS
helm template data-lake      198 行  PASS
helm lint clean-qa default/smoke/prod PASS
clean-qa render validation 13 objects/profile PASS
```

Cleaning/QA 渲染规模为 default 1,246 行、smoke 1,246 行、production 1,126 行。本轮只做
静态验证，没有安装 Chart、修改集群、扩容 Worker 或提交生产 Job。

## 7. 兼容说明

旧 `cleaning_*` Dagster Job 仍显式依赖 `legacy.k12_cleaner`，以避免破坏历史入口。新的
Stage 1 规则只能进入 `clean_qa.k12_clean_qa_pipeline.stage1_clean`。待历史 Job 和保存的
Launchpad 配置全部退役后，可单独删除 legacy namespace。

