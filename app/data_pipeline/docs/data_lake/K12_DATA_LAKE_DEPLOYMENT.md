# K12 Data Lake Deployment

本文档只描述 K12 数据湖基础设施和教材采集入湖。Dagster、Ray、MinerU、Stage 1、
Stage 2、Qwen 和训练数据汇集属于独立的 Cleaning/QA Pipeline，不由 Data Lake Chart
部署。

## 1. 职责边界

```text
教材来源/tchMaterial-parser
        ↓
data_lake.k12_ingest
        ↓
MinIO/S3
  ├─ raw PDF
  ├─ metadata/manifest
  ├─ MinerU output bucket
  ├─ cleaned corpus bucket
  ├─ vector artifacts
  └─ rejected/quarantine
```

Data Lake 负责：

* MinIO StatefulSet、Service 和持久卷；
* bucket 初始化；
* S3 credential Secret 的引用或可选创建；
* 教材下载、校验、上传、失败记录和断点续传；
* 数据保留、备份和 S3 访问合同。

Data Lake 不负责：

* Dagster webserver/daemon；
* KubeRay、Ray Head 或 CPU/NPU Worker；
* MinerU、Qwen 或模型权重；
* Stage 1/Stage 2 处理逻辑；
* NPU 生命周期和扩缩容。

这些内容见 `docs/clean_qa/K12_CLEAN_QA_PIPELINE_DEPLOYMENT.md`。

## 2. 目录

```text
helm/data-lake/                  MinIO 和 bucket Chart
scripts/data_lake/               部署、状态、采集和清理脚本
src/data_lake/k12_ingest/        教材采集与恢复
src/data_lake/data_pipeline_tools/ S3 smoke
src/data_lake/s3_lake_batch/     manifest 与进度工具
docs/data_lake/                  本文档
```

## 3. 数据桶合同

| Bucket | 所有者 | 内容 |
| --- | --- | --- |
| `k12-textbook-raw` | Data Lake | 原始 PDF，只追加或按版本写入 |
| `k12-textbook-meta` | Data Lake | 计划、manifest、状态、失败和汇总 |
| `k12-mineru-output` | Production Pipeline | MinerU Markdown、JSON、图片归档 |
| `k12-cleaned-corpus` | Production Pipeline | Stage 1、Stage 2 和 Training JSONL |
| `k12-vector-artifacts` | 下游系统 | 切分、索引和向量制品 |
| `k12-rejected` | 共享 | 校验失败、隔离和待重跑对象 |

Chart 只创建 bucket，不解释后四个 bucket 内部的数据语义。生产管线通过外部 S3
endpoint、bucket、prefix 和 Secret 与数据湖连接。

## 4. 前置条件

* Kubernetes 1.25+ 和 Helm；
* 能提供 500Gi RWO PVC 的 StorageClass；
* MinIO 节点具备足够磁盘和稳定挂载；
* 部署端可拉取固定 digest 的 MinIO 镜像和 MinIO Client 镜像；
* 采集机具有 Python 3.11、网络访问能力和合法平台 token；
* 生产环境必须有对象级备份、replication 或其他灾备机制。

单副本 `local-path` MinIO 不提供节点级冗余。重建 Pod 不等于恢复数据。

## 5. Secret

默认 Secret 名为 `minio-k12-root`，包含：

```text
MINIO_ROOT_USER
MINIO_ROOT_PASSWORD
```

推荐由 External Secrets 或平台预置。也可以在不提交凭证的本地环境执行：

```bash
cd app/data_pipeline
export AWS_ACCESS_KEY_ID='<access-key>'
export AWS_SECRET_ACCESS_KEY='<secret-key>'
./scripts/data_lake/init-secrets.sh
```

真实密码不得写入 `values.yaml`、命令历史、日志或 Git。

## 6. Helm Chart

Chart 路径：

```text
helm/data-lake
```

静态验证：

```bash
./scripts/data_lake/validate.sh
```

部署：

```bash
./scripts/data_lake/deploy.sh
./scripts/data_lake/status.sh
```

等价 Helm 命令：

```bash
helm upgrade --install k12-data-lake helm/data-lake \
  --namespace k12-lake --create-namespace \
  --set-string credentials.existingSecret=minio-k12-root \
  --set-string minio.persistence.storageClass=local-path \
  --set-string minio.persistence.size=500Gi
```

Chart 生成的对象只有 Namespace（可选）、Secret（可选）、MinIO StatefulSet、Headless/
访问 Service 和 bucket-init Job。不会生成 Dagster Deployment 或 RBAC。

## 7. 环境参数

脚本从顶层 `.env` 读取可选参数：

```text
HELM_RELEASE=k12-data-lake
DATA_LAKE_NAMESPACE=k12-lake
MINIO_EXISTING_SECRET=minio-k12-root
MINIO_STORAGE_CLASS=local-path
MINIO_STORAGE_SIZE=500Gi
MINIO_API_NODE_PORT=30900
MINIO_CONSOLE_NODE_PORT=31901
S3_ENDPOINT_URL=http://minio-k12.k12-lake.svc.cluster.local:9000
```

自定义 Helm values 可通过 `HELM_VALUES=/path/to/private-values.yaml` 提供。

## 8. S3 smoke

在能访问 endpoint 的主机执行：

```bash
export AWS_ACCESS_KEY_ID='<access-key>'
export AWS_SECRET_ACCESS_KEY='<secret-key>'
export S3_ENDPOINT_URL='http://127.0.0.1:30900'
./scripts/data_lake/smoke.sh
```

Smoke 会列出 bucket，并执行唯一对象的写入、读回和删除，不依赖 Dagster Pod。

## 9. 教材采集

采集运行在授权的图形化机器或采集节点，不运行在 MinIO Pod：

```bash
export K12_ACCESS_TOKEN='<token>'
export K12_BATCH_ID='batch-YYYYMMDD'
export AWS_ACCESS_KEY_ID='<access-key>'
export AWS_SECRET_ACCESS_KEY='<secret-key>'
export S3_ENDPOINT_URL='http://server-00:30900'

./scripts/data_lake/run-ingest.sh --plan-only
./scripts/data_lake/run-ingest.sh --limit 3 --keep-local
./scripts/data_lake/run-ingest.sh --scope tagged
```

采集键前缀为 `source=tchMaterial-parser/batch_id=<batch>`。每个批次保留：

```text
resource_plan.jsonl
status.jsonl
manifest.jsonl
failures.jsonl
ingest_summary.json
```

同一 `K12_BATCH_ID` 重跑时，成功对象按状态和 hash 跳过，失败对象重新尝试。失败记录
不能被无证据地改写成成功。

## 10. 网络与代理

平台 CDN 和 MinIO 可能使用不同网络路径：

* 平台下载是否走代理由采集环境决定；
* 内网 S3 endpoint 应加入 `NO_PROXY`；
* Console 端口是 9001，S3 API 是 9000；
* NodePort 只用于集群外访问，Pod 内优先使用 ClusterIP DNS；
* 上传慢时分别检查代理、multipart、重试、DNS 和远端校验，不能仅凭耗时推断代理问题。

## 11. 验收

```bash
kubectl -n k12-lake get sts,pod,svc,pvc,job
kubectl -n k12-lake exec minio-k12-0 -- df -h /data
kubectl -n k12-lake get events --sort-by=.lastTimestamp
```

最小验收必须同时确认：

* MinIO readiness 正常；
* PVC Bound 且容量正确；
* 六个 bucket 存在；
* S3 smoke 写读删成功；
* 三本采集 smoke 产生有效 PDF、raw object、meta manifest 和汇总；
* 失败记录保留且可以按相同 batch 恢复。

## 12. 常见故障

* `secret not found`：确认 Secret 位于 `k12-lake` namespace 且 key 名正确。
* PVC Pending：检查 StorageClass、节点空间、provisioner 和 PVC event。
* Console 可用但 S3 失败：检查是否错误连接 9001。
* bucket-init 失败：检查 Secret、endpoint、DNS 和建桶权限。
* 下载 401/403：刷新合法平台 token；不要把认证失败当成代理故障。
* 上传超时：确认 S3 endpoint 在 `NO_PROXY`，检查 multipart 和网络重试。
* PDF 损坏：保留失败对象和 hash，重新下载，不手工覆盖审计记录。

## 13. 卸载和数据保留

默认卸载不会删除 PVC：

```bash
./scripts/data_lake/cleanup.sh
```

永久删除数据需要双重确认：

```bash
DELETE_DATA=true \
CONFIRM_DELETE_DATA=k12-data-lake \
./scripts/data_lake/cleanup.sh
```

删除 PVC 前必须确认对象级备份。Data Pipeline Chart 的卸载脚本不能用于删除数据湖。

## 14. 与 Cleaning/QA Pipeline 的接口

Cleaning/QA Pipeline 只需要：

```text
S3 endpoint
region/path-style
credential Secret name/key
input/output bucket
input/output prefix
NO_PROXY
```

未来 umbrella Chart 可以把这些值连接给两个子 Chart，但不能让 Data Lake Chart 管理
Dagster、Ray 或 NPU Worker，也不能让 Cleaning/QA Chart拥有 MinIO PVC 生命周期。
