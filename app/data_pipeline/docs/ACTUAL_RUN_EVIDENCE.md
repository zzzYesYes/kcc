# 已运行基线与证据边界

本文记录工程化整理时审计到的实际运行状态，不代表安装脚本在任何集群上自动复现了外部硬件环境。

## 实际部署基线

- K3s 裸机集群，存储节点为 `server-00`。
- `k12-lake/minio-k12`：单副本 StatefulSet，MinIO API 9000、Console 9001，`local-path`、RWO、500Gi PVC。
- MinIO requests 为 250m CPU/512Mi，limits 为 2 CPU/4Gi。
- `k12/mineru-dagster`：单 Deployment，webserver 与 daemon 两个容器，Dagster 1.13.13。
- 外部计算基线：Python 3.11.13、Ray 2.48.0、Daft 0.7.19；KubeRay、Ascend NPU worker 和 Qwen/vLLM 独立部署。
- RBAC 只允许 Dagster 对目标命名空间的 ConfigMap、Pod 和 Deployment 做已验证的查询/修补操作。

MinIO server 镜像在审计时解析到的 digest 已固定在 `values.yaml`。内部 pipeline/NPU 镜像仓库地址没有写入仓库，由部署者通过 `image.repository` 和外部 RayCluster 配置提供。

## 实际数据结果

- 采集计划：3,013 个教材版本。
- 成功上传：2,595 个 PDF，约 184.973 GiB。
- 失败并保留记录：418 个版本。
- MinerU 全量口径：2,595 个文档；2,365 个新成功，230 个恢复/跳过，0 个最终失败，共 250,750 页。
- Stage 1 清洗：10 本 smoke 已通过；全量配置和 dry-run 已实现。
- Stage 2 QA：10 本 smoke 已通过；全量配置和 dry-run 已实现。

失败下载可能来自平台 URL/token、网络或源版本不可用，不能仅凭失败数认定为代理问题。`failures.jsonl` 和 meta bucket 是后续重试依据。

## 本轮离线验证边界

本模块整理只进行源码、脚本、Docker context 和 Helm 静态/离线验证，不执行用户集群部署，也不改动当前 MinIO、Ray、Dagster 或其他人的资源。完整 MinerU/QA 复现仍依赖外部 NPU 驱动、模型权重、专用 worker 镜像和有效平台授权。
