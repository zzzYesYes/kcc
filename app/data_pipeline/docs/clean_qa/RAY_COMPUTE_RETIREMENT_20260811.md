# K12 Ray/MinerU/Qwen Compute Retirement

退役日期：2026-08-11。目标是在 `k12` namespace 只保留 Dagster 入口，停止并删除闲置的
Ray、MinerU 和 Qwen 计算平面。MinIO、Secret、Dagster 数据和其他 namespace 不在范围内。

## 保留对象

```text
Deployment/mineru-dagster
Service/mineru-dagster
ServiceAccount/dagster-qwen-controller
Role/dagster-qwen-controller
RoleBinding/dagster-qwen-controller
ConfigMap/kube-root-ca.crt
ServiceAccount/default
```

Dagster Pod 内仍包含 webserver 和 daemon，所以状态应为 `2/2 Running`。退役计算平面后
Dagster UI 和历史 Run 数据仍可查看，但任何需要 Ray、MinerU 或 Qwen 的 Job 都会失败，
直到恢复计算平面或配置新的外部 Ray Dashboard。

## 删除对象

```text
RayCluster/raycluster-k12-smoke
RayCluster/raycluster-k12-autoscale-nojudge

Deployment/mineru-npu-worker-12-13
Deployment/qwen36-35b-a3b-worker-14-15
Deployment/qwen36-35b-a3b-worker-8npu

Service/qwen36-35b-a3b
Service/qwen36-35b-a3b-8npu

ConfigMap/k12-smoke-script
ConfigMap/k12-autoscale-mineru-launcher
ConfigMap/k12-autoscale-qwen-tp1-launcher
ConfigMap/mineru-dual-service-scripts
ConfigMap/mineru-dual-service-scripts-12-13
ConfigMap/qwen36-35b-launcher
ConfigMap/qwen36-35b-launcher-8npu
```

KubeRay 生成的 Head Service、Job summary ConfigMap、Ray ServiceAccount、Role 和
RoleBinding 随 RayCluster 一并清理；如果 OwnerReference 未完成清理，则显式删除。

## 退役前状态

两个 RayCluster 均无运行中 Job 或资源需求：

```text
raycluster-k12-smoke                 idle
raycluster-k12-autoscale-nojudge    idle
MinerU NPU Worker replicas          0
Qwen Worker replicas                0
```

常驻 autoscale CPU Worker 虽然实际利用率很低，但在调度层请求 `16 CPU / 64Gi`。删除后会
释放这部分可调度容量。退役不会删除 S3/MinIO 中的任何输入、结果或 `_SUCCESS.json`。

## 快照位置

服务器原工程目录中的快照：

```text
/home/admin/testpanxy/ray_job_test/mineru_dual_npu_20260717/
  k8s_retired_compute_20260811/
```

内容：

```text
manifests/configmaps/   launcher 与 smoke 配置
manifests/services/     独立 Qwen Service
manifests/deployments/  3 个初始 0 副本的 NPU Deployment
manifests/rbac/         两套 Ray autoscaler 权限
manifests/rayclusters/  smoke 与 autoscale RayCluster
evidence/               退役前清单、Ray 状态、Job 历史和保留对象快照
SHA256SUMS              文件完整性校验
restore_retired_compute.sh
```

Manifest 已移除 UID、resourceVersion、managedFields、status、OwnerReference 和 Service
ClusterIP，不包含 Kubernetes Secret 对象或 Secret 明文。

## 恢复

前置条件：KubeRay Operator、RayCluster CRD、Ascend device plugin、镜像、模型 HostPath、
MinIO Secret 和目标物理 NPU 均可用。

在 server-00 执行：

```bash
cd /home/admin/testpanxy/ray_job_test/mineru_dual_npu_20260717/
cd k8s_retired_compute_20260811
sha256sum --check SHA256SUMS

sudo env KUBECTL_COMMAND='/usr/local/bin/k3s kubectl' \
  ./restore_retired_compute.sh "$PWD"
```

恢复脚本按 RBAC、ConfigMap、Service、Deployment、RayCluster 顺序应用。3 个独立 NPU
Deployment 会保持 `replicas=0`；`raycluster-k12-autoscale-nojudge` 会恢复 Head 和固定 CPU
Worker，NPU Worker Group 保持 0 并等待 Ray resource demand。

恢复后至少验证：

```bash
sudo /usr/local/bin/k3s kubectl -n k12 get raycluster,pod,svc
sudo /usr/local/bin/k3s kubectl -n k12 exec <head-pod> -c ray-head -- ray status
```

历史 Ray Job 列表仅作为证据保存，不会因恢复 RayCluster 自动重新执行。
