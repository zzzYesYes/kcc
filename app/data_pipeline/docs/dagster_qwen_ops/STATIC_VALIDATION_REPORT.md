# Dagster Qwen Ops 静态验证报告

## 验证范围

本报告覆盖独立模块：

```text
src/dagster_qwen_ops/
scripts/dagster_qwen_ops/
helm/dagster-qwen-ops/
docs/dagster_qwen_ops/
```

验证仅针对源码、Dagster Definitions、配置和 Helm 渲染结果。未连接线上
Kubernetes，未扩容 Qwen Worker，未调用生产 Qwen 服务，也未执行任何 Git
提交或推送操作。

## 验证环境

```text
宿主 Python：3.13.5
隔离验证环境：/tmp 下的临时 virtualenv
Dagster：1.13.13
Helm：v3.17.3，安装于 /tmp 下的临时工具目录
运行镜像目标 Python：3.11
```

临时工具仅用于静态验证，不写入系统 Python 环境。

## 已执行检查

| 检查项 | 方法 | 结果 |
| --- | --- | --- |
| Python 语法 | `python -m compileall` | PASS |
| Python 导入 | 导入 `dagster_qwen_ops.definitions` | PASS |
| Definitions 加载 | `dagster definitions validate` | PASS |
| Job 注册 | 查询 Definitions 中的 Job 名称 | PASS |
| Run config | 对四份示例配置调用 Dagster 校验 API | PASS |
| Shell 语法 | 对全部脚本及渲染后的容器启动脚本执行 `bash -n` | PASS |
| YAML 语法 | 解析 Chart values、metadata 和 Job run config | PASS |
| Helm lint | `helm lint helm/dagster-qwen-ops` | PASS |
| 默认模板 | `helm template` 加自定义渲染约束检查 | PASS |
| Smoke 模板 | `helm template -f values-smoke.yaml` 加约束检查 | PASS |
| scale-to-zero | 检查两个 Qwen Deployment 初始副本数 | PASS |
| 最小 RBAC | 检查 Role 资源和 verbs，无 ClusterRole | PASS |
| 模块隔离 | 检查无 Ray、MinIO、MinerU、Cleaning/QA Python 导入 | PASS |
| 可移植性 | 检查无当前服务器 IP、用户目录或真实 Secret | PASS |

## Job 注册结果

Definitions 精确注册以下三个入口：

```text
qwen_vllm_lifecycle_job
qwen_vllm_8npulifecycle_job
qwen_chat_job
```

标准与 8 NPU lifecycle Job 共用同一套 Kubernetes 生命周期实现，通过独立
资源配置选择对应 Deployment 和 Service。Chat Job 直接调用
OpenAI-compatible HTTP API，不依赖 Ray。

## Helm 渲染约束

默认配置和 smoke profile 均验证：

* Qwen Worker Deployment 初始 `replicas: 0`；
* Worker 不自动挂载 Kubernetes ServiceAccount token；
* Dagster 使用独立 ServiceAccount；
* Role 仅允许 Deployment `get/list/watch/patch`、Pod `get/list/watch`、
  Service `get/list`；
* Deployment patch 通过 `resourceNames` 限制到两个受管 Worker；
* 未生成 ClusterRole 或 ClusterRoleBinding；
* 未生成 Ray、MinIO、MinerU、Stage 1 或 Stage 2 资源；
* API Key 仅通过用户预先创建的 Secret 引用，不写入 ConfigMap 或 values。

## 未执行项

本轮按安全边界未执行：

```text
helm install/upgrade
kubectl apply/patch/scale
Qwen Worker 0 -> N -> 0
生产 /health、/v1/models 或 /v1/chat/completions 请求
```

真实生命周期验收需要目标集群具备 Kubernetes、Ascend/CANN、NPU Device
Plugin、兼容的 Qwen/vLLM-Ascend 镜像和模型权重。部署后应按运行手册依次
执行 start、chat、stop，并观察 Worker 副本数从 0 变为 N，再安全恢复为 0。
