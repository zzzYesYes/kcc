# K12 Cleaning/QA Pipeline Engineering Audit

审计日期：2026-08-11。审计范围仅限 `app/data_pipeline` 及其对应的现行
`clean_qa.k12_clean_qa_pipeline`、Kubernetes、KubeRay 和 Dagster 资源；未执行部署、扩容或生产 Job。

## 1. 真实环境观察

审计时 `k12` namespace 中 Dagster、Ray Head 和 CPU Worker 常驻；以下独立 NPU
Deployment 均为 0 副本：

* `mineru-npu-worker-12-13`
* `qwen36-35b-a3b-worker-14-15`
* `qwen36-35b-a3b-worker-8npu`

现行 autoscale RayCluster 包含固定 CPU Worker、零副本 MinerU Group 和多个零副本 Qwen
Group。Ray Head 不执行普通 CPU task。

## 2. 已有资源

找到并对照了：

* 原 `app/data_pipeline/helm/data-pipeline` 曾混合 MinIO 与 Dagster；结构整理后已拆为纯
  `helm/data-lake` 和独立 `helm/k12-clean-qa-pipeline`。
* Dagster Deployment/Service/ServiceAccount/Role/RoleBinding。
* `raycluster-k12-autoscale-nojudge` 的生成器与有效 Worker Group 配置。
* MinerU 双设备独立 Deployment 和物理/逻辑设备发现脚本。
* Qwen 双设备与 8-NPU 独立 Deployment/Service/launcher。
* Stage 1/Stage 2、collector、autoscale、lifecycle 和进度脚本。
* 三个当前生产 summary 及两个 collection summary。

历史绝对路径 `/home/admin/testpanxy/ray_job_test/...` 是部署环境参数，不是处理合同；新
Chart 使用镜像内代码、PVC 或 values，不再固化该路径。

## 3. 版本冲突处理

代码和生产结果确认有效版本为 Stage 1 `stage1-v1.0.2`、Stage 2
`stage2-v1.1.0`、Prompt `k12-qa-zh-v1.2`、模型 `qwen3.6-35b-a3b`。旧报告值未进入
Chart 默认值。

现行静态 8-NPU launcher 使用 TP2；autoscale launcher 使用 TP1。本轮按需求将 Helm
production profile 定义为 TP1，并修改 Dagster lifecycle renderer，使其从 endpoint specs
和 `QWEN_DATA_PARALLEL_SIZE`/`QWEN_TENSOR_PARALLEL_SIZE` 生成脚本，不再硬编码
TP2。此变更已静态检查，未获得新
8-NPU profile 的真实集群运行证据。

## 4. Helm 迁移结果

新增模块 Chart `helm/k12-clean-qa-pipeline`，覆盖：

* Dagster webserver/daemon Deployment、NodePort Service、ConfigMap。
* 最小权限 ServiceAccount、Role 和 RoleBinding。
* KubeRay RayCluster、Head、固定 CPU Worker。
* 零副本 MinerU Worker Group 和零副本独立 Deployment/Service。
* 零副本 Qwen Worker Groups 和零副本独立 Deployment/Service。
* MinerU 设备映射/双 Serve launcher、Qwen 多 endpoint launcher。
* 外部 S3 数据合同、Stage 1/Stage 2/collector 配置。
* HostPath/PVC 模型挂载和 Ascend host mount。

没有创建 MinIO、bucket-init、数据湖 PVC、S3 credential Secret 或 KubeRay Operator。

## 5. 参数化清单

已迁入 values 的硬编码包括 namespace、所有镜像/tag/pull policy、S3 endpoint、bucket、
prefix、Secret/key、代理、Dagster NodePort、Ray 版本/资源、CPU Worker、NPU resource
name、物理设备、节点选择、affinity、toleration、driver mount、模型 HostPath/PVC、
MinerU/Qwen replicas、endpoint/port/CPU set、TP size、vLLM 调度参数、Stage 1/Stage 2
版本与全部主要并发参数。

生产密码、Access Key 和 Secret Key 未写入 values 或脚本。

## 6. Scale-to-zero 与权限

MinerU/Qwen 独立 Deployment 和 Ray Worker Group 初始 replicas/minReplicas 均为 0。
独立对象由 lifecycle Job 或 scale 脚本控制；Worker Group 由 Ray resource demand 拉起并按
idle timeout 回收。两种控制路径同时存在但必须互斥使用相同物理设备。

Dagster Role 是 namespace 范围的最小权限：读取 Pod/Service/Endpoint/log，修改指定
launcher ConfigMap 和指定独立 Deployment/scale，读取并修改本 release 的 RayCluster。
没有 ClusterRole 或 cluster-admin。

## 7. 外部 S3 接入

所有组件通过 `externalS3.endpoint` 和一个已有 Secret 连接 MinIO/S3。Chart 不拥有数据湖
生命周期。原始 PDF、MinerU、Stage 1、Stage 2 和 collection prefix 独立配置，并保持源
数据只读、单文档 `_SUCCESS.json` 最后写入和 fail-closed 语义。

## 8. Profile

Smoke：两设备、两 TP1 Serve、10 本，Stage 2 并发 3/8/8/4。

Production：零副本 8-NPU 独立 Deployment、四个 `TP1/DP2` endpoint、全量 prefix，
Stage 2 并发 8/4/8/8。Production profile 的 endpoint/物理设备映射需要在目标集群
smoke 中确认，因为旧静态 8-NPU YAML 是 TP2，不能作为 TP1 部署证据。

## 9. 静态验证

使用 server-00 的 Helm v4.1.0 完成：

```text
helm lint default       PASS
helm template default   PASS
helm template smoke     PASS
helm template production PASS
```

最终验证的三套 profile 均渲染 13 个 Kubernetes 对象：default 1,246 行、smoke
1,246 行、production 1,126 行。自定义结构检查同时验证了 selector/label、Service
targetPort、volume mount、NPU request/limit、RBAC 无 wildcard/ClusterRole，以及独立
NPU Deployment 和 Ray NPU Worker Group 的零副本状态。没有执行 `helm install/upgrade`，
没有修改集群对象，没有启动 NPU Worker 或生产任务。

## 10. 新增和修改文件

新增：

* `helm/k12-clean-qa-pipeline/Chart.yaml`
* `helm/k12-clean-qa-pipeline/values*.yaml`
* `helm/k12-clean-qa-pipeline/templates/*`
* `scripts/clean_qa/pipeline-helm-common.sh`
* `scripts/clean_qa/helm_lint.sh`、`helm_render.sh`
* `scripts/clean_qa/install_pipeline.sh`、`upgrade_pipeline.sh`、`status_pipeline.sh`
* `scripts/clean_qa/scale_mineru.sh`、`scale_qwen.sh`、`smoke_pipeline.sh`、`uninstall_pipeline.sh`
* 本部署文档和审计报告。

修改：

* `src/clean_qa/mineru_dagster/resources/qwen_kubernetes_resource.py`：生命周期 launcher 改为显式
  endpoint specs 和可配置 TP，修正 Pod label 查询。
* `src/clean_qa/mineru_dagster/definitions.py`：统一使用移动后的 `clean_qa.*` Definitions 和
  Pipeline Job 导入。
* `src/clean_qa/mineru_dagster/ops/*.py`：Ray Job 入口改为可安装包的 `python -m` 形式。
* `src/runtime/mineru_pipeline/official_concurrent_runner.py`：移除
  `/tmp/mineru_flash30` 源码依赖，改为调用包内 window runner。
* `src/runtime/mineru34_hybrid_lake/hybrid_lake_ray_job.py`：Hybrid 子进程改为包模块入口，
  保证相对导入在 Ray runtime_env 中有效。
* `Dockerfile`、`pyproject.toml` 和 Dagster run config：同步新的 Python namespace 和
  `config/clean_qa` 路径。
* `README.md`：区分外部数据湖与数据生产 Chart。

本轮同时完成源码边界整理：

```text
src/data_lake/  数据采集、S3 smoke、manifest
src/clean_qa/   当前 Stage 1/2 与 Dagster
src/runtime/    MinerU 3.4、window/concurrency、双 NPU 运行适配
src/legacy/     旧 cleaning_* Job 的兼容 cleaner
```

旧 cleaner 只能通过显式 `legacy.k12_cleaner` 导入；当前 Stage 1 固定为
`clean_qa.k12_clean_qa_pipeline.stage1_clean`。

## 11. 尚不能自动复现的依赖

外部 MinIO 和 bucket policy、私有 registry、KubeRay Operator、Ascend device plugin、
CANN/driver、物理卡分配、Qwen 权重、代理和 DNS 均由环境提供。Chart 不能证明目标节点
有足够的 CPU/内存/HBM，也不能在不运行 Pod 的情况下证明 CANN 与镜像兼容。

## 12. Umbrella Chart 接口

后续根目录 umbrella Chart 需要传入：S3 endpoint/Secret/buckets/prefix、pipeline 镜像、
Ray/MinerU/Qwen 镜像、模型 PVC/HostPath、节点/NPU 调度参数、Dagster Service 暴露方式，
并依赖 KubeRay 和 Ascend capability。它不应复制本 Chart 的 Worker Group、RBAC 或处理
版本参数，也不应让 data-lake 子 Chart 反向管理 pipeline 的 NPU 生命周期。

## 13. 测试限制

Shell 语法、Python 全量编译、`pyproject.toml` 解析、Helm lint/template 和渲染结构检查
均已通过。使用标准库 `unittest` 实际执行了 33 个测试：common 合同测试 3 个、Stage 1
测试 13 个、Stage 2 测试 17 个，全部通过。当前开发机未安装完整 Dagster/Ray/Ascend
运行依赖，因此本轮没有启动 Dagster webserver、提交 Ray Job、创建 Pod 或执行 NPU smoke；
Helm 验证仅在 server-00 的临时目录执行静态渲染，没有修改集群。
