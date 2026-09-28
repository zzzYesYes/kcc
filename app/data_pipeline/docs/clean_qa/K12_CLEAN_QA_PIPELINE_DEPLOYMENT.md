# K12 Cleaning/QA Pipeline Deployment

本文档描述 `app/data_pipeline/src/clean_qa/k12_clean_qa_pipeline` 在 Kubernetes、KubeRay 和
Ascend 910C 环境中的部署与运行方式。它只负责数据湖之上的数据生产任务；MinIO、
bucket 初始化和对象存储持久化属于外部数据湖模块，不由本 Chart 创建。

## 1. 生产基线

本模块整理的是已经完成全量生产的系统，不是新的算法实现。当前有效基线为：

| 项目 | 有效值 |
| --- | --- |
| MinerU | 3.4 Hybrid，`hybrid-http-client`，`effort=high`，图片分析开启 |
| MinerU 全量 | 2,595 本，250,750 页，0 失败 |
| Stage 1 | `stage1-v1.0.2`，2,595/2,595 |
| Stage 2 | `stage2-v1.1.0`，8-NPU 全量完成 |
| Prompt | `k12-qa-zh-v1.2` |
| Qwen | `qwen3.6-35b-a3b` W8A8 |
| Training QA | 67,850 条 |
| Training MCQ | 64,606 条 |

旧报告中的 `stage2-v1.0.2` 和 `k12-qa-zh-v1.1` 只属于历史运行，不得作为新任务默认值。

## 2. 架构与职责

```text
Dagster (常驻)
  ├─ 资产、Job、Run 配置、生命周期与可视化
  └─ Ray Job Submission
       ↓
Ray Head (常驻，num-cpus=0)
  ├─ Daft/S3 manifest、任务分发、进度聚合
  ├─ CPU Worker (常驻，Stage 1 与 Stage 2 协调)
  ├─ MinerU Worker Group (按需 0→1→0)
  │    ├─ MinerU Serve A / 单卡
  │    └─ MinerU Serve B / 单卡
  └─ Qwen Worker Group (按需 0→N→0)
       ├─ vLLM-Ascend TP1 endpoint
       └─ Ray API Coordinator，通过 HTTP 调用 endpoint
             ↓
External MinIO/S3
  ├─ PDF 与 MinerU 结构化结果
  ├─ Stage 1 blocks.jsonl/clean.md
  ├─ Stage 2 candidates/verified/rejected
  └─ 严格 id/text Training JSONL
```

职责边界：

* Dagster 不下载或解析大文件，只编排、提交、记录和控制生命周期。
* Daft 在 Head/CPU Worker 上扫描对象和生成 manifest，不加载 Qwen 模型。
* Ray Head 设置 `num-cpus=0`，不承接普通文档任务。
* Stage 1 是确定性 CPU 任务，不能申请 NPU。
* Stage 2 文档 Worker 通过 HTTP 调用 Qwen，不能在 Ray Actor 内重新加载模型。
* MinerU/Qwen Pod 直接读写 S3，大对象不经过 Head。
* MinIO 是外部依赖，本 Chart 不创建 MinIO、bucket、PVC 或 root credential。

## 3. 当前运行拓扑

Chart 支持同时声明两种 NPU 控制对象，但所有 NPU 对象默认均为零副本：

1. KubeRay Worker Group：Ray 资源需求驱动扩缩容，适合
   `k12_e2e_autoscale_nojudge_job` 等自动伸缩链路。
2. 独立 Deployment：Dagster lifecycle Job 或人工脚本显式 scale，适合
   `qwen_vllm_lifecycle_job`、固定 smoke 和运维验证。

同一批物理 NPU 不允许同时由两种控制路径启动。启动前必须确认另一条路径为 0。

常驻资源：Dagster webserver/daemon、Ray Head、一个 CPU Worker。按需资源：MinerU
NPU Worker、Qwen NPU Worker。KubeRay `idleTimeoutSeconds` 默认 60 秒。

## 4. 数据依赖与 S3 合同

默认生产前缀来自已验证运行，可通过 values 覆盖：

| 阶段 | bucket/prefix |
| --- | --- |
| 原始 PDF | `k12-textbook-raw/source=tchMaterial-parser` |
| MinerU | `k12-mineru-output/full-output/mineru34-hybrid-a3-full-20260722T104600Z` |
| Stage 1 | `k12-cleaned-corpus/stage1/full/stage1-v1.0.2` |
| Stage 2 | `k12-cleaned-corpus/stage2/full/stage2-v1.1.0-8npu` |
| clean.md 汇集 | `k12-cleaned-corpus/stage1/clean-md-collection-v1` |
| Training 汇集 | `k12-cleaned-corpus/stage2/training-jsonl-collection-v1` |

Stage 1 单文档合同：

```text
<prefix>/<document_id>/
├── blocks.jsonl             # 规范主数据
├── clean.md                 # 由 blocks.jsonl 渲染的投影
├── exercises.jsonl
├── image_manifest.jsonl
├── quarantine.jsonl
├── cleaning_report.json
└── _SUCCESS.json            # 最后原子写入
```

Stage 2 只能读取 Stage 1 结构化产物，禁止从 `clean.md` 重新切块。生成、程序校验和
Judge 分开，`schema-valid-unjudged` 不能标记为 `verified`。Training 汇集器仅接受
`quality_status=verified` 且 `judge.accept=true` 的记录。

## 5. 镜像与运行时

默认 values 记录了现有环境中的实际基础镜像：

* Ray/CPU：`ray-data:py3.11.13-ray2.48.0-daft0.7.19-dagster1.13.13-s3-compress-20260716`
* MinerU：`mineru-vllm-a3:official-v0.11.0-20260715-ray248-lake-20260716`
* Qwen：`vllm-ascend-worker:v0.21.0rc1-a3-20260713-s3`

Dagster 默认镜像 `data-pipeline:dev` 需要由当前目录的 `Dockerfile` 构建并推送到目标
集群可访问的 registry，然后在环境 values 中替换。不要依赖历史
`/home/admin/testpanxy/...` HostPath 注入源码。

Dagster run storage 默认 `emptyDir` 只适合模板渲染和短 smoke。生产环境必须将
`dagster.storage.type` 设置为 `pvc` 或 `hostPath` 并提供已有持久目录；Chart 不自动创建
存储，以免与集群的 StorageClass/备份策略耦合。

镜像必须保持 Python 3.11、Ray 2.48.0、Daft 0.7.19、Dagster 1.13.13 的接口兼容。

## 6. Kubernetes/NPU 前置条件

* Kubernetes 1.25+；已安装 KubeRay Operator 和 `rayclusters.ray.io` CRD。
* Ascend device plugin 暴露 `huawei.com/Ascend910`。
* A3 节点可挂载 CANN/driver、DCMI 和 `npu-smi`。
* NPU 节点 label、toleration 和设备注解与实际调度器一致。
* Qwen 模型已通过 HostPath 或 PVC 提供。
* 外部 S3 endpoint 可从 Dagster、Head、CPU/NPU Worker 访问。
* endpoint 必须加入 `NO_PROXY`，避免上传和内部 HTTP 经过代理。

检查：

```bash
kubectl get crd rayclusters.ray.io
kubectl get nodes --show-labels
kubectl -n k12 get resourcequota,limitrange
kubectl -n k12 get secret k12-pipeline-s3
```

## 7. Secret 与外部 MinIO

真实凭证不能写入 values。先创建已有 Secret：

```bash
kubectl -n k12 create secret generic k12-pipeline-s3 \
  --from-literal=access-key='<ACCESS_KEY>' \
  --from-literal=secret-key='<SECRET_KEY>'
```

环境 values 只引用 Secret 名称和 key。使用非 MinIO S3 时也应确认 path-style、region、
multipart 和 endpoint 行为与 boto3 配置一致。

## 8. Helm Chart

Chart 位于：

```text
app/data_pipeline/helm/k12-clean-qa-pipeline
```

静态验证：

```bash
cd app/data_pipeline
./scripts/clean_qa/helm_lint.sh
PROFILE=smoke ./scripts/clean_qa/helm_render.sh /tmp/k12-smoke.yaml
PROFILE=production ./scripts/clean_qa/helm_render.sh /tmp/k12-production.yaml
```

安装前创建一个不入 Git 的环境 values，至少覆盖 S3、镜像、节点和模型路径：

```bash
cp helm/k12-clean-qa-pipeline/values-example.yaml /tmp/k12-site.yaml
PROFILE=/tmp/k12-site.yaml S3_SECRET_NAME=k12-pipeline-s3 \
  ./scripts/clean_qa/install_pipeline.sh
```

升级：

```bash
PROFILE=/tmp/k12-site.yaml ./scripts/clean_qa/upgrade_pipeline.sh
```

## 9. Dagster 入口

默认 Service 为 NodePort `30080`：

```bash
kubectl -n k12 get svc -l app.kubernetes.io/component=dagster
```

Dagster 使用最小权限 ServiceAccount。Role 只允许读取 Pod/Service/Endpoint/日志，修改
本 release 的 MinerU/Qwen Deployment 与 launcher ConfigMap，以及读取/修改本 release
的 RayCluster。Chart 不创建 ClusterRole，也不授予 cluster-admin。

## 10. Ray Dashboard 与 CPU Worker

```bash
kubectl -n k12 get raycluster
kubectl -n k12 get pods -l app.kubernetes.io/component=ray-head
kubectl -n k12 port-forward svc/<raycluster>-head-svc 8265:8265
```

Head `num-cpus=0`。默认 CPU Worker 申请 16 CPU/64Gi，limit 18 CPU/72Gi，注册
`CPU_DATA=1`。Stage 1 和 Stage 2 协调均在 CPU 资源上运行。

## 11. MinerU Worker 生命周期

生产参数：双 Serve、每个单卡，`inference_slots=4`、`document_inflight=5`、
`window_prefetch=1`、`max_num_seqs=288`、`max_num_batched_tokens=2560`。Pod 申请
64 CPU/256Gi/2 NPU。启动脚本先用 `npu-smi info -m` 校验物理到逻辑设备映射，再启动
Serve，避免假设容器内设备编号等于物理 ID。

KubeRay 模式由 Ray resource demand 拉起。独立模式可人工控制：

```bash
./scripts/clean_qa/scale_mineru.sh 1
./scripts/clean_qa/scale_mineru.sh 0
```

状态必须按 `requested → ready → busy → draining → released` 处理；缩容前停止派发新文档，
等待活动文档完成并写入 `_SUCCESS.json`。

## 12. Qwen Worker 生命周期与拓扑

Smoke profile 使用一个 Ray Worker Group 内的两个独立 TP1 endpoint。Production profile
使用零副本独立 Deployment，包含四个 endpoint；每个 endpoint 为 `TP=1, DP=2`，覆盖
八个逻辑设备，不使用 TP2。生命周期代码读取 `QWEN_ENDPOINT_SPECS`、
`QWEN_DATA_PARALLEL_SIZE` 和 `QWEN_TENSOR_PARALLEL_SIZE`，不会在启动时覆写回 TP2。

```bash
./scripts/clean_qa/scale_qwen.sh 1 all
./scripts/clean_qa/scale_qwen.sh 0 all
```

独立 Deployment 由 `qwen_vllm_lifecycle_job` 控制；Ray Worker Group 由任务资源需求控制。
两种路径不能同时占用相同卡。服务异常时停止新请求、排空连接池，再缩容；不得删除或
重启健康的另一组 Serve。

## 13. Stage 1 执行

Stage 1 是纯 CPU、确定性处理。`blocks.jsonl` 为主数据，保留稳定 `block_id`、
quarantine、source SHA/ETag 和公式修复追踪。执行入口：

```bash
kubectl -n k12 exec deployment/<dagster> -c webserver -- \
  dagster job execute -m clean_qa.mineru_dagster.definitions \
  -j cleanjopbstage1_10 \
  -c /opt/data-pipeline/src/clean_qa/k12_clean_qa_pipeline/configs/cleanjopbstage1_10.yaml
```

全量 Job `cleanjopbstage1_ful` 支持 resume；运行前必须使用新的输出前缀或确认源 SHA、
版本和产物 SHA 与成功标记一致。

## 14. Stage 2 执行

Smoke 的已验证基线为：

```text
document_inflight=3
block_inflight=8
generation_max_inflight=8
judge_max_inflight=4
http_pool_size=16
microbatch_size=2
judge_batch_size=8
document_max_units=48
```

8-NPU 全量基线为：

```text
document_inflight=8
block_inflight=4
generation_max_inflight=8
judge_max_inflight=8
http_pool_size=16
microbatch_size=2
judge_batch_size=8
```

执行：

```bash
kubectl -n k12 exec deployment/<dagster> -c webserver -- \
  dagster job execute -m clean_qa.mineru_dagster.definitions \
  -j qajobstage2_10 \
  -c /opt/data-pipeline/src/clean_qa/k12_clean_qa_pipeline/configs/qajobstage2_10.yaml
```

必须使用 `stage2-v1.1.0`、`k12-qa-zh-v1.2` 和 `qwen3.6-35b-a3b`。no-Judge
链路只用于吞吐实验，其结果不能进入 verified 或训练汇集。

## 15. clean.md 与 Training JSONL 汇集

clean.md 汇集器只复制 Stage 1 成功文档的投影，并保持 `document_id` 对应关系。
Training 汇集器输出严格两个顶层字段：`id` 和字符串类型 `text`；`text` 内是序列化
JSON。QA 保留 question/analysis/evidence/answer，MCQ 额外保留 type/options。

汇集前必须验证所有输入来自完整质量门。禁止把 candidates、rejected 或 unjudged 文件
混入 training prefix。

## 16. Resume、原子发布与 fail-closed

每个阶段以 `_RUN_MANIFEST.json`、`_PROGRESS.json`、`_SUMMARY.json` 和
`_FAILED.jsonl` 记录批次状态。单文档 `_SUCCESS.json` 最后原子写入。

仅当以下全部匹配时允许跳过：源 SHA/ETag、处理版本、Prompt/模型版本（Stage 2）、
产物 SHA、成功标记。临时文件应写入独立 key，校验后 copy/rename 到最终 key；失败时不写
成功标记。

以下情况 fail-closed：输出前缀可能覆盖源数据、结构化产物不可解析、数学错误或多正确
答案进入 verified、Qwen 持续不可用且重试耗尽。单文档/单块失败只隔离该项。

## 17. 从空闲集群做 10 本 smoke

1. 确认外部 S3、Secret、KubeRay CRD、NPU device plugin 和模型卷。
2. `PROFILE=smoke ./scripts/clean_qa/helm_lint.sh`。
3. 安装 Chart，确认 Head/CPU Worker/Dagster Ready，NPU 副本均为 0。
4. 确认所选物理 NPU 空闲，且没有另一条 lifecycle/Worker Group 占用。
5. 提交 MinerU 10 本 smoke，观察 MinerU Group 0→1、双 Serve health 和 Ray Actor。
6. 验证 10/10 MinerU `_SUCCESS.json`，再运行 `cleanjopbstage1_10`。
7. 验证 Stage 1 10/10、哈希稳定和 blocks/clean 投影合同。
8. 启动 Qwen lifecycle 或提交 Stage 2 资源需求，检查所有 endpoint `/health`。
9. 运行 `qajobstage2_10`，确认 Judge 开启、verified 与 rejected 分离。
10. 验证 10/10 `_SUCCESS.json`、数学与 schema 质量门，再排空并缩容 Qwen/MinerU。

已有生产 MinerU 结果时可执行：

```bash
./scripts/clean_qa/smoke_pipeline.sh
```

若还要先执行仓库中的 MinerU smoke 配置：

```bash
RUN_MINERU_SMOKE=1 ./scripts/clean_qa/smoke_pipeline.sh
```

## 18. 从已有生产结果恢复

1. 固定原 batch 的 bucket/prefix，不创建新的覆盖性前缀。
2. 读取 `_RUN_MANIFEST.json` 和 `_SUMMARY.json`，确认版本/Prompt/模型一致。
3. 以单文档 `_SUCCESS.json` 和产物 SHA 构建完成集合，不以目录存在判断完成。
4. 对缺少成功标记或校验失败的文档重新入队；不要删除已成功对象。
5. 使用 `resume=true` 和相同输出前缀提交。
6. 运行结束后重新生成汇总，核对总数、失败数、verified 数和 collection 数。

## 19. 观测与诊断

```bash
./scripts/clean_qa/status_pipeline.sh
kubectl -n k12 get events --sort-by=.lastTimestamp
kubectl -n k12 logs <pod> -c ray-worker --tail=200
kubectl -n k12 logs <pod> -c vllm-ascend --tail=200
kubectl -n k12 describe pod <pod>
```

常见故障：

* Worker 不扩容：检查 Ray custom resource 名称、maxReplicas、资源请求、NPU 注解和配额。
* Pod Pending：检查实际空闲物理卡、nodeSelector、Ascend plugin、CPU/内存总量。
* Qwen health 超时：检查模型挂载、CANN/driver、endpoint specs、HBM 和 vLLM 日志。
* 上传慢：记录实际进程代理环境，确保 S3 endpoint 在 `NO_PROXY`，检查 multipart/retry。
* Ray Actor 无资源：对比 `ray status` 中 `MINERU_NPU`、`qa_vllm_npu` 和代码声明。
* resume 重跑全部：检查 source SHA/ETag、版本、Prompt 和 `_SUCCESS.json` 产物哈希。
* Training 条目异常：确认汇集器只读取 verified 且 Judge accept 的文件。

## 20. 缩容、清理与卸载

先停止新任务并等待 active documents 为 0，再执行：

```bash
./scripts/clean_qa/scale_mineru.sh 0
./scripts/clean_qa/scale_qwen.sh 0 all
CONFIRM_UNINSTALL=k12-pipeline ./scripts/clean_qa/uninstall_pipeline.sh
```

卸载 Chart 不删除外部 S3 数据、credential Secret、模型 PVC/HostPath、KubeRay CRD 或
Ascend 驱动。不要用 `kubectl delete namespace` 代替模块卸载。

## 21. Profile 差异

`values-smoke.yaml` 使用两设备、两 TP1 Serve 和 10 本任务；Stage 2 使用 3/8/8/4
保守并发。`values-production.yaml` 表达四个 `TP1/DP2` endpoint 的 8-NPU 独立
Deployment 和 8/4/8/8 全量并发。
两者共享模板，不复制 Kubernetes 对象。

历史静态 `qwen36-35b-a3b-8npu` YAML 使用四个 TP2 endpoint，而当前自动伸缩实现使用
双 TP1 Serve/Pod。本 Chart 按当前要求选择 TP1，并将 `tensorParallelSize`、endpoint 和
device specs 暴露为 values。由于本轮只做静态验证，目标环境必须在首次生产运行前完成
设备映射与 endpoint health smoke，不能把旧 TP2 YAML 的运行证据自动等同于新 profile。

## 22. 外部依赖和限制

当前 Chart 无法自动创建或验证：外部 MinIO、bucket policy、真实 NPU 空闲情况、CANN 与
镜像兼容性、私有 registry 登录、模型权重、企业代理以及 KubeRay Operator。这些必须由
环境提供。根目录后续 umbrella Chart 只需连接本 Chart 的 S3 Secret、bucket/prefix、
registry、模型卷、KubeRay/Ascend 能力和 Dagster/Ray Service 输出，不应重新模板化内部
Stage 1/2 数据语义。
