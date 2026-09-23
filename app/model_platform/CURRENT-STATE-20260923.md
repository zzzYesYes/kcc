# 模型集成平台当前状态（2026-09-23）

> 本文是对生产 `server-00` 的定向只读实时核验快照（2026-09-23 08:02 UTC 采集），
> 作为“当前事实”的最新入口，更新并优先于 `CURRENT-STATE-20260917.md` 的当前事实表述；
> 2026-09-18 至 09-20 的发布证据（K12 r9、Direct Operations、cache reaper）继续有效。
> 本文不包含密码、Token、Secret data、kubeconfig 或 Docker config 内容。

## 0. 结论

三条主线保持可用：

1. K12 数据管线：CPU Stage 1 平台主线与 NPU 全链路均已有生产验收；当前 Dagster 与
   CPU Ray 在线，MinerU/Qwen 全部为 0 副本。
2. 推理控制面：Direct Operations 生产验收（真实 Start → `/v1/models` 与
   `/v1/chat/completions` → 受控 Stop）已完成并清理；`qwen38-27b` 保持 Stopped 基线，
   此前的事件堆积告警已恢复为 `Responsive=True`。
3. 停止态自动化：cache reaper 每分钟运行，最近调度 Job 均 Completed；Retain 缓存 PV
   保持 `Released`，数据未删除。

当前最重要异常（外部阻塞）：`a3-server-00` 自 `2026-09-23T01:28:27Z` 起
`NotReady`（`NodeStatusUnknown`）；且在此之前 A3 exporter 报告全部 16 个设备 ID
均存在无法归属 Kubernetes 的进程（2026-09-20 记录）。因此 NPU 启动路径当前不可用，
Running Window 必须保持关闭。

## 1. 2026-09-23 生产核验

| 项目 | 观察到的生产事实 | 结论 |
| --- | --- | --- |
| K3s 节点 | 10 个节点中 9 个 Ready；`a3-server-00` 自 `2026-09-23T01:28:27Z` 起 `NotReady`（`NodeStatusUnknown`，主机 `110.123.0.3` 可 ping 通，自动附加 unreachable `NoSchedule`/`NoExecute` taints） | A3/NPU 工作负载当前不可调度 |
| Argo CD Application | `k12-data-pipeline`、`model-platform-bootstrap`、`model-platform-deployment-requests` 均 `Synced/Healthy`，revision `7575c15f5a935eec5bf5c2d0b02946a9f19e1a13`；`model-platform-training-system` 为 `OutOfSync/Healthy`（控制器消息“Application has 13 orphaned resources”） | 平台与 K12 期望态已收敛；训练侧待收敛 |
| automated sync | 仅 `model-platform-deployment-requests` 开启（`allowEmpty=false`、`prune=false`、`selfHeal=false`）；其余 Application 仍为人工同步 | 与既有自动化边界一致 |
| ModelDeployment | `qwen38-27b` generation 103：`desiredState=Stopped`，`Synced=True (ReconcileSuccess)`、`Ready=True (Available)`、`Responsive=True (WatchCircuitClosed)`；`qwen38-stopped-auto-smoke`、`qwen36-pd-crossplane-zero` 均 Synced/Ready | 停止态干净，此前 `WatchCircuitOpen` 告警已恢复 |
| Ray | 全集群无 RayService；RayCluster 仅 `ds`、`k12`（head + 1 CPU worker）、`ray-demo` 为 ready | 无 Qwen 运行负载 |
| 缓存 PV | `model-cache-a3-qwen38-27b-w8a8` 40Gi `Released`/`Retain`，claim `model-serving/qwen38-27b-cache` | 停止态保留缓存数据，符合合同 |
| cache reaper | CronJob 每分钟调度，最近 Job `model-deployment-cache-reaper-<ts>` 完成；与 09-20 发布记录的 `cache_reaper=PASS deleted=none` 行为一致 | 自动化在线且无遗留对象 |
| Backstage | `0.6.23-direct-accept-6601e62` @ `sha256:dcc269a6...`（与 09-19 验收记录一致），Deployment 1/1、0 重启、运行约 4 天；PostgreSQL 1/1 | 门户与 Direct Operations 可用 |
| K12 | Dagster `0.5.3-autoscale16-20260918-r9` @ `sha256:5aedca9e...` 2/2；CPU Ray head/worker Running 约 5 天；MinerU/Qwen Deployment 全部 0 副本 | CPU 主线在线、NPU 关闭 |
| 训练集成 | controller `1.1.8-personal-wandb-b41a23b` 2/2（约 16 天）；`wandb-egress-proxy` 1/1；`pretrain-ray`/`pretrain-ray-smoke`/`testtraining` 无 Pod；TrainingRun 历史含 Succeeded 与 Suspended/ManualRequired | 训练侧独立实施，平台仅记录集成事实 |
| 控制面 | Artifact Keeper backend 1.6.4 / web 1.5.8、Gitea 1.26.1、Crossplane/Tekton/Argo CD/Backstage Pod 全部 Running；多个平台 Pod 重启计数为 1（13 天前，与主机维护时间点一致） | 控制面健康；重启原因需在主维护记录中留痕 |
| 主机 | `server-00` uptime 13 天 22 小时、load 约 3、内存 43/754 GiB、根盘 69%、`/mnt/data` 78%、zombie 进程 23 | 根盘占用较 08-28 快照（约 52%）明显上升；维护窗口仍待安排 |
| 全局 Pod | `kubectl get pods -A` 无非 Running/Completed 条目 | 无 Pending/CrashLoop/Error |

## 2. 2026-09-18 至 09-20 增量（已有独立记录）

- 2026-09-18：K12 CPU Stage 1 从生产 Backstage 工作台完成 E2E（Dagster run
  `1c852c5c-3f55-4b61-9d6d-31abffc4a577`，45.555 秒，10/10 文档，84 个输出对象），
  见 `data-pipeline/k12-backstage-gitops-phase-b-start-record-20260918.md`。
- 2026-09-18：MinerU 静态设备声明与实际分配不一致后的受控 Stop（config PR `face37dbe`），
  见 `data-pipeline/k12-mineru-device-mismatch-stop-record-20260918.md`。
- 2026-09-18：autoscale16 r9 收敛与最终 NPU 全链路成功（run `bd5388b4-...`，10/10 文档、
  Dagster 139 步），见 `data-pipeline/k12-r9-convergence-and-final-npu-run-record-20260918.md`。
- 2026-09-19：Direct Operations 生产验收完成（真实推理 57/8/65 tokens；随后用
  default-scheduler 对照证明 Volcano/MindX 静态设备映射缺陷，Operator 已恢复
  `--batch-scheduler=volcano`），见 `backstage/direct-operations-production-acceptance-20260919.md`。
- 2026-09-19：`model-platform-config#62` 固定 head `2 CPU/16Gi`、worker `48 CPU/256Gi`
  资源合同并扩展 RuntimeProfile schema。
- 2026-09-20：cache reaper 生产发布（`model-platform-config#63`），见
  `backstage/model-deployment-cache-reaper-production-release-20260920.md`。
- 2026-09-20：reaper 发布后的首次 Start/Stop preflight 被独立容量检查阻止：A3 exporter
  16 个设备 ID 均报告进程数 1 且无法归属任何 Kubernetes 对象；未开 Running Window、
  未创建任何 Qwen 资源，见同文件“First post-release end-to-end preflight”。

## 3. 当前阻塞与风险

1. `a3-server-00` NotReady（`2026-09-23T01:28Z` 起）：需要主机/节点侧恢复 kubelet
   心跳；在节点恢复并通过容量检查前，不得尝试任何 NPU Start 或 Running Window。
2. A3 无归属进程：16 个设备 ID 的占用未与任何 Kubernetes Pod 关联，来源在平台管理
   范围之外；需要设备侧确认后再评估 Running。
3. 训练 Application `OutOfSync` 与 13 个 orphaned resources：由训练责任人收敛，
   平台不代管其源码，也不在 Material 复制其实现。
4. Artifact Keeper `container-images` 超配额与匿名读策略：按 `ROADMAP.md` P4 处理，
   发布新镜像前需要扩容或安全清理。
5. `server-00` 维护：根盘 69%、`/mnt/data` 78%、uptime 13 天，仍待受控维护窗口。

## 4. 下一步最小动作

1. 恢复 `a3-server-00` 节点健康，并确认 exporter 进程归属；在此之前保持
   Running Window=false 与零 NPU。
2. 节点恢复后，按 `backstage/direct-operations-production-acceptance-20260919.md`
   的门禁执行一次独立容量检查，再决定是否重开 Running Window。
3. 训练侧先收敛 OutOfSync/13 orphans 与 schema 告警，再评估 Backstage 只读集成。
4. 安排 Artifact Keeper 容量处理与 `server-00` 维护窗口。
5. Material 事实归档继续按职责拆分提交（见 `ROADMAP.md` P0.1）。

## 5. 证据入口

- 本次核验为只读 SSH 采集（`jumper-0041-pub` → `server-00`，`sudo k3s kubectl`），
  未执行任何写操作。
- 推理验收：`backstage/direct-operations-production-acceptance-20260919.md`
- 停止态自动化：`backstage/model-deployment-cache-reaper-production-release-20260920.md`
- K12：`data-pipeline/k12-backstage-gitops-phase-b-start-record-20260918.md`、
  `data-pipeline/k12-r9-convergence-and-final-npu-run-record-20260918.md`
- 上一快照：`CURRENT-STATE-20260917.md`
