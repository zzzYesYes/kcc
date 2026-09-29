# Ray 正式预训练启动链路

这个目录是当前可直接使用的正式入口。它把原来的 Shell 多机启动方式串成：

```text
环境与占用检查
→ 根据本次 --node 列表渲染 RayCluster
→ 启动 RayCluster
→ 自动发现 worker/NPU 拓扑
→ 生成并验证 RankTable
→ 原生 HCCL AllReduce
→ 为每个 worker 复制并注入正式训练脚本
→ Ray Jobs API 托管 driver，再由整机 actor 并发启动各节点 torchrun
```

`raycluster.yaml` 是基础模板，每个 worker 当前申请整机 8 张 NPU。主入口会
根据本次重复传入的 `--node` 自动生成一份 create-only YAML，设置 worker
副本数和节点 affinity；参数注入器与 Ray driver 同样不固定为 2×8。

## 一行启动

启动前可单独检查目标节点及其 NPU 是否健康、空闲；该命令不会创建 Ray 资源：

```bash
kcc_ray check
```

不传 `--node` 时检查 `config/cluster.yaml` 中的
`topology.activeNodes + topology.spareNodes`；显式传入
`--node` 时只检查指定节点。

```bash
kcc_ray start --run-id pretrain-150m-20260803
```

普通 `start` 默认创建独立的 Kubernetes Supervisor Job 后立即返回；Job 内仍运行
现有恢复监督器：`activeNodes` 训练、`spareNodes` 备用，保留训练脚本中的 `--load`
和 checkpoint 路径。每个 active worker 仍由现有 RayCluster 和训练 actor 申请
整机 8 张 NPU。正式训练 driver 由 Ray Jobs API 托管，不再依赖
长时间 `kubectl exec` 会话。外层 Job 固定在 `supervisor.node` 配置的机器上，
并复用该机持久状态目录。

如果希望配置中的 active/spare 节点全部参与训练，使用 `--all-nodes`。Python 入口会
从同一配置展开现有单次六阶段流程的节点列表；此模式没有备用节点：

```bash
kcc_ray start --all-nodes
```

需要从 iteration 0 重新训练时，显式使用现有 fresh 模式：

```bash
kcc_ray start --fresh --run-id qwen3-from-zero-001
```

fresh 不修改提交的源脚本。HCCL 通过且 worker 预检完成后，rank 0 在共享
`/mnt/models` 上 create-only 创建：

```text
/mnt/models/pretrain-ray-platform/archive/qwen3-from-zero-001/
├── checkpoints/
└── logs/
    ├── node-rank-<N>.log
    ├── ray-driver/node-rank-<N>/
    ├── wandb/
    └── tensorboard/
```

同名目录已存在时直接停止，不会覆盖或复用。注入副本的 `--load` 和
`--exit-on-missing-checkpoint` 会移除，`--save` 会指向新归档的
`checkpoints/`。fresh 运行不进入基于已有 checkpoint 的备用机恢复监督器。
`--fresh` 可以与 `--all-nodes` 组合，让全部配置节点从 iteration 0 开始；
`--all-nodes` 不能再与显式 `--node` 混用。

运行中可从另一终端执行：

```bash
kcc_ray status --run-id <本次逻辑-run-id>
kcc_ray logs --run-id <本次逻辑-run-id>
kcc_ray logs --run-id <本次逻辑-run-id> --supervisor
kcc_ray cancel --run-id <本次逻辑-run-id>
```

原有停止命令仍可使用：

```bash
kcc_ray stop --run-id <本次逻辑-run-id>
kcc_ray stop-after-checkpoint --run-id <本次逻辑-run-id>
```

`stop` 先写 create-only 停止标记；RayCluster 已存在时核对 run-id 和 UID 后删除，
尚处于前置阶段时则由 Supervisor 在下一阶段边界安全退出。
`stop-after-checkpoint` 先记录当前 tracker，只在所有 worker 一致看到更大的正整数
iteration 后执行同一停止。后者保持前台等待，`Ctrl-C` 仅取消 watcher。停止工具
只读取 checkpoint，不移动、改名或删除任何 checkpoint；停止标记确保 Supervisor
不再启用备用机或创建下一 attempt。

项目根目录的 `config/cluster.yaml` 是日常运行默认值的唯一入口。当前完整结构为：

```yaml
schemaVersion: kcc-ray-config/v1
kubernetes:
  kubectlCommand: /usr/local/bin/k3s kubectl
  kubeconfig: /home/ywj/.kube/k3s-learning.yaml
  namespace: pretrain-ray
  clusterName: pretrain-gpu00-gpu01
topology:
  headNode: server-00
  activeNodes:
    - gpu-server-00
    - gpu-server-01
    - gpu-server-02
    - gpu-server-03
    - gpu-server-05
    - gpu-server-06
  spareNodes: [gpu-server-07, gpu-server-08]
npuCheck:
  resourceName: huawei.com/Ascend910
  exporterNamespace: npu-exporter
  exporterApp: npu-exporter
  exporterPort: 8082
training:
  defaultTemplate: ../ray_startup_bundle/training_templates/pretrain_150M.sh
  workingDirectory: /mnt/models/CODE/MindSpeed-LLM-v2.3.0
supervisor:
  node: server-00
  serviceAccount: pretrain-ray-supervisor
  image: 110.120.0.3:8889/pretrain/ray-head@sha256:121fff1a4b0f991121ba7dc85cbb7a77643d28c79cc355ba3f1536abb51c865b
  imagePullPolicy: IfNotPresent
  kubectlHostPath: /usr/local/bin/k3s
  backoffLimit: 3
  finishedTtlSeconds: 604800
timeouts:
  rayStartupSeconds: 1800
  hcclGateSeconds: 3600
  trainingSeconds: 0
  failedResourceRetentionSeconds: 1800
  recoveryCleanupSeconds: 300
```

`check` 和 `--all-nodes` 都由两组节点派生，不再维护额外列表。外层 Supervisor Job 会把创建时
解析出的运行值写成显式参数并纳入参数摘要；Job Pod 重启继续使用已冻结参数，不会
重新读取已修改的 YAML。显式 CLI 参数优先于 YAML，只影响本次调用。如需使用系统
配置，可设置 `KCC_RAY_CONFIG=/etc/kcc-ray/cluster.yaml`；相对路径相对该 YAML 所在
目录解析。

模型结构、数据与 tokenizer 路径、checkpoint 读写路径、batch size、学习率和保存
间隔不放在这个文件中，仍只修改 `training.defaultTemplate` 指向的训练模板。Ray
容器镜像、CPU/内存/NPU 数量、挂载和 runtimeClass 仍只修改 `raycluster.yaml`，
避免同一训练含义出现两份配置。

每次机器数不同时重复传 `--node` 即可，worker 数由节点列表自动得出：

```bash
kcc_ray start \
  --node gpu-server-02 \
  --node gpu-server-03 \
  --node gpu-server-05 \
  --spare-node gpu-server-07 \
  --spare-node gpu-server-08
```

恢复模式下，显式 `--node` 是 active 列表，显式 `--spare-node` 是不重叠的
备用池。二者同时接受 Kubernetes 节点名和 InternalIP。基础 YAML 不需要为每次
训练手工修改。

## start 默认启用两台备用机恢复

统一入口只负责用 `supervisor_job.py` 创建外层 Job；Job 内再调用现有
`recovery_supervisor.py --resume`，不重新实现六阶段或恢复逻辑。配置中的
`activeNodes` 是本次 active，`spareNodes` 是备用池：

```bash
kcc_ray start --run-id pretrain-150m-20260803
```

显式传入重复的 `--node` 或 `--spare-node` 会分别临时覆盖对应默认列表。

`--run-id` 在这里是逻辑任务 ID；实际每轮证据使用
`<run-id>-a00`、`<run-id>-a01`。正式训练明确导出 `FAIL` 后，恢复入口会：

1. 重发当前 RayCluster 的幂等删除，等待旧 RayCluster、Pod、Service 和
   RankTable ConfigMap 全部消失；删除 Pod 会结束该 RayCluster 内的旧训练进程。
2. 只做一次 active + 当前剩余备用机快照。Node 消失或 NotReady、Kubernetes
   NPU 数量不足、exporter 少卡或报卡异常均记为坏机；证据读不到记为 unknown。
3. 只有所有存活 active 已空闲，且坏机数不大于健康、空闲备用机数时，才整批
   替换。备用机按声明顺序放入原 active 列表位置；新一轮实际 `node_rank` 仍由
   新发现的 Pod/RankTable 拓扑重新冻结，不假设物理机 rank 不变。
4. 用替换后的完整 active 列表从六阶段起点创建新 Ray world；不会复用上一轮
   actor、RankTable 或 checkpoint 检测结果。
5. 新 world 的所有 worker 重新读取 `/mnt/models` 上最新已提交 checkpoint；
   一致性检查通过后，训练脚本才按原来的 `--load`/`--save` 拉起 `torchrun`。

### 最新可恢复 checkpoint 的检测

恢复模式不会按目录名扫描编号最大的 `iter_*`。每一轮正式 `torchrun` 拉起前，
6 个 Ray worker 都会从共享的 `CKPT_LOAD_DIR` 读取 Megatron 自己的提交指针：

```text
/mnt/models/0717/latest_checkpointed_iteration.txt
```

指针中的正整数 `N` 唯一选择 `iter_%07d`（例如 `1000` 对应
`iter_0001000`）。以当前模板的
`TP=1`、`PP=1` 和 distributed optimizer 为例，所有 worker 必须同时看到并能
读取以下两个非空文件：

```text
/mnt/models/0717/iter_XXXXXXX/mp_rank_00/model_optim_rng.pt
/mnt/models/0717/iter_XXXXXXX/mp_rank_00/distrib_optim.pt
```

各 worker 的 tracker 内容、tracker 摘要、所选目录、文件相对路径和文件大小还
必须完全一致。这样，故障保存遗留的更大 `iter_*` 半成品不会被误选。tracker
缺失/损坏、指向目录或文件不完整、值为 `release`，或者各 worker 视图不一致时，
本轮导出 `CHECKPOINT_UNAVAILABLE`；恢复入口清理本轮 RayCluster 后写入
`MANUAL_REQUIRED`，不会进入坏机诊断，也不会消耗备用机。平台不修改 tracker，
也不会自动回退到更旧目录。文件内容以及模型/优化器参数是否真正兼容，仍由
MindSpeed/Megatron 的正式加载做最终确认。

checkpoint 内容错误和机器掉线分开处理：tracker/分片确实有问题时按上述规则
人工处理；如果 worker 在 checkpoint 检查期间掉线、actor 丢失或 Ray RPC 失败，
该轮记为 worker runtime failure，仍会在清理旧 world 后进入坏机诊断。actor
身份检查、worker 预检和 checkpoint RPC 的等待上限均为 300 秒，避免共享存储
或失联 actor 令 supervisor 无限等待。换机后的下一轮再从头检查 checkpoint，
避免把机器故障误报成 checkpoint 损坏。driver 内部错误、协议证据异常和人为
中断则清理后直接 `MANUAL_REQUIRED`，不会因软件错误消耗备用机。

因此两个备用机既可以处理两轮各坏一台，也可以处理同一轮同时确认坏两台；
若坏机多于剩余健康备用机、任一 active 状态不确定，或清理后 exporter 仍看到
NPU 进程，则停止并写入 `MANUAL_REQUIRED`，不会部分换机或扫描宿主机 PID。

恢复状态原子写入：

```text
log/training-jobs/<run-id>/state.json
```

其中记录初始/剩余备用机数、累计替换数、隔离节点和每次 attempt 的诊断结果。
失败 attempt 保存坏机到备用机的 `replacements`；下一 attempt 保存替换后的
`activeNodes`，并在结束时保存正式 `trainingResult`。只有该结果属于当前 run ID、
状态为 `PASS`，且含全部 worker 一致的 checkpoint iteration/目录证据，整个恢复
任务才会写成 `PASS`。
恢复模式会把已确认失败的 RayCluster 保留时间强制为 0；checkpoint 和
`/mnt/models` 中的训练日志不会被删除。

默认正式模板是：

```text
training_templates/pretrain_150M.sh
```

当前默认模板采用 6 台、每台 8 张 NPU、`GBS=96` 的正式基线。也可以显式提交
安装目录内另一份同结构脚本：

```bash
kcc_ray start \
  --train-script /opt/kcc/pretrain-ray-main/ray_startup_bundle/training_templates/custom.sh
```

`--train-script PATH` 就是单次模板选择命令；不传时使用
`training.defaultTemplate`，当前为 `training_templates/pretrain_150M.sh`。

默认恢复任务首次启动时，会在
`log/training-jobs/<run-id>/training-template.sh` create-only 保存所选模板快照，
并把来源路径、快照路径、大小和 SHA256 写入 `state.json` 及同目录元数据文件。
当前 attempt、备用机恢复 attempt 和 Supervisor Pod 重启都只使用这个快照；源模板
之后被修改不会改变该 logical run。快照或元数据缺失、被替换、使用符号链接或摘要
不一致时会安全停止。升级前没有模板快照的旧 state 不会被自动接管。

Supervisor Job 只能读取项目挂载中的控制侧文件，因此自定义训练脚本、manifest、
runtime 和 HCCL 证据文件必须位于实际安装目录（例如 `/opt/kcc/pretrain-ray-main`）下；worker 内的
训练目录和 `/mnt/models` 数据路径不受这个限制。

默认训练工作目录是 worker 内已经验证过的：

```text
/mnt/models/CODE/MindSpeed-LLM-v2.3.0
```

如镜像内源码位置变化，使用 `--training-cwd /new/path`。

## 自动注入与保持不变的内容

HCCL 成功后，`inject_training_params.py` 根据本次证据为每个 worker 生成
一份脚本，只修改：

- `RANK_TABLE_FILE`
- `NPUS_PER_NODE`
- `MASTER_ADDR`、`MASTER_PORT`
- `NNODES`、`NODE_RANK`
- 每个 node rank 独立的 `LOG_FILE`
- 未注释的 `torchrun`，改为
  `/root/miniconda3/envs/ms/bin/torchrun`

仅当显式使用 `--fresh` 时，还会在注入副本中把 checkpoint 和日志参数改到
本次 archive，并移除活动的 `--load`、`--exit-on-missing-checkpoint`；源脚本
始终不变。

`MASTER_PORT` 默认保留源脚本值，可用 `--master-port` 覆盖。

如果 HCCL 发现的 `NNODES × NPUS_PER_NODE` 与源脚本不同，注入阶段默认停止，
不会启动训练。缩容或扩容必须由用户显式传入
`--allow-topology-change`；该确认会写入 `injection.json`，供前端审计。
改变拓扑前需要同时核对 checkpoint 是否支持新 world size，并确保
`CKPT_SAVE_DIR` 没有被另一训练任务写入。工具不再要求无实际互斥能力的人工确认参数，
也不提供跨任务 checkpoint 写锁；平台不会擅自改 checkpoint 目录。

普通续训模式下，以下正式训练语义不会被改写：

- `TRAIN_ITERS` 及模型、并行、batch、学习率参数
- 数据路径与数据缓存参数
- checkpoint 的 `--load`、`--save`、`--save-interval`
- `--exit-on-missing-checkpoint`
- W&B 和 TensorBoard 参数

流程不删除任何 checkpoint。训练通过 `/bin/bash -o pipefail` 执行，避免
尾部 `tee` 掩盖 `torchrun` 的失败退出码。

## 文件职责

- `config/cluster.yaml`：唯一的日常运行默认配置；不存训练超参数和密钥。
- `cluster_config.py`：有界、安全加载严格 schema，并派生本次有效默认值。
- `bin/kcc_ray`：统一用户入口；默认 `start` 接入恢复监督，`--all-nodes`
  接入全部配置节点的单次流程，`--fresh` 接入从零训练实现。
- `supervisor_job.py`：create-only 创建并校验固定 RayCluster 对应的外层
  Kubernetes Job，提供状态和 Supervisor 日志查询。
- `supervisor-rbac.yaml`：按 namespace/cluster 渲染的外层 Job ServiceAccount、
  集群内 kubeconfig 与最小 Kubernetes 权限模板；不要绕过入口直接 apply。
- `start_ray.py`：单次六阶段内部入口；第 6 阶段通过 Ray Jobs API 提交 driver。
- `environment_check.py`：只读检查 Kubernetes NPU 占用、硬件健康和 NPU
  进程；发现占用时打印原因并退出，不停止别人的进程。
- `ray_cluster_start.py`：打包 HCCL runtime、应用 YAML、等待 Ray 就绪。
- `render_raycluster.py`：把节点名或 InternalIP 解析为本次 worker 列表，
  生成对应副本数和 affinity 的运行 YAML。
- `hccl_gate.py`、`hccl_runtime/`：拓扑发现、RankTable 和真实 HCCL gate。
- `inject_training_params.py`：读取 PASS 证据，创建每节点正式脚本和冻结清单。
- `ray_training_submit.py`：把清单与 driver 复制到 Ray head，并用 Ray Jobs API 提交、查询和导出结果。
- `ray_training_driver.py`：每个 worker 申请其全部 NPU，核对 Pod/RankTable/
  `ms` 环境后并发执行正式脚本。
- `recovery_supervisor.py`：失败清理、单次诊断、备用机计数、整批替换和重启。
- `recovery_diagnostics.py`：一次性读取 Kubernetes/exporter 证据并给出
  fail-closed 的 N 坏机换 N 备用机决策。
- `training_templates/`：正式训练模板；运行时只修改生成的注入副本。
- `raycluster.yaml`：按 namespace/cluster/head/NPU/worker 列表渲染的 Ray 基础模板，
  也是镜像、挂载和资源声明的唯一来源；不要绕过入口直接 apply。

仓库旧的 `scripts/start_ray.py` 两阶段入口已经删除，避免误用旧 smoke
manifest。正式训练统一从 `kcc_ray start` 启动，内部仍复用本目录已有模块。

运行日志、测试归档、历史产物和报告不属于运行工具，不应进入发行包。HCCL 代码中的
`ranktable_smoke` 是通信验证证据类型，不是旧训练 smoke。

## 证据、日志与资源生命周期

单次续训或 fresh 运行自动生成唯一 `run-id`。主要本地结果为：

```text
log/hccl-startup/<run-id>/
log/training-runs/<run-id>/injection/
log/training-runs/<run-id>/execution-result.json
```

默认 `start` 把 `--run-id` 作为逻辑任务 ID，各轮结果使用
`<run-id>-a00`、`<run-id>-a01`，监督状态另存为：

```text
log/training-jobs/<run-id>/state.json
```

这些控制端目录应挂载持久卷。worker 上的正式训练日志统一写入共享
`/mnt/models`：

```text
/mnt/models/pretrain-ray-platform/log/<run-id>/logs/node-rank-<N>.log
/mnt/models/pretrain-ray-platform/log/<run-id>/ray-driver/node-rank-<N>/
```

fresh 模式改为写入同一存档目录：

```text
/mnt/models/pretrain-ray-platform/archive/<run-id>/checkpoints/
/mnt/models/pretrain-ray-platform/archive/<run-id>/logs/
```

成功时默认删除 RayCluster 以释放 NPU，但保留 checkpoint、训练日志和本地
证据。使用 `--keep-success-resources` 可保留 RayCluster。

`--all-nodes` 单次续训或 fresh 运行在 Ray 启动、HCCL 或训练明确失败时，默认
保留资源 1800 秒后删除 RayCluster；可用 `--failure-retention-seconds` 调整。
默认 `start` 为了先清理旧 world 再诊断换机，会把失败保留时间强制为 0。
Namespace、checkpoint 和日志不会删除。训练状态不确定或结果未能导出时，
RayCluster 会保留，避免误删仍在运行的任务。

需要提前删除时，可中断等待进程后执行：

```bash
/usr/local/bin/k3s kubectl \
  --kubeconfig /home/ywj/.kube/k3s-learning.yaml \
  delete raycluster pretrain-gpu00-gpu01 \
  -n pretrain-ray --wait=false
```

当前第 6 阶段通过 `ray_training_submit.py` 把 driver 提交给 Ray Jobs API。
提交成功后，driver 不依赖原终端；控制链路中断时不会删除 checkpoint 或状态不明的
RayCluster。恢复 Supervisor 由固定在 `supervisor.node` 的 Kubernetes Job 托管；
`kcc_ray status` 聚合外层 Job、恢复状态和内层 Ray Job，`logs --supervisor` 查看
外层日志。Pod 重启时内部自动带 `--resume`；手工执行 `kcc_ray start --resume` 时还会
核对相同 run ID、参数摘要和 Job ownership。进程锁保证同一逻辑任务只有一个接管者；
已有结果会被验证并重放，否则只重连 create-only record 中的原 Ray Job。没有可信
record 时不会重跑六阶段或重复 submit。
Ray Job 状态默认每 30 秒查询一次；短暂失败按 5、10、20、30 秒退避，连续
失败超过 300 秒才按状态不确定退出并保留 RayCluster。

## 为前端预留

前端只需调用 `kcc_ray start` 对应的持久化后端任务，不需要了解 Kubernetes、
RankTable 或 HCCL。已经预留的提交字段包括：

- 训练脚本、训练工作目录
- 节点列表（数量可变）、基础 YAML、namespace、RayCluster 名称
- 期望 worker/world size
- master port
- 是否明确同意改变源脚本拓扑（缩容/扩容）
- 启动模式（默认恢复续训、`--all-nodes` 全节点单次训练或 `--fresh` 从零开始）；
  fresh 的 archive 路径与恢复备用节点列表
- 各阶段 timeout、失败资源保留时间
- caller 提供的 run ID
- 成功后是否保留 RayCluster

`run-id` 可作为任务 ID；上述 JSON 目录可直接映射为阶段状态、注入参数、
节点日志和最终结果接口。
