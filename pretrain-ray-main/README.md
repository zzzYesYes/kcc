# KCC Ray 内部预训练工具

`kcc_ray` 是当前 K3s + KubeRay + Ascend 集群的内部预训练入口。它负责节点检查、
RayCluster 创建、HCCL 验证、Ray Job 提交、checkpoint 续训、备用机恢复和安全停止。

本目录是运行工具，不是通用 Kubernetes 发行版。默认节点、私有镜像、Ascend 资源、
`/mnt/models` 挂载和训练路径均针对现有内部集群。

## 目录

```text
bin/kcc_ray                       统一命令入口
config/cluster.yaml              集群和运行默认值
ray_startup_bundle/              Ray/HCCL/恢复实现
  raycluster.yaml                Ray Pod 镜像、资源和挂载
  training_templates/
    pretrain_150M.sh             默认训练模板
requirements.txt                 控制端 Python 依赖
log/                             本机运行状态，安装时创建，不参与分发
```

模型、数据、checkpoint 和训练日志位于各 Worker 共同可见的 `/mnt/models`，不在
软件包中。

## 集群前提

安装前确认现有集群已经具备：

- `server-00` 可运行 `/usr/local/bin/k3s kubectl`，并有可用 kubeconfig；
- KubeRay Operator、Volcano、Ascend runtime/device plugin 和 `npu-exporter` 正常；
- head/worker/Supervisor 私有镜像可拉取；
- Worker 上 `/mnt/models` 是所有训练节点看到内容一致的共享存储；
- Worker 上存在 CANN/NNAL、8 张 Ascend NPU，以及模板需要的数据、tokenizer、
  checkpoint 和训练源码；
- `training.workingDirectory` 中存在常规文件 `pretrain_gpt.py`；
- 操作者拥有创建 Namespace、RBAC、Job、RayCluster 和 ConfigMap 的权限。

当前镜像和路径还要求 Python 3.10、Ray 2.49.0、
`/root/miniconda3/envs/ms/bin/torchrun`、`make/g++` 和 CANN 头文件。它们已经包含在
现有内部镜像中。

## 安装

推荐只在 `config/cluster.yaml` 的 `supervisor.node`（当前为 `server-00`）安装和
执行命令。Supervisor Pod 会通过 `hostPath` 挂载软件的真实绝对路径，因此安装目录
必须位于该宿主机，并且运行期间不能移动或删除。

从共同项目的已审核 tag 安装到固定目录：

```bash
sudo git clone --branch <已审核-tag> --depth 1 \
  https://github.com/zzzYesYes/kcc.git /opt/kcc

# Supervisor 当前以 UID/GID 1001 写入持久状态。
sudo install -d -o 1001 -g 1001 -m 0770 \
  /opt/kcc/pretrain-ray-main/log

cd /opt/kcc/pretrain-ray-main
python3 -m pip install --user -r requirements.txt
python3 -c 'import yaml; print(yaml.__version__)'

sudo ln -sfn /opt/kcc/pretrain-ray-main/bin/kcc_ray /usr/local/bin/kcc_ray
readlink -f /usr/local/bin/kcc_ray
kcc_ray --help
```

`bin/kcc_ray` 使用操作者当前的 `python3`，所以 PyYAML 必须安装到这个 Python
能够读取的位置。不要只在另一个未激活的虚拟环境中安装依赖。

如果使用内部 tar 包或其他安装目录，替换上述路径即可，但安装目录必须保持固定，并
满足相同的 Supervisor hostPath 和 `log/` 写权限要求。

## 首次配置

先修改 [`config/cluster.yaml`](config/cluster.yaml)：

- `kubernetes`：kubectl、kubeconfig、namespace 和唯一 RayCluster 名；
- `topology`：head、默认训练节点和按顺序使用的备用节点；
- `npuCheck`：Ascend 资源名和 exporter 地址；
- `training.defaultTemplate`：默认训练模板；
- `training.workingDirectory`：每个 Worker 容器内的训练源码目录；
- `supervisor`：外层 Kubernetes Job 的节点、ServiceAccount、镜像和重试策略；
- `timeouts`：Ray、HCCL、训练、资源保留和恢复清理超时。

显式命令参数只覆盖本次调用。也可以通过
`KCC_RAY_CONFIG=/path/to/cluster.yaml` 选择另一份完整配置；如果外置配置使用相对
`defaultTemplate`，路径相对该 YAML 文件解析，建议改为安装目录下的绝对路径。

训练内容分别维护在两个地方：

- [`training_templates/pretrain_150M.sh`](ray_startup_bundle/training_templates/pretrain_150M.sh)：
  模型参数、数据/tokenizer、checkpoint、batch size、学习率、训练步数和保存间隔；
- [`raycluster.yaml`](ray_startup_bundle/raycluster.yaml)：head/worker 镜像、CPU/内存/NPU、
  runtimeClass 和 hostPath 挂载。

`training.workingDirectory` 不是另一份训练模板。实际执行关系是：

```text
pretrain_150M.sh（参数和启动命令）
  -> cd /mnt/models/CODE/MindSpeed-LLM-v2.3.0
  -> torchrun ... pretrain_gpt.py ...
```

默认模板当前关闭 W&B。需要启用时通过集群 Secret 或运行环境提供凭据，不要把 API
key 写入仓库或训练模板。

## 部署前检查

```bash
# 默认检查 activeNodes + spareNodes，不创建 Ray 资源。
kcc_ray check

# 只检查指定节点。
kcc_ray check --node gpu-server-00 --node gpu-server-07
```

只有检查通过后再启动正式任务。

## 启动训练

普通 `start` 默认从模板中的 checkpoint 续训，并启用配置中的备用机自动恢复。
建议显式记录 `run-id`：

```bash
kcc_ray start --run-id pretrain-150m-001
```

不传 `--run-id` 时会自动生成；请保存命令输出中的 ID。`start` 创建 Kubernetes
Supervisor Job 后立即返回，后续训练不依赖当前终端。

常用变体：

```bash
# 从 iteration 0 开始；使用单次前台流程。
kcc_ray start --fresh --run-id pretrain-150m-fresh-001

# activeNodes + spareNodes 全部参加训练，不保留备用机。
kcc_ray start --all-nodes --run-id pretrain-150m-all-001

# 临时指定训练节点和备用节点。
kcc_ray start \
  --run-id pretrain-150m-custom-001 \
  --node gpu-server-00 \
  --node gpu-server-01 \
  --spare-node gpu-server-07

# 选择安装目录内的另一份训练模板。
kcc_ray start \
  --run-id pretrain-150m-template-001 \
  --train-script /opt/kcc/pretrain-ray-main/ray_startup_bundle/training_templates/custom.sh
```

显式 `--node` 或 `--all-nodes` 会自动允许本次拓扑与模板原始 `NNODES` 不同。
checkpoint 是否支持该 world size 仍由提交者负责确认。

## 查询和恢复连接

```bash
kcc_ray status --run-id pretrain-150m-001
kcc_ray logs --run-id pretrain-150m-001
kcc_ray logs --run-id pretrain-150m-001 --supervisor
```

Supervisor Pod 重启时会自动续接已有状态和原 Ray Job，不会重复提交。只有需要手工
重新接入同一个逻辑任务时才使用：

```bash
kcc_ray start --resume --run-id pretrain-150m-001
```

手工续接的参数必须与首次启动一致。每个逻辑任务首次启动时会把训练模板快照保存到
`log/training-jobs/<run-id>/`；之后修改源模板不会改变该任务或它的备用机恢复轮次。

## 停止

```bash
# 立即停止，保留 checkpoint。
kcc_ray stop --run-id pretrain-150m-001

# 等所有 Worker 一致看到下一次已提交 checkpoint 后停止。
kcc_ray stop-after-checkpoint --run-id pretrain-150m-001

# 直接停止当前 Ray Job 并清理其 RayCluster，作为控制/维护入口。
kcc_ray cancel --run-id pretrain-150m-001
```

`stop-after-checkpoint` 在前台等待；按 `Ctrl-C` 只取消等待，不停止训练。停止标记会
阻止 Supervisor 把人工停止误判为故障并启用备用机。这些命令不会删除 checkpoint。

## 自动恢复边界

正式训练阶段失败后，Supervisor 会先清理旧 RayCluster，再检查 checkpoint 和节点。
只有同时满足以下条件才会消耗备用机并创建下一轮 `a01/a02`：

- 所有 Worker 看到同一个已提交 checkpoint；
- 能明确诊断出不健康的 active 节点；
- 其余 active 节点已经空闲；
- 有数量足够、健康且空闲的备用节点；
- 没有收到人工停止请求，且未超过备用机预算。

网络瞬断或 Ray Actor 断联本身不等于整机故障。诊断不明确时会进入
`MANUAL_REQUIRED`，不会盲目换机。

## 状态和日志

控制状态位于安装目录：

```text
log/hccl-startup/<attempt-id>/
log/training-runs/<attempt-id>/
log/training-jobs/<run-id>/state.json
```

Worker 训练日志和 fresh 归档位于共享存储：

```text
/mnt/models/pretrain-ray-platform/log/<attempt-id>/
/mnt/models/pretrain-ray-platform/archive/<run-id>/
```

`log/` 包含集群拓扑、注入脚本和恢复状态，是运行数据而不是软件源码。分发包和 Git
提交必须排除 `log/**`、`state.json`、`__pycache__/`、测试归档和历史报告。

## 升级与卸载

长任务运行期间不要覆盖、移动或删除安装目录。Supervisor Pod 重启时仍会从该
hostPath 读取控制代码。升级前先停止全部任务，并备份 `log/`：

```bash
kcc_ray stop-after-checkpoint --run-id <run-id>
sudo cp -a /opt/kcc/pretrain-ray-main/log /opt/kcc-ray-log-backup
```

确认没有 Supervisor Job 或 RayCluster 后再替换代码，并恢复 `log/` 所有者为
`1001:1001`。卸载软件不会自动删除 `/mnt/models` 中的 checkpoint 和训练日志；
如需删除这些数据，必须单独人工确认目标路径。

## 内部分发清单

发行包只应包含：

```text
README.md
requirements.txt
bin/
config/
ray_startup_bundle/
```

仓库根目录的 `LICENSE` 也必须随源码发行。不要直接打包开发工作目录；应从经过审核
的 Git commit/tag 生成发行包，避免把被 `.gitignore` 忽略但仍存在于磁盘的运行状态
带进去。

实现细节、证据格式和故障恢复状态机见
[`ray_startup_bundle/README.md`](ray_startup_bundle/README.md)。
