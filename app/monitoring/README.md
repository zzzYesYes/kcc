## TODO (Priority from high to low)
1. [ ] cluster resource observability (cpu, mem, npu etc.)
2. [ ] app trace observabilty
3. [ ] app log observability
4. [ ] llm api usage record
5. [ ] llm agent tool call record
6. [ ] L7 observability for apps (exclude ray workers traffics) 


## kube-prometheus-opentelemetry-stack

### 架构说明

在kube-prometheus-stack中添加OpenTelemetryCollector(otelcol)：
- Otelcol通过 TargetAllocator(ta) 读取 ServiceMonitor / PodMonitor 来感知原本由Prometheus抓取的采集目标，从而采集实际的指标数据
- Prometheus修改为从otelcol中采集指标数据，并在Grafana中展示

### 修改说明

#### Chart.yaml

声明第三方 Chart 依赖

[kube-prometheus-stack](https://artifacthub.io/packages/helm/prometheus-community/kube-prometheus-stack/84.4.0)

[opentelemetry-operator](https://artifacthub.io/packages/helm/opentelemetry-helm/opentelemetry-operator/0.120.0)

#### values.yaml

kube-prometheus-stack
- 将grafana和prometheus的service类型修改为NodePort，端口号分别为32000和30090，便于访问
- 将grafana管理员用户(admin)的默认密码修改为`Admin@9000`，便于访问
- 修改prometheus匹配ServiceMonitor和PodMonitor的逻辑，只采集otelcol的指标数据：
  - 通过otelcol=prometheus-otelcol标签来匹配otelcol对应的ServiceMonitor
  - 由于在当前业务中不需要匹配PodMonitor，因此指定了一个不存在的标签non-existent-label=true，避免意外匹配

opentelemetry-operator
- 关闭Webhook功能
- (TODO) 原本通过使用自签名证书来开启Webhook功能，但在otelcol相关资源配置Hook后出现了竞争时序问题，因此暂时先把Webhook功能关闭了。后续如果需要，可以结合cert-manager来开启Webhook功能

#### templates/NOTES.txt

提示用户在安装成功后如何查看POD状态和获取grafana管理员用户(admin)的密码

#### templates/otelcol.yaml

ServiceAccount
- 使OpenTelemetryCollector和TargetAllocator能够从K8s集群中读取相应的资源数据

TargetAllocator
- 通过release=`{{ .Release.Name }}`标签来匹配 Prometheus 的 ServiceMonitor / PodMonitor
- 默认情况下，kube-prometheus-stack无法采集etcd/controller-manager/scheduler/proxy的指标数据，因此显式排除掉，避免otelcol日志打印大量采集失败的日志

OpenTelemetryCollector
- 从TargetAllocator中获取待采集的target
- 暴露8889端口，供prometheus来拉取(pull)指标数据

Hook
- 相关资源在所依赖的chart资源安装成功之后再安装

#### templates/otelcol-smon.yaml

otelcol对应的ServiceMonitor，用于配置prometheus采集otelcol的指标数据

#### templates/npu-dashboard.yaml

在grafana中自动导入npu-dashboard

### 安装命令

```bash
cd kube-prometheus-opentelemetry-stack

helm dependency update .

helm install promotel . -n monitoring --create-namespace
```

相关调试命令

```bash
# 在安装成功后，需要等待opentelemetry-operator初始化成功后，才会创建otelcol和ta
# 可以通过日志查看相应的初始化进度，并观测是否存在异常
kubectl -n monitoring logs -f promotel-opentelemetry-operator-xxx

# 可以在ta中查看所有待采集的target
kubectl -n monitoring port-forward --address 0.0.0.0 svc/otelcol-prometheus-targetallocator 38080:80
# 可以直接在otelcol中获取原始的指标数据
kubectl -n monitoring port-forward --address 0.0.0.0 pod/otelcol-prometheus-collector-0 38889:8889
```

### 采集NPU指标

- 删除npu-exporter的networkpolicy

```bash
kubectl -n npu-exporter delete networkpolicy exporter-network-policy
```

- 创建npu-exporter对应的ServiceMonitor，元数据中包含release=promotel标签，并在指标中设置`node_name`标签

```bash
kubectl apply -f npu-exporter.yaml
```
