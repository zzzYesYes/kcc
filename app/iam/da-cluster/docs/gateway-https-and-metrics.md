# AgentGateway HTTPS 证书与性能统计配置指南

> 版本：v1.0 | 日期：2026-04-02

---

## 1 Gateway HTTPS Listener 配置

### 1.1 为什么要配

用户到 Gateway 的通信需要加密，保护 JWT token 和请求数据不被窃听。Gateway 做 TLS Terminate（解密），后端服务收到的是明文 HTTP，不用关心证书。

### 1.2 配置后系统架构

```
用户浏览器/客户端
  │ HTTPS（加密）
  ▼
AgentGateway（TLS Terminate，解密）
  │ HTTP（内网明文）
  ├──→ Keycloak(:8080)         ← 登录/认证
  ├──→ keycloak-proxy(:8090)   ← IAM 管理 API
  ├──→ memory-service          ← 记忆库
  └──→ kb-service              ← 知识库

证书只在 Gateway 这一层，后端全部走内网 HTTP，不需要各自配证书。
```

### 1.3 配置步骤

> 官方教程：https://agentgateway.dev/docs/kubernetes/latest/setup/listeners/https/

#### 第 1 步：生成证书

生产环境用企业证书或 Let's Encrypt；测试环境可以自签：

```bash
mkdir example_certs

# 1. 生成自签 CA
openssl req -x509 -sha256 \
  -nodes -days 365 \
  -newkey rsa:2048 \
  -subj '/O=any domain/CN=*' \
  -keyout example_certs/root.key \
  -out example_certs/root.crt

# 2. 创建 OpenSSL 配置文件（改成实际域名）
cat <<'EOF' > example_certs/gateway.cnf
[ req ]
default_bits = 2048
prompt = no
default_md = sha256
distinguished_name = dn
req_extensions = req_ext
[ dn ]
CN = *.example.com
O = any domain
[ req_ext ]
subjectAltName = @alt_names
[ alt_names ]
DNS.1 = *.example.com
DNS.2 = example.com
EOF

# 3. 生成证书签名请求
openssl req -new -nodes \
  -keyout example_certs/gateway.key \
  -out example_certs/gateway.csr \
  -config example_certs/gateway.cnf

# 4. 用 CA 签发证书
openssl x509 -req -sha256 -days 365 \
  -CA example_certs/root.crt \
  -CAkey example_certs/root.key -set_serial 0 \
  -in example_certs/gateway.csr \
  -out example_certs/gateway.crt \
  -extfile example_certs/gateway.cnf -extensions req_ext
```

#### 第 2 步：创建 Kubernetes Secret

```bash
kubectl create secret tls https \
  -n agentgateway-system \
  --key example_certs/gateway.key \
  --cert example_certs/gateway.crt
```

#### 第 3 步：配置 Gateway

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: https
  namespace: agentgateway-system
spec:
  gatewayClassName: agentgateway
  listeners:
    - protocol: HTTPS
      port: 8443
      name: https
      tls:
        mode: Terminate              # Gateway 解密，后端收到明文 HTTP
        certificateRefs:
          - name: https              # 引用上面创建的 Secret
            kind: Secret
      allowedRoutes:
        namespaces:
          from: All
```

#### 第 4 步：配置 HTTPRoute

```yaml
# 记忆库路由
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: memory-route
  namespace: agentgateway-system
spec:
  parentRefs:
    - name: https
      namespace: agentgateway-system
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /memory/
      filters:
        - type: URLRewrite
          urlRewrite:
            path:
              type: ReplacePrefixMatch
              replacePrefixMatch: /
      backendRefs:
        - name: memory-service
          port: 80
---
# 知识库路由
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: kb-route
  namespace: agentgateway-system
spec:
  parentRefs:
    - name: https
      namespace: agentgateway-system
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /knowledgebase/
      filters:
        - type: URLRewrite
          urlRewrite:
            path:
              type: ReplacePrefixMatch
              replacePrefixMatch: /
      backendRefs:
        - name: kb-service
          port: 80
---
# Keycloak 路由（登录/认证）
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: keycloak-route
  namespace: agentgateway-system
spec:
  parentRefs:
    - name: https
      namespace: agentgateway-system
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /realms/
      backendRefs:
        - name: keycloak
          port: 8080
    - matches:
        - path:
            type: PathPrefix
            value: /admin/
      backendRefs:
        - name: keycloak
          port: 8080
```

#### 第 5 步：验证

```bash
# 获取 Gateway 外部地址
export GW_ADDRESS=$(kubectl get svc -n agentgateway-system https \
  -o jsonpath="{.status.loadBalancer.ingress[0]['hostname','ip']}")

# 测试 HTTPS 连接（自签证书用 -k 跳过验证）
curl -vik --resolve "https.example.com:8443:${GW_ADDRESS}" \
  https://https.example.com:8443/memory/v1/memories

# 预期：TLS 握手成功，返回 HTTP/2 200
```

---

## 2 Keycloak 为什么不需要配置证书

### 2.1 原因

Keycloak 已经在 Gateway 后面，通过 HTTPRoute 路由 `/realms/*`、`/admin/*` 到 Keycloak。用户访问 Keycloak 的流量先经过 Gateway 解密，再以 HTTP 转发到 Keycloak 内部端口 8080。

```
用户浏览器 ── HTTPS ──→ Gateway ── HTTP(内网) ──→ Keycloak(:8080)
              加密的      ↑ 解密      明文的
                     证书在这里
```

Keycloak 自己的配置也印证了这一点：
- `KC_HTTP_ENABLED: true` — 启用 HTTP（不是 HTTPS）
- `KC_PROXY_HEADERS: xforwarded` — 信任 Gateway 转发的 X-Forwarded 头
- 监听端口 8080（HTTP）

### 2.2 如果 Keycloak 不走 Gateway 呢

如果 Keycloak 直接暴露给用户（不经过 Gateway），就需要自己配证书：

```yaml
env:
  - name: KC_HTTPS_CERTIFICATE_FILE
    value: /opt/keycloak/conf/tls.crt
  - name: KC_HTTPS_CERTIFICATE_KEY_FILE
    value: /opt/keycloak/conf/tls.key
```

但我们的架构中 Keycloak 在 Gateway 后面，所以不需要。

### 2.3 SAML 场景也不需要

SAML 登录是通过浏览器重定向完成的，Keycloak 服务端不直接访问客户的 AD：

```
1. 浏览器 → Keycloak（请求登录）
2. Keycloak 返回 302 → 浏览器跳转到客户 AD 登录页
3. 浏览器 → 客户 AD（输入账号密码）
4. AD 成功 → 浏览器带 SAML 断言跳转回 Keycloak
5. Keycloak 验证断言，签发 JWT
```

所有通信经过浏览器，Keycloak 不需要正向代理，也不需要额外证书。

---

## 3 证书替换

### 3.1 替换操作

```bash
# 用新证书替换 Secret（不需要重启 Gateway）
kubectl create secret tls https \
  -n agentgateway-system \
  --key new-gateway.key \
  --cert new-gateway.crt \
  --dry-run=client -o yaml | kubectl apply -f -
```

Gateway 自动检测到 Secret 变更，热加载新证书，不中断服务。

### 3.2 可能遇到的问题

| 问题 | 原因 | 解决方案 |
|------|------|---------|
| 证书和私钥不匹配 | 客户提供的 .crt 和 .key 不是一对 | 验证：`openssl x509 -noout -modulus -in new.crt | md5sum` 和 `openssl rsa -noout -modulus -in new.key | md5sum` 输出应一致 |
| 证书链不完整 | 客户只提供了服务端证书，没包含中间 CA | 拼接完整链：`cat server.crt intermediate.crt > fullchain.crt`，用 fullchain.crt 创建 Secret |
| 域名不匹配 | 新证书的 SAN（Subject Alternative Name）不包含实际访问的域名 | 检查：`openssl x509 -noout -text -in new.crt | grep DNS` 确认包含所需域名 |
| 证书已过期 | 客户提供的证书已经过期或即将过期 | 检查：`openssl x509 -noout -dates -in new.crt` |
| Secret 命名不一致 | 新建的 Secret 名字和 Gateway 引用的不一致 | 确认 Gateway YAML 中 `certificateRefs.name` 和 Secret 名字一致 |
| 客户端不信任新 CA | 换了 CA 签发的证书，客户端没有信任新 CA | 客户端需要更新 CA 信任库，或使用公共 CA（如 Let's Encrypt）签发的证书 |

### 3.3 替换前验证脚本

```bash
#!/bin/bash
# 替换前验证证书有效性

CERT_FILE=$1
KEY_FILE=$2

echo "=== 检查证书有效期 ==="
openssl x509 -noout -dates -in $CERT_FILE

echo "=== 检查证书域名 ==="
openssl x509 -noout -text -in $CERT_FILE | grep -A1 "Subject Alternative Name"

echo "=== 检查证书和私钥是否匹配 ==="
CERT_MD5=$(openssl x509 -noout -modulus -in $CERT_FILE | md5sum | awk '{print $1}')
KEY_MD5=$(openssl rsa -noout -modulus -in $KEY_FILE | md5sum | awk '{print $1}')

if [ "$CERT_MD5" == "$KEY_MD5" ]; then
    echo "✅ 证书和私钥匹配"
else
    echo "❌ 证书和私钥不匹配！"
    exit 1
fi

echo "=== 检查证书链 ==="
openssl verify -CAfile ca.crt $CERT_FILE
```

### 3.4 对业务的影响

证书替换**不影响任何业务逻辑**：
- 不影响 JWT 认证（JWT 用 Keycloak 的签名密钥，和 TLS 证书无关）
- 不影响 OPA 鉴权
- 不影响后端应用
- Gateway 热加载，不中断服务

---

## 4 性能统计（Metrics）方案

### 4.1 三种方案对比

| 方案 | 组件 | 能力 | 复杂度 | 适合场景 |
|------|------|------|--------|---------|
| **Gateway 自带 Metrics** | 无需额外组件 | 控制面指标（Prometheus 格式） | 最简单 | 快速查看系统健康状态 |
| **Jaeger** | Jaeger + TrafficPolicy | 分布式链路追踪（每个请求的耗时分解） | 中等 | 排查单个请求性能问题 |
| **Grafana + Prometheus + Tempo** | OTel Collector + Prometheus + Tempo + Grafana | 完整可观测性（metrics + traces + logs + 仪表盘） | 较高 | 生产环境长期监控 |

### 4.2 Gateway 自带 Prometheus Metrics

> 官方文档：https://agentgateway.dev/docs/kubernetes/latest/observability/control-plane-metrics/

Gateway 控制面**默认就暴露 Prometheus 格式的 metrics**，不需要任何额外配置。

#### 访问方式

```bash
kubectl -n agentgateway-system port-forward deployment/agentgateway 9092
curl http://localhost:9092/metrics
```

#### 包含的指标

| 指标名 | 类型 | 标签 | 说明 |
|--------|------|------|------|
| `agentgateway_controller_reconcile_duration_seconds` | Histogram | controller, name, namespace | 控制面处理配置变更的耗时分布 |
| `agentgateway_controller_reconciliations_running` | Gauge | controller, name, namespace | 当前正在处理的配置变更数量 |
| `agentgateway_controller_reconciliations_total` | Counter | controller, name, namespace, result | 配置变更总次数（按成功/失败分） |
| `agentgateway_xds_auth_rq_total` | Counter | — | xDS 认证请求总数 |
| `agentgateway_xds_auth_rq_success_total` | Counter | — | xDS 认证成功数 |
| `agentgateway_xds_auth_rq_failure_total` | Counter | — | xDS 认证失败数 |
| `agentgateway_xds_rejects_total` | Counter | — | 被代理拒绝的 xDS 响应数 |

#### 数据格式（Prometheus 文本格式，不是 OTLP）

```
# HELP agentgateway_controller_reconcile_duration_seconds Reconcile duration for controller
# TYPE agentgateway_controller_reconcile_duration_seconds histogram
agentgateway_controller_reconcile_duration_seconds_bucket{controller="gateway",name="https",namespace="agentgateway-system",le="0.005"} 10
agentgateway_controller_reconcile_duration_seconds_bucket{controller="gateway",name="https",namespace="agentgateway-system",le="0.01"} 15
agentgateway_controller_reconcile_duration_seconds_bucket{controller="gateway",name="https",namespace="agentgateway-system",le="0.025"} 20
agentgateway_controller_reconcile_duration_seconds_bucket{controller="gateway",name="https",namespace="agentgateway-system",le="+Inf"} 25
agentgateway_controller_reconcile_duration_seconds_sum{controller="gateway",name="https",namespace="agentgateway-system"} 0.342
agentgateway_controller_reconcile_duration_seconds_count{controller="gateway",name="https",namespace="agentgateway-system"} 25

# HELP agentgateway_controller_reconciliations_total Total number of controller reconciliations
# TYPE agentgateway_controller_reconciliations_total counter
agentgateway_controller_reconciliations_total{controller="gateway",name="https",namespace="agentgateway-system",result="success"} 23
agentgateway_controller_reconciliations_total{controller="gateway",name="https",namespace="agentgateway-system",result="error"} 2

# HELP agentgateway_xds_auth_rq_total Total number of xDS auth requests
# TYPE agentgateway_xds_auth_rq_total counter
agentgateway_xds_auth_rq_total 1520
agentgateway_xds_auth_rq_success_total 1518
agentgateway_xds_auth_rq_failure_total 2
```

**格式说明：**
- 这是 Prometheus 标准文本格式，不是 OTLP/JSON
- 每行一个数据点，格式为 `metric_name{label="value"} 数值`
- `# HELP` 是指标说明，`# TYPE` 是指标类型
- Histogram 类型会自动生成 `_bucket`、`_sum`、`_count` 三组数据

### 4.3 方案 A：Jaeger（链路追踪）

> 官方教程：https://agentgateway.dev/docs/kubernetes/latest/tutorials/telemetry/

#### 部署 Jaeger

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: telemetry
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: jaeger
  namespace: telemetry
spec:
  replicas: 1
  selector:
    matchLabels:
      app: jaeger
  template:
    metadata:
      labels:
        app: jaeger
    spec:
      containers:
        - name: jaeger
          image: jaegertracing/all-in-one:latest
          ports:
            - containerPort: 16686
              name: ui
            - containerPort: 4317
              name: otlp-grpc
---
apiVersion: v1
kind: Service
metadata:
  name: jaeger
  namespace: telemetry
spec:
  selector:
    app: jaeger
  ports:
    - port: 16686
      targetPort: 16686
      name: ui
    - port: 4317
      targetPort: 4317
      name: otlp-grpc
```

#### 配置 TrafficPolicy

```yaml
apiVersion: gateway.networking.k8s.io/v1alpha2
kind: TrafficPolicy
metadata:
  name: tracing
  namespace: agentgateway-system
spec:
  targetRefs:
    - kind: Gateway
      name: https
      group: gateway.networking.k8s.io
  frontend:
    tracing:
      backendRef:
        name: jaeger
        namespace: telemetry
        port: 4317
      protocol: GRPC
      randomSampling: "true"       # 测试：全量采样；生产改 "0.1"（10%）
```

#### Trace Span 完整字段

每个请求经过 Gateway 时产生一条 Span，格式为 OTLP JSON。以下是官方文档给出的实际字段：

**完整 JSON 示例：**

```json
{
  "traceId": "2864d2f682a85ba0c44cb5122d2d11e5",
  "spanId": "947515b6316f7931",
  "parentId": "",
  "name": "POST /*",
  "kind": "Server",
  "startTime": "2026-04-02T10:00:00.123Z",
  "endTime": "2026-04-02T10:00:00.281Z",
  "status": "Unset",
  "attributes": {
    "http.method": "GET",
    "http.path": "/knowledgebase/v1/kb",
    "http.host": "gateway.aidp.com",
    "http.version": "HTTP/1.1",
    "http.status": 200,

    "src.addr": "192.168.1.100:50314",
    "url.scheme": "https",
    "protocol": "http",
    "network.protocol.version": "1.1",
    "duration": "158ms",

    "gateway": "agentgateway-system/agentgateway-proxy",
    "listener": "https",
    "route": "agentgateway-system/kb-route",
    "endpoint": "10.244.0.31:8080"
  }
}
```

**字段详解：**

| 分类 | 字段 | 类型 | 说明 | 示例 |
|------|------|------|------|------|
| **Span 元数据** | `traceId` | string | 唯一链路 ID，同一请求的所有 Span 共享 | `2864d2f682a85ba0c44cb5122d2d11e5` |
| | `spanId` | string | 当前 Span 的唯一 ID | `947515b6316f7931` |
| | `parentId` | string | 父 Span ID（根 Span 为空） | `""` |
| | `name` | string | 操作名称 | `POST /*` |
| | `kind` | string | Span 类型 | `Server` |
| | `status` | string | 状态 | `Unset` |
| **时间与耗时** | `startTime` | timestamp | Gateway 收到请求的时间 | `2026-04-02T10:00:00.123Z` |
| | `endTime` | timestamp | Gateway 返回响应的时间 | `2026-04-02T10:00:00.281Z` |
| | `duration` | string | 总耗时（endTime - startTime） | `158ms` |
| **HTTP 请求** | `http.method` | string | HTTP 方法 | `GET` / `POST` / `PUT` / `DELETE` |
| | `http.path` | string | 请求路径 | `/knowledgebase/v1/kb` |
| | `http.host` | string | 请求域名 | `gateway.aidp.com` |
| | `http.version` | string | HTTP 版本 | `HTTP/1.1` |
| | `http.status` | integer | 响应状态码 | `200` / `403` / `500` |
| **网络** | `src.addr` | string | 客户端 IP 和端口 | `192.168.1.100:50314` |
| | `url.scheme` | string | 协议方案 | `https` / `http` |
| | `protocol` | string | 协议类型 | `http` |
| | `network.protocol.version` | string | 协议版本 | `1.1` / `2` |
| **Gateway 路由** | `gateway` | string | Gateway 资源名称 | `agentgateway-system/agentgateway-proxy` |
| | `listener` | string | 匹配的 Listener 名称 | `https` |
| | `route` | string | 匹配的 HTTPRoute 名称 | `agentgateway-system/kb-route` |
| | `endpoint` | string | 后端 Pod 地址 | `10.244.0.31:8080` |

**耗时说明：**

`duration` = 请求从进入 Gateway 到返回响应的总时间，包含：
- Gateway 自身处理（路由匹配、TLS 解密）— 通常 1-5ms
- pep-proxy 鉴权（JWT 验证 + OPA）— 通常几毫秒
- 后端服务处理 — 主要耗时
- 后端返回 → Gateway 返回

```
Gateway 收到请求 (startTime)
  │  Gateway 路由匹配 + TLS          ~2ms
  │  pep-proxy JWT + OPA             ~5ms
  │  转发到后端 → 后端处理            ~150ms  ← 主要耗时
  │  后端返回 → Gateway 返回          ~1ms
Gateway 返回响应 (endTime)
duration = 158ms
```

无法拆分 Gateway 自身耗时和后端耗时。但 Gateway 通常只占几毫秒，可近似认为 `duration ≈ 后端耗时`。如需精确拆分，后端服务需自行接入 OpenTelemetry 上报 Span，两边通过 `traceId` 自动串联。

**可用于统计的场景：**

| 统计需求 | 用哪些字段 |
|----------|-----------|
| 每个应用的 QPS | 按 `route` 分组计数 |
| 每个接口的延迟 | 按 `http.path` 分组，统计 `duration` 均值/P99 |
| 错误率 | `http.status >= 400` 的比例 |
| 哪个后端 Pod 慢 | 按 `endpoint` 分组，统计 `duration` |
| 按来源 IP 分析 | 按 `src.addr` 分组 |

#### 自定义属性（可选）

通过 TrafficPolicy 的 `attributes` 配置，可以把 Header 信息也加入 Span：

```yaml
frontend:
  tracing:
    backendRef:
      name: jaeger
      namespace: telemetry
      port: 4317
    protocol: GRPC
    randomSampling: "true"
    attributes:
      add:
        - expression: 'request.headers["X-Auth-User-Id"]'
          name: user_id
        - expression: 'request.headers["X-Auth-Tenant"]'
          name: tenant_id
        - expression: 'request.headers["X-Auth-Groups"]'
          name: groups
```

配置后 Span 中会多出：

```json
{
  "attributes": {
    "user_id": "zhangsan",
    "tenant_id": "aidp",
    "groups": "data-team,all-users,knowledgebase-admins",
    "http.method": "GET",
    "http.path": "/knowledgebase/v1/kb",
    "route": "agentgateway-system/kb-route",
    ...
  }
}
```

增加自定义属性后可额外统计：

| 统计需求 | 字段 |
|----------|------|
| 每个用户的请求量 | `user_id` |
| 每个租户的 QPS | `tenant_id` |
| 每个角色组的访问分布 | `groups` |

#### 查看 Jaeger UI

```bash
kubectl port-forward -n telemetry svc/jaeger 16686:16686
# 浏览器打开 http://localhost:16686
```

### 4.4 方案 B：Grafana + Prometheus + Tempo（完整可观测性）

> 官方文档：https://agentgateway.dev/docs/kubernetes/latest/observability/otel-stack/

#### 架构

```
Gateway Proxy
  │
  ├── traces(OTLP) ──→ OTel Collector(traces) ──→ Tempo ──→ Grafana
  ├── metrics ────────→ OTel Collector(metrics) ──→ Prometheus ──→ Grafana
  └── logs(OTLP) ────→ OTel Collector(logs) ────→ Loki ──→ Grafana
```

三个独立的 OTel Collector，分别负责采集 metrics、traces、logs，发到不同的后端存储，Grafana 统一展示。

#### 部署步骤

```bash
# 1. 部署 Tempo（存储 traces）
helm upgrade --install tempo grafana/tempo \
  --namespace telemetry --create-namespace \
  --set tempo.receivers.otlp.protocols.grpc.endpoint="0.0.0.0:4317"

# 2. 部署 Loki（存储 logs）
helm upgrade --install loki grafana/loki \
  --namespace telemetry

# 3. 部署 Prometheus + Grafana
helm upgrade --install kube-prometheus-stack prometheus-community/kube-prometheus-stack \
  --namespace telemetry \
  --set grafana.enabled=true \
  --set prometheus.prometheusSpec.enableRemoteWriteReceiver=true

# 4. 部署 OTel Collectors（metrics/traces/logs 各一个）
# 参考官方文档的 Helm values 配置
```

#### Grafana 访问

```bash
kubectl port-forward -n telemetry svc/kube-prometheus-stack-grafana 3000:80
# 浏览器打开 http://localhost:3000
# 默认账号 admin/prom-operator
```

AgentGateway 提供了现成的 Grafana Dashboard，通过 ConfigMap 自动导入。

---

## 5 自定义 Metrics 采集方案

如果不使用 Jaeger/Grafana/Prometheus 等工具，可以自己采集 Gateway 的 metrics 数据。

### 5.1 方案：定时抓取 Prometheus 端点，写入共享文件

Gateway 的 metrics 是标准的 Prometheus 文本格式，可以用任何 HTTP 客户端定时抓取。

#### 采集脚本

```python
#!/usr/bin/env python3
"""
定时抓取 Gateway metrics，写入共享文件供其他系统读取
"""
import requests
import time
import json
import re
from datetime import datetime

METRICS_URL = "http://localhost:9092/metrics"    # port-forward 后的地址
OUTPUT_FILE = "/shared/gateway-metrics.jsonl"    # 共享文件路径
INTERVAL = 30                                    # 采集间隔（秒）

def parse_prometheus_metrics(text):
    """解析 Prometheus 文本格式为结构化数据"""
    metrics = []
    for line in text.strip().split("\n"):
        if line.startswith("#") or not line.strip():
            continue

        # 解析格式：metric_name{label="value"} 数值
        match = re.match(r'^(\w+)(\{(.+?)\})?\s+(.+)$', line)
        if match:
            name = match.group(1)
            labels_str = match.group(3) or ""
            value = float(match.group(4))

            # 解析标签
            labels = {}
            if labels_str:
                for pair in re.findall(r'(\w+)="([^"]*)"', labels_str):
                    labels[pair[0]] = pair[1]

            metrics.append({
                "name": name,
                "labels": labels,
                "value": value
            })
    return metrics

def collect():
    while True:
        try:
            resp = requests.get(METRICS_URL, timeout=5)
            metrics = parse_prometheus_metrics(resp.text)

            record = {
                "timestamp": datetime.utcnow().isoformat() + "Z",
                "metrics": metrics
            }

            # 追加写入 JSONL 文件（每行一条 JSON）
            with open(OUTPUT_FILE, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")

            print(f"[{record['timestamp']}] 采集到 {len(metrics)} 条指标")

        except Exception as e:
            print(f"采集失败: {e}")

        time.sleep(INTERVAL)

if __name__ == "__main__":
    collect()
```

#### 输出文件格式（JSONL）

```json
{"timestamp":"2026-04-02T10:00:00Z","metrics":[{"name":"agentgateway_controller_reconciliations_total","labels":{"controller":"gateway","result":"success"},"value":23},{"name":"agentgateway_xds_auth_rq_total","labels":{},"value":1520}]}
{"timestamp":"2026-04-02T10:00:30Z","metrics":[{"name":"agentgateway_controller_reconciliations_total","labels":{"controller":"gateway","result":"success"},"value":25},{"name":"agentgateway_xds_auth_rq_total","labels":{},"value":1580}]}
```

#### 在 K8s 中部署为 Sidecar 或 CronJob

```yaml
# 方式 1：作为 CronJob 定时运行
apiVersion: batch/v1
kind: CronJob
metadata:
  name: metrics-collector
  namespace: agentgateway-system
spec:
  schedule: "*/1 * * * *"    # 每分钟执行一次
  jobTemplate:
    spec:
      template:
        spec:
          containers:
            - name: collector
              image: python:3.11-slim
              command: ["python", "/scripts/collect.py"]
              volumeMounts:
                - name: shared
                  mountPath: /shared
                - name: scripts
                  mountPath: /scripts
          volumes:
            - name: shared
              persistentVolumeClaim:
                claimName: metrics-pvc
            - name: scripts
              configMap:
                name: metrics-collector-script
          restartPolicy: OnFailure
```

### 5.2 方案：OTel Collector 转存到文件

如果需要采集 Trace 数据（不仅是 metrics），可以部署 OTel Collector，把数据直接写文件：

```yaml
# OTel Collector 配置
receivers:
  otlp:
    protocols:
      grpc:
        endpoint: "0.0.0.0:4317"

exporters:
  file:
    path: /shared/traces.jsonl       # 每行一条 JSON
    rotation:
      max_megabytes: 100             # 单文件最大 100MB
      max_days: 7                    # 保留 7 天
      max_backups: 5                 # 最多 5 个备份文件

service:
  pipelines:
    traces:
      receivers: [otlp]
      exporters: [file]
```

TrafficPolicy 指向这个 OTel Collector 即可，trace 数据会以 OTLP JSON 格式写入文件。

### 5.3 两种数据格式对比

| 数据来源 | 格式 | 内容 | 采集方式 |
|----------|------|------|---------|
| `/metrics` 端点 | Prometheus 文本格式 | 控制面聚合指标（QPS、延迟分布、错误率） | HTTP GET 定时抓取 |
| TrafficPolicy tracing | OTLP JSON | 每个请求的链路明细（路径、耗时、状态码） | OTel Collector 接收后写文件 |

两者可以同时使用：metrics 看整体趋势，traces 看单个请求细节。
