# OPA Dynamic Policy — 动态授权策略系统

基于 [OPA](https://www.openpolicyagent.org/) + [OPAL](https://github.com/permitio/opal) 的多租户动态授权系统。策略无需重启服务即可实时更新，所有变更在秒级内生效。

---

## 目录

- [项目目录结构](#项目目录结构)
- [架构概览](#架构概览)
- [组件说明](#组件说明)
- [端口与 URL 一览](#端口与-url-一览)
- [JWT 认证配置](#jwt-认证配置)
- [三层角色模型](#三层角色模型)
- [策略存储位置](#策略存储位置)
- [策略模板](#策略模板)
- [对外暴露的 API](#对外暴露的-api)
  - [PEP Proxy（端口 8000）](#pep-proxy端口-8000)
  - [Bundle Server（端口 8001）](#bundle-server端口-8001)
- [Agentgateway 集成（gRPC ext-authz）](#agentgateway-集成grpc-ext-authz)
- [动态更新策略流程](#动态更新策略流程)
- [Bundle Server 如何拉取并分发策略](#bundle-server-如何拉取并分发策略)
- [部署](#部署)
- [本地测试](#本地测试)
- [已知限制与注意事项](#已知限制与注意事项)

---

## 项目目录结构

```
opal-dynamic-policy/
│
├── Dockerfile                        # 统一镜像构建：pep-proxy + bundle-server 打包为 opal-proxy
├── supervisord.conf                  # 进程管理：在单容器内同时运行 pep-proxy(:8000) 和 bundle-server(:8001)
├── requirements.txt                  # 所有 Python 依赖（pep-proxy + bundle-server 共用）
│
├── pep-proxy/                        # 策略执行点（Policy Enforcement Point）
│   ├── app/
│   │   ├── main.py                   # FastAPI 入口：REST API 路由 + gRPC 服务启动
│   │   ├── auth.py                   # JWT 验证：OIDC RS256 / HS256 双模式，含 JWKS 缓存；提取 roles + role_ids
│   │   ├── grpc_server.py            # gRPC ext-authz 服务（agentgateway 集成，端口 9000）
│   │   ├── models.py                 # Pydantic 数据模型：PolicyCreateRequest / RoleBindingRequest / AuthRequest
│   │   └── storage.py                # 模板内存存储（仅用于 Rego 模板读取，策略已迁移至 PostgreSQL）
│   └── proto/
│       └── ext_authz.proto           # Envoy ext-authz v3 协议定义（最小化自包含，无外部依赖）
│
├── bundle-server/
│   └── app/
│       └── main.py                   # FastAPI 入口：PostgreSQL 策略持久化、OPA bundle 生成、直接推送 OPA
│
├── k8s/
│   ├── deployment.yaml               # K8s 部署：PostgreSQL / opal-server / pep-proxy（含 opal-client sidecar）
│   ├── service.yaml                  # K8s Service：pep-proxy(8000/9000) / bundle-server(8001) / opal-server(7002)
│   └── agentgateway.yaml             # Agentgateway 路由：Gateway / HTTPRoute / AgentgatewayPolicy（gRPC ext-authz）
│
├── deploy.sh                         # 一键部署：构建镜像 → kind load → kubectl apply（含 agentgateway 可选安装）
├── build-image.sh                    # 仅构建并加载 Docker 镜像（不部署）
├── test.sh                           # 完整集成测试：10 个测试节（含 gRPC TCP 检测和 gateway 端到端测试）
├── cleanup.sh                        # 清理所有 K8s 资源
└── diagnose.sh                       # 故障诊断：检查各组件状态、日志、OPA 策略
```

### 各文件详细说明

#### 根目录

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `Dockerfile` | 使用 `python:3.10-slim` 基础镜像。安装依赖 → 编译 proto → 冒烟测试验证导入。`ENV PYTHONPATH=/app` 确保生成的 proto 存根（`/app/ext_authz_pb2.py`）始终可被 Python 找到。 | `grpcio-tools` 在构建时将 `ext_authz.proto` 编译为 `ext_authz_pb2.py` / `ext_authz_pb2_grpc.py` 并输出到 `/app`。修改 proto 文件后需重新构建镜像。 |
| `supervisord.conf` | 在同一容器内以两个独立进程运行 pep-proxy 和 bundle-server。两个进程的日志均写入 `/var/log/supervisor/*.err` 文件，**不输出到 stdout**，因此 `kubectl logs` 看不到应用日志。 | 查看应用日志须用：`kubectl exec -n opal-dynamic-policy deploy/pep-proxy -c opal-proxy -- tail -f /var/log/supervisor/pep-proxy.err` |
| `requirements.txt` | pep-proxy 和 bundle-server 共用同一份依赖。包含 `grpcio==1.59.3` 和 `grpcio-tools==1.59.3`（用于 ext-authz gRPC 服务）。 | 升级 grpcio 版本时需同步升级 grpcio-tools，两者版本号必须一致。 |

#### pep-proxy/app/

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `main.py` | FastAPI 主入口。定义所有 REST 路由，在 startup 事件中加载模板并启动 gRPC 服务。 | `_grpc_task` 是模块级变量，用于持有 gRPC Task 的强引用。若不持有强引用，asyncio 可能在 GC 时静默回收该 Task，导致端口 9000 无监听。 |
| `auth.py` | JWT 验证核心。优先使用 OIDC RS256（通过 discovery 端点动态获取 JWKS），`OIDC_BASE_URL` 未设置时回退到 HS256。JWKS 按 issuer 缓存 5 分钟（`JWKS_CACHE_TTL=300`）。 | HS256 模式仅供开发/测试，生产环境必须配置 `OIDC_BASE_URL`。JWKS 缓存在内存中，不跨 Pod 共享，多副本场景各 Pod 独立缓存。 |
| `grpc_server.py` | 实现 `envoy.service.auth.v3.Authorization/Check` gRPC 接口（端口 9000）。优先读取 agentgateway 注入的 `dev.agentgateway.jwt` gRPC metadata（已预验证），回退到直接解析 Bearer token（不验签，开发/无 agentgateway 场景）。 | `super_admin` 角色不受租户 ID 限制，OPA 直接放行。ext-authz 路径中 `input.token` 可能为空（agentgateway 可能不转发原始 Bearer Header），OPA Rego 需从 `input.roles` / `input.tenant_id` 作为 fallback。 |
| `models.py` | Pydantic 数据模型。`PolicyCreateRequest`（resource + effect）、`RoleBindingRequest`（policy_ids + tenant_id）、`AuthRequest`（resource + tenant_id，无 action）。 | — |
| `storage.py` | 模板内存存储（仅用于 `role_based` Rego 模板的读取）。策略数据已全部迁移至 PostgreSQL，此文件不再负责策略持久化。 | — |

#### pep-proxy/proto/

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `ext_authz.proto` | Envoy ext-authz v3 协议的最小化自包含版本，只保留 agentgateway 实际用到的字段。无外部 proto import 依赖。 | `package envoy.service.auth.v3` 声明必须与真实 Envoy ext-authz 服务名一致，agentgateway 通过该服务名路由 gRPC 请求。修改 package 名会导致 agentgateway 无法找到服务。 |

#### bundle-server/app/

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `main.py` | Bundle Server 全部逻辑：**PostgreSQL 策略持久化**（`policies` 表存 resource+effect，`role_policy_bindings` 表存角色绑定）、生成 OPA bundle tar.gz、通过 OPA REST API 直接推送 Rego + 数据、通知 OPAL Server 广播更新。`_generate_combined_rego()` 动态生成包含 UUID role 判断逻辑的 Rego。 | `_generate_combined_rego()` 使用 Python raw string（`r"""`）生成 Rego 代码，无需转义花括号。OPA 数据结构：`data.tenants[tenant_id].policies`（id→{resource,effect}）+ `data.tenants[tenant_id].role_bindings`（role_uuid→[policy_ids]）。 |

#### k8s/

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `deployment.yaml` | 定义三个工作负载：PostgreSQL StatefulSet（opal-server pub/sub 后端）、opal-server Deployment（2 副本）、pep-proxy Deployment（1 副本，含 `opal-proxy` 和 `opal-client` 两个容器）。pep-proxy 使用 `emptyDir` 卷存储数据，限制为 1 副本。 | 文件中 `OPA_URL` 环境变量出现了两次（第 176、178 行），后者覆盖前者，值相同所以不影响运行，可清理。 |
| `service.yaml` | 为每个组件创建 ClusterIP Service。`bundle-server` Service 指向与 `pep-proxy` 相同的 Pod（selector 为 `app: pep-proxy`）。`pep-proxy` Service 同时暴露 8000（HTTP）和 9000（gRPC）。 | bundle-server 没有独立的 Deployment，其 Service selector 指向 pep-proxy Pod 的 :8001 端口。 |
| `agentgateway.yaml` | 配置 agentgateway 路由和授权策略。`AgentgatewayPolicy` 通过 gRPC 调用 pep-proxy:9000 进行 ext-authz 检查，`targetRefs` 指向 `HTTPRoute/policy-api-route`（而非 Gateway），确保只对该路由生效。`ReferenceGrant` 允许 `agentgateway-system-opa` 命名空间跨命名空间引用 `opal-dynamic-policy` 中的 Service。 | agentgateway 控制器在 `agentgateway-system` 命名空间，代理 Pod 自动创建在 `agentgateway-system-opa` 命名空间。MCP 监听器（:3000）默认已注释，需要 MCP 工具服务器时再取消注释。 |

#### 脚本

| 文件 | 说明 | 注意事项 |
|------|------|---------|
| `deploy.sh` | 完整部署流程：构建镜像 → kind load → 创建命名空间 → apply K8s YAML → 等待 Pod 就绪。`DEPLOY_AGENTGATEWAY=true` 时通过 helm 安装 agentgateway，并 patch 代理 Deployment 使用本地镜像。 | `SKIP_BUILD=true` 可跳过镜像构建步骤。agentgateway 安装需要 helm 和网络访问 `ghcr.io`。 |
| `test.sh` | 10 个测试节覆盖全部功能。Section 2 检查 gRPC 端口 9000 TCP 连通性；Section 8 测试 issuer 租户提取和伪造 token 拒绝；Section 10 需要 `TEST_GATEWAY=true` 才会运行 agentgateway 端到端测试。 | gateway 测试（Section 10）中列表、策略、auth/check 路由均需使用 `ADMIN_JWT`，因为 OPA ext-authz 会检查这些操作的权限，普通 viewer 没有 `templates:list`、`policies:list`、`auth:read` 权限。 |
| `cleanup.sh` | 删除 `opal-dynamic-policy` 和 `agentgateway-system-opa` 命名空间下所有资源。 | **不可逆操作**，执行前确认不需要保留任何数据。 |
| `diagnose.sh` | 诊断脚本：检查各 Pod 状态、打印关键日志、查询 OPA 当前策略内容和数据。遇到问题时首先运行此脚本。 | — |

---

## 架构概览

```
外部请求
    │
    ▼
┌─────────────────────────────────────────────────────────┐
│  pep-proxy Pod（单 Pod，replicas=1）                      │
│                                                         │
│  ┌──────────────────┐   ┌──────────────────┐           │
│  │  opal-proxy      │   │  opal-client     │           │
│  │  容器（supervisord）│  │  容器            │           │
│  │                  │   │                  │           │
│  │  pep-proxy :8000 │   │  OPAL Client     │           │
│  │  bundle-server   │   │  + 内嵌 OPA      │           │
│  │  :8001           │   │  :8181           │           │
│  │  gRPC ext-authz  │   │                  │           │
│  │  :9000           │   │                  │           │
│  │                  │   │                  │           │
│  │  /app/data  ◄────┼───┼── emptyDir       │           │
│  │  /app/bundles    │   │                  │           │
│  └──────────────────┘   └──────────────────┘           │
└─────────────────────────────────────────────────────────┘
         │ WebSocket                │ REST
         │ 策略数据推送              │ OPA 查询 :8181
         ▼                          │
┌─────────────────┐                 │
│  opal-server    │                 │
│  :7002          │                 │
│  （2 replicas）  │                 │
└────────┬────────┘                 │
         │ pub/sub                  │
         ▼                          │
┌─────────────────┐       pep-proxy ──► OPA /v1/data/authz/allow
│  PostgreSQL     │
│  :5432          │
└─────────────────┘
```

**核心设计原则**：
- pep-proxy 和 bundle-server 打包为同一镜像（`opal-proxy`），通过 supervisord 在一个容器内运行，共享 `/app/data` 文件系统。
- OPA 以 OPAL Client 内嵌模式运行（`OPAL_INLINE_OPA_ENABLED=true`），与 pep-proxy 在同一 Pod 内通过 localhost:8181 通信。
- 策略变更时，bundle-server 直接通过 OPA REST API 推送，**无需等待轮询**，变更立即生效。

---

## 组件说明

| 组件 | 镜像 | 职责 |
|------|------|------|
| **pep-proxy** | `opal-proxy:latest` | 策略执行点：接收 API 请求，调用 OPA 进行授权检查；提供策略管理 REST API（委托 bundle-server 持久化）|
| **bundle-server** | `opal-proxy:latest`（同镜像）| 策略数据源：PostgreSQL 策略持久化、生成 OPA bundle、直接推送策略和数据到 OPA |
| **opal-client** | `permitio/opal-client:0.7.4` | 内嵌 OPA 引擎（:8181），连接 OPAL Server 接收实时数据推送 |
| **opal-server** | `permitio/opal-server:0.7.4` | 策略分发中枢：接收 bundle-server 的变更通知，通过 WebSocket 广播给所有 OPAL Client |
| **PostgreSQL** | `postgres:17` | 双重用途：① OPAL Server 多副本 pub/sub 后端；② bundle-server 策略持久化（policies + role_policy_bindings 表）|

---

## 端口与 URL 一览

### 进程端口

| 端口 | 进程 | 说明 |
|------|------|------|
| **8000** | pep-proxy (uvicorn) | 对外唯一入口：授权检查 + 策略管理 API |
| **8001** | bundle-server (uvicorn) | OPA bundle 端点 + 数据 API（集群内部使用）|
| **8181** | OPA（opal-client 内嵌）| OPA REST API，仅 Pod 内通信（localhost）|
| **7002** | opal-server | OPAL Server HTTP + WebSocket |
| **5432** | PostgreSQL | OPAL Server pub/sub 数据库 |
| **9000** | gRPC ext-authz（pep-proxy 内）| agentgateway ext-authz 检查端口，由 FastAPI startup 事件异步启动 |

### K8s Service（ClusterIP）

| Service 名称 | 端口 | 路由目标 |
|-------------|------|---------|
| `pep-proxy` | 8000 | pep-proxy Pod :8000 |
| `pep-proxy` | 9000 | pep-proxy Pod :9000（gRPC ext-authz）|
| `bundle-server` | 8001 | pep-proxy Pod :8001（同 Pod，不同进程）|
| `opal-server` | 7002 | opal-server Pod :7002 |
| `postgres` | 5432 | PostgreSQL StatefulSet |

### 环境变量 URL

| 环境变量 | 默认值 | 说明 |
|---------|--------|------|
| `OPA_URL` | `http://localhost:8181` | OPA REST API 地址（Pod 内 localhost）|
| `BUNDLE_SERVER_URL` | `http://localhost:8001` | pep-proxy 调用 bundle-server 的地址（Pod 内）|
| `BUNDLE_SERVER_SELF_URL` | `http://bundle-server:8001` | OPAL Server 回调 bundle-server 的 K8s Service 地址 |
| `OPAL_SERVER_URL` | `http://opal-server:7002` | bundle-server 通知 OPAL Server 的地址 |
| `OIDC_BASE_URL` | `""` | OIDC 提供商基础 URL（生产环境设置，见下文）|
| `JWT_SECRET` | `jwt-secret` | HS256 共享密钥（仅开发/测试用）|

---

## JWT 认证配置

系统支持两种 JWT 验证模式，优先使用 OIDC：

### 生产模式：OIDC（RS256）

设置 `OIDC_BASE_URL`（如 `https://auth.example.com`）后，系统通过以下流程验证 JWT：

**Python 层（pep-proxy auth.py）**：
```
GET {iss}/.well-known/openid-configuration
  → 提取 jwks_uri
  → GET {jwks_uri}  （JWKS 按 issuer 缓存 5 分钟）
  → 使用 RSA 公钥验证 JWT 签名
```

**OPA 层（Rego 策略内）**：
```
直接 GET {iss}/protocol/certs （Keycloak JWKS 端点）
  → io.jwt.decode_verify(input.token, {"jwks_url": url})
```

issuer 格式为 `{OIDC_BASE_URL}/realms/{tenant_id}`，系统从 `/realms/` 后提取 `tenant_id`，实现**多租户 JWKS 隔离**。

### 开发模式：HS256 fallback

`OIDC_BASE_URL` 为空时，使用 `JWT_SECRET` 环境变量进行 HMAC-SHA256 验证。OPA Rego 在无法构造 JWKS URL 时同样回退到不验签模式（仅解析 claims）。

### JWT Payload 必须包含的 Claims

```json
{
  "sub": "user-id",
  "iss": "https://auth.example.com/realms/tenant-001",
  "tenant_id": "tenant-001",
  "roles": ["viewer"],
  "role_ids": ["11111111-1111-1111-1111-111111111111"],
  "email": "user@example.com"
}
```

| Claim | 说明 |
|-------|------|
| `roles` | 字符串列表，用于系统角色判断（`super_admin`、`tenant_admin`）|
| `role_ids` | UUID 列表，用于普通用户的业务权限查询（role_bindings → policies）|

> `tenant_id` 的提取优先从 `iss` 的 `/realms/` 路径段提取，其次才读取 `tenant_id` claim。两者建议保持一致。

---

## 三层角色模型

| 角色 | 判断依据 | 作用域 | 权限 |
|------|---------|--------|------|
| `super_admin` | JWT `roles` 字符串 | 跨租户 | 无限制访问任意租户任意资源 |
| `tenant_admin` | JWT `roles` 字符串 | 单租户内 | 本租户全部资源完整权限；可创建/修改策略和角色绑定 |
| 普通用户 | JWT `role_ids`（UUID 列表）| 单租户内 | 通过 `role_ids` → `role_bindings` → `policies` 匹配，仅能访问绑定了 allow 策略的资源 |

**普通用户授权流程**：
```
JWT role_ids: ["uuid-A", "uuid-B"]
    │
    ▼  OPA 查询
data.tenants[tenant_id].role_bindings["uuid-A"] → ["documents_allow", "reports_allow"]
    │
    ▼  策略匹配
data.tenants[tenant_id].policies["documents_allow"] → {resource: "documents", effect: "allow"}
    │
    ▼  input.resource == "documents" AND effect == "allow"  →  ALLOW
```

**租户隔离双重保障**：
1. **PEP Proxy 层**：Python 代码校验 JWT 中的 `tenant_id` 必须与请求体中的 `tenant_id` 一致，拒绝跨租户操作
2. **OPA 层**：Rego 策略中 `input.tenant_id == _user_tenant` 再次校验，即使绕过 PEP Proxy 也无法越权

---

## 策略存储位置

策略数据持久化在 **PostgreSQL** 中（Pod 重启后数据不丢失）：

### 数据库表结构

```sql
-- 策略表：resource + effect，无 role 字段
CREATE TABLE policies (
    tenant_id   VARCHAR NOT NULL,
    id          VARCHAR NOT NULL,   -- {resource}_{effect}
    resource    VARCHAR NOT NULL,
    effect      VARCHAR NOT NULL DEFAULT 'allow',  -- 'allow' | 'deny'
    conditions  JSONB,
    created_at  TIMESTAMP,
    updated_at  TIMESTAMP,
    PRIMARY KEY (tenant_id, id)
);

-- 角色绑定表：role UUID → policy ID
CREATE TABLE role_policy_bindings (
    tenant_id   VARCHAR NOT NULL,
    role_id     VARCHAR NOT NULL,   -- UUID（来自 IdP）
    policy_id   VARCHAR NOT NULL,   -- 对应 policies.id
    created_at  TIMESTAMP,
    PRIMARY KEY (tenant_id, role_id, policy_id)
);
```

`policy_id` 由服务端自动生成：`{resource}_{effect}`，例如 `documents_allow`。

### OPA 数据文档结构

bundle-server 将 PostgreSQL 数据转换为以下格式推送到 OPA：

```json
{
  "tenants": {
    "tenant-001": {
      "policies": {
        "documents_allow": {"resource": "documents", "effect": "allow"},
        "reports_allow":   {"resource": "reports",   "effect": "allow"}
      },
      "role_bindings": {
        "11111111-1111-1111-1111-111111111111": ["documents_allow", "reports_allow"]
      }
    }
  }
}
```

### bundle 文件（OPA 轮询）

```
/app/bundles/
└── combined_bundle.tar.gz   # 合并所有租户的 OPA bundle（OPA 每 30-60 秒轮询）
```

---

## 策略模板

系统内置一个 Rego 策略模板，供 `tenant_admin` 参考策略写法：

### `role_based` — 基于角色的访问控制

```rego
package authz.templates.role_based

default allow = false

allow {
    input.resource  == "{{resource}}"
    input.action    == "{{action}}"
    input.tenant_id == "{{tenant_id}}"
    contains(input.roles[_], "{{role}}")
}
```

**参数**：`role`, `resource`, `action`, `tenant_id`

> 模板通过 `GET /api/v1/policies/templates` 查询可用列表，通过 `POST /api/v1/policies/template/{name}` 渲染（参数替换）后查看结果，**不会保存到 OPA**，仅供理解策略结构使用。
> 实际生产授权通过 PostgreSQL 中的 `policies` + `role_policy_bindings` 表配合 OPA Rego 进行判定。

---

## 对外暴露的 API

### PEP Proxy（端口 8000）

所有 `/api/v1/` 接口（除 `/health`）均需 `Authorization: Bearer <JWT>` Header。

#### 健康检查

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| GET | `/health` | 无 | 返回服务状态 |

```json
{"status": "healthy", "service": "pep-proxy", "timestamp": "..."}
```

#### 授权检查

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| POST | `/api/v1/auth/check` | 任意有效 JWT | 调用 OPA 判断当前用户是否有权访问指定资源 |

```json
// 请求体（无 action 字段）
{"resource": "documents", "tenant_id": "tenant-001"}

// 响应体
{"allowed": true, "user": "user-id", "tenant_id": "tenant-001", "resource": "documents", "reason": "Allowed by policy"}
```

#### 策略管理

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| GET | `/api/v1/policies` | 任意有效 JWT | 列出当前租户所有策略 |
| POST | `/api/v1/policies` | `tenant_admin` | 创建策略（resource + effect，不绑定角色）|
| PUT | `/api/v1/policies/{policy_id}` | `tenant_admin` | 更新策略 |
| DELETE | `/api/v1/policies/{policy_id}?tenant_id=xxx` | `tenant_admin` | 删除策略（级联删除所有绑定）|

```json
// POST /api/v1/policies 请求体
{"resource": "documents", "effect": "allow", "tenant_id": "tenant-001"}

// 响应
{"status": "success", "policy_id": "documents_allow", "tenant_id": "tenant-001"}
```

`effect` 枚举值：`allow` | `deny`

#### 角色绑定

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| POST | `/api/v1/roles/{role_id}/policies` | `tenant_admin` | 将策略列表绑定到角色 UUID |
| GET | `/api/v1/roles/{role_id}/policies` | 任意有效 JWT | 列出角色 UUID 的所有绑定策略 |
| DELETE | `/api/v1/roles/{role_id}/policies/{policy_id}?tenant_id=xxx` | `tenant_admin` | 解除角色与策略的绑定 |

```json
// POST /api/v1/roles/{role_id}/policies 请求体
{"policy_ids": ["documents_allow", "reports_allow"], "tenant_id": "tenant-001"}

// 响应
{"status": "success", "role_id": "uuid", "bound_policies": ["documents_allow", "reports_allow"], "tenant_id": "tenant-001"}
```

#### 策略模板

| 方法 | 路径 | 认证 | 说明 |
|------|------|------|------|
| GET | `/api/v1/policies/templates` | 任意有效 JWT | 列出所有可用模板名称 |
| POST | `/api/v1/policies/template/{name}` | `tenant_admin` | 渲染模板（参数替换，不保存）|

---

### Bundle Server（端口 8001）

Bundle Server 主要供集群内部使用（OPA 轮询、OPAL Client 拉取）。

> **安全提示**：bundle-server 接口无 JWT 验证，生产环境应通过 NetworkPolicy 限制只允许集群内部访问。

#### 健康检查

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/health` | 返回服务状态及已加载租户数量 |

#### OPA Bundle（供 OPA 轮询）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/opa-bundle` | 返回合并所有租户的 OPA bundle（tar.gz）|

OPA 通过 `OPAL_INLINE_OPA_CONFIG` 配置，每 30–60 秒轮询此端点：
```json
{
  "services": {"bundle-server": {"url": "http://localhost:8001"}},
  "bundles": {
    "authz": {
      "service": "bundle-server",
      "resource": "/api/v1/opa-bundle",
      "polling": {"min_delay_seconds": 30, "max_delay_seconds": 60}
    }
  }
}
```

#### 数据端点（供 OPAL Client 拉取）

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/data` | 返回所有租户的 OPA data 文档 |
| GET | `/api/v1/data/{tenant_id}` | 返回指定租户的 OPA data 文档 |

#### 策略 CRUD

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/tenants` | 列出所有租户 |
| GET | `/api/v1/tenants/{tenant_id}/policies` | 列出租户所有策略 |
| POST | `/api/v1/policies` | 创建策略并触发 bundle 更新 |
| PUT | `/api/v1/policies/{policy_id}?tenant_id=xxx` | 更新策略 |
| DELETE | `/api/v1/policies/{policy_id}?tenant_id=xxx` | 删除策略 |

#### 变更通知

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | `/api/v1/notify` | 接收变更通知，重新生成 bundle 并推送到 OPA |

```json
// 请求体
{"tenant_id": "tenant-001", "event": "policy_updated", "policy_id": "viewer_documents_read"}
```

---

## Agentgateway 集成（gRPC ext-authz）

```
AI Agent / MCP Client
    │
    ▼  HTTP（端口 8080）
agentgateway 代理（agentgateway-system-opa 命名空间）
    │
    ├──[gRPC ext-authz]──► pep-proxy:9000
    │                      envoy.service.auth.v3.Authorization/Check
    │                      │
    │                      ├── 读取 dev.agentgateway.jwt gRPC metadata（已预验证）
    │                      ├── 提取 tenant_id（从 iss 的 /realms/ 路径段）
    │                      ├── 查询 OPA: POST /v1/data/authz/allow
    │                      └── ALLOW(code=0) / DENY(code=7 PERMISSION_DENIED)
    │
    └──[若放行]──► pep-proxy:8000（业务 API）
```

### HTTPRoute 与资源/操作映射

agentgateway 通过 HTTPRoute 的 `RequestHeaderModifier` 为每条路由注入 `x-authz-resource` 和 `x-authz-action` Header，pep-proxy gRPC server 据此确定本次请求的资源和操作：

| 路由 | resource | action |
|------|----------|--------|
| GET `/api/v1/policies` | `policies` | `list` |
| POST `/api/v1/policies` | `policies` | `create` |
| PUT `/api/v1/policies` | `policies` | `update` |
| DELETE `/api/v1/policies` | `policies` | `delete` |
| GET `/api/v1/policies/templates` | `templates` | `list` |
| POST `/api/v1/auth/check` | `auth` | `read` |

### OPA fallback（gRPC 路径）

agentgateway 在调用 ext-authz 时**不一定转发原始 Bearer Header**，导致 `input.token` 可能为空，OPA 无法通过 `io.jwt.decode_verify` 解析 claims。

OPA Rego 的处理策略：
- 若 token 可解析：从 JWT claims 中提取 `roles` 和 `tenant_id`
- 若 token 为空：从 `input.roles` 和 `input.tenant_id` 直接读取（由 gRPC server 从 agentgateway metadata 解析后注入）

---

## 动态更新策略流程

```
tenant_admin 发送请求
    │
    ▼
POST /api/v1/policies  (pep-proxy :8000)
    │  1. 验证 JWT（角色 + 租户匹配）
    │  2. 转发到 bundle-server
    │
    ▼
POST /api/v1/policies  (bundle-server :8001，同 Pod localhost)
    │  3. INSERT INTO policies (tenant_id, id, resource, effect, ...)
    │
    ▼  BackgroundTask: generate_and_notify(tenant_id)
    │
    ├─── 4. 重建 combined_bundle.tar.gz → /app/bundles/
    │
    ├─── 5. 直接推送到 OPA REST API（即时生效，无轮询延迟）
    │        PUT http://localhost:8181/v1/policies/authz_main   ← Rego 策略
    │        PUT http://localhost:8181/v1/data/tenants/{id}     ← {policies, role_bindings}
    │
    └─── 6. 通知 OPAL Server → WebSocket 广播 → 其他 OPAL Client 拉取
             POST /v1/data/config（告知 OPAL 重新拉取 /api/v1/data/{tenant_id}）
```

同理，`POST /api/v1/roles/{role_id}/policies`（角色绑定）也会触发同样的 BackgroundTask 流程。

**关键点**：步骤 5 使用 OPA REST API 直接注入，策略在收到请求后**毫秒级**生效，无需等待 OPA 的 30-60 秒 bundle 轮询周期。

---

## Bundle Server 如何拉取并分发策略

Bundle Server 在以下三种时机生成/更新策略：

### 时机 1：服务启动

```python
# startup_event
for tenant_id in 已有租户:
    generate_tenant_bundle(tenant_id)   # 生成 tar.gz
    push_to_opa(tenant_id)              # 推送到 OPA REST API
rebuild_combined_bundle()               # 生成合并 bundle
```

### 时机 2：收到 /api/v1/notify

由 pep-proxy 在策略变更后调用，触发完整的 bundle 重建 + OPA 推送 + OPAL 通知流程。

### 时机 3：OPA 主动轮询（保底机制）

OPA 每 30-60 秒轮询 `GET /api/v1/opa-bundle`，即使通知机制异常，OPA 最终也会同步到最新策略。

---

## 部署

### 前置条件

- Kubernetes 集群（已测试：Kind）
- kubectl 已配置
- Docker

### 命令

```bash
# 仅构建镜像
./build-image.sh

# 部署所有组件（含镜像构建）
./deploy.sh

# 同时安装 agentgateway
DEPLOY_AGENTGATEWAY=true ./deploy.sh

# 查看部署状态
kubectl get pods -n opal-dynamic-policy

# 运行集成测试
./test.sh

# 含 agentgateway 端到端测试（需要 agentgateway 已部署）
TEST_GATEWAY=true ./test.sh

# 清理所有资源
./cleanup.sh
```

### 生产环境配置

修改 `k8s/deployment.yaml` 中的环境变量：

```yaml
# OIDC 提供商（RS256 验证，如 Keycloak）
- name: OIDC_BASE_URL
  value: "https://auth.example.com"

# 关闭 HS256 fallback（生产环境应使用 Secret 而非明文）
- name: JWT_SECRET
  value: ""

# OPAL Server token（建议通过 Secret 注入）
- name: OPAL_SERVER_TOKEN
  valueFrom:
    secretKeyRef:
      name: opal-tokens
      key: server-token
```

### 数据持久化

当前使用 `emptyDir` 卷，**Pod 重启后策略数据丢失**。生产环境需改为 PVC：

```yaml
volumes:
- name: app-data
  persistentVolumeClaim:
    claimName: opal-proxy-data

- name: app-bundles
  persistentVolumeClaim:
    claimName: opal-proxy-bundles
```

> 使用 PVC 后可将 pep-proxy `replicas` 改为多副本（当前限制为 1 是因为 emptyDir 是 Pod 级别的）。

---

## 本地测试

```bash
# 自动端口转发 + 完整测试套件
./test.sh

# 手动指定 URL（使用已有端口转发）
AUTO_PORT_FORWARD=false ./test.sh

# 自定义租户和密钥
TENANT_ID=my-tenant JWT_SECRET=my-secret ./test.sh

# 含 agentgateway 端到端测试
TEST_GATEWAY=true ./test.sh
```

测试覆盖：
- 健康检查（pep-proxy + bundle-server）
- Bundle Server 直接创建策略 + 角色绑定
- PEP Proxy 策略创建/更新/删除/跨租户隔离
- 角色绑定创建（tenant_admin 可绑，普通用户拒绝）
- 授权检查（UUID `role_ids` → role_bindings → ALLOW/DENY，`tenant_admin` 全权，`super_admin` 跨租户）
- 无效/缺失 token 拒绝
- gRPC ext-authz 端口 9000 TCP 连通性检测
- issuer 租户提取 + 伪造 token 拒绝
- agentgateway 端到端（Section 10，需要 `TEST_GATEWAY=true`）

---

## 已知限制与注意事项

### 关键陷阱速查

| 问题现象 | 原因 | 解决方案 |
|---------|------|---------|
| gRPC 端口 9000 无监听，agentgateway 返回 `Connection refused` | `asyncio.create_task()` 返回的 Task 无强引用，被 GC 静默回收 | `main.py` 用 `_grpc_task` 模块变量持有引用，并注册 done callback 记录异常 |
| `kubectl logs` 无应用输出 | supervisord 将日志写入文件而非 stdout | `kubectl exec -- tail -f /var/log/supervisor/pep-proxy.err` |
| Pod 重启后 bundle-server 无法连接 PostgreSQL | 旧 `policies` 表 schema 有 `action` 列而无 `effect` 列（旧版本遗留）| 执行迁移：`ALTER TABLE policies DROP COLUMN action; ALTER TABLE policies ADD COLUMN effect VARCHAR NOT NULL DEFAULT 'allow';` |
| OPA 普通用户权限始终 deny | JWT 中缺少 `role_ids` claim（UUID 列表），OPA 找不到 role_bindings | 确认 IdP 在 token 中输出 `role_ids` 字段；测试时用 `make_jwt` 第 4 个参数传入 UUID 列表 |
| Docker 构建时 `ModuleNotFoundError: ext_authz_pb2` | 包导入链中 `/app` 不在 `sys.path` | Dockerfile 中设置 `ENV PYTHONPATH=/app` |
| gateway 测试 Section 10 全部返回 403 | viewer 未绑定 auth/templates 资源的策略，ext-authz 拒绝 | Section 10 中列表/策略/auth 路由使用 `ADMIN_JWT` |

### 架构限制

- **单副本限制**：pep-proxy `replicas=1`，bundle 文件（`/app/bundles/`）存储在 emptyDir 上。若需多副本，可改为共享 PVC 或依赖 OPA bundle 轮询（bundle 从 PostgreSQL 重建，数据不丢失）。

- **PostgreSQL 为单点**：当前 PostgreSQL 为单副本 StatefulSet，生产环境建议使用托管 PostgreSQL 或 PG HA 方案。

- **bundle-server 无认证**：端口 8001 上的所有接口均无鉴权，依赖 NetworkPolicy 做网络隔离。生产环境必须确保该端口不对外暴露。

- **JWKS 缓存不跨 Pod**：JWKS 数据缓存在内存中，多副本时各 Pod 独立缓存，密钥轮换后需等待缓存过期（默认 5 分钟）。

- **gRPC 明文传输**：agentgateway → pep-proxy:9000 使用非加密 gRPC（`grpc: {}`）。如需加密可配置 mTLS。

### OPA Rego 注意事项

- **Complete rule 冲突**：OPA 中同一 package 内相同名称的 complete rule 只能有一个定义。bundle-server 使用单一共享模块 `authz_main`，通过 `data.tenants[tenant_id]` 区分租户，避免了多 package 冲突问题。

- **OPA 版本兼容性**：Rego 中使用 `rule := value { condition }` 语法（花括号体），而非 `rule := value if condition`（需要 OPA ≥ 0.44 并开启 `future.keywords`）。当前写法兼容更广泛的 OPA 版本。

- **token 为空时的 fallback**：agentgateway ext-authz 路径中 `input.token` 可能为空字符串。OPA Rego 通过 `_user_roles := input.roles { not _claims }` 等 fallback 规则处理此情况，确保 gRPC 路径下正常决策。
