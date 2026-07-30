# da-cluster 设计说明书

---

## 1 概述

### 1.1 目的

本文档是 da-cluster 多租户统一鉴权网关系统的详细设计说明书，用于描述系统的整体架构、各模块的设计思路、功能实现原理和接口规范，指导开发实施和后续维护。

**读者对象：** 开发工程师、测试工程师、项目经理、运维人员。

### 1.2 范围

本文档覆盖 da-cluster 系统的以下模块：

| 模块 | 覆盖内容 |
|------|---------|
| **Gateway（流量层）** | AgentGateway 路由、ext-authz 鉴权、Header 注入 |
| **Keycloak（身份层）** | 身份联合、JWT 签发、租户/角色/用户管理 API |
| **OPA（策略层）** | 动态策略评估、gRPC ext-authz、策略持久化 |

**不在范围内：** 前端 WebUI 实现细节、具体业务应用（DA App）的开发、数据库表结构的 DDL 设计。

---

## 4 特性/功能实现原理

### 4.1 总体方案

#### 4.1.1 设计思路

##### 业务背景

da-cluster 是一套**多租户统一鉴权网关系统**，面向的核心场景是：产品需要交付给多个企业客户，每个客户拥有各自的员工身份体系（AD/LDAP/企业微信/SAML SSO/OIDC），不可能要求每个客户的员工重新注册一套账号。系统通过联合客户已有的身份源，统一签发标准化 JWT Token，使下游业务服务零改动接入完整的认证与授权能力。

**系统建设带来的核心价值：**

| 价值维度 | 说明 |
|---------|------|
| **身份联合接入** | 客户无需改变现有身份系统，通过 SAML/OIDC 协议联合对接，员工使用已有账号即可登录，降低客户接入成本 |
| **统一安全边界** | 所有微服务共享同一套认证授权基础设施，避免各服务重复实现鉴权逻辑，消除安全短板 |
| **动态策略管理** | 权限规则支持运行时修改，无需重新部署或重启服务即可生效，满足业务快速迭代的需要 |
| **多租户隔离** | 数据与权限天然按租户隔离，新客户接入不影响已有客户，支持独立的角色体系和策略配置 |
| **业务零侵入** | 下游服务只需读取 Gateway 注入的 `X-Auth-*` Headers，不涉及 JWT 解析、密钥管理等鉴权细节 |
| **统一流量治理** | 基于 Envoy 的 Gateway 层天然支持负载均衡、流量控制（限流/熔断）、超时重试、TLS 终止、CORS 管理、可观测性（日志/Metrics/Tracing）等能力，为后续扩展提供基础 |
| **合规审计基础** | ext-authz 链路天然记录了"谁、在什么时间、通过什么角色、访问了什么资源"的完整审计信息 |

##### 三层解耦设计

系统采用**流量层 - 身份层 - 策略层**三层解耦架构，每一层专注解决一个核心问题，层与层之间通过标准协议通信，可以独立演进和扩展。

**流量层（Gateway）：** 基于 AgentGateway（Envoy 内核）构建统一的流量入口。所有外部请求通过 Gateway 的单一端口进入系统，由 HTTPRoute 规则进行路径匹配和后端分发。Gateway 同时承载 ext-authz 外部授权过滤器，在请求到达业务后端之前拦截并调用策略层进行鉴权判定。这一层的核心价值在于将"流量如何路由"和"请求是否放行"两个关注点统一收口，业务服务无需关心网络拓扑和安全策略。

**身份层（Keycloak）：** 基于 Keycloak 构建企业级身份联合平台。每个客户对应一个独立的 Realm（租户），通过 SAML/OIDC Identity Provider 联合客户已有的身份系统。员工通过 SSO 登录后，Keycloak 在本地创建影子用户并签发标准化的 JWT Token，Token 中包含租户标识（`iss` 字段中的 realm）和结构化角色信息（`[{id, name}]` 格式）。这一层将"用户是谁"的问题标准化，下游无需关心客户原来用的是 AD 还是企业微信。

**策略层（OPA）：** 基于 Open Policy Agent 构建动态策略评估引擎。pep-proxy 作为策略执行点（PEP），接收 Gateway 的 ext-authz gRPC 请求，从 JWT 中提取用户身份和角色信息，构造策略查询输入提交给 OPA。OPA 基于 Rego 策略进行三层授权判定（super-admin / tenant-admin / 角色策略绑定）。策略数据通过 bundle-server 持久化到 PostgreSQL，并经 OPAL 实时同步到 OPA 实例。这一层将"用户能做什么"的逻辑从业务代码中完全剥离，支持运行时动态变更。

##### 关键设计原则

**一个 Realm = 一个租户 = 一个客户：** Keycloak 的 Realm 机制天然提供了租户隔离的基础。每个客户独占一个 Realm，拥有独立的用户目录、角色定义、IDP 配置和 Client 设置。JWT 的 `iss` 字段中嵌入了 Realm 名称（如 `http://localhost/realms/customer-a`），下游系统可以从中直接提取租户标识，无需额外的租户路由逻辑。这种设计确保了租户之间在身份层面的强隔离。

**业务后端零改动接入：** 所有鉴权逻辑都在 Gateway + pep-proxy 层完成，业务后端只需读取 Gateway 注入的 HTTP Headers（`X-Auth-User-Id`, `X-Auth-Tenant`, `X-Auth-Roles` 等）即可获取已验证的用户身份信息。业务服务不需要引入 JWT 库、不需要管理密钥、不需要实现 OIDC 发现协议，接入成本降至最低。新微服务只需注册一条 HTTPRoute 规则即可纳入统一鉴权体系。

**策略动态可变，无需重新部署：** 权限规则以数据的形式存储在 PostgreSQL 中，通过 bundle-server 生成 OPA Bundle 并直接推送到 OPA 实例（绕过轮询延迟），同时通知 OPAL Server 触发多副本同步。这意味着管理员通过 API 创建或修改策略后，新规则在秒级内对所有请求生效，无需重启任何服务或重新部署容器。

**ext-authz 透明鉴权模式：** 采用 Envoy 原生的外部授权协议（ext-authz v3），Gateway 在转发请求之前通过 gRPC 调用 pep-proxy 进行鉴权。这种模式对业务完全透明 — 业务后端不知道也不需要知道鉴权的存在。同时，ext-authz 是 Envoy 生态的标准协议，未来如果替换 Gateway 实现（如迁移到 Istio），鉴权逻辑无需修改。

**路径即权限声明：** 业务 API 路径遵循 `/{tenant-id}/{app}/{resource}` 规范，OPA 从 URL 中自动提取租户标识和资源名称，与 JWT 中的 Realm 做交叉校验实现租户隔离。路径中的 `/admin/` 段作为权限升级标记，OPA 检测到后自动要求 `tenant-admin` 角色。这种设计让权限规则与 URL 结构天然绑定，减少了显式权限配置的工作量。

**默认拒绝（Default Deny）：** Gateway 对所有受保护路由默认拒绝访问，只有通过 ext-authz 鉴权（AuthN + AuthZ）后才放行请求。这种安全姿态确保即使配置遗漏，也不会意外暴露未授权的接口。

##### 典型使用场景

以下场景展示了系统从租户创建到日常使用的完整业务闭环。图中虚线表示通用鉴权协议（ext-authz / JWKS 验证），对所有受保护请求自动生效。

**场景1：超级管理员创建租户（Tenant）**

```plantuml
@startuml
actor "super admin" as sa
rectangle "webui" as ui
rectangle "DA" {
    rectangle "pdp(agentgateway)" as pdp
    rectangle "idb(keycloak)" as idb
    rectangle "idb proxy" as idbp
    rectangle "pep(OPA)" as pep
    rectangle "pep proxy" as pepp
}
sa --> ui: 1. add tenant
ui --> pdp: 2. POST /api/v1/tenants (with JWT)
pdp <..> pep: 2.1 validate (ext authz)
idb <...> pep: 2.2 validate (JWKS)
pdp --> idbp: 3. route request
idbp --> idb: 4. POST /admin/realms (with api key)
@enduml
```

**场景2：租户管理员导入 SAML 2.0 配置**

```plantuml
@startuml
actor "tenant admin" as sa
rectangle "**idp (customer's identity platform)**" as idp
rectangle "webui" as ui
rectangle "DA" {
    rectangle "pdp(agentgateway)" as pdp
    rectangle "idb(keycloak)" as idb
    rectangle "idb proxy" as idbp
    rectangle "pep(OPA)" as pep
    rectangle "pep proxy" as pepp
}
sa --> ui: 1. setup saml2.0 mapping
ui --> pdp: 2. POST /api/v1/{realm}/idp/saml/import (with JWT)
pdp <..> pep: 2.1 validate (ext authz)
idb <...> pep: 2.2 validate (JWKS)
pdp --> idbp: 3. route request
idbp --> idb: 4. POST /admin/realms/{realm}/identity-provider/import-config (with api key)
idb <--> idp: 5. request saml2.0 config json
idbp <-- idb: 6. response
pdp <-- idbp: 7. response
ui <-- pdp: 8. response
@enduml
```

**场景3：租户管理员创建 SAML 2.0 IDP 实例**

```plantuml
@startuml
actor "tenant admin" as sa
rectangle "webui" as ui
rectangle "DA" {
    rectangle "pdp(agentgateway)" as pdp
    rectangle "idb(keycloak)" as idb
    rectangle "idb proxy" as idbp
    rectangle "pep(OPA)" as pep
    rectangle "pep proxy" as pepp
}
sa --> ui: 1. create saml idp instance
ui --> pdp: 2. POST /api/v1/{realm}/idp/saml/instances (with JWT)
pdp <..> pep: 2.1 validate (ext authz)
idb <...> pep: 2.2 validate (JWKS)
pdp --> idbp: 3. route request
idbp --> idb: 4. POST /admin/realms/{realm}/identity-provider/instances (with api key)
@enduml
```

> **场景4（角色与群组管理）** 流程与场景3 相同，调用不同的 API 端点（`/api/v1/{realm}/roles`, `/api/v1/{realm}/groups`）。支持配置强制同步 SAML 2.0 属性到 Keycloak 角色映射。

**场景5：租户管理员配置资源访问策略**

```plantuml
@startuml
actor "tenant admin" as sa
rectangle "webui" as ui
rectangle "DA" {
    rectangle "pdp(agentgateway)" as pdp
    rectangle "idb(keycloak)" as idb
    rectangle "idb proxy" as idbp
    rectangle "pep(OPA)" as pep
    rectangle "pep proxy" as pepp
    rectangle "OPA bundle server" as pepb
}
sa --> ui: 1. create policy
ui --> pdp: 2. POST /api/v1/policies (with JWT)
pdp <..> pep: 2.1 validate (ext authz)
idb <...> pep: 2.2 validate (JWKS)
pdp --> pepp: 3. route request
pepp --> pepb: 4. persist policy
pepb --> pep: 5. PUT /v1/policies/{policy_id} (push bundle)
@enduml
```

**场景6：用户日常使用 — 完成 AuthN + AuthZ，透传 metadata 至业务应用**

```plantuml
@startuml
actor "user" as sa
rectangle "webui" as ui
rectangle "DA" {
    rectangle "pdp(agentgateway)" as pdp
    rectangle "idb(keycloak)" as idb
    rectangle "pep(OPA)" as pep
    rectangle "da app{1 ... n}" as app
}
sa --> ui: 1. access business function
ui --> pdp: 2. {ACTION} /{tenant}/da/{resource} (with JWT)
pdp <..> pep: 2.1 validate (ext authz)
idb <...> pep: 2.2 validate (JWKS)
pdp --> app: 3. {ACTION} /{resource} (metadata in X-Auth-* headers)
@enduml
```

---

#### 4.1.2 实体关系分析

系统核心实体分布在 Keycloak（身份层）和 OPA（策略层）两侧，通过 **角色 UUID** 作为桥梁关联。

##### 实体关系图

```mermaid
erDiagram
    Tenant ||--o{ User : "包含"
    Tenant ||--o{ Role : "定义"
    Tenant ||--o{ Group : "划分"
    Tenant ||--o{ IDP : "对接"
    Tenant ||--|| Client : "关联"

    Group }o--o{ Role : "绑定角色"
    User }o--o{ Group : "归属"
    User }o--o{ Role : "直接分配"

    IDP ||--o{ ProtocolMapper : "配置"
    Client ||--o{ ProtocolMapper : "配置"

    Role ||--o{ RolePolicyBinding : "策略绑定"
    Policy ||--o{ RolePolicyBinding : "被绑定"
    Policy ||--o{ Rule : "包含规则"

    Tenant {
        string realm_name PK "Keycloak Realm 名称"
        string display_name "显示名"
    }
    User {
        string id PK "UUID"
        string username "用户名（影子用户）"
        string email "邮箱"
    }
    Role {
        string id PK "UUID（跨系统关联键）"
        string name "角色名称"
    }
    Group {
        string id PK "UUID"
        string name "群组名称"
    }
    IDP {
        string alias PK "IDP 别名"
        string provider_id "saml / oidc"
    }
    Client {
        string client_id PK "data-agent"
        string protocol "openid-connect"
    }
    ProtocolMapper {
        string id PK "UUID"
        string name "映射器名称"
        string protocol_mapper "mapper 类型"
    }
    Policy {
        string id PK "策略ID"
        string tenant_id FK "所属租户"
        string resource "资源标识"
        string effect "allow / deny"
        json conditions "条件"
    }
    RolePolicyBinding {
        string role_id FK "角色UUID（= Role.id）"
        string policy_id FK "策略ID"
        string tenant_id FK "所属租户"
    }
    Rule {
        string resource "资源名称"
        string effect "allow / deny"
    }
```

##### 实体归属说明

| 实体 | 所属系统 | 存储位置 | 说明 |
|------|---------|---------|------|
| Tenant (Realm) | Keycloak | keycloak DB | 一个 Realm = 一个客户 |
| User | Keycloak | keycloak DB | 客户员工，通过 IDP 联合创建的影子用户 |
| Role | Keycloak | keycloak DB | 业务角色，`id` 为 UUID |
| Group | Keycloak | keycloak DB | 部门/项目分组，可批量绑定角色 |
| IDP | Keycloak | keycloak DB | SAML/OIDC 身份源配置 |
| Client | Keycloak | keycloak DB | OIDC 客户端（data-agent），承载 Token 签发 |
| ProtocolMapper | Keycloak | keycloak DB | 包括自定义 SPI：structured-realm-role-mapper |
| Policy | OPA (bundle-server) | opal DB (PostgreSQL) | 资源访问策略 |
| RolePolicyBinding | OPA (bundle-server) | opal DB (PostgreSQL) | 角色 → 策略的绑定关系 |

##### 跨系统关联

两个系统通过 **Role UUID** 关联：

```
Keycloak 侧                           OPA 侧
┌──────────────────┐                  ┌──────────────────────┐
│ Role              │                  │ RolePolicyBinding     │
│  id: "abc-123"   │◄── role UUID ──►│  role_id: "abc-123"  │
│  name: "viewer"  │                  │  policy_id: "pol-1"  │
└──────────────────┘                  └──────────────────────┘
```

JWT Token 中通过自定义 SPI 将角色输出为 `[{id: "abc-123", name: "viewer"}]` 结构，pep-proxy 从 JWT 中取出 `role_ids` 列表，在 OPA 中查询 `role_policy_bindings` 表，找到对应的 Policy 和 Rules，完成鉴权判定。

---

#### 4.1.3 实现分析

##### 系统分层架构图

```mermaid
graph TB
    subgraph 流量层 - agentgateway-system namespace
        direction TB
        subgraph CRDs[CRD 资源定义]
            GW_CRD["Gateway<br/>监听端口 & GatewayClass"]
            HR_CRD["HTTPRoute<br/>路径 → 后端映射"]
            RG_CRD["ReferenceGrant<br/>跨namespace授权"]
            AP_CRD["AgentgatewayPolicy<br/>ext-authz策略挂载"]
        end
        subgraph 控制面[控制面 Controller]
            GW_CTRL["Gateway Controller<br/>监听CRD变更<br/>生成Envoy配置"]
        end
        subgraph 数据面[数据面 Proxy]
            ENVOY["Envoy Proxy<br/>流量代理 & 路由"]
            EXTAUTH_FILTER["ext-authz Filter<br/>gRPC外部授权拦截"]
        end
    end

    subgraph 身份层 - keycloak namespace
        direction TB
        subgraph KC_Core[Keycloak Server x2]
            KC["Keycloak 26.5.2<br/>身份联合 / OIDC / SAML"]
            SPI["自定义SPI<br/>structured-realm-role-mapper<br/>角色输出为 [{id,name}]"]
        end
        KC_PROXY["keycloak-proxy x2<br/>租户/角色/用户/IDP<br/>管理API (FastAPI :8090)"]
        KC_INIT["keycloak-init Job<br/>初始化 super-admin<br/>默认租户 & 用户"]
    end

    subgraph 策略层 - opa namespace
        direction TB
        subgraph PEP_POD[PEP Proxy Pod]
            PEP["pep-proxy<br/>JWT验证 / gRPC ext-authz<br/>(:8000 REST, :9000 gRPC)"]
            BUNDLE["bundle-server<br/>策略持久化 / Bundle生成<br/>(:8001)"]
            OPA_CLIENT["opal-client + OPA<br/>Rego策略评估<br/>(:8181)"]
        end
        OPAL_SRV["OPAL Server x2<br/>策略数据同步<br/>(:7002)"]
    end

    subgraph 数据层
        PG["PostgreSQL<br/>共享实例<br/>keycloak DB + opal DB"]
    end

    CRDs --> GW_CTRL
    GW_CTRL --> ENVOY
    ENVOY --> EXTAUTH_FILTER

    EXTAUTH_FILTER -->|gRPC :9000| PEP
    ENVOY -->|HTTP :8080| KC
    ENVOY -->|HTTP :8090| KC_PROXY
    ENVOY -->|HTTP :8000| PEP

    PEP -->|POST /v1/data/authz/allow| OPA_CLIENT
    PEP -->|OIDC Discovery + JWKS| KC
    BUNDLE -->|推送 Bundle| OPA_CLIENT
    BUNDLE -->|通知同步| OPAL_SRV
    OPAL_SRV -->|数据同步| OPA_CLIENT

    KC --> PG
    BUNDLE --> PG
    OPAL_SRV --> PG
    KC_PROXY --> KC
```

##### 物理部署视图（多节点）

```plantuml
@startuml
skinparam rectangle {
    BackgroundColor<<node>> #F0F4FF
    BackgroundColor<<gw>> #D6EAF8
    BackgroundColor<<kc>> #FADBD8
    BackgroundColor<<opa>> #D5F5E3
    BackgroundColor<<db>> #E8DAEF
}

rectangle "生产主机 (Host)" {
    rectangle "External IP 1" as eip1
    rectangle "External IP 2" as eip2

    rectangle "Kubernetes Cluster" {
        rectangle "Node A" <<node>> {
            rectangle "AgentGateway 1\n(Envoy Proxy)" as gw1 <<gw>>
            rectangle "Keycloak 1" as kc1 <<kc>>
            rectangle "Infinispan\n(嵌入式缓存)" as is1 <<kc>>
            rectangle "keycloak-proxy 1" as kp1 <<kc>>
            rectangle "PEP Proxy Pod 1\n(pep-proxy + bundle-server\n+ OPA sidecar)" as pep1 <<opa>>
            rectangle "OPAL Server 1" as opal1 <<opa>>
            rectangle "App Instance 1" as app1
        }

        rectangle "Node B" <<node>> {
            rectangle "AgentGateway 2\n(Envoy Proxy)" as gw2 <<gw>>
            rectangle "Keycloak 2" as kc2 <<kc>>
            rectangle "Infinispan\n(嵌入式缓存)" as is2 <<kc>>
            rectangle "keycloak-proxy 2" as kp2 <<kc>>
            rectangle "PEP Proxy Pod 2\n(pep-proxy + bundle-server\n+ OPA sidecar)" as pep2 <<opa>>
            rectangle "OPAL Server 2" as opal2 <<opa>>
            rectangle "App Instance 2" as app2
        }

        database "PostgreSQL\n(keycloak DB + opal DB)" as pg <<db>>
    }
}

eip1 -- gw1
eip2 -- gw2

is1 <--> is2 : JGroups\n集群同步
kc1 -- is1
kc2 -- is2
kc1 --> pg
kc2 --> pg

gw1 --> app1
gw1 --> app2
gw2 --> app1
gw2 --> app2

kp1 --> kc1
kp1 --> kc2
kp2 --> kc1
kp2 --> kc2

opal1 <--> opal2 : PostgreSQL\npub/sub
@enduml
```

> **注意：** 当前环境不使用 MetalLB（L2 需要宿主 ARP，L3 需要宿主 iptable + BGP 端口），提供 2 个 External IP，路由委托给客户网络。

##### 模块依赖关系图

```mermaid
graph LR
    subgraph External[外部]
        CLIENT["客户端<br/>(浏览器/API)"]
        IDP["客户IDP<br/>(AD/SAML SSO)"]
    end

    subgraph GW[Gateway 模块]
        direction TB
        GW_PROXY["Envoy Proxy"]
        GW_EXTAUTH["ext-authz Filter"]
        GW_ROUTE["HTTPRoute 规则"]
    end

    subgraph KC[Keycloak 模块]
        direction TB
        KC_SERVER["Keycloak Server"]
        KC_PROXY["keycloak-proxy API"]
        KC_SPI["自定义 SPI"]
    end

    subgraph OPA[OPA 模块]
        direction TB
        PEP["pep-proxy"]
        OPA_ENGINE["OPA Engine"]
        BUNDLE_SRV["bundle-server"]
        OPAL["OPAL Server"]
    end

    subgraph DATA[数据层]
        PG["PostgreSQL"]
    end

    CLIENT -->|HTTP :80| GW_PROXY
    GW_PROXY --> GW_ROUTE
    GW_ROUTE -->|无鉴权路由| KC_SERVER
    GW_ROUTE -->|受保护路由| GW_EXTAUTH
    GW_EXTAUTH -->|gRPC :9000| PEP
    GW_ROUTE -->|转发| KC_PROXY
    GW_ROUTE -->|转发| PEP

    IDP -->|SAML/OIDC| KC_SERVER
    KC_PROXY -->|Admin API| KC_SERVER
    KC_SPI -.->|内嵌| KC_SERVER

    PEP -->|OIDC JWKS| KC_SERVER
    PEP -->|策略查询| OPA_ENGINE
    BUNDLE_SRV -->|推送Bundle| OPA_ENGINE
    BUNDLE_SRV -->|通知| OPAL
    OPAL -->|数据同步| OPA_ENGINE

    KC_SERVER --> PG
    BUNDLE_SRV --> PG
    OPAL --> PG
```

##### 模块职责总览

| 模块 | 命名空间 | 核心组件 | 主要职责 | 对外端口 |
|------|---------|---------|---------|---------|
| **Gateway** | agentgateway-system | Controller, Envoy Proxy, CRDs | 统一流量入口、路由分发、ext-authz 鉴权拦截、Header 注入 | 80 (HTTP) |
| **Keycloak** | keycloak | Keycloak Server x2, keycloak-proxy x2, keycloak-init, PostgreSQL | 身份联合（SAML/OIDC）、JWT 签发、租户/角色/用户/IDP 管理 API | 8080 (Keycloak), 8090 (proxy API) |
| **OPA** | opa | pep-proxy, bundle-server, OPA (opal-client), OPAL Server x2 | 动态策略评估、gRPC ext-authz 鉴权、策略持久化与同步 | 8000 (REST), 9000 (gRPC) |

##### 各模块功能需求清单

**Gateway（PolicyDecisionPoint）模块：**

| 编号 | 功能需求 | 说明 |
|------|---------|------|
| GW-F1 | 提供 OpenAPI Routing 能力 | 基于 HTTPRoute 的路径匹配和后端分发 |
| GW-F2 | WebUI 通过 oidc-client-ts 与 Gateway 交互 | 前端使用标准 OIDC 客户端库完成认证流程 |
| GW-F3 | 使用 ext-authz 进行 API 身份认证/RBAC 鉴权 | gRPC 调用 pep-proxy 完成 AuthN + AuthZ |
| GW-F4 | 鉴权通过后 metadata 转移至 header，清洗路径为 upstream 格式 | 注入 X-Auth-* Headers，重写路径去除租户前缀 |
| GW-F5 | 默认 deny 所有请求 | 通过 AuthN + AuthZ 后才放行请求 |

**Keycloak（ID Broker）模块：**

| 编号 | 功能需求 | 说明 |
|------|---------|------|
| KC-F1 | 提供 IDB Proxy 服务（Python + FastAPI） | 角色/群组/用户 CRUD，用户-群组-角色关系管理，SAML 属性映射 |
| KC-F2 | 提供 SAML 2.0 接入能力 | 联合客户已有的 SAML IDP |
| KC-F3 | 提供 JWKS 接口校验 Token 合法性 | 供 pep-proxy 和 OPA 验证 JWT 签名 |
| KC-F4 | 提供 Admin Web UI 界面 | Keycloak 原生管理控制台 |

**OPA（PolicyEnforcementPoint）模块：**

| 编号 | 功能需求 | 说明 |
|------|---------|------|
| OPA-F1 | 提供 PEP Proxy 服务（Python + FastAPI） | 策略编辑、预制 Rego 模板，用户通过 REST API + JSON 注入角色与资源（Policy as Data） |
| OPA-F2 | 通过 JWT Token issuer 动态组装 JWKS URL 进行验证 | 多租户动态 OIDC Discovery |
| OPA-F3 | 配置 ext-authz 接口进行 API 访问鉴权 | gRPC :9000 实现 Authorization/Check |

##### 各模块非功能需求清单

| 模块 | 编号 | 非功能需求 |
|------|------|-----------|
| **Gateway** | GW-NF1 | 高可用部署，1 node 1 replica |
| | GW-NF2 | 配置 HPA 规则支持弹性扩缩 |
| | GW-NF3 | 打包为 Helm chart 及镜像 |
| **Keycloak** | KC-NF1 | 高可用部署，1 node 1 replica（Infinispan 集群） |
| | KC-NF2 | 配置 HPA 规则支持弹性扩缩 |
| | KC-NF3 | 打包为 Helm chart 及镜像 |
| | KC-NF4 | Helm chart 预置超级管理员账户及角色 |
| | KC-NF5 | 提供 tenant/user 管理 WebUI |
| | KC-NF6 | Helm chart 包含 K8s Job，按序部署 Keycloak 及 keycloak-proxy |
| **OPA** | OPA-NF1 | 高可用部署，1 node 1 replica |
| | OPA-NF2 | 配置 HPA 规则支持弹性扩缩 |
| | OPA-NF3 | 打包为 Helm chart 及镜像 |
| | OPA-NF4 | Helm chart 预置超级管理员策略 |
| | OPA-NF5 | Helm chart 包含 K8s Job，按序部署 OPA 及 OPA proxy |
| | OPA-NF6 | 提供 policy 管理 WebUI |

##### 认证流时序图（用户登录 → 获取 Token）

```mermaid
sequenceDiagram
    autonumber
    participant User as 用户浏览器
    participant GW as Gateway :80
    participant KC as Keycloak :8080
    participant IDP as 客户 IDP<br/>(AD/SAML SSO)

    User->>GW: GET /realms/{tenant}/protocol/openid-connect/auth<br/>（发起登录）
    GW->>KC: 转发（/realms/* 无鉴权路由）
    KC-->>User: 返回登录页面<br/>（含 IDP 选项列表）

    User->>KC: 选择企业 SSO 登录
    KC->>IDP: SAML AuthnRequest<br/>（重定向到客户 IDP）
    IDP-->>User: 客户 SSO 登录页面
    User->>IDP: 输入企业账号密码

    IDP-->>KC: SAML Response<br/>（包含用户属性 & 断言）

    Note over KC: 1. 验证 SAML Response 签名<br/>2. 创建/更新影子用户<br/>3. 映射角色（通过 SPI 输出 [{id,name}]）<br/>4. 签发 JWT Token

    KC-->>User: 重定向回前端<br/>Authorization Code

    User->>GW: POST /realms/{tenant}/protocol/openid-connect/token<br/>（授权码换 Token）
    GW->>KC: 转发
    KC-->>GW: Access Token (JWT)
    GW-->>User: Access Token (JWT)

    Note over User: JWT 包含：<br/>iss: http://localhost/realms/{tenant}<br/>sub: user-uuid<br/>roles: [{id:"uuid", name:"role-name"}]<br/>preferred_username, email...
```

##### 鉴权流时序图（带 Token 请求 → 允许/拒绝）

```mermaid
sequenceDiagram
    autonumber
    participant Client as 客户端
    participant GW as Gateway<br/>Envoy Proxy :80
    participant Filter as ext-authz Filter<br/>(Envoy 内置)
    participant PEP as pep-proxy :9000<br/>(gRPC)
    participant OPA as OPA :8181
    participant Backend as 业务后端

    Client->>GW: HTTP 请求<br/>Authorization: Bearer {JWT}<br/>GET /{tenant}/app/resource

    GW->>GW: HTTPRoute 匹配<br/>命中受保护路由

    GW->>Filter: 触发 ext-authz 过滤器

    Filter->>PEP: gRPC Check(CheckRequest)<br/>携带 HTTP headers + metadata

    Note over PEP: Step 1: 获取 JWT Claims<br/>优先: dev.agentgateway.jwt metadata<br/>备选: Bearer Token 解码

    PEP->>PEP: Step 2: 从 iss 提取 tenant_id<br/>.../realms/{tenant} → tenant_id

    PEP->>PEP: Step 3: 解析角色<br/>roles: [{id,name}] → role_names + role_ids

    PEP->>PEP: Step 4: 解析资源<br/>从 path 或 x-authz-resource header

    PEP->>OPA: POST /v1/data/authz/allow<br/>input: {user, roles, role_ids,<br/>tenant_id, resource, path, is_admin}

    Note over OPA: Rego 三层判定:<br/>Layer 1: super-admin → ALLOW<br/>Layer 2: tenant-admin + 本租户 → ALLOW<br/>Layer 3: role_id → binding → policy → rules

    alt 允许 (result: true)
        OPA-->>PEP: {"result": true}
        PEP-->>Filter: CheckResponse: OK<br/>+ OkHttpResponse Headers
        Note over Filter: 注入 Headers:<br/>x-auth-user-id<br/>x-auth-username<br/>x-auth-roles<br/>x-auth-role-ids<br/>x-auth-issuer<br/>x-auth-tenant
        Filter-->>GW: ALLOW + Headers
        GW->>GW: 清洗路径 + 注入 Headers
        GW->>Backend: 转发请求 + X-Auth-* Headers
        Backend-->>GW: 业务响应
        GW-->>Client: 业务响应
    else 拒绝 (result: false)
        OPA-->>PEP: {"result": false}
        PEP-->>Filter: CheckResponse: DENIED<br/>HTTP 403, "Forbidden by policy"
        Filter-->>GW: DENY
        GW-->>Client: HTTP 403 Forbidden
        Note over Backend: 请求未到达业务后端
    end
```

---

### 4.2 Gateway

#### 4.2.1 设计思路

##### 为什么需要统一网关？

在微服务架构下，每个服务独立对外暴露端口会带来严重的管理和安全问题：端口管理混乱、每个服务需要各自实现 TLS/认证/限流、跨服务的安全策略难以统一执行。统一网关将所有外部流量收口到单一入口，由网关统一处理安全策略、路由分发和流量治理，业务服务只需专注于业务逻辑。

##### 为什么选择 AgentGateway？

| 选型考量 | AgentGateway 优势 | 对比 Nginx/Traefik |
|---------|-------------------|-------------------|
| **ext-authz 原生支持** | 基于 Envoy 内核，原生支持 gRPC ext-authz v3 外部授权协议，可将鉴权逻辑完全外置 | Nginx 需要额外模块（auth_request），仅支持 HTTP 子请求，不支持 gRPC |
| **Gateway API 标准** | 基于 Kubernetes Gateway API（HTTPRoute），是 Ingress 的官方继任标准，表达能力更强 | Traefik 支持 Gateway API 但生态较新，Nginx Ingress 仍依赖私有注解 |
| **跨 namespace 路由** | 原生支持 ReferenceGrant 机制，三个 namespace 的服务可统一暴露 | 需要复杂的 ExternalName Service 或手动 Endpoint 配置 |
| **自定义策略 CRD** | 提供 AgentgatewayPolicy CRD，声明式配置 ext-authz 规则 | 通常需要修改全局配置文件或使用注解 |
| **Header 注入** | ext-authz ALLOW 响应可携带 Headers，Envoy 自动注入到原始请求 | auth_request 的 Header 传递需要额外配置 |
| **生产级能力** | 继承 Envoy 的负载均衡、熔断、重试、可观测性等企业级能力 | 功能对等但配置方式不同 |

##### 核心设计决策

**单一 Gateway 实例，多 namespace 路由：** 只创建一个 Gateway 资源（`agentgateway-proxy`），监听 :80 端口，通过 `allowedRoutes.namespaces.from: All` 允许所有 namespace 注册 HTTPRoute。搭配 ReferenceGrant 实现跨 namespace 的安全引用，避免了多 Ingress 的管理复杂度。

**路由分为鉴权组和免鉴权组：** Keycloak OIDC 端点（`/realms/*`, `/resources/*`, `/admin/*`）走免鉴权路由（用户需要先登录才能获取 Token，登录端点本身不能要求 Token）。其余 API 路由通过 AgentgatewayPolicy 挂载 ext-authz 策略，统一走 pep-proxy 鉴权。

**ext-authz 采用 gRPC 协议：** 相比 HTTP ext-authz，gRPC 模式传输效率更高，支持结构化的 CheckRequest/CheckResponse，且 agentgateway 可以在 gRPC metadata 中预注入已验证的 JWT Claims（`dev.agentgateway.jwt`），pep-proxy 无需重复验签。

**默认 Deny + 鉴权后路径清洗：** 所有受保护路由默认拒绝，通过 AuthN + AuthZ 后才放行。鉴权通过后，Gateway 将身份元数据（tenant、user、roles）从 JWT 转移到 HTTP Headers，并清洗 URL 路径为上游服务期望的格式（去除租户前缀），使业务后端完全无感知。

---

#### 4.2.2 功能描述

##### 功能需求清单

| 编号 | 功能 | 说明 |
|------|------|------|
| GW-F1 | **OpenAPI Routing** | 基于 HTTPRoute CRD 的路径匹配和后端分发，支持 PathPrefix 和正则匹配 |
| GW-F2 | **OIDC 客户端交互** | WebUI 使用 oidc-client-ts 与 Gateway 交互，Gateway 透传 OIDC 请求到 Keycloak |
| GW-F3 | **ext-authz 鉴权** | 对受保护路由通过 gRPC ext-authz 调用 pep-proxy 完成身份认证 + RBAC 鉴权 |
| GW-F4 | **Metadata 透传 + 路径清洗** | 鉴权通过后将 JWT 中的身份信息转移到 X-Auth-* Headers，清洗路径为 upstream 格式 |
| GW-F5 | **默认 Deny** | 所有受保护请求默认拒绝，仅通过 AuthN + AuthZ 后放行 |
| GW-F6 | **跨命名空间路由** | 通过 ReferenceGrant 实现 agentgateway-system → keycloak/opa 的跨 namespace 路由 |

##### 用例图

```plantuml
@startuml
left to right direction
skinparam actorStyle awesome
skinparam usecase {
    BackgroundColor #F0F4FF
    BorderColor #4A90D9
}

actor "外部用户\n(浏览器/API客户端)" as User
actor "系统管理员" as Admin
actor "pep-proxy\n(鉴权服务)" as PEP
actor "业务后端服务" as Backend

rectangle "AgentGateway" {
    usecase "访问公开端点\n(Keycloak登录/OIDC)" as UC1
    usecase "访问受保护API\n(业务接口)" as UC2
    usecase "访问管理API\n(租户/策略管理)" as UC3
    usecase "ext-authz 鉴权\n(AuthN + AuthZ)" as UC4
    usecase "Header 注入\n(X-Auth-*)" as UC5
    usecase "路由分发" as UC6
    usecase "路径清洗/重写" as UC7
    usecase "跨namespace引用" as UC8
    usecase "默认 Deny\n未鉴权请求" as UC9
}

User --> UC1
User --> UC2
Admin --> UC3

UC2 ..> UC4 : <<include>>
UC3 ..> UC4 : <<include>>
UC4 --> PEP
UC4 ..> UC5 : <<ALLOW>>
UC4 ..> UC9 : <<DENY>>

UC1 ..> UC6 : <<include>>
UC5 ..> UC6 : <<include>>
UC6 ..> UC7 : <<extend>>
UC6 ..> UC8 : <<extend>>
UC6 --> Backend
@enduml
```

##### Gateway 视角的典型请求流转

```mermaid
sequenceDiagram
    autonumber
    participant Client as 客户端
    participant GW as Gateway :80
    participant PEP as pep-proxy :9000
    participant APP as 业务后端

    Client->>GW: GET /data-agent/da/patients<br/>Authorization: Bearer {JWT}

    Note over GW: 1. 路由匹配: tenant-api-route<br/>2. 检查 AgentgatewayPolicy: opa-ext-authz<br/>3. 触发 ext-authz Filter

    GW->>PEP: gRPC Check(CheckRequest)<br/>headers + dev.agentgateway.jwt metadata

    Note over PEP: 验证 JWT → 提取 tenant/roles<br/>→ 查询 OPA → 判定 ALLOW

    PEP-->>GW: CheckResponse: OK<br/>Headers: x-auth-user-id=xxx<br/>x-auth-tenant=data-agent<br/>x-auth-roles=viewer

    Note over GW: 4. 注入 X-Auth-* Headers 到请求<br/>5. 清洗路径: /data-agent/da/patients<br/>   → URLRewrite → /anything/data-agent/da/patients<br/>6. 转发到上游服务

    GW->>APP: GET /anything/data-agent/da/patients<br/>X-Auth-User-Id: user-uuid<br/>X-Auth-Tenant: data-agent<br/>X-Auth-Roles: viewer

    APP-->>GW: 200 OK + 业务数据
    GW-->>Client: 200 OK + 业务数据
```

##### 路由规则清单

| 路由名称 | 路径匹配 | 匹配类型 | 目标服务 | 目标 Namespace | 端口 | 是否鉴权 |
|---------|---------|---------|---------|---------------|------|---------|
| keycloak-route | `/realms/*` | PathPrefix | keycloak | keycloak | 8080 | 否 |
| keycloak-static-route | `/resources/*` | PathPrefix | keycloak | keycloak | 8080 | 否 |
| keycloak-admin-route | `/admin/*` | PathPrefix | keycloak | keycloak | 8080 | 否 |
| keycloak-proxy-route | `/api/v1/tenants/*`<br/>`/api/v1/common/*` | PathPrefix | keycloak-proxy | keycloak | 8090 | 是 |
| policy-api-route | `/api/v1/auth/*`<br/>`/api/v1/policies/*`<br/>`/api/v1/roles/*` | PathPrefix | pep-proxy | opa | 8000 | 是 |
| identity-api-route | `/api/v1/{realm}/roles\|groups\|users\|idp` | RegularExpression | keycloak-proxy | keycloak | 8090 | 是 |
| tenant-api-route | `/` (catch-all) | PathPrefix | httpbin (测试) | httpbin | 8000 | 是 |

---

#### 4.2.3 实现分析

##### Gateway 组件结构图

```mermaid
graph TB
    subgraph AgentGateway 模块
        direction TB

        subgraph CRD_Layer["资源定义层 (CRDs)"]
            direction LR
            GW["Gateway<br/>agentgateway-proxy<br/>监听 :80<br/>className: agentgateway"]
            HR1["HTTPRoute (免鉴权)<br/>keycloak-route<br/>keycloak-static-route<br/>keycloak-admin-route"]
            HR2["HTTPRoute (受保护)<br/>keycloak-proxy-route<br/>policy-api-route<br/>identity-api-route<br/>tenant-api-route"]
            AP["AgentgatewayPolicy<br/>opa-ext-authz<br/>→ pep-proxy:9000 gRPC"]
            RG["ReferenceGrant x3<br/>→ keycloak ns<br/>→ opa ns<br/>→ httpbin ns"]
        end

        subgraph Control_Plane["控制面"]
            CTRL["Gateway Controller<br/>cr.agentgateway.dev/controller:v2.2.0<br/>监听 CRD 变更<br/>生成 Envoy xDS 配置"]
        end

        subgraph Data_Plane["数据面"]
            ENVOY["Envoy Proxy<br/>cr.agentgateway.dev/agentgateway:0.11.1"]
            subgraph Filters["过滤器链"]
                ROUTE_FILTER["路由匹配"]
                EXTAUTH_FILTER["ext-authz Filter<br/>gRPC → pep-proxy:9000"]
                HEADER_FILTER["Header 注入/重写"]
            end
        end
    end

    CRD_Layer -->|xDS 推送| Control_Plane
    Control_Plane -->|配置下发| Data_Plane
    ENVOY --> ROUTE_FILTER
    ROUTE_FILTER --> EXTAUTH_FILTER
    EXTAUTH_FILTER --> HEADER_FILTER
```

##### Gateway 请求处理时序图

```mermaid
sequenceDiagram
    autonumber
    participant Client as 客户端
    participant Listener as Envoy Listener :80
    participant RouteMatch as 路由匹配引擎
    participant ExtAuth as ext-authz Filter
    participant PEP as pep-proxy :9000
    participant Upstream as 上游服务

    Client->>Listener: HTTP 请求进入

    Listener->>RouteMatch: 匹配 HTTPRoute 规则<br/>(PathPrefix / RegularExpression)

    alt 匹配免鉴权路由 (/realms/*, /resources/*, /admin/*)
        RouteMatch->>Upstream: 直接转发到 Keycloak :8080
        Upstream-->>Client: 响应
    else 匹配受保护路由
        RouteMatch->>RouteMatch: 检查是否关联 AgentgatewayPolicy

        RouteMatch->>ExtAuth: 触发 ext-authz 过滤器

        ExtAuth->>PEP: gRPC Check(CheckRequest)<br/>包含: HTTP method, path, headers<br/>metadata: dev.agentgateway.jwt

        alt pep-proxy 返回 ALLOW
            PEP-->>ExtAuth: CheckResponse: OK<br/>OkHttpResponse + Headers
            ExtAuth->>ExtAuth: 将 OkHttpResponse 中的 Headers<br/>注入到原始请求
            Note over ExtAuth: x-auth-user-id: {sub}<br/>x-auth-username: {name}<br/>x-auth-tenant: {tenant}<br/>x-auth-roles: {roles}<br/>x-auth-role-ids: {role_ids}<br/>x-auth-issuer: {iss}
            ExtAuth->>Upstream: 转发请求 + X-Auth-* Headers
            Upstream-->>Client: 业务响应

        else pep-proxy 返回 DENY
            PEP-->>ExtAuth: CheckResponse: DENIED<br/>HTTP 403
            ExtAuth-->>Client: HTTP 403 Forbidden<br/>"Forbidden by policy"
            Note over Upstream: 请求未到达上游服务

        else pep-proxy 不可用
            PEP-->>ExtAuth: gRPC 连接失败
            ExtAuth-->>Client: HTTP 503 Service Unavailable
        end
    end
```

##### 关键实现细节

**1. Gateway 资源定义**

系统只创建一个 Gateway 实例，通过 Helm chart 模板化管理：

```yaml
# charts/agentgateway/templates/gateway.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: agentgateway-proxy          # 唯一的 Gateway 实例名
  namespace: agentgateway-system
spec:
  gatewayClassName: agentgateway     # 由 AgentGateway Controller 提供
  listeners:
  - name: http
    protocol: HTTP
    port: 80                         # 单一对外端口
    allowedRoutes:
      namespaces:
        from: All                    # 允许所有 namespace 注册路由
```

**2. HTTPRoute 路由配置**

路由分为两组文件管理：

- **`keycloak-routes.yaml`** — 3 条免鉴权路由，将 Keycloak OIDC 端点（登录、静态资源、管理控制台）直接路由到 Keycloak 服务，不经过 ext-authz。
- **`protected-routes.yaml`** — 4 条受保护路由 + 1 条 AgentgatewayPolicy，覆盖租户管理、策略管理、身份管理和业务 API。

路由匹配支持两种模式：
- `PathPrefix`：精确的路径前缀匹配（如 `/api/v1/tenants`）
- `RegularExpression`：正则匹配（如 `/api/v1/[^/]+/(roles|groups|users|idp)(/.*)?`），用于处理路径中包含动态租户名的场景

**3. AgentgatewayPolicy ext-authz 配置**

```yaml
apiVersion: agentgateway.dev/v1alpha1
kind: AgentgatewayPolicy
metadata:
  name: opa-ext-authz
  namespace: agentgateway-system
spec:
  traffic:
    extAuth:
      backendRef:
        name: pep-proxy              # opa namespace 的 pep-proxy Service
        namespace: opa
        port: 9000                   # gRPC ext-authz 端口
      grpc: {}                       # 使用 gRPC 协议
  targetRefs:                        # 对以下 HTTPRoute 生效
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: keycloak-proxy-route       # 租户管理 API
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: policy-api-route           # 策略管理 API
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: identity-api-route         # 身份管理 API
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: tenant-api-route           # 业务 API (catch-all)
```

**4. 跨命名空间引用（ReferenceGrant）**

Gateway API 默认禁止跨 namespace 引用 Service，必须在目标 namespace 显式创建 ReferenceGrant 授权。系统创建了 3 个 ReferenceGrant：

| ReferenceGrant 名称 | 所在 Namespace | 授权来源 | 允许引用 |
|---------------------|---------------|---------|---------|
| allow-gateway-to-keycloak | keycloak | agentgateway-system 的 HTTPRoute | keycloak ns 中的 Service |
| allow-gateway-to-opa | opa | agentgateway-system 的 HTTPRoute + AgentgatewayPolicy | opa ns 中的 Service |
| allow-gateway-to-httpbin | httpbin | agentgateway-system 的 HTTPRoute | httpbin ns 中的 Service |

其中 `allow-gateway-to-opa` 同时授权了 HTTPRoute（路由到 pep-proxy REST API）和 AgentgatewayPolicy（ext-authz gRPC 调用），因为两者都需要跨 namespace 引用 opa namespace 中的 pep-proxy Service。

---

#### 4.2.4 资源分析

##### 容器镜像

| 镜像 | 版本 | 来源 | 用途 | 备注 |
|------|------|------|------|------|
| `cr.agentgateway.dev/controller` | v2.2.0-main | AgentGateway 官方 | Gateway Controller（控制面） | 监听 CRD 变更，生成 Envoy 配置 |
| `cr.agentgateway.dev/agentgateway` | 0.11.1 | AgentGateway 官方 | Envoy Proxy（数据面） | 实际处理流量的代理实例 |

##### 部署视图

```plantuml
@startuml
skinparam rectangle {
    BackgroundColor<<node>> #F0F4FF
    BackgroundColor<<gw>> #D6EAF8
    BackgroundColor<<ctrl>> #FEF9E7
}

rectangle "生产主机 (Host)" {
    rectangle "External IP 1" as eip1
    rectangle "External IP 2" as eip2

    rectangle "Kubernetes Cluster" {
        rectangle "Gateway Controller x1\n(集群级，无状态)" as ctrl <<ctrl>>

        rectangle "Node A" <<node>> {
            rectangle "AgentGateway Proxy 1\n(Envoy)" as gw1 <<gw>>
            rectangle "App Instance 1" as app1
        }

        rectangle "Node B" <<node>> {
            rectangle "AgentGateway Proxy 2\n(Envoy)" as gw2 <<gw>>
            rectangle "App Instance 2" as app2
        }
    }
}

eip1 -- gw1
eip2 -- gw2
gw1 --> app1
gw1 --> app2
gw2 --> app1
gw2 --> app2
ctrl ..> gw1 : xDS 配置推送
ctrl ..> gw2 : xDS 配置推送
@enduml
```

> **部署说明：** 不使用 MetalLB（L2 需要宿主 ARP，L3 需要宿主 iptable + BGP 端口），提供 2 个 External IP，路由委托给客户网络。每个节点部署 1 个 Proxy 副本，Controller 仅需 1 个。

##### 计算资源

| 组件 | 副本数 | 部署策略 | CPU Request | CPU Limit | Memory Request | Memory Limit |
|------|--------|---------|-------------|-----------|----------------|--------------|
| Gateway Controller | 1 | Deployment | 官方默认 | 官方默认 | 官方默认 | 官方默认 |
| Envoy Proxy | 2 | 1 node 1 replica | 官方默认 | 官方默认 | 官方默认 | 官方默认 |

> Gateway 组件为无状态服务，资源配置由 AgentGateway 官方 Helm chart 管理。配置 HPA 规则可根据流量规模水平扩展 Proxy 副本数。

##### 存储资源

Gateway 模块**无持久化存储需求**。路由配置和策略规则以 CRD 资源形式存储在 Kubernetes etcd 中。

##### 网络资源

| 需求 | 说明 |
|------|------|
| 对外端口 | 80 (HTTP)，生产环境建议增加 443 (HTTPS/TLS 终止) |
| External IP | 每节点 1 个，路由委托给客户网络 |
| 集群内网络 | 需要跨 namespace 网络连通（agentgateway-system <-> keycloak / opa / httpbin） |
| DNS 解析 | 依赖 Kubernetes 集群内 DNS（CoreDNS）进行 Service 发现 |

##### 外部依赖

| 依赖项 | 类型 | 必要性 | 说明 |
|--------|------|--------|------|
| Kubernetes Gateway API CRDs | 集群级 CRD | 必需 | 提供 Gateway、HTTPRoute、ReferenceGrant 资源定义 |
| AgentGateway CRDs | 集群级 CRD | 必需 | 提供 AgentgatewayPolicy 等自定义资源定义 |
| pep-proxy Service (opa ns) | ClusterIP Service | 必需 | ext-authz gRPC 鉴权后端，Gateway 核心依赖 |
| keycloak Service (keycloak ns) | ClusterIP Service | 必需 | Keycloak OIDC/SAML 端点 |
| keycloak-proxy Service (keycloak ns) | ClusterIP Service | 必需 | 租户管理 API 后端 |
| ReferenceGrant 资源 | namespace 级 | 必需 | 跨 namespace 路由的授权前提，缺失则路由不生效 |

---

#### 4.2.5 接口标准

##### 对外暴露端口

| 端口 | 协议 | 说明 |
|------|------|------|
| 80 | HTTP/1.1 | 唯一的外部入口端口，所有客户端请求通过此端口进入 |

在 Kind 开发环境中，通过 NodePort 或 `kubectl port-forward` 将 Gateway :80 暴露到宿主机。

##### 内部通信协议

| 通信方向 | 协议 | 端口 | 说明 |
|---------|------|------|------|
| Gateway → pep-proxy (ext-authz) | gRPC | 9000 | Envoy ext-authz v3 Authorization/Check |
| Gateway → Keycloak | HTTP/1.1 | 8080 | Keycloak OIDC/SAML 端点 |
| Gateway → keycloak-proxy | HTTP/1.1 | 8090 | 租户管理 REST API |
| Gateway → pep-proxy (REST) | HTTP/1.1 | 8000 | 策略管理 REST API |
| Gateway → httpbin | HTTP/1.1 | 8000 | 测试后端 |
| Controller → Envoy Proxy | xDS (gRPC) | 内部 | 控制面配置推送 |

##### ext-authz gRPC 接口规范

Gateway 通过 ext-authz 过滤器调用 pep-proxy，遵循 Envoy ext-authz v3 协议：

**Service 定义：**

```protobuf
service Authorization {
  rpc Check(CheckRequest) returns (CheckResponse);
}
```

**请求内容（CheckRequest）：**

Gateway 自动将以下信息封装到 CheckRequest 中：

| 字段 | 来源 | 说明 |
|------|------|------|
| `attributes.request.http.method` | 原始请求 | HTTP 方法 (GET/POST/PUT/DELETE) |
| `attributes.request.http.path` | 原始请求 | 请求路径 |
| `attributes.request.http.headers` | 原始请求 | 全部 HTTP Headers（含 Authorization） |
| gRPC metadata `dev.agentgateway.jwt` | Gateway 预验证 | 已验证的 JWT Claims JSON |

**响应格式（CheckResponse）：**

| 响应类型 | gRPC Status Code | HTTP 行为 | 包含内容 |
|---------|------------------|-----------|---------|
| ALLOW | 0 (OK) | 请求继续转发到上游 | OkHttpResponse: 要注入的 Headers 列表 |
| DENY (401) | 16 (UNAUTHENTICATED) | 直接返回客户端 | DeniedHttpResponse: HTTP 401 + 错误消息 |
| DENY (403) | 7 (PERMISSION_DENIED) | 直接返回客户端 | DeniedHttpResponse: HTTP 403 + 错误消息 |
| DENY (503) | 14 (UNAVAILABLE) | 直接返回客户端 | DeniedHttpResponse: HTTP 503 + 错误消息 |

##### Gateway 注入的 Headers

鉴权通过后，pep-proxy 通过 OkHttpResponse 返回以下 Headers，由 Gateway 注入到转发给业务后端的请求中：

| Header 名称 | 值来源 | 示例值 | 业务用途 |
|-------------|-------|--------|---------|
| `x-auth-user-id` | JWT `sub` claim | `f47ac10b-58cc-4372-a567-0e02b2c3d479` | 识别当前用户 |
| `x-auth-username` | JWT `preferred_username` | `john.doe` | 显示用户名 |
| `x-auth-roles` | JWT `roles[].name` | `viewer,editor` | 界面级权限控制 |
| `x-auth-role-ids` | JWT `roles[].id` | `uuid1,uuid2` | 细粒度策略绑定 |
| `x-auth-issuer` | JWT `iss` | `http://localhost/realms/tenant-a` | Token 来源追溯 |
| `x-auth-tenant` | 从 `iss` 提取 | `tenant-a` | 租户标识，数据隔离 |

##### 对外 API 路径规范

Gateway 定义了系统对外的统一 API 路径体系：

**管理面 API（通过 Gateway 鉴权后路由到各后端）：**

| 路径 | 目标 | 说明 |
|------|------|------|
| `POST /api/v1/tenants` | keycloak-proxy | 创建租户 |
| `DELETE /api/v1/tenants/{realm}` | keycloak-proxy | 删除租户 |
| `POST /api/v1/{realm}/idp/saml/import` | keycloak-proxy | 导入 SAML 配置 |
| `POST /api/v1/{realm}/idp/saml/instances` | keycloak-proxy | 创建 SAML IDP |
| `GET/POST/PUT/DELETE /api/v1/{realm}/roles` | keycloak-proxy | 角色 CRUD |
| `GET/POST/PUT/DELETE /api/v1/{realm}/groups` | keycloak-proxy | 群组 CRUD |
| `GET /api/v1/{realm}/users` | keycloak-proxy | 查看用户 |
| `POST /api/v1/policies` | pep-proxy | 创建策略 |
| `GET/PUT/DELETE /api/v1/policies/{id}` | pep-proxy | 策略 CRUD |
| `POST /api/v1/roles/{role_id}/policy` | pep-proxy | 角色绑定策略 |

**业务面 API（PATH 规范）：**

```
普通用户接口:   /{tenant-id}/{app}/{resource}
管理员接口:     /{tenant-id}/{app}/admin/{resource}
```

Gateway 通过 catch-all 路由将业务请求路由到后端，OPA 从路径中自动提取 tenant-id 和 resource 进行鉴权。

##### CRD 资源接口汇总

| CRD | API Group | 作用 | 关键字段 |
|-----|-----------|------|---------|
| Gateway | gateway.networking.k8s.io/v1 | 定义网关监听器 | `spec.listeners[].port`, `spec.gatewayClassName` |
| HTTPRoute | gateway.networking.k8s.io/v1 | 定义路由规则 | `spec.rules[].matches`, `spec.rules[].backendRefs` |
| ReferenceGrant | gateway.networking.k8s.io/v1beta1 | 跨 namespace 授权 | `spec.from[]`, `spec.to[]` |
| AgentgatewayPolicy | agentgateway.dev/v1alpha1 | ext-authz 策略 | `spec.traffic.extAuth`, `spec.targetRefs` |

---

#### 4.2.6 工作量估计

| 工作项 | 预估工时 | 状态 | 说明 |
|--------|---------|------|------|
| Gateway Helm chart 编写 | 1 人天 | 已完成 | charts/agentgateway/ 模板 |
| HTTPRoute 路由配置（免鉴权组） | 0.5 人天 | 已完成 | keycloak-routes.yaml |
| HTTPRoute 路由配置（受保护组） | 1 人天 | 已完成 | protected-routes.yaml，含正则匹配 |
| AgentgatewayPolicy ext-authz 配置 | 0.5 人天 | 已完成 | gRPC ext-authz 策略 |
| ReferenceGrant 跨 namespace 配置 | 0.5 人天 | 已完成 | 3 个 namespace 的授权 |
| 部署脚本与测试验证 | 1 人天 | 已完成 | setup.sh + test.sh 中 Gateway 相关 |
| TLS/HTTPS 终止配置 | 1 人天 | 待开发 | Gateway listener TLS 证书 |
| HPA 弹性扩缩配置 | 0.5 人天 | 待开发 | 基于 CPU/连接数 |
| 流量限速（Rate Limiting） | 2 人天 | 待开发 | Envoy RateLimit filter + 外部服务 |
| 可观测性（日志/Metrics/Tracing） | 2 人天 | 待开发 | Envoy access log + Prometheus |

##### 后续扩展能力（基于 Envoy）

| 能力 | 实现方式 | 当前状态 |
|------|---------|---------|
| TLS 终止 / HTTPS | Gateway listener 配置 TLS 证书 | 待配置 |
| 流量限速（Rate Limiting） | Envoy RateLimit filter + 外部限速服务 | 待扩展 |
| 负载均衡 | Envoy 内置（Round Robin / Weighted / Least Request） | 已生效（默认 Round Robin） |
| 熔断降级 | Envoy Circuit Breaker（最大连接数/请求数/重试数） | 待配置 |
| 超时与重试 | HTTPRoute filter 或 BackendPolicy | 待配置 |
| CORS 管理 | Gateway API 或 Envoy CORS filter | 待配置 |
| 请求/响应改写 | URLRewrite filter（已用于 catch-all 路由） | 部分已用 |
| 访问日志 | Envoy access log + 文件/gRPC 输出 | 待配置 |
| 指标采集 | Envoy 内置 Prometheus metrics 端点 | 待配置 |
| 链路追踪 | Envoy Tracing（Jaeger/Zipkin/OpenTelemetry） | 待配置 |
