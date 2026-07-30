# da-cluster 架构设计文档

## 整体定位

da-cluster 是一个**多租户统一鉴权网关**，解决的核心问题是：

> 产品要卖给多个客户，每个客户有自己的员工体系（AD/LDAP/企业微信/SAML SSO）。不可能让每个客户的员工重新注册一套账号。da-cluster 通过 Keycloak 联合客户已有的身份系统，统一签发 JWT，下游服务零改动接入鉴权。

---

## 整体架构（请求流）

```
客户员工浏览器
    |
    v
AgentGateway (统一入口, port 80)
    |
    +-- /realms/*           --> Keycloak (登录/Token签发, 无鉴权)
    +-- /api/v1/tenants/*   --> keycloak-proxy (租户管理, ext-authz鉴权)
    +-- /api/v1/policies/*  --> pep-proxy (策略管理, ext-authz鉴权)
    +-- /{tenant-id}/**     --> 业务后端 (ext-authz鉴权)
          |
          v
      ext-authz (gRPC) --> pep-proxy --> OPA
          |                              |
          |    +-------------------------+
          |    | 三层判断:
          |    |  1. super-admin? --> 全部放行
          |    |  2. tenant-admin + 本租户? --> 放行
          |    |  3. 普通用户 --> 查 role_binding --> policy.rules
          |    |
          v    v
      ALLOW --> 注入 X-Auth-* Headers --> 转发到业务后端
      DENY  --> 返回 403
```

---

## 命名空间划分

| 命名空间 | 组件 | 职责 |
|----------|------|------|
| `agentgateway-system` | Gateway Controller + Proxy | 统一流量入口、路由分发、ext-authz 挂载 |
| `keycloak` | Keycloak, PostgreSQL, keycloak-proxy, keycloak-init | 身份联合、Token 签发、租户/角色/用户管理 API |
| `opa` | OPAL Server, PEP Proxy (pep-proxy + bundle-server + opal-client) | 动态策略引擎、ext-authz gRPC 服务 |
| `httpbin` | httpbin 测试后端 | 模拟业务服务，验证鉴权流程 |

---

## 各组件职责

### 1. Keycloak（身份联合）

**不是让客户重新注册，而是"联合"他们已有的身份系统。**

```
客户 A 的 AD ---SAML---+
                        +--> Keycloak realm "customer-a" --> 签发统一格式的 JWT
客户 B 的 OIDC SSO ----+
                        +--> Keycloak realm "customer-b" --> 签发统一格式的 JWT
```

- **一个 realm = 一个租户 = 一个客户**
- 客户员工通过 IDP 联合登录（SAML/OIDC），Keycloak 在本地建一个影子用户
- 登录后 Keycloak 签发标准 JWT，包含 `iss=.../realms/{tenant-id}`, `roles=[{id, name}]`
- 下游服务只看这个 JWT，不关心客户原来用的是 AD 还是企业微信

**自定义 SPI**：`keycloak-custom` 镜像内置 `structured-realm-role-mapper` SPI，将 realm roles 输出为 `[{id, name}]` 结构（而非 Keycloak 默认的字符串数组），使 OPA 可以通过 role UUID 做细粒度策略绑定。

### 2. keycloak-proxy（租户管理 API）

给**运维/管理员**用的管理面板后端（源码：`da-idb-proxy/app/`）：

| API | 用途 |
|-----|------|
| `POST /api/v1/tenants` | 新客户来了 -> 创建 realm + 默认 client + mapper + 管理员角色/用户 |
| `DELETE /api/v1/tenants/{realm}` | 客户下线 -> 删除整个 realm |
| `GET /api/v1/{realm}/roles` | 管理该客户的业务角色 |
| `GET/PUT/DELETE /api/v1/{realm}/roles/by-id/{uuid}` | 通过 UUID 管理角色（支持改名） |
| `POST /api/v1/{realm}/groups` | 按部门/项目分组，批量绑角色 |
| `GET /api/v1/{realm}/users` | 查看租户下的用户列表 |
| `POST /api/v1/{realm}/idp/saml/instances` | 对接客户的 SSO -> 配置 SAML IDP |
| `GET/POST/PUT/DELETE .../mappers` | 管理 IDP Protocol Mappers |
| `POST /api/v1/{realm}/idp/saml/import` | 通过 XML 文件导入 SAML 配置 |
| `GET /api/v1/common/health` | 健康检查 |

### 3. pep-proxy + OPA（动态策略引擎）

**解决的问题：不同客户有不同的权限规则，而且规则可能随时变。**

```
policy 示例：
  name: "documents-allow"
  rules: [{resource: "documents", effect: "allow"}]

role_binding：
  role_id (普通用户角色UUID) --> policy_id ("documents-allow")
```

**OPA 三层授权逻辑（Rego 策略）：**

1. **super-admin** -> 全放行（运维人员，master realm 签发的 token）
2. **tenant-admin** -> 本租户内全放行（客户的管理员）
3. **普通用户** -> 查 `role UUID -> role_binding -> policy -> rules`，匹配 resource + effect

**数据流：**
- bundle-server 从 PostgreSQL 读取 policies 和 role_bindings
- OPAL 将数据实时同步到 OPA
- pep-proxy 启动时将 Rego 策略直接推送到 OPA

| API | 用途 |
|-----|------|
| `POST /api/v1/policies` | 创建策略 |
| `POST /api/v1/roles/{role_id}/policy` | 绑定角色到策略 |
| `POST /api/v1/auth/check` | 手动鉴权检查（调试用） |
| `GET /api/v1/policies/templates` | 获取策略模板列表 |

### 4. AgentGateway（流量入口 + ext-authz）

所有请求统一入口，挂 ext-authz 策略后：
- 每个请求先 gRPC 调 pep-proxy -> OPA 判断
- 通过 -> 注入 `X-Auth-User-Id`, `X-Auth-Tenant`, `X-Auth-Roles` 等 Headers
- 拒绝 -> 直接 403

---

## 业务 API 路径设计

```
普通用户接口:   /{tenant-id}/{app}/{resource}
管理员接口:     /{tenant-id}/{app}/admin/{resource}
```

**这是给业务后端用的路径规范，不是 keycloak-proxy 的路径。** 设计意图：

1. **路径即权限声明** — OPA 从 URL 自动提取 `tenant-id` 和 `resource`，对比 JWT 中的 realm 做租户隔离，业务后端零代码改动
2. **`/admin/` 段是权限升级标记** — OPA Rego 检测路径中是否包含 `admin`，有则要求 `tenant-admin` 角色
3. **多应用共存** — `{app}` 段让同一租户下的多个微服务各自独立路由，互不干扰
4. **后端无感知** — 业务后端只需读 `X-Auth-*` Headers，完全不碰 JWT 解析、不碰 Keycloak

### 示例

假设应用叫 `da`，租户是 `data-agent`：

| API 路径 | 权限级别 | 谁能访问 |
|---------|---------|---------|
| `/data-agent/da/patients` | 普通用户 | 有对应 policy 绑定的用户 |
| `/data-agent/da/records/detail/123` | 普通用户 | 有对应 policy 绑定的用户 |
| `/data-agent/da/admin/settings` | 租户管理员 | tenant-admin 及以上 |
| `/data-agent/da/admin/users` | 租户管理员 | tenant-admin 及以上 |

---

## 完整接入流程（一个新客户来了）

```
1. 运维调 POST /api/v1/tenants {realm: "customer-a"}
   --> Keycloak 创建 realm + client + structured-role-mapper + tenant-admin 角色/用户

2. 运维调 POST /api/v1/customer-a/idp/saml/instances
   --> 配置客户 A 的 SAML SSO 对接

3. 客户 A 的管理员登录后，调 POST /api/v1/policies
   --> 创建业务策略 (如 "documents-allow")

4. 管理员调 POST /api/v1/roles/{role-uuid}/policy
   --> 把策略绑到角色上

5. 客户 A 的普通员工通过 SSO 登录 --> Keycloak 签发 JWT
   --> 访问 /customer-a/da/documents --> OPA 检查 role->policy->rules --> 放行
   --> 访问 /customer-a/da/admin/settings --> OPA 检查 is_admin --> 403
```

---

## 镜像清单

| 镜像 | 源码 | 说明 |
|------|------|------|
| `keycloak-proxy:v2` | `da-idb-proxy/app/` | 租户/角色/用户/IDP 管理 API |
| `opal-proxy:v1` | `opal-dynamic-policy/` | pep-proxy + bundle-server (supervisord 管理) |
| `keycloak-init:v1` | `da-cluster/images/keycloak-init/` | 初始化 Job: 创建 super-admin、默认租户、service client |
| `keycloak-custom:26.5.2` | `da-cluster/images/keycloak-custom/` | Keycloak + structured-realm-role-mapper SPI |
| `postgres:17` | 官方镜像 | 共享数据库 (keycloak + opal 两个库) |
| `cr.agentgateway.dev/controller` | 官方镜像 | AgentGateway 控制器 |
| `cr.agentgateway.dev/agentgateway` | 官方镜像 | AgentGateway 代理 |
| `permitio/opal-server` | 官方镜像 | OPAL 策略/数据同步服务器 |
| `permitio/opal-client` | 官方镜像 | OPAL 客户端 (含内嵌 OPA) |

---

## 可扩展方向

| 方向 | 说明 |
|------|------|
| OIDC IDP 联合 | 目前只做了 SAML，可以加 OIDC IDP 对接（微信/钉钉/Google） |
| 细粒度数据权限 | 当前 policy.rules 只到 resource 级别，可以扩展 conditions（如 `department=xxx`） |
| 审计日志 | pep-proxy 的 ext-authz 日志可以收集到 ELK，做"谁在什么时候访问了什么" |
| Token Exchange | 已支持 `/token/exchange`，授权码换 token（前端 SPA 场景） |
| 动态路由注册 | 目前新服务接入要手动写 HTTPRoute YAML，可以做成 API 自动注册 |
