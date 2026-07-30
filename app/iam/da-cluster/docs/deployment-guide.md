# da-cluster 部署指南

## 目录

- [总体架构](#总体架构)
- [Phase 1: 基础集群部署](#phase-1-基础集群部署)
- [Phase 2: 部署前端项目到 nginx](#phase-2-部署前端项目到-nginx)
- [Phase 3: 替换 httpbin 为自定义后端](#phase-3-替换-httpbin-为自定义后端)
- [Phase 4: 验证](#phase-4-验证)
- [附录](#附录)

---

## 总体架构

```
用户浏览器
    |
    +-- :8080 (port-forward / NodePort)
        |
        AgentGateway Proxy (统一入口)
        |
        +-- /realms/*                --> Keycloak (OIDC, 无鉴权)
        +-- /admin/*                 --> Keycloak Admin Console (无鉴权)
        |
        |  -- ext-authz (OPA) 鉴权 --
        |
        +-- /api/v1/tenants          --> keycloak-proxy (租户管理)
        +-- /api/v1/common           --> keycloak-proxy (健康检查)
        +-- /api/v1/{realm}/roles    --> keycloak-proxy (角色管理)
        +-- /api/v1/{realm}/groups   --> keycloak-proxy (组管理)
        +-- /api/v1/{realm}/users    --> keycloak-proxy (用户管理)
        +-- /api/v1/{realm}/idp      --> keycloak-proxy (IDP/SAML)
        +-- /api/v1/{realm}/by-id    --> keycloak-proxy (角色 UUID 管理)
        +-- /api/v1/{realm}/token    --> keycloak-proxy (Token Exchange)
        +-- /api/v1/policies         --> pep-proxy (策略管理)
        +-- /api/v1/roles            --> pep-proxy (角色-策略绑定)
        +-- /api/v1/auth             --> pep-proxy (鉴权检查)
        |
        +-- /app                     --> nginx (你的前端 UI)
        +-- /{tenant-id}/**          --> your-backend (你的后端服务)
```

## Phase 1: 基础集群部署

```bash
# 1. 传输到服务器
scp -r auth/ user@server:/opt/auth/

# 2. SSH 到服务器
ssh user@server
cd /opt/auth/da-cluster
chmod +x scripts/setup.sh
```

### 模式 A: Kind 集群（默认，开发测试用）

前置条件: docker, kind, kubectl, helm

```bash
./scripts/setup.sh
```

### 模式 B: 已有 K8s 集群 (`--no-kind`)

前置条件: kubectl, helm, ctr (containerd 自带)

```bash
# 单节点集群
./scripts/setup.sh --no-kind

# 多节点集群
K8S_NODES="192.168.1.10 192.168.1.11" \
K8S_NODE_USER=root \
./scripts/setup.sh --no-kind
```

### 模式 C: 无网络环境代码热更新 (`--fat-base`)

当服务器无网络，只修改了业务代码（不改依赖）时使用。从预装好依赖的"胖基础镜像"重建，无需 pip install / apt-get。

前置条件: 胖基础镜像已预加载（首次部署时由 offline tar 提供）

```bash
./scripts/setup.sh --no-kind --fat-base
```

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `PLATFORM` | 强制指定平台: `amd64` 或 `arm64` | 自动检测 (`uname -m`) |
| `CLUSTER_NAME` | Kind 集群名 | `da-cluster` |
| `K8S_NODES` | 空格分隔的节点 IP（多节点时必填） | 空（本机模式） |
| `K8S_NODE_USER` | SSH 登录节点的用户名 | `root` |
| `IMAGE_DIR` | 节点上镜像临时存放目录 | `/tmp/da-images` |
| `KUBECONFIG` | kubeconfig 路径 | `~/.kube/config` |

### 部署后

```bash
# 开启端口转发（Kind 模式）
kubectl -n agentgateway-system port-forward svc/agentgateway-proxy 8080:80 &

# 或暴露 NodePort（K8s 模式，生产推荐）
kubectl -n agentgateway-system patch svc agentgateway-proxy \
  -p '{"spec":{"type":"NodePort","ports":[{"port":80,"nodePort":30080}]}}'

# 验证
./scripts/test.sh
```

---

## Phase 2: 部署前端项目到 nginx

将任意前端项目（Vue/React/Angular/纯静态）打包为 nginx 镜像，部署到集群，通过 Gateway 对外服务。

### Step 1: 准备 nginx 配置

在前端项目根目录创建 `nginx.conf`:

```nginx
server {
    listen 80;
    server_name _;

    root /usr/share/nginx/html;
    index index.html;

    # SPA 路由: 所有未匹配文件的请求回退到 index.html
    location / {
        try_files $uri $uri/ /index.html;
    }

    # 静态资源缓存
    location ~* \.(js|css|png|jpg|jpeg|gif|ico|svg|woff2?)$ {
        expires 30d;
        add_header Cache-Control "public, immutable";
    }

    # API 代理到 AgentGateway (集群内 Service 地址)
    location /api/ {
        proxy_pass http://agentgateway-proxy.agentgateway-system.svc.cluster.local:80;
        proxy_set_header Host localhost;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    }

    # Keycloak OIDC 代理 (前端获取 token 用)
    location /realms/ {
        proxy_pass http://agentgateway-proxy.agentgateway-system.svc.cluster.local:80;
        proxy_set_header Host localhost;
    }
}
```

> `proxy_set_header Host localhost` 不可省略。Gateway HTTPRoute 的 hostnames 配置为 localhost，Host 头不匹配会路由失败。

### Step 2: 构建前端镜像

```dockerfile
FROM nginx:alpine
COPY dist/ /usr/share/nginx/html/
COPY nginx.conf /etc/nginx/conf.d/default.conf
EXPOSE 80
```

```bash
# 构建并导出（在联网机上）
docker build -t my-frontend:v1 .
docker save -o da-cluster/offline/images/amd64/my-frontend_v1.tar my-frontend:v1

# ARM64 服务器需要交叉编译
docker buildx build --platform linux/arm64 -t my-frontend:v1-arm64 --load .
docker save -o da-cluster/offline/images/arm64/my-frontend_v1.tar my-frontend:v1-arm64
```

### Step 3: 部署到集群

创建 `gateway-routes/my-frontend.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: frontend
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-frontend
  namespace: frontend
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-frontend
  template:
    metadata:
      labels:
        app: my-frontend
    spec:
      containers:
      - name: nginx
        image: my-frontend:v1
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 80
---
apiVersion: v1
kind: Service
metadata:
  name: my-frontend
  namespace: frontend
spec:
  type: ClusterIP
  ports:
  - port: 80
    targetPort: 80
  selector:
    app: my-frontend
```

```bash
kind load docker-image my-frontend:v1 --name da-cluster   # Kind 模式
# 或: ctr -n k8s.io images import my-frontend_v1.tar       # K8s 模式
kubectl apply -f gateway-routes/my-frontend.yaml
```

### Step 4: 配置 Gateway 路由

在 `gateway-routes/reference-grants.yaml` 中追加:

```yaml
---
apiVersion: gateway.networking.k8s.io/v1beta1
kind: ReferenceGrant
metadata:
  name: allow-agentgateway-to-frontend
  namespace: frontend
spec:
  from:
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    namespace: agentgateway-system
  to:
  - group: ""
    kind: Service
```

创建 `gateway-routes/frontend-route.yaml`:

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: frontend-route
  namespace: agentgateway-system
spec:
  parentRefs:
  - name: agentgateway-proxy
    namespace: agentgateway-system
  hostnames:
  - "localhost"
  rules:
  - matches:
    - path:
        type: PathPrefix
        value: /app
    backendRefs:
    - name: my-frontend
      namespace: frontend
      port: 80
```

> 前端路由不需要挂 ext-authz，鉴权发生在前端调 API 时。

```bash
kubectl apply -f gateway-routes/reference-grants.yaml
kubectl apply -f gateway-routes/frontend-route.yaml
curl http://localhost:8080/app/   # 应返回 index.html
```

---

## Phase 3: 替换 httpbin 为自定义后端

当前集群使用 httpbin 作为测试后端。生产环境替换为你自己的后端服务。

### 后端 API 路径规范

OPA 根据路径自动判断权限，你的后端 API 必须遵循:

```
普通用户接口:   /{tenant-id}/{app}/{resource}
管理员接口:     /{tenant-id}/{app}/admin/{resource}
```

详见 [鉴权应用接入最佳实践.md](鉴权应用接入最佳实践.md)。

### Step 1: 准备后端镜像

```bash
cd /path/to/your-backend
docker build -t my-backend:v1 .
docker save -o da-cluster/offline/images/amd64/my-backend_v1.tar my-backend:v1
```

### Step 2: 创建 K8s 资源

创建 `gateway-routes/my-backend.yaml`:

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: backend
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-backend
  namespace: backend
spec:
  replicas: 1
  selector:
    matchLabels:
      app: my-backend
  template:
    metadata:
      labels:
        app: my-backend
    spec:
      containers:
      - name: app
        image: my-backend:v1
        imagePullPolicy: IfNotPresent
        ports:
        - containerPort: 8000
---
apiVersion: v1
kind: Service
metadata:
  name: my-backend
  namespace: backend
spec:
  type: ClusterIP
  ports:
  - port: 8000
    targetPort: 8000
  selector:
    app: my-backend
```

### Step 3: 配置路由和鉴权

ReferenceGrant + HTTPRoute + ext-authz:

```yaml
# reference-grants.yaml 追加
---
apiVersion: gateway.networking.k8s.io/v1beta1
kind: ReferenceGrant
metadata:
  name: allow-agentgateway-to-backend
  namespace: backend
spec:
  from:
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    namespace: agentgateway-system
  to:
  - group: ""
    kind: Service
```

```yaml
# gateway-routes/backend-route.yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: my-backend-route
  namespace: agentgateway-system
spec:
  parentRefs:
  - name: agentgateway-proxy
    namespace: agentgateway-system
  hostnames:
  - "localhost"
  rules:
  - matches:
    - path:
        type: PathPrefix
        value: /data-agent/da       # /{tenant-id}/{app}
    backendRefs:
    - name: my-backend
      namespace: backend
      port: 8000
```

在 `protected-routes.yaml` 的 AgentgatewayPolicy 中添加:

```yaml
  targetRefs:
  # ... 现有 targets ...
  - group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: my-backend-route
```

### Step 4: 部署

```bash
kind load docker-image my-backend:v1 --name da-cluster
kubectl apply -f gateway-routes/my-backend.yaml
kubectl apply -f gateway-routes/reference-grants.yaml
kubectl apply -f gateway-routes/backend-route.yaml
kubectl apply -f gateway-routes/protected-routes.yaml
```

后端通过 ext-authz 鉴权后，可以从请求 Headers 获取用户信息:

| Header | 说明 |
|--------|------|
| `X-Auth-User-Id` | 用户 UUID |
| `X-Auth-Username` | 用户名 |
| `X-Auth-Roles` | 角色名（逗号分隔） |
| `X-Auth-Role-Ids` | 角色 UUID（逗号分隔） |
| `X-Auth-Tenant` | 租户 ID |
| `X-Auth-Issuer` | Token 签发者 |

---

## Phase 4: 验证

### 自动化测试

```bash
kubectl -n agentgateway-system port-forward svc/agentgateway-proxy 8080:80 &
./scripts/test.sh
```

### 手动 API 验证

```bash
# 获取 token
CLIENT_SECRET=$(kubectl -n keycloak get secret keycloak-idb-proxy-client \
  -o jsonpath='{.data.client-secret}' | base64 -d)

TOKEN=$(curl -s -X POST http://localhost:8080/realms/master/protocol/openid-connect/token \
  -d "grant_type=client_credentials" \
  -d "client_id=idb-proxy-client" \
  -d "client_secret=${CLIENT_SECRET}" | jq -r '.access_token')

# 测试各 API
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/tenants
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/data-agent/roles
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/data-agent/users
curl -H "Authorization: Bearer $TOKEN" http://localhost:8080/api/v1/policies
```

### 绕过 Gateway 直接测试服务接口

当需要排查某个服务本身是否正常（排除 Gateway 路由和 ext-authz 的干扰）时，可以在集群内启动一个临时 curl Pod，通过 K8s 内部 DNS 直接访问各服务：

```bash
# 启动临时 curl Pod（退出后自动删除）
kubectl run curl-test --image=curlimages/curl -it --rm -- sh
```

进入 Pod 后，使用 `{service}.{namespace}.svc:{port}` 格式访问：

```bash
# ---- Keycloak 服务 ----
# Keycloak Server 原生端点
curl http://keycloak.keycloak.svc:8080/realms/master
curl http://keycloak.keycloak.svc:8080/realms/master/.well-known/openid-configuration

# keycloak-proxy 管理 API
curl http://keycloak-proxy.keycloak.svc:8090/api/v1/common/health

# ---- OPA 服务 ----
# pep-proxy REST API
curl http://pep-proxy.opa.svc:8000/health

# bundle-server
curl http://bundle-server.opa.svc:8001/health

# OPAL Server
curl http://opal-server.opa.svc:7002/

# ---- 需要 Token 的接口 ----
# 先从 Keycloak 获取 Token（集群内部地址）
TOKEN=$(curl -s -X POST http://keycloak.keycloak.svc:8080/realms/master/protocol/openid-connect/token \
  -d "grant_type=password" \
  -d "client_id=admin-cli" \
  -d "username=admin" \
  -d "password=admin" | sed 's/.*"access_token":"\([^"]*\)".*/\1/')

# 直接测 keycloak-proxy（绕过 Gateway 和 ext-authz）
curl -H "Authorization: Bearer $TOKEN" http://keycloak-proxy.keycloak.svc:8090/api/v1/tenants

# 直接测 pep-proxy
curl -H "Authorization: Bearer $TOKEN" http://pep-proxy.opa.svc:8000/api/v1/policies
```

> **说明：** 这种方式绕过了 Gateway 的 ext-authz 鉴权，仅用于验证服务本身的接口是否可用。生产环境的正式请求必须通过 Gateway 入口。OPA 引擎（:8181）运行在 pep-proxy 同 Pod 的 sidecar 容器中，不对外暴露 Service，只能通过 `kubectl exec` 进入 Pod 后访问 `localhost:8181`。

---

## 附录

### 完整 API 路由表

#### keycloak-proxy 路由

| Method | Path | 功能 | 状态码 |
|--------|------|------|--------|
| GET | `/api/v1/common/health` | 健康检查 | 200 |
| GET | `/api/v1/tenants` | 租户列表 | 200 |
| POST | `/api/v1/tenants` | 创建租户（自动创建 client + mapper + admin） | 201 |
| DELETE | `/api/v1/tenants/{realm}` | 删除租户 | 204 |
| GET | `/api/v1/{realm}/roles` | 角色列表 | 200 |
| POST | `/api/v1/{realm}/roles` | 创建角色 | 201 |
| GET | `/api/v1/{realm}/roles/{name}` | 角色详情 | 200 |
| PUT | `/api/v1/{realm}/roles/{name}` | 更新角色 | 200 |
| DELETE | `/api/v1/{realm}/roles/{name}` | 删除角色 | 204 |
| GET | `/api/v1/{realm}/roles/by-id/{uuid}` | 按 UUID 查角色 | 200 |
| PUT | `/api/v1/{realm}/roles/by-id/{uuid}` | 按 UUID 改角色（支持改名） | 200 |
| DELETE | `/api/v1/{realm}/roles/by-id/{uuid}` | 按 UUID 删角色 | 204 |
| GET | `/api/v1/{realm}/groups` | 组列表 | 200 |
| POST | `/api/v1/{realm}/groups` | 创建组 | 201 |
| GET | `/api/v1/{realm}/groups/{id}` | 组详情（含成员和角色） | 200 |
| PUT | `/api/v1/{realm}/groups/{id}` | 更新组 | 204 |
| DELETE | `/api/v1/{realm}/groups/{id}` | 删除组 | 204 |
| GET | `/api/v1/{realm}/users` | 用户列表 | 200 |
| GET | `/api/v1/{realm}/users/{id}/details` | 用户详情（组 + 角色） | 200 |
| GET | `/api/v1/{realm}/idp/saml/instances` | IDP 实例列表 | 200 |
| POST | `/api/v1/{realm}/idp/saml/instances` | 创建 SAML IDP | 201 |
| PUT | `/api/v1/{realm}/idp/saml/instances` | 更新 SAML IDP | 200 |
| DELETE | `/api/v1/{realm}/idp/saml/instances/{alias}` | 删除 IDP | 204 |
| POST | `/api/v1/{realm}/idp/saml/import` | 导入 SAML XML | 200 |
| GET | `/api/v1/{realm}/idp/saml/instances/{alias}/mappers` | IDP Mapper 列表 | 200 |
| POST | `/api/v1/{realm}/idp/saml/instances/{alias}/mappers` | 创建 IDP Mapper | 201 |
| PUT | `/api/v1/{realm}/idp/saml/instances/{alias}/mappers/{id}` | 更新 IDP Mapper | 204 |
| DELETE | `/api/v1/{realm}/idp/saml/instances/{alias}/mappers/{id}` | 删除 IDP Mapper | 204 |
| POST | `/api/v1/{realm}/token/exchange` | 授权码换 Token | 200 |

#### pep-proxy 路由

| Method | Path | 功能 | 状态码 |
|--------|------|------|--------|
| GET | `/api/v1/policies` | 策略列表 | 200 |
| GET | `/api/v1/policies/{id}` | 策略详情 | 200 |
| POST | `/api/v1/policies` | 创建策略 | 200/201 |
| PUT | `/api/v1/policies/{id}` | 更新策略 | 200 |
| DELETE | `/api/v1/policies/{id}` | 删除策略 | 200 |
| GET | `/api/v1/policies/templates` | 模板列表 | 200 |
| POST | `/api/v1/policies/template/{name}` | 渲染模板 | 200 |
| GET | `/api/v1/roles/{role_id}/policy` | 查询角色绑定的策略 | 200 |
| POST | `/api/v1/roles/{role_id}/policy` | 创建角色-策略绑定 | 200 |
| PUT | `/api/v1/roles/{role_id}/policy` | 更新角色-策略绑定 | 200 |
| POST | `/api/v1/auth/check` | 鉴权检查 | 200 |

### 角色权限矩阵

| 角色 | 租户管理 | 角色/组/用户 | 策略 CRUD | IDP 管理 | 业务 API |
|------|---------|-------------|----------|---------|---------|
| super-admin | 全部 CRUD | 所有 Realm | 全部 | 全部 | 全部 |
| tenant-admin | 禁止 | 本 Realm 内 | 本租户内 | 本 Realm | 本租户内 |
| normal-user | 禁止 | 禁止 | 禁止 | 禁止 | 按 role-policy 绑定 |

### 默认账户

| 用户 | Realm | 密码 | 角色 |
|------|-------|------|------|
| super-admin | master | SuperInit@123 | super-admin, create-realm |
| tenant-admin | data-agent | TenantAdmin@123 | tenant-admin |
| normal-user | data-agent | NormalUser@123 | normal-user |

### Service Client

| Client ID | Realm | Secret 存储 | 用途 |
|-----------|-------|------------|------|
| idb-proxy-client | master | K8s Secret `keycloak/keycloak-idb-proxy-client` | 服务间调用（super-admin 权限） |
| data-agent-client | data-agent | K8s Secret `keycloak/keycloak-data-agent-client` | 租户用户登录 |

### 离线资源目录

```
da-cluster/offline/
+-- images/
|   +-- amd64/                    # x86_64 镜像
|   |   +-- keycloak-proxy_v2.tar
|   |   +-- opal-proxy_v1.tar
|   |   +-- keycloak-init_v1.tar
|   |   +-- keycloak-custom_26.5.2.tar
|   |   +-- postgres_17.tar
|   |   +-- cr.agentgateway.dev_controller_v2.2.0-main.tar
|   |   +-- cr.agentgateway.dev_agentgateway_0.11.1.tar
|   |   +-- permitio_opal-server_0.7.4.tar
|   |   +-- permitio_opal-client_0.7.4.tar
|   |   +-- mccutchen_go-httpbin_2.24.0.tar
|   |   +-- nginx_alpine.tar
|   |   +-- kindest_node_v1.35.0.tar       # Kind 模式专用
|   |   +-- base-keycloak-proxy_v1.tar     # 胖基础镜像（--fat-base 用）
|   |   +-- base-opal-proxy_v1.tar
|   |   +-- base-keycloak-init_v1.tar
|   +-- arm64/                    # ARM64 镜像（同名文件，不同架构）
|       +-- (同上，不含 kindest_node)
+-- charts/
|   +-- agentgateway-crds-v2.2.1.tgz
|   +-- agentgateway-v2.2.1.tgz
+-- crds/
    +-- gateway-api-v1.4.0.yaml
```

### Gateway 路由优先级

Gateway API 按最长匹配优先排序:

1. `/api/v1/tenants` --> keycloak-proxy
2. `/api/v1/common` --> keycloak-proxy
3. `/api/v1/auth` --> pep-proxy
4. `/api/v1/policies` --> pep-proxy
5. `/api/v1/roles` --> pep-proxy
6. `/api/v1/{realm}/roles|groups|users|idp|by-id` --> keycloak-proxy (RegularExpression)
7. `/realms/*` --> Keycloak (无鉴权)
8. `/admin/*` --> Keycloak Admin (无鉴权)
9. `/` --> httpbin catch-all (或你的后端/前端)

### setup.sh 参数速查

```bash
./scripts/setup.sh [OPTIONS]

OPTIONS:
  (default)         Kind 模式: 创建 Kind 集群 + 部署
  --no-kind         K8s 模式: 部署到已有 K8s 集群
  --build           从源码重建自定义镜像（需要网络）
  --fat-base        从胖基础镜像重建（无需网络，只拷贝代码）
  --existing-kind   使用已有 Kind 集群，跳过创建

ENVIRONMENT:
  PLATFORM=arm64    强制指定平台（默认自动检测）
  CLUSTER_NAME=xxx  Kind 集群名（默认 da-cluster）
  K8S_NODES="..."   多节点 IP（K8s 模式）
  K8S_NODE_USER=xxx SSH 用户名（默认 root）

EXAMPLES:
  ./scripts/setup.sh                              # Kind 开发环境
  ./scripts/setup.sh --no-kind                    # 部署到已有 K8s
  ./scripts/setup.sh --no-kind --fat-base         # 离线服务器代码热更新
  PLATFORM=arm64 ./scripts/setup.sh --no-kind     # ARM64 服务器部署
```

### 前置条件

| 工具 | Kind 模式 | K8s 模式 | 用途 |
|------|-----------|----------|------|
| docker | 必需 | --fat-base 时需要 | 容器运行时 / 镜像构建 |
| kind | 必需 | 不需要 | 本地 K8s 集群 |
| kubectl | 必需 | 必需 | K8s 命令行 |
| helm | 必需 | 必需 | Helm chart 部署 |
| ctr | 不需要 | 推荐 | containerd 镜像导入 |
