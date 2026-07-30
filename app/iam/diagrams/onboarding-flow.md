# 接入流程视角 — 新应用怎么接入、各方做什么

> 版本：v2.0 | 日期：2026-04-07
>
> **架构变更要点**：Gateway 直连后端服务，resource-sync 通过 ext_proc 在响应阶段自动完成 ACL 同步，应用团队零 SDK 集成。

---

## 1 接入全流程概览

```mermaid
flowchart TD
    START([新应用要接入]) --> STEP1[第1步：约定]
    STEP1 --> STEP2[第2步：IAM 侧配置]
    STEP2 --> STEP3[第3步：Gateway 路由配置]
    STEP3 --> STEP4[第4步：应用侧部署]
    STEP4 --> STEP5[第5步：验证]
    STEP5 --> DONE([接入完成])

    STEP1 -.- WHO1[应用团队 + IAM 团队]
    STEP2 -.- WHO2[平台管理员]
    STEP3 -.- WHO3["应用团队（路由）+ IAM 团队（策略绑定）"]
    STEP4 -.- WHO4[应用团队]
    STEP5 -.- WHO5[双方一起]

    style START fill:#845ef7,color:#fff
    style DONE fill:#51cf66,color:#fff
```

**与旧架构的关键区别：**

```mermaid
flowchart LR
    subgraph 旧架构
        direction LR
        GW1[Gateway] --> RS1[resource-sync<br/>反向代理] --> APP1[后端服务]
    end

    subgraph 新架构
        direction LR
        GW2[Gateway] --> APP2[后端服务]
        GW2 -.->|ext_authz| PEP[pep-proxy<br/>认证+鉴权]
        GW2 -.->|ext_proc 响应阶段| RS2[resource-sync<br/>ACL 同步]
    end

    style RS1 fill:#ff6b6b,color:#fff
    style PEP fill:#4a9eff,color:#fff
    style RS2 fill:#51cf66,color:#fff
```

---

## 2 第 1 步：约定（应用团队 + IAM 团队）

双方坐下来对齐三件事，不需要写代码：

```mermaid
flowchart LR
    subgraph 约定内容
        A1[应用名称<br/>如: newapp]
        A2[URL 前缀<br/>如: /newapp/]
        A3[RESTful 资源路径<br/>如: /v1/items]
    end

    A1 --> RESULT1[apps 表的 app_name]
    A2 --> RESULT2[Gateway 路由 + OPA 匹配]
    A3 --> RESULT3[resource_patterns 表]

    style A1 fill:#4a9eff,color:#fff
    style A2 fill:#4a9eff,color:#fff
    style A3 fill:#4a9eff,color:#fff
```

**约定清单：**

| 约定项 | 示例 | 用在哪 |
|--------|------|--------|
| 应用名称 | `newapp` | apps 表、{app}-admins 组名 |
| URL 前缀 | `/newapp/` | Gateway 路由、OPA 路径匹配 |
| 资源路径 | `/v1/items` | resource_patterns 表 |
| 资源类型名 | `item` | resource_acl 表的 resource_type |
| 管理接口路径（可选） | `/v1/admin/` | path_rules 表（如果用 IAM 保护管理路径） |
| 响应体格式 | `{"id": "xxx"}` | ext_proc 从 POST 响应中提取资源 ID |

> **注意**：apps、path_rules、resource_patterns 均为系统级表，没有 tenant_id 字段。

**RESTful 规范要求（必须遵守）：**

```
POST   /v1/items          → 创建，返回 201 + {"id": "item-001"}
GET    /v1/items           → 列表
GET    /v1/items/item-001  → 查看
PUT    /v1/items/item-001  → 更新
DELETE /v1/items/item-001  → 删除，返回 200 或 204
```

---

## 3 第 2 步：IAM 侧配置（平台管理员操作）

```mermaid
sequenceDiagram
    participant ADMIN as 平台管理员
    participant KP as keycloak-proxy
    participant PG as PostgreSQL
    participant KC as Keycloak
    participant BS as bundle-server
    participant OPA as OPA

    Note over ADMIN,KP: 3.1 注册应用

    ADMIN->>KP: POST /api/v1/apps<br/>{ app_name: "newapp",<br/>  path_prefix: "/newapp/",<br/>  display_name: "新应用" }

    par keycloak-proxy 自动执行
        KP->>PG: INSERT INTO apps（系统级，无 tenant_id）
        KP->>PG: INSERT INTO resource_patterns<br/>(newapp, /v1/items, item)
        KP->>KC: 创建组 newapp-admins
    end

    KP-->>ADMIN: 201 注册成功

    Note over BS,OPA: bundle-server 自动同步
    BS->>PG: 读取最新 apps
    BS->>OPA: 推送 bundle（newapp.enabled=true）

    Note over ADMIN,KP: 3.2 配置路径保护规则（可选）

    ADMIN->>KP: POST /api/v1/path-rules<br/>{ path_prefix: "/newapp/v1/admin/",<br/>  required_group: "newapp-admins" }
    KP->>PG: INSERT INTO path_rules（系统级，无 tenant_id）
    KP-->>ADMIN: 201

    Note over ADMIN,KP: 3.3 分配管理员

    ADMIN->>KP: PUT /api/v1/aidp/groups/newapp-admins/members<br/>{ user_id: "wangwu" }
    KP->>KC: 把 wangwu 加入 newapp-admins
    KP-->>ADMIN: 200
```

**IAM 侧完成后数据库状态：**

```
apps 表新增（系统级）：
| app_name | path_prefix | enabled |
|----------|-------------|---------|
| newapp   | /newapp/    | true    |

resource_patterns 表新增（系统级）：
| app_name | resource_prefix | resource_type |
|----------|-----------------|---------------|
| newapp   | /v1/items       | item          |

path_rules 表新增（可选，系统级）：
| path_prefix        | required_group |
|--------------------|----------------|
| /newapp/v1/admin/  | newapp-admins  |

Keycloak 新增：
  组: newapp-admins → [wangwu]
```

---

## 4 第 3 步：Gateway 路由配置 — 关键变更

新架构下，路由配置由两个团队分工完成：

```mermaid
flowchart TD
    subgraph 应用团队负责
        ROUTE["创建 HTTPRoute<br/>/newapp/ → newapp-service<br/>（Gateway 直连后端）"]
    end

    subgraph IAM 团队负责
        AUTHZ["绑定 ext_authz 策略<br/>（认证 + 路径鉴权）"]
        EXTPROC["绑定 ext_proc 策略<br/>（ACL 自动同步）"]
    end

    subgraph 请求流转
        direction LR
        CLIENT[客户端] -->|1. 请求| GW[Gateway]
        GW -->|"2. ext_authz"| PEP[pep-proxy]
        PEP -->|3. 鉴权通过| GW
        GW -->|4. 转发| APP[newapp-service]
        APP -->|5. 响应| GW
        GW -->|"6. ext_proc（响应阶段）"| RS[resource-sync]
        RS -->|7. ACL 已同步| GW
        GW -->|8. 返回| CLIENT
    end

    style ROUTE fill:#4a9eff,color:#fff
    style AUTHZ fill:#845ef7,color:#fff
    style EXTPROC fill:#51cf66,color:#fff
```

### 4.1 应用团队创建 HTTPRoute

应用团队在自己的命名空间或项目 Helm chart 中添加路由：

```yaml
apiVersion: gateway.networking.k8s.io/v1
kind: HTTPRoute
metadata:
  name: newapp-route
spec:
  parentRefs:
    - name: agentgateway
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /newapp/
      filters:
        - type: URLRewrite
          urlRewrite:
            path:
              type: ReplacePrefixMatch
              replacePrefixMatch: /
      backendRefs:
        - name: newapp-service    # 直连后端服务，不经过 resource-sync
          port: 80
```

> **关键变更**：`backendRefs` 直接指向 `newapp-service`，不再经过 `resource-sync` 代理。

### 4.2 IAM 团队绑定 ext_authz + ext_proc 策略

IAM 团队将认证鉴权和 ACL 同步策略绑定到该路由：

```yaml
# ext_authz：认证 + 路径鉴权（可能已在 Gateway 级别配置）
apiVersion: agentgateway.dev/v1alpha1
kind: AgentgatewayPolicy
metadata:
  name: iam-ext-authz
spec:
  traffic:
    extAuth:
      backendRef:
        name: pep-proxy
        namespace: iam
        port: 9000
      grpc: {}
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: newapp-route
```

```yaml
# ext_proc：ACL 自动同步（响应阶段）
apiVersion: agentgateway.dev/v1alpha1
kind: AgentgatewayPolicy
metadata:
  name: iam-ext-proc
spec:
  traffic:
    extProc:
      backendRef:
        name: resource-sync
        namespace: iam
        port: 8082
      failureMode: failOpen
      processingMode:
        request:
          headers: SEND      # 需要 method + path + X-Auth-* headers
          body: SKIP
        response:
          headers: SEND      # 需要 status code
          body: BUFFERED     # 需要 POST 的响应体（提取 id）
  targetRefs:
    - group: gateway.networking.k8s.io
      kind: HTTPRoute
      name: newapp-route
```

**ext_proc 工作原理：**

```mermaid
sequenceDiagram
    participant C as 客户端
    participant GW as Gateway
    participant APP as newapp-service
    participant EP as resource-sync<br/>(ext_proc :8082)
    participant DB as PostgreSQL

    C->>GW: POST /newapp/v1/items
    Note over GW: ext_authz 鉴权通过
    GW->>EP: 请求阶段：发送 headers<br/>(method=POST, path=/v1/items,<br/>X-Auth-User-Id=zhangsan)
    EP-->>GW: 继续（记录请求上下文）
    GW->>APP: POST /v1/items
    APP-->>GW: 201 {"id": "item-001"}
    GW->>EP: 响应阶段：发送 headers + body<br/>(status=201, body={"id":"item-001"})
    EP->>DB: INSERT INTO resource_acl<br/>(owner=zhangsan, resource_id=item-001)
    EP-->>GW: 继续（不修改响应）
    GW-->>C: 201 {"id": "item-001"}
```

---

## 5 第 4 步：应用侧部署（应用团队）— 大幅简化

```mermaid
flowchart TD
    subgraph 应用团队要做的
        D1[部署应用到 K8s]
        D2[遵守 RESTful 规范<br/>POST 返回 201 + id<br/>DELETE 返回 200/204]
        D3[读取 X-Auth-User-Id header<br/>用于数据归属]
        D4["list/search 接口调 resource-sync:8081<br/>内部 API 过滤（可选）"]
    end

    subgraph 应用团队不用做的
        N1[不用集成任何 SDK]
        N2[不用验证 JWT]
        N3[不用检查 permission]
        N4[不用维护权限表]
        N5[不用配置 APP_NAME 环境变量]
    end

    style D1 fill:#51cf66,color:#fff
    style D2 fill:#51cf66,color:#fff
    style D3 fill:#51cf66,color:#fff
    style D4 fill:#ffd43b,color:#000
    style N1 fill:#dee2e6,color:#000
    style N2 fill:#dee2e6,color:#000
    style N3 fill:#dee2e6,color:#000
    style N4 fill:#dee2e6,color:#000
    style N5 fill:#dee2e6,color:#000
```

> D4 标黄表示这是可选步骤，只有需要 list/search 接口过滤时才需要。

应用的 Deployment 示例：

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: newapp-service
spec:
  replicas: 2
  template:
    spec:
      containers:
        - name: newapp
          image: newapp:latest
          ports:
            - containerPort: 80
---
apiVersion: v1
kind: Service
metadata:
  name: newapp-service
spec:
  selector:
    app: newapp
  ports:
    - port: 80
```

> **注意**：不再需要 `APP_NAME` 和 `RESOURCE_SYNC_URL` 环境变量。ext_proc 自动从请求路径匹配应用和资源模式。

**应用代码示例（极简）：**

```python
@app.post("/v1/items")
def create_item(request):
    # 鉴权已在 pep-proxy 完成（ext_authz）
    # ACL 同步由 ext_proc 自动完成（无需任何代码）
    user_id = request.headers.get("X-Auth-User-Id")  # Gateway 注入的用户信息
    item = db.create_item(data=request.body, created_by=user_id)
    return JSONResponse({"id": item.id}, status_code=201)  # 必须返回 201 + id

@app.get("/v1/items/{item_id}")
def get_item(item_id, request):
    # 能走到这里，说明 pep-proxy 已确认用户有权限访问此资源
    item = db.get_item(item_id)
    return item

@app.delete("/v1/items/{item_id}")
def delete_item(item_id, request):
    # ext_proc 会在看到 200/204 响应后自动清除 ACL 记录
    db.delete_item(item_id)
    return JSONResponse(status_code=200)

@app.get("/v1/items")
def list_items(request):
    # 这是唯一需要调 resource-sync 内部 API 的场景（可选）
    import requests
    user_id = request.headers.get("X-Auth-User-Id")
    resp = requests.get(
        "http://resource-sync:8081/internal/v1/accessible-resources",
        params={"app_name": "newapp", "resource_type": "item", "user_id": user_id}
    )
    allowed_ids = resp.json()["resource_ids"]
    items = db.get_items_by_ids(allowed_ids)
    return items
```

---

## 6 第 5 步：验证

```mermaid
flowchart TD
    V1[验证1：创建资源] --> CHECK1{POST /newapp/v1/items<br/>返回 201?}
    CHECK1 -->|是| V1_ACL{resource_acl 里<br/>有 owner 记录?}
    V1_ACL -->|是| V2

    V2[验证2：访问资源] --> CHECK2{GET /newapp/v1/items/item-001<br/>owner 能访问?}
    CHECK2 -->|是| CHECK3{其他用户访问<br/>返回 403?}
    CHECK3 -->|是| V3

    V3[验证3：分享资源] --> CHECK4{POST /acl/v1/resources/item-001/permissions<br/>分享给李四?}
    CHECK4 -->|是| CHECK5{李四能访问?<br/>权限是 viewer?}
    CHECK5 -->|是| V4

    V4[验证4：删除资源] --> CHECK6{DELETE /newapp/v1/items/item-001<br/>返回 200?}
    CHECK6 -->|是| CHECK7{resource_acl 里<br/>记录已清除?}
    CHECK7 -->|是| V5

    V5[验证5：管理接口] --> CHECK8{newapp-admins 能访问<br/>/newapp/v1/admin/?}
    CHECK8 -->|是| CHECK9{普通用户访问<br/>返回 403?}
    CHECK9 -->|是| DONE([接入验证通过])

    style DONE fill:#51cf66,color:#fff

    CHECK1 -->|否| FIX1[检查 HTTPRoute<br/>和后端 Service 配置]
    V1_ACL -->|否| FIX2[检查 ext_proc 策略绑定<br/>和 resource_patterns 配置]
    CHECK3 -->|否| FIX3[检查 ext_authz 策略绑定<br/>和 pep-proxy resource_acl 查询]

    style FIX1 fill:#ff6b6b,color:#fff
    style FIX2 fill:#ff6b6b,color:#fff
    style FIX3 fill:#ff6b6b,color:#fff
```

**验证命令：**

```bash
# 获取 JWT
TOKEN=$(curl -s -X POST "https://gateway.aidp.com/realms/aidp/protocol/openid-connect/token" \
  -d "grant_type=password&client_id=data-agent&username=zhangsan&password=xxx" \
  | jq -r '.access_token')

# 验证1：创建资源
curl -X POST https://gateway.aidp.com/newapp/v1/items \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"name": "测试数据"}' -v
# 预期：201 + {"id": "item-001"}
# ext_proc 自动写入 resource_acl（owner=zhangsan）

# 检查 ACL 是否自动写入
curl https://gateway.aidp.com/acl/v1/resources/item-001/permissions \
  -H "Authorization: Bearer $TOKEN"
# 预期：[{"subject_id": "zhangsan", "permission": "owner"}]

# 验证2：其他用户访问（用李四的 token）
curl https://gateway.aidp.com/newapp/v1/items/item-001 \
  -H "Authorization: Bearer $LISI_TOKEN" -v
# 预期：403

# 验证3：分享给李四
curl -X POST https://gateway.aidp.com/acl/v1/resources/item-001/permissions \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"app_name":"newapp","resource_type":"item","subject_type":"user","subject_id":"lisi","permission":"viewer"}'
# 预期：201

# 验证4：李四现在能访问
curl https://gateway.aidp.com/newapp/v1/items/item-001 \
  -H "Authorization: Bearer $LISI_TOKEN" -v
# 预期：200

# 验证5：删除资源
curl -X DELETE https://gateway.aidp.com/newapp/v1/items/item-001 \
  -H "Authorization: Bearer $TOKEN" -v
# 预期：200 + resource_acl 记录全部清除（ext_proc 自动处理）
```

---

## 7 接入清单（Checklist）

```mermaid
flowchart TD
    subgraph 约定阶段
        C1["[ ] 确定应用名称 app_name"]
        C2["[ ] 确定 URL 前缀 path_prefix"]
        C3["[ ] 确定资源路径 resource_prefix"]
        C4["[ ] 确定资源类型 resource_type"]
        C5["[ ] 确认遵守 RESTful 规范"]
        C6["[ ] 确定是否需要管理接口保护"]
    end

    subgraph IAM侧配置
        I1["[ ] POST /api/v1/apps 注册应用"]
        I2["[ ] 确认 newapp-admins 组已创建"]
        I3["[ ] 配置 path_rules（可选）"]
        I4["[ ] 分配管理员到 newapp-admins"]
        I5["[ ] 确认 resource_patterns 已写入"]
    end

    subgraph 应用团队创建路由
        R1["[ ] 创建 HTTPRoute（指向自己的 Service）"]
    end

    subgraph IAM侧策略绑定
        P1["[ ] 绑定 ext_authz 策略到路由"]
        P2["[ ] 绑定 ext_proc 策略到路由"]
    end

    subgraph 应用侧部署
        A1["[ ] 部署应用到 K8s"]
        A2["[ ] POST 返回 201 + id 字段"]
        A3["[ ] DELETE 返回 200 或 204"]
        A4["[ ] 读取 X-Auth-User-Id header"]
        A5["[ ] list/search 调 resource-sync:8081（可选）"]
    end

    subgraph 验证
        V1["[ ] 创建资源 → ACL 自动写入"]
        V2["[ ] 访问资源 → owner 可访问"]
        V3["[ ] 无权用户 → 403"]
        V4["[ ] 分享 → 被分享者可访问"]
        V5["[ ] 删除资源 → ACL 自动清除"]
        V6["[ ] 管理接口 → 只有 admins 可访问"]
    end

    C1 --> C2 --> C3 --> C4 --> C5 --> C6
    C6 --> I1 --> I2 --> I3 --> I4 --> I5
    I5 --> R1
    R1 --> P1 --> P2
    P2 --> A1 --> A2 --> A3 --> A4 --> A5
    A5 --> V1 --> V2 --> V3 --> V4 --> V5 --> V6
```

---

## 8 对比：接入前 vs 接入后应用的工作量

| 维度 | 没有 IAM（应用自己做） | 接入 IAM 后 |
|------|----------------------|------------|
| JWT 验证 | 自己实现中间件 | 不用做（ext_authz → pep-proxy） |
| 用户/组管理 | 自己建表和 API | 不用做（Keycloak 管理） |
| 路径权限 | 自己写中间件 | 不用做（OPA 管理） |
| 资源权限表 | 自己建 shares 表 | 不用做（resource_acl 由 ext_proc 自动维护） |
| 分享功能 | 自己写分享 API | 不用做（IAM ACL API） |
| 鉴权逻辑 | 每个接口都要写 | 不用做（ext_authz + ext_proc 全自动） |
| SDK 集成 | 引入鉴权 SDK | **不需要任何 SDK** |
| 环境变量 | 配置各种密钥 | **不需要特殊环境变量** |
| Gateway 路由 | IAM 团队统一管理 | **应用团队自己管理路由，IAM 只绑定策略** |
| **应用只需要做** | 全部自己做 | **遵守 RESTful 规范 + 读 X-Auth-User-Id + list/search 调一次内部接口（可选）** |

### 工作量直观对比

```mermaid
flowchart LR
    subgraph 没有IAM时应用要做的
        direction TB
        W1[JWT 验证中间件]
        W2[用户管理模块]
        W3[权限检查中间件]
        W4[ACL 表设计 + 维护]
        W5[分享 API]
        W6[鉴权 SDK 集成]
        W7[业务逻辑]
    end

    subgraph 接入IAM后应用只做
        direction TB
        S1["遵守 RESTful 规范<br/>（POST→201+id, DELETE→200/204）"]
        S2["读 X-Auth-User-Id header"]
        S3["list/search 调内部 API（可选）"]
        S4[业务逻辑]
    end

    style W1 fill:#ff6b6b,color:#fff
    style W2 fill:#ff6b6b,color:#fff
    style W3 fill:#ff6b6b,color:#fff
    style W4 fill:#ff6b6b,color:#fff
    style W5 fill:#ff6b6b,color:#fff
    style W6 fill:#ff6b6b,color:#fff
    style W7 fill:#51cf66,color:#fff
    style S1 fill:#ffd43b,color:#000
    style S2 fill:#ffd43b,color:#000
    style S3 fill:#dee2e6,color:#000
    style S4 fill:#51cf66,color:#fff
```

红色 = 不需要做了 | 黄色 = 轻量约定 | 灰色 = 可选 | 绿色 = 业务逻辑
