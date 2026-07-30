# 故障场景视角 — 什么会出错、怎么兜底

> 版本：v2.0 | 日期：2026-04-07

**架构要点：resource-sync 不是反向代理，而是通过 ext_proc 被调用。Gateway 直接路由到后端服务。ext_proc 配置 `failureMode: failOpen`，resource-sync 宕机不影响业务请求。**

---

## 1 故障影响全景

```mermaid
flowchart TB
    subgraph 组件故障影响
        GW_FAIL["Gateway 挂了<br/>🔴 全部不可用"]
        PEP_FAIL["pep-proxy 挂了<br/>🔴 全部请求 403（ext_authz 失败）"]
        OPA_FAIL["OPA 挂了<br/>🔴 路径鉴权失败"]
        PG_FAIL["PostgreSQL 挂了<br/>🔴 资源鉴权失败 + ACL 同步失败"]
        RS_FAIL["resource-sync 挂了<br/>🟡 业务请求正常！<br/>ACL 不同步（新建/删除无 ACL）"]
        KC_FAIL["Keycloak 挂了<br/>🟡 无法登录/续 token<br/>已登录用户不受影响"]
        EP_FAIL["ext_proc 超时<br/>🟡 failOpen 放行<br/>ACL 未写入 → pending 重试"]
        KP_FAIL["keycloak-proxy 挂了<br/>🟢 管理 API 不可用<br/>业务不受影响"]
        BS_FAIL["bundle-server 挂了<br/>🟢 OPA 用旧数据<br/>业务不受影响"]
    end

    style GW_FAIL fill:#ff6b6b,color:#fff
    style PEP_FAIL fill:#ff6b6b,color:#fff
    style OPA_FAIL fill:#ff6b6b,color:#fff
    style PG_FAIL fill:#ff6b6b,color:#fff
    style RS_FAIL fill:#ffd43b,color:#000
    style KC_FAIL fill:#ffd43b,color:#000
    style EP_FAIL fill:#ffd43b,color:#000
    style KP_FAIL fill:#51cf66,color:#fff
    style BS_FAIL fill:#51cf66,color:#fff
```

| 影响等级 | 含义 |
|---------|------|
| 🔴 严重 | 业务完全不可用 |
| 🟡 中等 | 部分功能受影响，有降级/兜底方案 |
| 🟢 轻微 | 业务不受影响，只影响管理操作 |

### 新旧架构对比（关键改进）

```mermaid
flowchart LR
    subgraph 旧架构
        direction TB
        OLD_REQ[用户请求] --> OLD_GW[Gateway]
        OLD_GW --> OLD_RS[resource-sync<br/>反向代理]
        OLD_RS -->|挂了| OLD_502["🔴 502 Bad Gateway<br/>所有业务全挂"]
        OLD_RS -->|正常| OLD_APP[后端应用]
    end

    subgraph 新架构
        direction TB
        NEW_REQ[用户请求] --> NEW_GW[Gateway]
        NEW_GW --> NEW_APP[后端应用<br/>直接路由]
        NEW_GW -.->|ext_proc 回调| NEW_RS[resource-sync]
        NEW_RS -.->|挂了| NEW_OPEN["🟡 failOpen 放行<br/>业务正常，ACL 延迟同步"]
    end

    style OLD_502 fill:#ff6b6b,color:#fff
    style NEW_OPEN fill:#ffd43b,color:#000
    style OLD_RS fill:#ff6b6b,color:#fff
    style NEW_RS fill:#51cf66,color:#fff
```

> **核心改进：** 旧架构中 resource-sync 宕机 = 全部业务 502。新架构中 resource-sync 宕机 = 业务正常运行，仅 ACL 同步延迟。

---

## 2 逐个场景分析

### 2.1 🟡 ext_proc (resource-sync) 写 ACL 失败（最可能发生）

用户创建资源，后端返回 201，ext_proc 调用 resource-sync 写 ACL 时数据库写入失败。

```mermaid
sequenceDiagram
    participant U as 用户
    participant GW as Gateway
    participant KB as kb-service
    participant EP as ext_proc<br/>(resource-sync:8082)
    participant DB as resource_acl 表
    participant PA as pending_acl 表
    participant RETRY as 后台重试

    U->>GW: POST /knowledgebase/v1/kb
    GW->>KB: 直接路由到后端
    KB-->>GW: 201 { "id": "kb-001" }

    Note over GW: ext_proc 拦截响应

    GW->>EP: 响应回调（201, body 含 kb-001）
    EP->>DB: INSERT resource_acl (kb-001, owner=zhangsan)
    DB-->>EP: ❌ 超时/连接失败

    alt ACL 写入失败 → 写 pending_acl
        EP->>PA: INSERT pending_acl<br/>(kb-001, owner, zhangsan, retry=0)
        PA-->>EP: ✅ 成功
        EP-->>GW: 放行响应
        GW-->>U: 201（资源创建成功）

        Note over RETRY: 后台定时重试 pending_acl
        loop 每 5 秒重试，最多 10 次
            RETRY->>DB: INSERT resource_acl
            DB-->>RETRY: ✅ 成功 → 删除 pending 记录
        end
    end

    alt pending_acl 也写不进去（极端）
        EP->>PA: INSERT pending_acl
        PA-->>EP: ❌ 也失败了
        Note over EP: failOpen → 仍然放行响应
        EP-->>GW: 放行响应
        GW-->>U: 201
        Note over U: ACL 丢失<br/>等待定期对账修复
    end
```

**影响：** 用户创建了资源但暂时无法访问，直到 pending 重试成功或对账修复。

**pending_acl 表：**

```sql
CREATE TABLE pending_acl (
    id           SERIAL PRIMARY KEY,
    tenant_id    VARCHAR(128) NOT NULL,
    app_name     VARCHAR(128) NOT NULL,
    resource_type VARCHAR(128) NOT NULL,
    resource_id  VARCHAR(256) NOT NULL,
    subject_type VARCHAR(32) NOT NULL,
    subject_id   VARCHAR(128) NOT NULL,
    permission   VARCHAR(32) NOT NULL,
    action       VARCHAR(16) NOT NULL,     -- 'create' | 'delete'
    retry_count  INTEGER NOT NULL DEFAULT 0,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    next_retry   TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**兜底策略：** pep-proxy 鉴权时如果 resource_acl 无记录，可回退检查 pending_acl：

```mermaid
flowchart TD
    REQ["用户访问 /v1/kb/kb-001"] --> CHECK_ACL{"resource_acl 有记录?"}
    CHECK_ACL -->|有| NORMAL[正常鉴权]
    CHECK_ACL -->|没有| FALLBACK{"回退：检查 pending_acl"}
    FALLBACK -->|"有 pending 记录<br/>且 subject 匹配"| ALLOW[临时放行]
    FALLBACK -->|没有| DENY[403]

    style ALLOW fill:#ffd43b,color:#000
    style DENY fill:#ff6b6b,color:#fff
```

---

### 2.2 🟡 resource-sync 整个挂了

**新架构下这不再是严重故障。** Gateway 直接路由到后端，resource-sync 通过 ext_proc 调用，配置了 `failureMode: failOpen`。

```mermaid
flowchart TD
    U[用户请求] --> GW[Gateway]
    GW --> PEP[pep-proxy ext_authz 鉴权]
    PEP -->|通过| GW2[Gateway 路由]
    GW2 --> APP[后端应用<br/>直接到达，不经过 resource-sync]
    APP --> RESP[后端返回响应]
    RESP --> EP{"ext_proc 回调<br/>resource-sync"}

    EP -->|正常| SYNC[写 ACL 成功]
    EP -->|"resource-sync 挂了<br/>connection refused / timeout"| FAILOPEN["failOpen 放行<br/>响应正常返回给用户"]

    FAILOPEN --> IMPACT["影响范围"]
    IMPACT --> I1["✅ 业务请求正常工作"]
    IMPACT --> I2["❌ 新建资源无 ACL<br/>用户暂时无法访问刚创建的资源"]
    IMPACT --> I3["❌ 删除资源有孤儿 ACL<br/>对账清理"]
    IMPACT --> I4["❌ 分享/取消分享 API 返回 502<br/>resource-sync:8080 管理端口不可用"]
    IMPACT --> I5["❌ list/search API 受影响<br/>后端调不了内部接口，返回空或 503"]

    style FAILOPEN fill:#ffd43b,color:#000
    style I1 fill:#51cf66,color:#fff
    style I2 fill:#ffd43b,color:#000
    style I3 fill:#ffd43b,color:#000
    style I4 fill:#ff6b6b,color:#fff
    style I5 fill:#ff6b6b,color:#fff
```

**关键区别：**

| 对比项 | 旧架构 | 新架构 |
|-------|--------|--------|
| resource-sync 宕机时 | 🔴 所有业务 502 | 🟡 业务正常，ACL 延迟 |
| 请求链路 | 请求必须经过 resource-sync | Gateway 直连后端 |
| 故障模式 | 串联故障，一挂全挂 | failOpen，优雅降级 |

**恢复后：** resource-sync 恢复后，pending_acl 后台重试 + 定期对账自动修复。

**部署建议：**

```yaml
# resource-sync 部署配置
apiVersion: apps/v1
kind: Deployment
metadata:
  name: resource-sync
spec:
  replicas: 2                        # 至少 2 副本
  template:
    spec:
      containers:
        - name: resource-sync
          ports:
            - containerPort: 8080     # 管理 API（分享/取消分享）
              name: management
            - containerPort: 8081     # 内部 API（list/search ID 查询）
              name: internal
            - containerPort: 8082     # ext_proc gRPC（响应回调）
              name: extproc
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /health/ready
              port: 8080
            periodSeconds: 5
```

---

### 2.3 🔴 PostgreSQL 挂了

影响范围最广 -- pep-proxy 查不了 resource_acl，resource-sync 写不了 ACL，pending_acl 也写不了。

```mermaid
flowchart TD
    PG_DOWN["PostgreSQL 挂了"] --> IMPACT1["pep-proxy 查 resource_acl 失败<br/>→ 资源级鉴权全挂"]
    PG_DOWN --> IMPACT2["resource-sync 写 ACL 失败<br/>→ pending_acl 也写不了（PG 全挂）"]
    PG_DOWN --> IMPACT3["bundle-server 读不到数据<br/>→ OPA 用旧 bundle，不受影响"]

    IMPACT1 --> STRATEGY1{"降级策略"}
    STRATEGY1 --> OPT1["方案A：资源级鉴权降级为只查 OPA<br/>路径有权限就放行，资源级暂时不管<br/>安全性降低但业务可用"]
    STRATEGY1 --> OPT2["方案B：所有资源级请求返回 503<br/>集合请求正常，实例请求不可用<br/>安全但影响体验"]

    IMPACT2 --> QUEUE["ext_proc failOpen 放行<br/>ACL 完全丢失<br/>等 PG 恢复后对账修复"]

    style PG_DOWN fill:#ff6b6b,color:#fff
    style IMPACT1 fill:#ff6b6b,color:#fff
    style IMPACT2 fill:#ff6b6b,color:#fff
    style IMPACT3 fill:#51cf66,color:#fff
```

**pep-proxy 降级逻辑：**

```python
async def check_resource_permission(user_id, groups, resource_id):
    try:
        acl = await db.query(
            "SELECT permission FROM resource_acl WHERE resource_id=%s AND ...",
            resource_id, user_id
        )
        return acl.permission if acl else None
    except DatabaseConnectionError:
        # 数据库不可用 → 降级
        logger.warning(f"resource_acl 查询失败，降级放行: {resource_id}")
        return "degraded"  # 标记为降级模式
        # 降级模式下放行，后端正常处理
```

**缓解措施：** PostgreSQL 主备部署或使用云托管数据库，确保高可用。

---

### 2.4 🟡 Keycloak 挂了

```mermaid
flowchart TD
    KC_DOWN["Keycloak 挂了"] --> IMPACT1["新用户无法登录<br/>拿不到 JWT"]
    KC_DOWN --> IMPACT2["JWT 过期的用户<br/>无法续签 token"]
    KC_DOWN --> NOT_IMPACT["已登录且 JWT 未过期的用户<br/>完全不受影响"]

    IMPACT1 --> BLOCK1["❌ 新用户进不来"]
    IMPACT2 --> BLOCK2["❌ token 过期后被踢出"]
    NOT_IMPACT --> OK["✅ 正常使用<br/>pep-proxy 用缓存的 JWKS 验证 JWT 签名<br/>不需要连 Keycloak"]

    style KC_DOWN fill:#ffd43b,color:#000
    style NOT_IMPACT fill:#51cf66,color:#fff
    style BLOCK1 fill:#ff6b6b,color:#fff
    style BLOCK2 fill:#ff6b6b,color:#fff
```

**缓解措施：**
- Keycloak `replicas: 2`
- JWT 过期时间设长一些（如 30 分钟），给 Keycloak 恢复的时间窗口
- pep-proxy 缓存 JWKS 公钥，Keycloak 挂了也能验证 JWT 签名

---

### 2.5 🟡 后端返回 201 但实际失败（幽灵资源）

后端返回 201 但数据库事务实际回滚了，资源并不存在。ext_proc 拿到 201 后照常写 ACL。

```mermaid
sequenceDiagram
    participant U as 用户
    participant GW as Gateway
    participant KB as kb-service
    participant EP as ext_proc<br/>(resource-sync:8082)
    participant DB as resource_acl

    U->>GW: POST /knowledgebase/v1/kb
    GW->>KB: 直接路由
    KB-->>GW: 201 { "id": "kb-001" }
    Note over KB: 但实际上数据库事务回滚了<br/>kb-001 并没有真正创建

    GW->>EP: ext_proc 响应回调（201, kb-001）
    EP->>DB: INSERT resource_acl (kb-001, owner=zhangsan)
    Note over DB: ACL 写入成功<br/>但 kb-001 实际不存在

    GW-->>U: 201

    Note over U: zhangsan 访问 kb-001<br/>pep-proxy 鉴权通过<br/>但 kb-service 返回 404<br/>→ 孤儿 ACL 记录
```

**解决：定期对账**

```mermaid
flowchart LR
    CRON["定时任务<br/>每天凌晨"] --> SCAN[扫描 resource_acl]
    SCAN --> CHECK{"对每个 resource_id<br/>调后端 HEAD 请求<br/>资源还存在吗?"}
    CHECK -->|存在| KEEP[保留]
    CHECK -->|404 不存在| CLEAN[清理孤儿 ACL]

    style CRON fill:#845ef7,color:#fff
    style CLEAN fill:#ff6b6b,color:#fff
```

```python
# 对账脚本
async def reconcile():
    acl_resources = db.query(
        "SELECT DISTINCT app_name, resource_type, resource_id FROM resource_acl"
    )
    for r in acl_resources:
        resp = await http.head(f"http://{r.app_name}-service/v1/{r.resource_type}/{r.resource_id}")
        if resp.status_code == 404:
            db.execute(
                "DELETE FROM resource_acl WHERE app_name=%s AND resource_id=%s",
                r.app_name, r.resource_id
            )
            logger.info(f"清理孤儿 ACL: {r.app_name}/{r.resource_id}")
```

---

### 2.6 🟡 并发创建和删除同一资源

```mermaid
sequenceDiagram
    participant A as 用户A
    participant B as 用户B
    participant GW as Gateway
    participant KB as kb-service
    participant EP as ext_proc
    participant DB as resource_acl

    Note over A,B: 几乎同时发生

    A->>GW: POST /v1/kb → 创建 kb-001
    B->>GW: DELETE /v1/kb/kb-001 → 删除 kb-001

    GW->>KB: 转发 POST
    GW->>KB: 转发 DELETE
    KB-->>GW: 201 { "id": "kb-001" }
    KB-->>GW: 200 删除成功

    Note over GW: 两个响应几乎同时触发 ext_proc

    alt 场景1：先 INSERT 后 DELETE（正确）
        GW->>EP: ext_proc 回调 201
        EP->>DB: INSERT ACL (kb-001, owner=A)
        GW->>EP: ext_proc 回调 200
        EP->>DB: DELETE ACL WHERE resource_id='kb-001'
        Note over DB: 结果：ACL 干净
    end

    alt 场景2：先 DELETE 后 INSERT（问题）
        GW->>EP: ext_proc 回调 200
        EP->>DB: DELETE ACL WHERE resource_id='kb-001'（无记录可删）
        GW->>EP: ext_proc 回调 201
        EP->>DB: INSERT ACL (kb-001, owner=A)
        Note over DB: 结果：孤儿 ACL<br/>资源已删除但 ACL 还在
    end
```

**解决：** 同一个 resource_id 的操作加锁，或者靠定期对账清理。实际发生概率极低。

---

### 2.7 🟢 bundle-server 挂了

```mermaid
flowchart TD
    BS_DOWN["bundle-server 挂了"] --> OPA["OPA 继续使用<br/>最后一次拉取的 bundle"]
    OPA --> IMPACT["影响：管理员修改的 apps/path_rules<br/>不会生效，直到 bundle-server 恢复"]
    IMPACT --> OK["业务完全不受影响<br/>只是新配置暂时不生效"]

    style BS_DOWN fill:#51cf66,color:#fff
    style OK fill:#51cf66,color:#fff
```

**不需要特殊处理。** bundle-server 恢复后自动推送最新数据。

---

### 2.8 🔴 OPA 挂了

```mermaid
flowchart TD
    OPA_DOWN["OPA 挂了"] --> PEP["pep-proxy 调 OPA 失败"]
    PEP --> DENY["默认拒绝所有请求<br/>default deny 策略<br/>安全但全挂"]

    style OPA_DOWN fill:#ff6b6b,color:#fff
    style DENY fill:#ff6b6b,color:#fff
```

**建议：** OPA `replicas: 2`，加 readinessProbe。OPA 是纯内存服务，启动快，恢复快。

---

### 2.9 🟡 ext_proc 超时

ext_proc 有默认 200ms 超时（可配置）。超时后 failOpen 放行，ACL 未写入。

```mermaid
sequenceDiagram
    participant U as 用户
    participant GW as Gateway
    participant KB as kb-service
    participant EP as ext_proc<br/>(resource-sync:8082)
    participant PA as pending_acl
    participant RECON as 定期对账

    U->>GW: POST /knowledgebase/v1/kb
    GW->>KB: 直接路由
    KB-->>GW: 201 { "id": "kb-001" }

    GW->>EP: ext_proc 响应回调
    Note over EP: 处理耗时超过 200ms...
    EP--xGW: ⏰ 超时

    Note over GW: failureMode: failOpen<br/>超时 → 放行响应
    GW-->>U: 201（资源创建成功）

    Note over EP: ACL 未写入

    alt ext_proc 内部处理完成但响应已发出
        EP->>PA: 写入 pending_acl
        Note over PA: 后台重试补写 ACL
    end

    alt ext_proc 完全没处理
        Note over RECON: 定期对账发现<br/>后端有资源但无 ACL<br/>→ 补写 owner ACL
    end
```

**影响：** 用户创建了资源但暂时无法访问，等待 pending 重试或对账修复。

**缓解措施：**
- 调大 ext_proc 超时时间（如 500ms）以覆盖大多数正常场景
- resource-sync 优化写入性能，确保 P99 < 100ms

---

### 2.10 🟡 resource-sync 内部接口不可用

后端调 `http://resource-sync:8081/internal/v1/resources` 失败时，list/search 接口受影响。

```mermaid
flowchart TD
    RS_INTERNAL_DOWN["resource-sync 内部接口不可用<br/>8081 端口无响应"] --> IMPACT1["后端 list/search 拿不到可访问 ID 列表"]
    IMPACT1 --> STRATEGY{"降级策略"}

    STRATEGY --> OPT1["方案A：list/search 返回空列表<br/>安全但用户看不到任何资源"]
    STRATEGY --> OPT2["方案B：list/search 返回 503<br/>告知用户稍后重试"]
    STRATEGY --> OPT3["方案C：SDK 内置缓存<br/>用上一次成功的 ID 列表<br/>可能不是最新"]

    NOTE["注意：单资源访问不受影响<br/>GET/PUT/DELETE /v1/kb/kb-001<br/>走 pep-proxy 鉴权，不依赖内部接口"]

    style RS_INTERNAL_DOWN fill:#ffd43b,color:#000
    style OPT1 fill:#51cf66,color:#fff
    style OPT2 fill:#ffd43b,color:#000
    style OPT3 fill:#ffd43b,color:#000
    style NOTE fill:#dee2e6,color:#000
```

**SDK 降级逻辑：**

```python
# aidp_acl/client.py
import httpx

def get_allowed_resources(request, app_name, resource_type):
    try:
        resp = httpx.get(
            f"{RESOURCE_SYNC_URL}/internal/v1/resources",
            params={...},
            timeout=3
        )
        return resp.json()["resource_ids"]
    except (httpx.ConnectError, httpx.TimeoutException):
        # 降级：返回空列表（安全优先）
        logger.warning("resource-sync 内部接口不可用，降级返回空列表")
        return []
```

---

## 3 故障总览

| 故障 | 影响等级 | 影响描述 | 降级/兜底方案 | 预防措施 |
|------|---------|---------|-------------|---------|
| Gateway 挂了 | 🔴 | 全部不可用 | 无 | `replicas: 2` |
| pep-proxy 挂了 | 🔴 | 全部请求 403 | 无 | `replicas: 2` |
| OPA 挂了 | 🔴 | 路径鉴权全挂，default deny | 无 | `replicas: 2` |
| PostgreSQL 挂了 | 🔴 | 资源鉴权全挂 + ACL 同步全挂 | 降级为只查 OPA 放行，或资源级返回 503 | 主备部署 |
| resource-sync 挂了 | 🟡 | **业务正常！** ACL 不同步 | failOpen 放行，pending 重试 + 对账 | `replicas: 2` |
| ext_proc 超时 | 🟡 | 响应正常返回，ACL 未写入 | pending 重试 + 对账 | 调大超时，优化写入性能 |
| Keycloak 挂了 | 🟡 | 新用户无法登录 | 已登录用户不受影响（缓存 JWKS） | `replicas: 2` + 缓存 JWKS |
| 后端返回 201 但实际失败 | 🟡 | 孤儿 ACL 记录 | 定期对账清理 | 对账脚本（每天凌晨） |
| 并发竞争 | 🟡 | 孤儿 ACL 记录 | 定期对账清理 | 极小概率，对账兜底 |
| resource-sync 内部接口不可用 | 🟡 | list/search 拿不到 ID 列表 | SDK 降级返回空列表，单资源不受影响 | `replicas: 2` + SDK 超时 3s |
| keycloak-proxy 挂了 | 🟢 | 管理 API 不可用 | 业务不受影响 | 非热路径，1 副本即可 |
| bundle-server 挂了 | 🟢 | 新配置不生效 | OPA 用旧 bundle，业务不受影响 | 恢复后自动推送 |

---

## 4 ACL 一致性保障三道防线

```mermaid
flowchart TD
    LINE1["第一道防线<br/>ext_proc 实时写入"] --> SUCCESS{"写入成功?"}
    SUCCESS -->|是| DONE["ACL 正常"]
    SUCCESS -->|否| LINE2["第二道防线<br/>pending_acl 后台重试"]
    LINE2 --> RETRY{"重试成功?<br/>每 5 秒一次，最多 10 次"}
    RETRY -->|是| DONE
    RETRY -->|否| LINE3["第三道防线<br/>定期对账（每天凌晨）"]
    LINE3 --> RECON["扫描后端资源 vs ACL<br/>补写缺失 / 清理孤儿"]
    RECON --> DONE

    style LINE1 fill:#51cf66,color:#fff
    style LINE2 fill:#ffd43b,color:#000
    style LINE3 fill:#845ef7,color:#fff
    style DONE fill:#51cf66,color:#fff
```

| 防线 | 触发条件 | 延迟 | 覆盖场景 |
|------|---------|------|---------|
| ext_proc 实时写入 | 每次创建/删除响应 | 毫秒级 | 99%+ 正常场景 |
| pending_acl 后台重试 | ACL 写入失败时 | 5~50 秒 | DB 短暂不可用、ext_proc 部分失败 |
| 定期对账 | 每天凌晨定时执行 | 最多 24 小时 | 极端故障、幽灵资源、孤儿 ACL |

---

## 5 推荐的副本数配置

```mermaid
flowchart LR
    subgraph "必须高可用（热路径）×2"
        GW["Gateway ×2"]
        PEP["pep-proxy ×2"]
        OPA["OPA ×2"]
    end

    subgraph "重要但 failOpen（可降级）×2"
        RS["resource-sync ×2<br/>挂了业务不中断"]
    end

    subgraph "已有高可用 ×2"
        KC["Keycloak ×2"]
    end

    subgraph "1 副本即可"
        KP["keycloak-proxy ×1<br/>管理 API，非热路径"]
        BS["bundle-server ×1<br/>挂了 OPA 用旧数据"]
    end

    subgraph "建议主备"
        PG[("PostgreSQL<br/>主备或云托管")]
    end

    style GW fill:#ff6b6b,color:#fff
    style PEP fill:#ff6b6b,color:#fff
    style OPA fill:#ff6b6b,color:#fff
    style RS fill:#ffd43b,color:#000
    style KC fill:#ff6b6b,color:#fff
    style KP fill:#51cf66,color:#fff
    style BS fill:#51cf66,color:#fff
    style PG fill:#845ef7,color:#fff
```

| 组件 | 副本数 | 原因 |
|------|-------|------|
| Gateway | ×2 | 热路径，挂了全部不可用 |
| pep-proxy | ×2 | 热路径，挂了全部 403 |
| OPA | ×2 | 热路径，挂了路径鉴权全挂 |
| resource-sync | ×2 | 重要但 failOpen，挂了业务不中断 |
| Keycloak | ×2 | 已配置，挂了新用户无法登录 |
| keycloak-proxy | ×1 | 管理 API，非热路径 |
| bundle-server | ×1 | 挂了 OPA 用旧数据，不影响业务 |
| PostgreSQL | 主备 | 影响范围最广，建议高可用部署 |
