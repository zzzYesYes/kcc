# IDB Proxy API 文档

## 概述

IDB Proxy 是一个 Keycloak 的业务封装代理，提供 REST API 来管理 Keycloak 中的租户（Realm）、组（Group）、角色（Role）、用户（User）和 SAML 身份提供者（IDP）。

**基础URL**: `http://your-host:port/api/v1`

---

## 目录

1. [租户管理 (Tenants)](#租户管理-tenants)
2. [身份管理 (Identity - 用户、角色、组)](#身份管理-identity---用户角色组)
3. [身份提供者 (IDP)](#身份提供者-idp)
4. [Token 交换](#token-交换)
5. [通用接口 (Common)](#通用接口-common)

---

## 通用机制

### 受保护 Realm

系统保护 `master` realm，不允许对其进行操作。所有涉及 realm 的接口都会自动拦截对 master realm 的访问。

### 状态码规范

| 状态码 | 说明 |
|---------|------|
| 200 OK | 查询成功 |
| 201 Created | 创建成功，返回创建的资源 |
| 204 No Content | 更新/删除成功，无响应体 |
| 400 Bad Request | 请求参数错误或配置不完整 |
| 403 Forbidden | 操作被拒绝（如尝试操作 master realm） |
| 404 Not Found | 资源不存在 |
| 422 Unprocessable Entity | 请求体验证失败 |
| 500 Internal Server Error | 服务器内部错误 |

### 错误响应格式

所有错误响应采用统一格式：

```json
{
  "detail": "错误描述信息"
}
```

### REST 最佳实践

遵循以下 REST 最佳实践：
- **POST** (创建)：返回 201 状态码和创建的资源对象
- **PUT** (更新)：返回 200 状态码和更新后的资源对象（部分更新操作返回 204）
- **GET** (查询)：返回 200 状态码和资源数据
- **DELETE** (删除)：返回 204 No Content，无响应体

---

## 租户管理 (Tenants)

租户对应 Keycloak 中的 Realm。每个租户代表一个独立的认证命名空间，拥有自己的用户、角色和组。

### 创建租户

创建一个新租户并自动配置：
- 默认 OIDC Client（通过 `KC_NEW_CLIENT_ID` 环境变量配置）
- Script Mapper（用于注入租户和角色信息到 token）
- 租户管理员角色（默认为 `tenant-admin`），包含管理 realm、IDP、用户等内部权限
- 租户管理员用户
- 禁用首次登录的 Profile Review 流程

**接口**: `POST /tenants`

**用途**: 创建新的租户及其完整的初始配置

**请求 Body**:
```json
{
  "realm": "my-tenant",
  "displayName": "我的租户"
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | Realm 名称，必须唯一 |
| displayName | string | 是 | 租户显示名称 |

**响应 (201 Created)**:
```json
{
  "realm": "my-tenant",
  "id": "my-tenant",
  "admin_role": "tenant-admin",
  "admin_user": "tenant-admin"
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| realm | string | Realm 名称 |
| id | string | Realm ID（与 realm 相同） |
| admin_role | string | 租户管理员角色名称 |
| admin_user | string | 租户管理员用户名 |

**示例 cURL**:
```bash
curl -X POST http://localhost:8000/api/v1/tenants \
  -H "Content-Type: application/json" \
  -d '{
    "realm": "my-tenant",
    "displayName": "我的租户"
  }'
```

**环境变量配置**:
- `DEFAULT_TENANT_ADMIN_ROLE`: 默认租户管理员角色名（默认: `tenant-admin`）
- `DEFAULT_TENANT_ADMIN_NAME`: 默认租户管理员用户名（默认: `tenant-admin`）
- `KC_NEW_CLIENT_ID`: 默认 Client ID（默认: `data-agent`）
- `KC_SCRIPT_MAPPER`: Script Mapper 提供者名称（默认: `Data Agent Mapper`）

---

### 查询租户列表

获取所有租户列表，自动过滤掉受保护的 master realm。

**接口**: `GET /tenants`

**用途**: 获取所有租户列表（排除 master realm）

**请求参数**: 无

**响应 (200 OK)**:
```json
[
  {
    "id": "my-tenant",
    "realm": "my-tenant",
    "displayName": "我的租户",
    "displayNameHtml": "<div>我的租户</div>",
    "enabled": true,
    "notBefore": 0,
    "defaultSignatureAlgorithm": "RS256",
    "sslRequired": "external",
    "registrationAllowed": false,
    "loginWithEmailAllowed": true,
    "duplicateEmailsAllowed": false,
    "resetPasswordAllowed": true,
    "editUsernameAllowed": true,
    "bruteForceProtected": true
  }
]
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | Realm ID |
| realm | string | Realm 名称 |
| displayName | string | 显示名称 |
| enabled | boolean | 是否启用 |
| notBefore | number | 生效起始时间戳 |
| sslRequired | string | SSL 要求级别（all, external, none） |
| registrationAllowed | boolean | 是否允许用户注册 |
| loginWithEmailAllowed | boolean | 是否允许邮箱登录 |

**示例 cURL**:
```bash
curl http://localhost:8000/api/v1/tenants
```

---

### 删除租户

删除指定租户及其所有相关数据（用户、角色、组、IDP 配置等）。

**接口**: `DELETE /tenants/{realm_name}`

**用途**: 删除指定租户

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm_name | string | 是 | 要删除的租户名称 |

**响应 (204 No Content)**: 无响应体

**示例 cURL**:
```bash
curl -X DELETE http://localhost:8000/api/v1/tenants/my-tenant
```

**注意**:
- 删除操作不可逆，请谨慎操作
- `master` realm 受保护，无法删除
- 删除租户将移除该租户下的所有数据

---

## 身份管理 (Identity - 用户、角色、组)

身份管理 API 用于在指定租户内管理用户、角色和组。

---

### 角色管理

角色用于控制用户在租户内的权限和访问控制。

#### 查询角色列表

获取租户下的所有 Realm 级别角色列表。自动过滤掉：
- 客户端角色（Client Roles）
- Keycloak 内置角色（如 `default-roles-*`、`offline_access`、`uma_authorization`）

**接口**: `GET /{realm}/roles`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**响应 (200 OK)**:
```json
[
  {
    "id": "uuid-here",
    "name": "business_admin",
    "description": "业务管理员角色",
    "attributes": {
      "permissions": ["read", "write", "delete"]
    },
    "composite": false,
    "clientRole": false,
    "containerId": null,
    "policy": {
      "id": "documents-allow",
      "tenant_id": "my-tenant",
      "rules": [
        {
          "resource": "documents",
          "effect": "allow"
        }
      ],
      "created_at": "2026-03-19T12:00:00",
      "updated_at": "2026-03-19T12:00:00"
    }
  },
  {
    "id": "uuid-here-2",
    "name": "developer",
    "description": "开发者角色",
    "attributes": {},
    "composite": false,
    "clientRole": false,
    "containerId": null
  }
]
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | Keycloak 生成的角色 UUID |
| name | string | 角色名称 |
| description | string | 角色描述 |
| attributes | object | 自定义属性（键值为字符串数组） |
| composite | boolean | 是否为复合角色（包含其他角色） |
| clientRole | boolean | 是否为客户端角色（应始终为 false） |
| containerId | string | 容器 ID（Realm 角色为 null） |
| policy | object | **可选** - 绑定的策略信息（OPA 服务提供） |

**示例 cURL**:
```bash
curl http://localhost:8000/api/v1/my-tenant/roles
```

---

#### 创建角色

创建一个新的 Realm 级别角色。可选择性地为角色绑定策略。

**接口**: `POST /{realm}/roles`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body**:
```json
{
  "name": "business_user",
  "description": "标准业务用户角色",
  "attributes": {
    "permissions": ["read", "write"],
    "level": ["standard"]
  },
  "composite": false,
  "policy_id": "documents-allow"
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| name | string | 是 | 角色名称（在租户内唯一） |
| description | string | 否 | 角色描述 |
| attributes | object | 否 | 自定义属性（键值为字符串数组） |
| composite | boolean | 否 | 是否为复合角色（默认: false） |
| policy_id | string | 否 | **可选** - 要绑定的策略 ID（OPA 服务） |

**响应 (201 Created)**:
```json
{
  "id": "generated-uuid",
  "name": "business_user",
  "description": "标准业务用户角色",
  "attributes": {
    "permissions": ["read", "write"],
    "level": ["standard"]
  },
  "composite": false,
  "clientRole": false,
  "containerId": null,
  "policy": {
    "id": "documents-allow",
    "tenant_id": "my-tenant",
    "rules": [
      {
        "resource": "documents",
        "effect": "allow"
      }
    ],
    "created_at": "2026-03-19T12:00:00",
    "updated_at": "2026-03-19T12:00:00"
  }
}
```

**示例 cURL**:
```bash
curl -X POST http://localhost:8000/api/v1/my-tenant/roles \
  -H "Content-Type: application/json" \
  -d '{
    "name": "business_user",
    "description": "标准业务用户角色",
    "composite": false
  }'
```

---

#### 获取角色详情

通过角色名称获取单个角色的详细信息。

**接口**: `GET /{realm}/roles/{role_name}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_name | string | 是 | 角色名称（非 UUID） |

**响应 (200 OK)**: 返回完整的角色对象（同查询列表响应）

---

#### 更新角色

通过角色名称更新角色信息。支持部分更新，仅传入需要修改的字段。可选择性地更新或取消绑定策略。

**接口**: `PUT /{realm}/roles/{role_name}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_name | string | 是 | 角色名称（非 UUID） |

**请求 Body**:
```json
{
  "name": "business_user_v2",
  "description": "更新后的角色描述",
  "attributes": {
    "updated_field": ["new_value"]
  },
  "composite": false,
  "policy_id": "documents-read-only"
}
```

**请求字段**: 所有字段均为可选

| 字段 | 类型 | 说明 |
|------|------|------|
| name | string | 角色名称 |
| description | string | 角色描述 |
| attributes | object | 自定义属性 |
| composite | boolean | 是否为复合角色 |
| policy_id | string | **可选** - 新的策略 ID（传 null 表示取消绑定） |

**响应 (200 OK)**: 返回更新后的完整角色对象（包括策略信息）

---

#### 删除角色

通过角色名称删除角色。

**接口**: `DELETE /{realm}/roles/{role_name}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_name | string | 是 | 角色名称（非 UUID） |

**响应 (204 No Content)**: 无响应体

---

#### 通过 UUID 获取角色

通过角色 UUID（而不是名称）获取角色详情。适用于角色名称可能变更的场景。

**接口**: `GET /{realm}/roles/by-id/{role_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_id | string | 是 | 角色 UUID |

**响应 (200 OK)**: 返回完整的角色对象

---

#### 通过 UUID 更新角色

通过角色 UUID 更新角色信息。此接口支持角色重命名。可选择性地更新或取消绑定策略。

**接口**: `PUT /{realm}/roles/by-id/{role_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_id | string | 是 | 角色 UUID |

**请求 Body**:
```json
{
  "name": "new_role_name",
  "description": "重命名后的角色描述",
  "attributes": {
    "new_attribute": ["value"]
  },
  "composite": false,
  "policy_id": "documents-read-only"
}
```

**请求字段**: 所有字段均为可选

| 字段 | 类型 | 说明 |
|------|------|------|
| name | string | 角色名称 |
| description | string | 角色描述 |
| attributes | object | 自定义属性 |
| composite | boolean | 是否为复合角色 |
| policy_id | string | **可选** - 新的策略 ID（传 null 表示取消绑定） |

**响应 (200 OK)**: 返回更新后的完整角色对象（包括策略信息）

---

#### 通过 UUID 删除角色

通过角色 UUID 删除角色。

**接口**: `DELETE /{realm}/roles/by-id/{role_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| role_id | string | 是 | 角色 UUID |

**响应 (204 No Content)**: 无响应体

---

**关于策略绑定**:

角色管理接口支持与 OPA (Open Policy Agent) 服务集成，为角色绑定策略：

1. **每个角色最多绑定一条策略**
2. **事务可靠性**: 创建/更新角色时，如果 OPA 策略绑定失败，会自动回滚 Keycloak 角色操作
3. **查询返回策略信息**: 获取角色列表或详情时，会自动包含绑定的策略信息（如有）
4. **tenant_id 映射**: OPA 服务使用的是 URL 路径中的 realm 作为 tenant_id，无需在请求 body 中额外传递

**OPA 服务配置**:

需要在 `.env` 文件中配置 OPA 服务地址：

```bash
OPA_BASE_URL=http://bundle-server.opa.svc.cluster.local:8001
```

### 组管理

组用于组织用户和管理批量的用户权限。

#### 查询组列表

获取租户下的所有顶级组及其子组，支持 hierarchical 结构。

**接口**: `GET /{realm}/groups`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**响应 (200 OK)**:
```json
[
  {
    "id": "group-uuid-1",
    "name": "Engineering",
    "path": "/Engineering",
    "attributes": {
      "department": ["engineering"],
      "location": ["Shanghai"]
    },
    "subGroups": [
      {
        "id": "group-uuid-2",
        "name": "Backend",
        "path": "/Engineering/Backend",
        "attributes": {},
        "subGroups": []
      }
    ]
  },
  {
    "id": "group-uuid-3",
    "name": "Sales",
    "path": "/Sales",
    "attributes": {},
    "subGroups": []
  }
]
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 组 UUID |
| name | string | 组名称 |
| path | string | 组的全路径（如 `/Engine/Backend`） |
| attributes | object | 自定义属性 |
| subGroups | array | 子组列表（递归结构） |

**示例 cURL**:
```bash
curl http://localhost:8000/api/v1/my-tenant/groups
```

---

#### 创建组

创建新组，可选地添加用户和角色成员。

**接口**: `POST /{realm}/groups`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body**:
```json
{
  "name": "Engineering",
  "path": "/Engineering",
  "attributes": {
    "department": ["engineering"],
    "location": ["Shanghai"]
  },
  "users": ["user-uuid-1", "user-uuid-2"],
  "roles": ["business_admin", "developer"]
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| name | string | 是 | 组名称 |
| path | string | 否 | 组的全路径（如 `/Parent/Child`） |
| attributes | object | 否 | 自定义属性（键值为字符串数组） |
| users | array | 否 | 要添加到组的用户 UUID 列表 |
| roles | array | 否 | 要分配给组的角色名称列表 |

**响应 (201 Created)**:
```json
{
  "id": "generated-uuid",
  "name": "Engineering",
  "path": "/Engineering",
  "attributes": {
    "department": ["engineering"],
    "location": ["Shanghai"]
  }
}
```

---

#### 获取组详情

获取组的详细信息，包括成员列表和分配的角色。

**接口**: `GET /{realm}/groups/{group_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| group_id | string | 是 | 组 UUID |

**响应 (200 OK)**:
```json
{
  "id": "group-uuid",
  "name": "Engineering",
  "members": [
    {
      "id": "user-uuid-1",
      "username": "john.doe"
    },
    {
      "id": "user-uuid-2",
      "username": "jane.smith"
    }
  ],
  "roles": [
    {
      "id": "role-uuid-1",
      "name": "business_admin",
      "description": "业务管理员角色",
      "attributes": {},
      "composite": false,
      "clientRole": false
    },
    {
      "id": "role-uuid-2",
      "name": "developer",
      "description": "开发者角色",
      "attributes": {},
      "composite": false,
      "clientRole": false
    }
  ]
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 组 UUID |
| name | string | 组名称 |
| members | array | 组成员列表（仅包含 id 和 username 字段） |
| roles | array | 分配给组的角色列表（已过滤内置角色） |

**注意**:
- 成员列表仅包含 `id` 和 `username` 字段，以减少响应体积
- 角色列表已自动过滤掉 Keycloak 内置角色

---

#### 更新组

更新组信息，包括属性、成员和角色分配。组成员和角色分配会被完全替换（同步模式）。

**接口**: `PUT /{realm}/groups/{group_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| group_id | string | 是 | 组 UUID |

**请求 Body**:
```json
{
  "name": "Engineering_V2",
  "path": "/Engineering_V2",
  "attributes": {
    "new_location": ["Beijing"]
  },
  "users": ["user-uuid-1", "user-uuid-3"],
  "roles": ["updatedRole1", "updatedRole2"]
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| name | string | 否 | 组名称 |
| path | string | 否 | 组的全路径 |
| attributes | object | 否 | 自定义属性 |
| users | array | 否 | 用户 UUID 列表（完全替换现有成员） |
| roles | array | 否 | 角色名称列表（完全替换现有分配） |

**响应 (204 No Content)**: 无响应体

**注意**:
- 如果提供了 `users` 字段，会完全替换组的现有成员（不在列表中的用户会被移除）
- 如果提供了 `roles` 字段，会完全替换组的角色分配
- 如果不提供这些字段，则保持现有成员和角色不变

---

#### 删除组

删除指定组。

**接口**: `DELETE /{realm}/groups/{group_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| group_id | string | 是 | 组 UUID |

**响应 (204 No Content)**: 无响应体

---

### 用户管理

用户管理 API 用于查询用户信息和用户上下文。

#### 查询用户列表

获取租户下的所有用户。

**接口**: `GET /{realm}/users`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**响应 (200 OK)**:
```json
[
  {
    "id": "user-uuid-1",
    "username": "john.doe",
    "firstName": "John",
    "lastName": "Doe",
    "email": "john.doe@example.com",
    "emailVerified": true,
    "enabled": true,
    "attributes": {
      "department": ["Engineering"],
      "employee_id": ["12345"]
    },
    "createdTimestamp": 1234567890000,
    "totp": false,
    "federationLink": null,
    "serviceAccountClientId": null,
    "notBefore": 0
  }
]
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 用户 UUID |
| username | string | 用户名 |
| firstName | string | 名 |
| lastName | string | 姓 |
| email | string | 邮箱 |
| emailVerified | boolean | 邮箱是否已验证 |
| enabled | boolean | 是否启用 |
| attributes | object | 自定义属性 |
| createdTimestamp | number | 创建时间戳（毫秒） |
| totp | boolean | 是否启用了 TOTP（两步验证） |
| federationLink | string | 联邦链接 |

---

#### 获取用户完整上下文

获取用户所属的组和分配的角色，便于前端展示用户详细信息。

**接口**: `GET /{realm}/users/{user_id}/details`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| user_id | string | 是 | 用户 UUID |

**响应 (200 OK)**:
```json
{
  "groups": [
    {
      "id": "group-uuid-1",
      "name": "Engineering",
      "path": "/Engineering"
    },
    {
      "id": "group-uuid-2",
      "name": "Managers",
      "path": "/Managers"
    }
  ],
  "roles": [
    {
      "id": "role-uuid-1",
      "name": "business_admin",
      "description": "业务管理员角色",
      "attributes": {},
      "composite": false,
      "clientRole": false,
      "containerId": null
    },
    {
      "id": "role-uuid-2",
      "name": "custom_role",
      "description": "自定义角色",
      "attributes": {},
      "composite": false,
      "clientRole": false,
      "containerId": null
    }
  ]
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| groups | array | 用户所属的组列表 |
| roles | array | 用户分配的角色列表（已过滤内置角色） |

**注意**:
- 角色列表已自动过滤掉 Keycloak 内置角色（如 `default-roles-*`、`uma_authorization`）
- 角色以对象形式返回，包含完整的角色信息
- 前端显示用户角色时，应提取 `name` 字段，而非直接 join（会显示 `[Object]`）

**前端使用示例**:
```javascript
// 正确：提取角色名称
const roleNames = userRoles.map(r => r.name || r).join(', ');

// 错误：直接 join 会显示 [Object]
const wrong = userRoles.join(', ');
```

---

## 身份提供者 (IDP)

身份提供者管理 API 用于配置外部认证源，目前主要支持 SAML 2.0 协议。

---

### 导入 SAML 元数据

从 SAML 2.0 元数据 XML 文件中解析并返回 IDP 配置信息。

**接口**: `POST /{realm}/idp/saml/import`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body (multipart/form-data)**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| file | file | 是 | SAML 元数据 XML 文件 |

**响应 (200 OK)**:
```json
{
  "idpEntityId": "https://idp.example.com/entityid",
  "singleSignOnServiceUrl": "https://idp.example.com/sso",
  "singleLogoutServiceUrl": "https://idp.example.com/slo",
  "postBindingLogout": "https://idp.example.com/post-logout",
  "postBindingResponse": "https://idp.example.com/post-response",
  "signingCertificate": "MIIDdzCCAl+gAwIBAgIEb0p...",
  "nameIDPolicyFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified",
  "enabledFromMetadata": "true",
  "loginHint": "username",
  "validateSignature": "true",
  "wantAuthnRequestsSigned": "true",
  "postBindingAuthnRequest": "https://idp.example.com/post-authn-request",
  "artifactBindingResponse": "https://idp.example.com/artifact-response",
  "artifactResolutionServiceUrl": "https://idp.example.com/artifact-resolve",
  "metadataDescriptorUrl": "https://idp.example.com/metadata",
  "addExtensionsElementWithKeyInfo": "false"
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| idpEntityId | string \| null | IDP 实体标识符 |
| singleSignOnServiceUrl | string \| null | 单点登录服务 URL |
| singleLogoutServiceUrl | string \| null | 单点登出服务 URL |
| postBindingLogout | string \| null | POST 绑定登出 URL |
| postBindingResponse | string \| null | POST 绑定响应 URL |
| postBindingAuthnRequest | string \| null | POST 绑定认证请求 URL |
| signingCertificate | string \| null | 签名证书（Base64 编码） |
| nameIDPolicyFormat | string \| null | NameID 策略格式 |
| enabledFromMetadata | string \| null | 从元数据中获取的启用状态 |
| loginHint | string \| null | 登录提示 |
| validateSignature | string \| null | 是否验证签名 |
| wantAuthnRequestsSigned | string \| null | 是否要求签名认证请求 |
| artifactBindingResponse | string \| null | Artifact 绑定响应 URL |
| artifactResolutionServiceUrl | string \| null | Artifact 解析服务 URL |
| metadataDescriptorUrl | string \| null | 元数据描述符 URL |
| addExtensionsElementWithKeyInfo | string \| null | 是否添加带有 KeyInfo 的扩展元素 |

**注意**: 响应直接返回 Keycloak API 的原始对象，所有字段均为可选，具体包含哪些字段取决于元数据文件的内容。

**示例 cURL**:
```bash
curl -X POST http://localhost:8000/api/v1/my-tenant/idp/saml/import \
  -F "file=@metadata.xml"
```

---

### 创建 SAML IDP 实例

创建一个新的 SAML 2.0 身份提供者实例。每个租户最多允许一个 IDP 实例。

**接口**: `POST /{realm}/idp/saml/instances`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body**:
```json
{
  "displayName": "企业 SAML IDP",
  "enabled": true,
  "trustEmail": false,
  "config": {
    "entityId": "https://idp.example.com/entityid",
    "singleSignOnServiceUrl": "https://idp.example.com/sso",
    "singleLogoutServiceUrl": "https://idp.example.com/slo",
    "nameIDPolicyFormat": "urn:oasis:names:tc:SAML:1.1:nameid-format:unspecified"
  }
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| displayName | string | 否 | IDP 显示名称（默认: alias） |
| enabled | boolean | 否 | 是否启用（默认: true） |
| trustEmail | boolean | 否 | 是否信任 IDP 返回的邮箱（默认: false） |
| config | object | 详见下方 SAML 配置 |

**SAML 配置必填字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| singleSignOnServiceUrl | string | 是 | SSO 服务 URL |

**SAML 配置常用字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| entityId | string | 服务提供商（SP）实体 ID |
| singleLogoutServiceUrl | string | 单点登出服务 URL |
| nameIDPolicyFormat | string | NameID 策略格式 |

**响应 (201 Created)**:
```json
{
  "alias": "da-saml-idp",
  "displayName": "企业 SAML IDP",
  "internalId": "internal-uuid",
  "providerId": "saml",
  "enabled": true,
  "trustEmail": false,
  "storeToken": false,
  "addReadTokenRoleOnCreate": false,
  "authenticateByDefault": false,
  "linkOnly": false,
  "hideOnLogin": false,
  "firstBrokerLoginFlowAlias": "first broker login",
  "postBrokerLoginFlowAlias": null,
  "config": {
    "entityId": "https://idp.example.com/entityid",
    "singleSignOnServiceUrl": "https://idp.example.com/sso",
    "singleLogoutServiceUrl": "https://idp.example.com/slo"
  }
}
```

**注意**:
- IDP 的 `alias` 默认从环境变量 `DEFAULT_IDP_ALIAS` 获取（默认: `da-saml-idp`）
- 每个租户只能创建一个 IDP 实例，如果已存在则返回 400 Bad Request
- `singleSignOnServiceUrl` 是必填字段，验证不通过会返回 400 错误

**示例 cURL**:
```bash
curl -X POST http://localhost:8000/api/v1/my-tenant/idp/saml/instances \
  -H "Content-Type: application/json" \
  -d '{
    "displayName": "企业 SAML IDP",
    "enabled": true,
    "config": {
      "singleSignOnServiceUrl": "https://idp.example.com/sso",
      "entityId": "https://sp.example.com"
    }
  }'
```

---

### 更新 SAML IDP 实例

更新现有 SAML 2.0 身份提供者实例的配置。

**接口**: `PUT /{realm}/idp/saml/instances`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body**:
```json
{
  "displayName": "更新后的企业 SAML IDP",
  "enabled": false,
  "trustEmail": true,
  "config": {
    "singleSignOnServiceUrl": "https://new-idp.example.com/sso",
    "newConfigParam": "value"
  }
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| displayName | string | 否 | IDP 显示名称 |
| enabled | boolean | 否 | 是否启用 |
| trustEmail | boolean | 否 | 是否信任 IDP 返回的邮箱 |
| config | object | 否 | SAML 配置（与现有配置合并，不会删除未传字段） |

**响应 (200 OK)**: 返回更新后的 IDP 实例对象

**注意**:
- `config` 字段的更新是合并模式，不会删除请求体中未包含的现有配置项
- `alias` 从环境变量获取，不支持在请求体中修改

---

### 查询 IDP 实例列表

获取租户下所有身份提供者实例列表。

**接口**: `GET /{realm}/idp/saml/instances`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**响应 (200 OK)**:
```json
[
  {
    "alias": "da-saml-idp",
    "displayName": "企业 SAML IDP",
    "internalId": "internal-uuid",
    "providerId": "saml",
    "enabled": true,
    "trustEmail": false,
    "firstBrokerLoginFlowAlias": "first broker login",
    "config": {
      "entityId": "https://idp.example.com/entityid",
      "singleSignOnServiceUrl": "https://idp.example.com/sso"
    }
  }
]
```

---

### 删除 SAML IDP 实例

删除指定的 SAML 身份提供者实例。

**接口**: `DELETE /{realm}/idp/saml/instances/{alias}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| alias | string | 是 | IDP 别名（如 `da-saml-idp`） |

**响应 (204 No Content)**: 无响应体

**示例 cURL**:
```bash
curl -X DELETE http://localhost:8000/api/v1/my-tenant/idp/saml/instances/da-saml-idp
```

---

### 查询 IDP Mapper 列表

获取指定 IDP 实例的所有协议映射器（Mapper）列表（简化版）。

**接口**: `GET /{realm}/idp/saml/instances/{alias}/mappers`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| alias | string | 是 | IDP 别名 |

**响应 (200 OK)**:
```json
[
  {
    "id": "mapper-uuid-1",
    "name": "Department Mapper",
    "attributeKey": "department",
    "attributeValue": "department",
    "friendlyName": "Department"
  },
  {
    "id": "mapper-uuid-2",
    "name": "Email Mapper",
    "attributeKey": "email",
    "attributeValue": "email",
    "friendlyName": null
  }
]
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | Mapper UUID |
| name | string | Mapper 名称 |
| attributeKey | string | Remote Attribute（SAML 属性名，对应 Keycloak config 中的 user.attribute） |
| attributeValue | string | Local Attribute（Keycloak 用户属性名，对应 Keycloak config 中的 attribute.name） |
| friendlyName | string \| null | Friendly Name（可选，对应 Keycloak config 中的 friendly.name） |

**注意**:
- 此接口返回的是简化版 Mapper 信息，仅包含前端需要显示的核心字段
- 实际 Keycloak API 中的其他字段（如 identityProviderMapper、config.syncMode 等）在代理层已固定，无需显示
- 固定配置：<br>
  - Mapper 类型: `saml-user-attribute-idp-mapper`（Attribute Importer）<br>
  - Sync Mode: `INHERIT`<br>
  - Name Format: `ATTRIBUTE_FORMAT_BASIC` |

---

### 创建 IDP Mapper

为指定 IDP 实例创建新的协议映射器（简化版）。

**接口**: `POST /{realm}/idp/saml/instances/{alias}/mappers`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| alias | string | 是 | IDP 别名 |

**请求 Body**:
```json
{
  "name": "Department Mapper",
  "attributeKey": "department",
  "attributeValue": "department",
  "friendlyName": "Department"
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| name | string | 是 | Mapper 名称 |
| attributeKey | string | 是 | Remote Attribute（SAML 属性名，将被映射到 Keycloak 用户属性） |
| attributeValue | string | 是 | Local Attribute（Keycloak 用户属性名） |
| friendlyName | string | 否 | Friendly Name（可选的显示名称） |

**响应 (201 Created)**:
```json
{
  "id": "new-mapper-uuid",
  "name": "Department Mapper",
  "attributeKey": "department",
  "attributeValue": "department",
  "friendlyName": "Department"
}
```

**说明**:
- 此接口为简化版本，仅接受最基本的映射参数
- 下层 Mapper 类型固定为 `saml-user-attribute-idp-mapper`（Attribute Importer）
- 以下配置在代理层自动固定，无需前端传递：<br>
  - `syncMode`: `INHERIT`（同步模式继承）<br>
  - `nameFormat`: `ATTRIBUTE_FORMAT_BASIC`（名称格式：基础属性格式）<br>
  - 所有 Mapper 均为属性导入类型，用于从 SAML IDP 导入用户属性到 Keycloak |

---

### 更新 IDP Mapper

更新指定 IDP Mapper 的配置（简化版）。

**接口**: `PUT /{realm}/idp/saml/instances/{alias}/mappers/{mapper_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| alias | string | 是 | IDP 别名 |
| mapper_id | string | 是 | Mapper UUID |

**请求 Body**:
```json
{
  "name": "Updated Department Mapper",
  "attributeKey": "new_department",
  "attributeValue": "new_department_field",
  "friendlyName": "New Department Name"
}
```

**请求字段**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| name | string | 否 | Mapper 新名称 |
| attributeKey | string | 否 | 新的 Remote Attribute（SAML 属性名） |
| attributeValue | string | 否 | 新的 Local Attribute（Keycloak 用户属性名） |
| friendlyName | string | 否 | 新的 Friendly Name（可选） |

**响应 (204 No Content)**: 无响应体

**说明**:
- 此接口为简化版本，仅支持更新核心映射参数
- 未提供的字段保持原值不变
- Mapper 的固定配置（类型、syncMode、nameFormat）不可通过此接口修改 |

---

### 删除 IDP Mapper

删除指定的 IDP 协议映射器。

**接口**: `DELETE /{realm}/idp/saml/instances/{alias}/mappers/{mapper_id}`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |
| alias | string | 是 | IDP 别名 |
| mapper_id | string | 是 | Mapper UUID |

**响应 (204 No Content)**: 无响应体

---

## Token 交换

Token 管理 API 处理 OIDC 认证流程中的 Token 交换。

### OIDC 授权码换取 Token

通过 OIDC 授权码（Authorization Code）换取访问令牌（Access Token）。此接口是 OAuth2 Token Endpoint 的代理，并在响应中添加了自定义字段（`realm_id` 和 `role_ids`）以便前端识别用户上下文。

**接口**: `POST /{realm}/token/exchange`

**路径参数**:
| 参数 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| realm | string | 是 | 租户名称 |

**请求 Body (application/x-www-form-urlencoded)**:
| 字段 | 类型 | 必需 | 说明 |
|------|------|--------|------|
| grant_type | string | 是 | 授权类型，必须为 `authorization_code` |
| code | string | 是 | OIDC 登录后获取的授权码 |
| redirect_uri | string | 是 | 回调 URI，必须与登录时使用的一致 |
| code_verifier | string | 否 | PKCE 代码验证器（如果使用 PKCE 流程） |

**响应 (200 OK)**:
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLXV1aWQiLCJyZWFsbV9pZCI6Im15LXRlbmFudCJ9.signature",
  "token_type": "Bearer",
  "expires_in": 300,
  "refresh_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLXV1aWQiLCJyZWFsbV9pZCI6Im15LXRlbmFudCJ9.signature",
  "id_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ1c2VyLXV1aWQiLCJnaXZlbl9uYW1lIjoiSm9obiIsInJlYWxtIjoibXktdGVuYW50In0.signature",
  "scope": "openid profile email"
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| access_token | string | 访问令牌 |
| token_type | string | 令牌类型（通常为 Bearer） |
| expires_in | number | 访问令牌过期时间（秒） |
| refresh_token | string | 刷新令牌 |
| id_token | string | ID 令牌（包含用户身份信息） |
| scope | string | 授权范围 |

**注意**:
- 此接口代理了 Keycloak 的 OAuth2 Token Endpoint，返回标准 OAuth2.0 响应
- 如需获取用户角色信息，请调用 `GET /{realm}/users/{user_id}/details` 接口
- 实际 Token 交换的参数和响应与 Keycloak 标准 OAuth2 实现完全兼容

---

## 通用接口 (Common)

### 健康检查

用于监控系统健康状态和负载均衡器检查。

**接口**: `GET /common/health`

**用途**: 检查代理服务是否正常运行

**响应 (200 OK)**:
```json
{
  "status": "healthy",
  "code": 200,
  "timestamp": "2026-03-24T10:30:00.000000"
}
```

**响应字段**:
| 字段 | 类型 | 说明 |
|------|------|------|
| status | string | 服务状态，固定为 "healthy" |
| code | number | HTTP 状态码 |
| timestamp | string | UTC 时间戳（ISO 8601 格式） |

**示例 cURL**:
```bash
curl http://localhost:8000/api/v1/common/health
```

---

## 前端开发者注意事项

### 角色显示

用户角色以对象数组形式返回（包含 `id`、`name` 等字段），而非简单字符串。前端显示角色名称时需要提取 `name` 字段：

```javascript
// ✅ 正确：提取角色名称后再拼接
const roleNames = userRoles.map(r => r.name || r).join(', ');

// ❌ 错误：直接拼接会显示 [Object]
const wrong = userRoles.join(', ');
```

### 处理 204 响应

DELETE 操作和部分 PUT 操作返回 204 No Content，无响应体。前端需正确处理这种情况：

```javascript
const response = await apiCall('/api/v1/my-tenant/groups/group-uuid', {
  method: 'DELETE'
});

// response 将为 null（无内容）
console.log(response); // null
```

### 文件上传处理

上传文件（如 SAML 元数据导入）需使用 `FormData`：

```javascript
const formData = new FormData();
formData.append('file', fileInput.files[0]);

const response = await fetch('/api/v1/my-tenant/idp/saml/import', {
  method: 'POST',
  body: formData
});
```

### Master Realm 保护

所有涉及 realm 的接口都通过 `skip_master_realm` 依赖自动拦截对 `master` realm 的访问。如果尝试访问 master realm，会返回 403 Forbidden 错误。

---

## 环境变量

| 环境变量 | 说明 | 默认值 |
|-----------|------|--------|
| `KC_URL` | Keycloak 服务地址 | `http://localhost:8080` |
| `KC_REALM` | 受保护的租户名称 | `master` |
| `KC_NEW_CLIENT_ID` | 为新租户创建的默认 Client ID | `data-agent` |
| `KC_SCRIPT_MAPPER` | Script Mapper 提供者名称 | `Data Agent Mapper` |
| `DEFAULT_TENANT_ADMIN_ROLE` | 默认租户管理员角色名称 | `tenant-admin` |
| `DEFAULT_TENANT_ADMIN_NAME` | 默认租户管理员用户名 | `tenant-admin` |
| `DEFAULT_IDP_ALIAS` | 默认 IDP 别名 | `da-saml-idp` |
| `OPA_BASE_URL` | OPA (Open Policy Agent) 服务地址 | `http://bundle-server.opa.svc.cluster.local:8001` |

---

## 常用端点快速参考

| 功能 | 方法 | 路径 |
|------|------|--------|
| **租户管理** |
| 查询租户列表 | GET | /tenants |
| 创建租户 | POST | /tenants |
| 删除租户 | DELETE | /tenants/{realm_name} |
| **角色管理** |
| 查询角色列表 | GET | /{realm}/roles |
| 创建角色 | POST | /{realm}/roles |
| 获取角色详情 | GET | /{realm}/roles/{role_name} |
| 更新角色 | PUT | /{realm}/roles/{role_name} |
| 删除角色 | DELETE | /{realm}/roles/{role_name} |
| 通过 UUID 获取角色 | GET | /{realm}/roles/by-id/{role_id} |
| 通过 UUID 更新角色 | PUT | /{realm}/roles/by-id/{role_id} |
| 通过 UUID 删除角色 | DELETE | /{realm}/roles/by-id/{role_id} |
| **组管理** |
| 查询组列表 | GET | /{realm}/groups |
| 创建组 | POST | /{realm}/groups |
| 获取组详情 | GET | /{realm}/groups/{group_id} |
| 更新组 | PUT | /{realm}/groups/{group_id} |
| 删除组 | DELETE | /{realm}/groups/{group_id} |
| **用户管理** |
| 查询用户列表 | GET | /{realm}/users |
| 获取用户上下文 | GET | /{realm}/users/{user_id}/details |
| **IDP 管理** |
| 导入 SAML 元数据 | POST | /{realm}/idp/saml/import |
| 查询 IDP 列表 | GET | /{realm}/idp/saml/instances |
| 创建 IDP 实例 | POST | /{realm}/idp/saml/instances |
| 更新 IDP 实例 | PUT | /{realm}/idp/saml/instances |
| 删除 IDP 实例 | DELETE | /{realm}/idp/saml/instances/{alias} |
| 查询 IDP Mapper 列表 | GET | /{realm}/idp/saml/instances/{alias}/mappers |
| 创建 IDP Mapper | POST | /{realm}/idp/saml/instances/{alias}/mappers |
| 更新 IDP Mapper | PUT | /{realm}/idp/saml/instances/{alias}/mappers/{mapper_id} |
| 删除 IDP Mapper | DELETE | /{realm}/idp/saml/instances/{alias}/mappers/{mapper_id} |
| **Token 交换** |
| 授权码换 Token | POST | /{realm}/token/exchange |
| **通用** |
| 健康检查 | GET | /common/health |

---

## 版本历史

| 版本 | 日期 | 更新内容 |
|------|------|----------|
| v1.0.0 | 2025-03-20 | 初始版本，涵盖租户、角色、组、用户、IDP 的完整管理 API 文档 |

---

## 支持与反馈

如有关于 IDB Proxy API 的问题或建议，请联系开发团队或查阅项目仓库。
