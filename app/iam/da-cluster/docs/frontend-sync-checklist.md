# Frontend Integration Checklist

## UI Pages & API Mapping

### Super-admin Portal (`/super-portal`)

> Super-admin is a user in the **master** realm, has full cross-tenant access.

#### Page 1: Login

| UI Element | API |
|------------|-----|
| Username input | - |
| Password input | - |
| Login button | `POST /realms/master/protocol/openid-connect/token` |

**API Request:**

```
POST /realms/master/protocol/openid-connect/token
Content-Type: application/x-www-form-urlencoded

grant_type=password
client_id=idb-proxy-client
client_secret=<from K8s Secret keycloak-idb-proxy-client>
username=<input>
password=<input>
```

**API Response:**

```json
{
  "access_token": "eyJhbG...",
  "refresh_token": "eyJhbG...",
  "expires_in": 300
}
```

**Frontend Logic:**
- Store `access_token` and `refresh_token`
- All subsequent requests add header: `Authorization: Bearer <access_token>`
- When token expires, use refresh_token to get new one via same endpoint with `grant_type=refresh_token`

---

#### Page 2: Tenant List

| UI Element | API |
|------------|-----|
| Tenant list table | `GET /api/v1/tenants` |
| Each row: tenant name, realm, status | - |
| "Add Tenant" button | → navigate to Page 3 |
| Click tenant row | → navigate to tenant portal (`/{tenant_id}/portal`) |

**API Request:**

```
GET /api/v1/tenants
Authorization: Bearer <super-admin-token>
```

**API Response:**

```json
[
  {
    "id": "data-agent",
    "realm": "data-agent",
    "displayName": "Data Agent",
    "enabled": true
  }
]
```

---

#### Page 3: Add Tenant (Step 1 - Basic Info)

| UI Element | API |
|------------|-----|
| Realm name input | → `realm` field |
| Display name input | → `displayName` field |
| Identity Provider dropdown: `SAML 2.0` | → determines next step |
| "Next" button | → first create tenant, then navigate to Step 2 |

**On "Next" click — Create Tenant:**

```
POST /api/v1/tenants
Authorization: Bearer <super-admin-token>
Content-Type: application/json

{
  "realm": "new-customer",
  "displayName": "New Customer Inc."
}
```

**Response (201):**

```json
{
  "realm": "new-customer",
  "id": "new-customer",
  "admin_role": "tenant-admin",
  "admin_user": "tenant-admin"
}
```

---

#### Page 4: Add Tenant (Step 2 - SAML Configuration)

> This page appears after tenant creation when SAML 2.0 is selected.

**Option A: Fill in parameters manually**

| UI Element | Field | Required |
|------------|-------|----------|
| SSO Service URL | `config.singleSignOnServiceUrl` | Yes |
| Entity ID | `config.entityId` | No |
| Display Name | `displayName` | No |
| Enable toggle | `enabled` | Yes (default true) |
| Trust Email toggle | `trustEmail` | No (default false) |

**API Request:**

```
POST /api/v1/{realm}/idp/saml/instances
Authorization: Bearer <super-admin-token>
Content-Type: application/json

{
  "displayName": "Customer SSO",
  "enabled": true,
  "trustEmail": false,
  "config": {
    "singleSignOnServiceUrl": "https://customer-idp.example.com/sso",
    "entityId": "https://customer-idp.example.com/entity"
  }
}
```

**Option B: Upload XML Metadata**

```
POST /api/v1/{realm}/idp/saml/import
Authorization: Bearer <super-admin-token>
Content-Type: multipart/form-data

file=@metadata.xml
```

---

### Tenant Portal (`/{tenant_id}/portal`)

#### Page 5: Tenant Login

| UI Element | API |
|------------|-----|
| "Login" button (redirect to Keycloak login page) | Browser redirect |

**Redirect URL:**

```
/realms/{realm}/protocol/openid-connect/auth
  ?response_type=code
  &client_id=data-agent
  &redirect_uri=<frontend callback URL>
  &scope=openid profile email
```

**Flow:**
1. User clicks "Login" → browser redirects to Keycloak login page
2. User logs in (username/password or SSO)
3. Keycloak redirects back to `redirect_uri?code=xxx`
4. Frontend exchanges code for token:

```
POST /realms/{realm}/protocol/openid-connect/token
Content-Type: application/x-www-form-urlencoded

grant_type=authorization_code
client_id=data-agent
code=<authorization code>
redirect_uri=<same as above>
```

> Note: `data-agent` client is a public client (no client_secret needed)

---

### Tenant Admin Pages (after login as tenant-admin)

#### Page 7: User Management

| UI Element | API |
|------------|-----|
| User list table (username, email, status) | `GET /api/v1/{realm}/users` |
| Click "View" on a user | `GET /api/v1/{realm}/users/{user_id}/details` |

**User List Response:**

```json
[
  {
    "id": "user-uuid",
    "username": "john",
    "email": "john@example.com",
    "firstName": "John",
    "lastName": "Doe",
    "enabled": true
  }
]
```

**User Detail (View popup) Response:**

```json
{
  "groups": [
    {"id": "group-uuid", "name": "dev-team"}
  ],
  "roles": [
    {"id": "role-uuid", "name": "viewer"}
  ]
}
```

---

#### Page 8: Group Management

| UI Element | API |
|------------|-----|
| Group list table | `GET /api/v1/{realm}/groups` |
| Click "View" | `GET /api/v1/{realm}/groups/{group_id}` |
| Click "Edit" | `PUT /api/v1/{realm}/groups/{group_id}` |
| Click "Add Group" | `POST /api/v1/{realm}/groups` |
| Click "Delete" | `DELETE /api/v1/{realm}/groups/{group_id}` |

**Add Group form:**

| Field | Type | Source |
|-------|------|--------|
| Name | text input | user input |
| Description | text input | user input |
| Roles | multi-select dropdown (tag-style, can add multiple) | `GET /api/v1/{realm}/roles` for dropdown options |
| Members | multi-select dropdown (tag-style, can add multiple) | `GET /api/v1/{realm}/users` for dropdown options |

**Create Group Request:**

```
POST /api/v1/{realm}/groups
Authorization: Bearer <tenant-admin-token>
Content-Type: application/json

{
  "name": "dev-team",
  "roles": ["viewer", "editor"],
  "users": ["user-uuid-1", "user-uuid-2"]
}
```

> `roles` uses role **names**, `users` uses user **UUIDs**

**Create Group Response (201):**

```json
{
  "id": "group-uuid",
  "name": "dev-team",
  "subGroups": []
}
```

**Group Detail (View) Response:**

```json
{
  "id": "group-uuid",
  "name": "dev-team",
  "members": [
    {"id": "user-uuid", "username": "john"}
  ],
  "roles": [
    {"id": "role-uuid", "name": "viewer"}
  ]
}
```

**Edit Group Request (204 No Content):**

```
PUT /api/v1/{realm}/groups/{group_id}
Content-Type: application/json

{
  "name": "dev-team-v2",
  "roles": ["viewer"],
  "users": ["user-uuid-1"]
}
```

---

#### Page 9: Role Management

| UI Element | API |
|------------|-----|
| Role list table (role name + bound Resource Policy) | `GET /api/v1/{realm}/roles` + per-role `GET /api/v1/roles/{role_id}/policy` |
| Click "Edit" | Update role basic info + rebind policy |
| Click "Create Role" | `POST /api/v1/{realm}/roles` + optionally bind policy |

**Role list display logic:**

```
1. GET /api/v1/{realm}/roles → get role list with {id, name, description}
2. For each role, GET /api/v1/roles/{role.id}/policy → get bound policy name
3. Display: | Role Name | Description | Resource Policy |
```

**Create Role form:**

| Field | Type | Source |
|-------|------|--------|
| Name | text input | user input |
| Description | text input | user input |
| Resource Policy | single-select dropdown | `GET /api/v1/policies` for dropdown options |

**Create Role flow (2 API calls):**

```
Step 1: Create the role
POST /api/v1/{realm}/roles
Content-Type: application/json
{"name": "viewer", "description": "Read-only viewer"}

Response (201): full role object with id

Step 2: Bind policy to role (if policy selected)
POST /api/v1/roles/{role_id}/policy
Content-Type: application/json
{"policy_id": "docs-policy", "tenant_id": "{realm}"}
```

**Edit Role flow:**

```
Update basic info:
PUT /api/v1/{realm}/roles/{role_name}
{"description": "Updated description"}

Update/change bound policy:
PUT /api/v1/roles/{role_id}/policy
{"policy_id": "new-policy-id", "tenant_id": "{realm}"}
```

---

#### Page 10: Resource Policy Management

| UI Element | API |
|------------|-----|
| Policy list table | `GET /api/v1/policies` |
| Click "View" | `GET /api/v1/policies/{policy_id}` |
| Click "Edit" | `PUT /api/v1/policies/{policy_id}` |
| Click "Add Policy" | `POST /api/v1/policies` |
| Click "Delete" | `DELETE /api/v1/policies/{policy_id}` |

**Policy List Response:**

```json
{
  "policies": [
    {
      "id": "docs-policy",
      "tenant_id": "data-agent",
      "rules": [
        {"resource": "documents", "effect": "allow"},
        {"resource": "settings", "effect": "deny"}
      ]
    }
  ],
  "count": 1,
  "tenant_id": "data-agent"
}
```

**Add Policy form:**

| Field | Type | Source |
|-------|------|--------|
| Name | text input | user input |
| Resources | hardcoded list, each with Allow/Deny toggle | frontend hardcoded (future: dynamic API) |

**Resource list example (hardcoded in frontend):**

```
| Resource    | Allow | Deny |
|-------------|-------|------|
| documents   |  [x]  |  [ ] |
| reports     |  [ ]  |  [x] |
| settings    |  [x]  |  [ ] |
| billing     |  [ ]  |  [x] |
| invoices    |  [x]  |  [ ] |
```

**Create Policy Request:**

```
POST /api/v1/policies
Authorization: Bearer <tenant-admin-token>
Content-Type: application/json

{
  "name": "docs-policy",
  "tenant_id": "data-agent",
  "rules": [
    {"resource": "documents", "effect": "allow"},
    {"resource": "reports", "effect": "deny"},
    {"resource": "settings", "effect": "allow"},
    {"resource": "billing", "effect": "deny"},
    {"resource": "invoices", "effect": "allow"}
  ]
}
```

**Edit Policy Request (full replace):**

```
PUT /api/v1/policies/{policy_id}
Content-Type: application/json

{
  "name": "docs-policy",
  "tenant_id": "data-agent",
  "rules": [
    {"resource": "documents", "effect": "allow"},
    {"resource": "reports", "effect": "allow"},
    {"resource": "settings", "effect": "allow"}
  ]
}
```

---

## Key Points for Frontend

### Authentication Header

All API calls (except login and OIDC endpoints) must include:

```
Authorization: Bearer <access_token>
```

### Role-Policy Relationship

```
Role ──1:1──> Resource Policy ──1:N──> Rules (resource + allow/deny)
```

- One role binds to exactly one resource policy
- One policy contains multiple resource rules
- Each rule is a resource name + allow/deny toggle

### Status Codes to Handle

| Action | Success Code | Body |
|--------|-------------|------|
| Create tenant/role/group/IDP | 201 | resource object |
| Create policy | 200 | policy object |
| Update role/IDP | 200 | updated object |
| Update group | 204 | no body |
| Update policy | 200 | updated object |
| Delete tenant/role/group/IDP | 204 | no body |
| Delete policy | 200 | message |
| List/Get | 200 | data |
| Auth failed | 401/403 | error message |

### Environment Configuration

| Item | Development | Production |
|------|-------------|------------|
| Gateway URL | `http://localhost:8080` | `http://<server-ip>:8080` or domain |
| Keycloak Admin | `http://localhost:8080/admin/` | same via gateway |
| Super-admin client_secret | from K8s Secret | from K8s Secret |

### Test Accounts

| User | Realm | Password | Role | Portal |
|------|-------|----------|------|--------|
| super-admin | master | SuperInit@123 | super-admin | /super-portal |
| tenant-admin | data-agent | TenantAdmin@123 | tenant-admin | /data-agent/portal |
| normal-user | data-agent | NormalUser@123 | normal-user | /data-agent/portal |
