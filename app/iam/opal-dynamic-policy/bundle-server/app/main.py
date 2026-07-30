# bundle-server/app/main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import os
import json
import tarfile
import io
from datetime import datetime
import logging

import uuid

import asyncpg
import httpx

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Bundle Server")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUNDLES_PATH = "/app/bundles"
OPAL_SERVER_URL = os.getenv("OPAL_SERVER_URL", "http://opal-server:7002")
OPAL_SERVER_TOKEN = os.getenv("OPAL_SERVER_TOKEN", "opal-server-token")
BUNDLE_SERVER_SELF_URL = os.getenv("BUNDLE_SERVER_SELF_URL", "http://bundle-server:8001")
OPA_URL = os.getenv("OPA_URL", "http://localhost:8181")
DB_URL = os.getenv("DB_URL", "postgresql://postgres:postgres@postgres:5432/opal")

os.makedirs(BUNDLES_PATH, exist_ok=True)

db_pool: asyncpg.Pool = None


# ---------------------------------------------------------------------------
# Request / Response models
# ---------------------------------------------------------------------------

class PolicyRule(BaseModel):
    resource: str   # 资源名称或 URL 路径前缀（如 /api/v1/documents）
    effect: str     # "allow" | "deny"


class PolicyData(BaseModel):
    id: str = ""            # UUID policy_id（创建时自动生成，更新/重试时可显式传入）
    name: str = ""          # 用户可读的显示名称
    rules: List[PolicyRule] # each resource has its own allow/deny
    tenant_id: str
    conditions: Optional[Dict[str, Any]] = None
    created_at: Optional[str] = None
    updated_at: Optional[str] = None


class RoleBindingRequest(BaseModel):
    policy_id: str      # 1:1 binding
    tenant_id: str


class NotifyRequest(BaseModel):
    tenant_id: str
    event: str
    policy_id: Optional[str] = None


# ---------------------------------------------------------------------------
# Startup / shutdown
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def startup_event():
    global db_pool
    logger.info("Bundle Server starting up, connecting to PostgreSQL...")
    db_pool = await asyncpg.create_pool(DB_URL, min_size=2, max_size=10)

    async with db_pool.acquire() as conn:
        # policies: UUID id + name display label + rules JSONB
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS policies (
                tenant_id   VARCHAR NOT NULL,
                id          VARCHAR NOT NULL,
                name        VARCHAR NOT NULL DEFAULT '',
                rules       JSONB   NOT NULL DEFAULT '[]',
                conditions  JSONB,
                created_at  TIMESTAMP,
                updated_at  TIMESTAMP,
                PRIMARY KEY (tenant_id, id)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_policies_tenant
            ON policies (tenant_id)
        """)

        # Migration: add name column (populated from id for existing rows)
        name_col = await conn.fetchval(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='policies' AND column_name='name'"
        )
        if not name_col:
            logger.info("Adding name column to policies table...")
            await conn.execute(
                "ALTER TABLE policies ADD COLUMN name VARCHAR NOT NULL DEFAULT ''"
            )
            await conn.execute(
                "UPDATE policies SET name = id WHERE name = ''"
            )
            logger.info("name column added and backfilled from id.")

        # Migration: resources JSONB + effect VARCHAR → rules JSONB
        resources_col = await conn.fetchval(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='policies' AND column_name='resources'"
        )
        if resources_col:
            logger.info("Migrating policies resources+effect → rules JSONB...")
            await conn.execute(
                "ALTER TABLE policies ADD COLUMN IF NOT EXISTS rules JSONB"
            )
            # Convert each existing row: wrap each resource string into {resource, effect}
            await conn.execute("""
                UPDATE policies
                   SET rules = (
                       SELECT jsonb_agg(
                           jsonb_build_object('resource', r.val, 'effect', effect)
                       )
                       FROM jsonb_array_elements_text(resources) AS r(val)
                   )
                 WHERE rules IS NULL OR rules = '[]'::jsonb
            """)
            await conn.execute("ALTER TABLE policies DROP COLUMN IF EXISTS resources")
            await conn.execute("ALTER TABLE policies DROP COLUMN IF EXISTS effect")
            logger.info("policies migration complete.")

        # Migration: resource VARCHAR (older schema) → rules JSONB
        resource_col = await conn.fetchval(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='policies' AND column_name='resource'"
        )
        if resource_col:
            logger.info("Migrating policies resource+effect (v1) → rules JSONB...")
            await conn.execute(
                "ALTER TABLE policies ADD COLUMN IF NOT EXISTS rules JSONB"
            )
            await conn.execute("""
                UPDATE policies
                   SET rules = jsonb_build_array(
                       jsonb_build_object('resource', resource, 'effect', effect)
                   )
                 WHERE rules IS NULL OR rules = '[]'::jsonb
            """)
            await conn.execute("ALTER TABLE policies DROP COLUMN IF EXISTS resource")
            await conn.execute("ALTER TABLE policies DROP COLUMN IF EXISTS effect")
            logger.info("policies v1 migration complete.")

        # role_policy_bindings: 1:1 (PK = tenant_id + role_id)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS role_policy_bindings (
                tenant_id   VARCHAR NOT NULL,
                role_id     VARCHAR NOT NULL,
                policy_id   VARCHAR NOT NULL,
                created_at  TIMESTAMP,
                PRIMARY KEY (tenant_id, role_id)
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_rpb_tenant_role
            ON role_policy_bindings (tenant_id, role_id)
        """)

        # Migration: PK (tenant_id, role_id, policy_id) → (tenant_id, role_id)
        pk_cols = await conn.fetchval(
            "SELECT COUNT(*) FROM information_schema.key_column_usage "
            "WHERE constraint_name='role_policy_bindings_pkey' "
            "AND table_name='role_policy_bindings'"
        )
        if pk_cols and pk_cols > 2:
            logger.info("Migrating role_policy_bindings PK to 2-column...")
            await conn.execute("""
                DELETE FROM role_policy_bindings r1
                USING (
                    SELECT tenant_id, role_id, MAX(created_at) AS max_ts
                    FROM role_policy_bindings GROUP BY tenant_id, role_id
                ) keep
                WHERE r1.tenant_id = keep.tenant_id
                  AND r1.role_id   = keep.role_id
                  AND r1.created_at < keep.max_ts
            """)
            await conn.execute(
                "ALTER TABLE role_policy_bindings "
                "DROP CONSTRAINT role_policy_bindings_pkey"
            )
            await conn.execute(
                "ALTER TABLE role_policy_bindings "
                "ADD CONSTRAINT role_policy_bindings_pkey "
                "PRIMARY KEY (tenant_id, role_id)"
            )
            logger.info("PK migration complete.")

    logger.info("PostgreSQL schema ready.")

    # 无论是否有租户数据，始终先把 Rego 策略推送到 OPA，
    # 避免冷启动时 OPA 没有任何策略导致所有请求被拒绝（含 tenant-admin）
    await _push_base_rego_to_opa()

    tenants = await _list_tenants()
    for tenant_id in tenants:
        tenant_data = await _build_tenant_data(tenant_id)
        await _push_to_opa(tenant_id, tenant_data)
    await _rebuild_combined_bundle()
    logger.info("Initialized and pushed policies for %d tenants", len(tenants))


@app.on_event("shutdown")
async def shutdown_event():
    if db_pool:
        await db_pool.close()


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "bundle-server",
        "tenants": len(await _list_tenants()),
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Tenant helpers
# ---------------------------------------------------------------------------

async def _list_tenants() -> List[str]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT DISTINCT tenant_id FROM policies ORDER BY tenant_id"
        )
    return [row["tenant_id"] for row in rows]


@app.get("/api/v1/tenants")
async def list_tenants() -> List[str]:
    return await _list_tenants()


# ---------------------------------------------------------------------------
# Policy CRUD（id = UUID policy_id，name = 显示名称，rules JSONB）
# ---------------------------------------------------------------------------

@app.get("/api/v1/tenants/{tenant_id}/policies")
async def get_tenant_policies(tenant_id: str) -> List[Dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM policies WHERE tenant_id = $1 ORDER BY id",
            tenant_id,
        )
    return [_row_to_dict(row) for row in rows]


@app.get("/api/v1/tenants/{tenant_id}/policies/{policy_id}")
async def get_tenant_policy(tenant_id: str, policy_id: str) -> Dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM policies WHERE tenant_id = $1 AND id = $2",
            tenant_id, policy_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="Policy not found")
    return _row_to_dict(row)


@app.post("/api/v1/policies")
async def create_policy(policy: PolicyData, background_tasks: BackgroundTasks):
    # id 字段不为空时复用（幂等重试），否则自动生成 UUID
    policy_id = policy.id if policy.id else str(uuid.uuid4())
    name = policy.name or policy_id
    now = datetime.utcnow()
    rules_json = json.dumps([r.model_dump() for r in policy.rules])

    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO policies
                (tenant_id, id, name, rules, conditions, created_at, updated_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (tenant_id, id) DO UPDATE
                SET name       = EXCLUDED.name,
                    rules      = EXCLUDED.rules,
                    conditions = EXCLUDED.conditions,
                    updated_at = EXCLUDED.updated_at
            """,
            policy.tenant_id, policy_id, name,
            rules_json,
            json.dumps(policy.conditions) if policy.conditions else None,
            now, now,
        )

    background_tasks.add_task(generate_and_notify, policy.tenant_id)
    return {"status": "success", "policy_id": policy_id, "name": name, "tenant_id": policy.tenant_id}


@app.put("/api/v1/policies/{policy_id}")
async def update_policy(
    policy_id: str, policy: PolicyData, background_tasks: BackgroundTasks
):
    now = datetime.utcnow()
    rules_json = json.dumps([r.model_dump() for r in policy.rules])

    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE policies
               SET rules      = $1,
                   conditions = $2,
                   updated_at = $3
             WHERE tenant_id = $4 AND id = $5
            """,
            rules_json,
            json.dumps(policy.conditions) if policy.conditions else None,
            now,
            policy.tenant_id, policy_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="Policy not found")

    background_tasks.add_task(generate_and_notify, policy.tenant_id)
    return {"status": "success", "policy_id": policy_id}


@app.delete("/api/v1/policies/{policy_id}")
async def delete_policy(
    tenant_id: str, policy_id: str, background_tasks: BackgroundTasks
):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM role_policy_bindings WHERE tenant_id=$1 AND policy_id=$2",
            tenant_id, policy_id,
        )
        await conn.execute(
            "DELETE FROM policies WHERE tenant_id=$1 AND id=$2",
            tenant_id, policy_id,
        )
    background_tasks.add_task(generate_and_notify, tenant_id)
    return {"status": "success"}


# ---------------------------------------------------------------------------
# Role-Policy Binding（role UUID ↔ policy，1:1）
# ---------------------------------------------------------------------------

@app.post("/api/v1/roles/{role_id}/policy")
async def bind_policy_to_role(
    role_id: str, body: RoleBindingRequest, background_tasks: BackgroundTasks
):
    """1:1 绑定 role UUID → policy（upsert：新建或覆盖已有绑定）。"""
    now = datetime.utcnow()
    async with db_pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO role_policy_bindings (tenant_id, role_id, policy_id, created_at)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (tenant_id, role_id) DO UPDATE
                SET policy_id = EXCLUDED.policy_id
            """,
            body.tenant_id, role_id, body.policy_id, now,
        )

    background_tasks.add_task(generate_and_notify, body.tenant_id)
    return {
        "status": "success",
        "role_id": role_id,
        "policy_id": body.policy_id,
        "tenant_id": body.tenant_id,
    }


@app.put("/api/v1/roles/{role_id}/policy")
async def update_role_policy(
    role_id: str, body: RoleBindingRequest, background_tasks: BackgroundTasks
):
    """更新已有绑定（角色必须已有绑定，否则 404）。"""
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            """
            UPDATE role_policy_bindings
               SET policy_id = $1
             WHERE tenant_id = $2 AND role_id = $3
            """,
            body.policy_id, body.tenant_id, role_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(status_code=404, detail="No policy binding found for role")

    background_tasks.add_task(generate_and_notify, body.tenant_id)
    return {
        "status": "success",
        "role_id": role_id,
        "policy_id": body.policy_id,
        "tenant_id": body.tenant_id,
    }


@app.delete("/api/v1/roles/{role_id}/policy")
async def delete_role_policy(
    role_id: str, tenant_id: str, background_tasks: BackgroundTasks
):
    """删除 role UUID 的 policy 绑定。绑定不存在时返回 404。"""
    async with db_pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM role_policy_bindings WHERE tenant_id=$1 AND role_id=$2",
            tenant_id, role_id,
        )
    if result == "DELETE 0":
        raise HTTPException(status_code=404, detail="No policy binding found for role")

    background_tasks.add_task(generate_and_notify, tenant_id)
    return {"status": "success", "role_id": role_id, "tenant_id": tenant_id}


@app.get("/api/v1/roles/{role_id}/policy")
async def get_role_policy(role_id: str, tenant_id: str) -> Dict:
    """查询 role UUID 绑定的 policy 详情。"""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT p.*
              FROM role_policy_bindings rpb
              JOIN policies p
                ON p.tenant_id = rpb.tenant_id AND p.id = rpb.policy_id
             WHERE rpb.tenant_id = $1 AND rpb.role_id = $2
            """,
            tenant_id, role_id,
        )
    if row is None:
        raise HTTPException(status_code=404, detail="No policy binding found for role")
    return {"role_id": role_id, "policy": _row_to_dict(row)}


# ---------------------------------------------------------------------------
# OPA bundle endpoint
# ---------------------------------------------------------------------------

@app.get("/api/v1/opa-bundle")
async def get_opa_bundle():
    bundle_file = os.path.join(BUNDLES_PATH, "combined_bundle.tar.gz")
    if not os.path.exists(bundle_file):
        bundle_file = await _build_empty_bundle()
    return FileResponse(bundle_file, media_type="application/gzip", filename="bundle.tar.gz")


# ---------------------------------------------------------------------------
# Data endpoints for OPAL Client
# ---------------------------------------------------------------------------

@app.get("/api/v1/data")
async def get_all_data() -> Dict:
    tenants = await _list_tenants()
    tenants_data: Dict[str, Any] = {}
    for tenant_id in tenants:
        tenants_data[tenant_id] = await _build_tenant_data(tenant_id)
    return {"tenants": tenants_data}


@app.get("/api/v1/data/{tenant_id}")
async def get_tenant_data(tenant_id: str) -> Dict:
    return {"tenants": {tenant_id: await _build_tenant_data(tenant_id)}}


# ---------------------------------------------------------------------------
# Notify endpoint
# ---------------------------------------------------------------------------

@app.post("/api/v1/notify")
async def notify_update(notify: NotifyRequest):
    await generate_and_notify(notify.tenant_id)
    return {"status": "notified"}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row) -> Dict:
    d = dict(row)
    if d.get("conditions") and isinstance(d["conditions"], str):
        d["conditions"] = json.loads(d["conditions"])
    if d.get("rules") is not None and isinstance(d["rules"], str):
        d["rules"] = json.loads(d["rules"])
    if d.get("created_at"):
        d["created_at"] = d["created_at"].isoformat()
    if d.get("updated_at"):
        d["updated_at"] = d["updated_at"].isoformat()
    return d


async def _build_tenant_data(tenant_id: str) -> Dict:
    """
    构建 OPA 数据文档：
      policies:      { policy_id: {rules: [{resource, effect}, ...]} }
      role_bindings: { role_uuid: policy_id }  ← 1:1，string
    """
    async with db_pool.acquire() as conn:
        policy_rows = await conn.fetch(
            "SELECT id, rules FROM policies WHERE tenant_id=$1",
            tenant_id,
        )
        binding_rows = await conn.fetch(
            "SELECT role_id, policy_id FROM role_policy_bindings WHERE tenant_id=$1",
            tenant_id,
        )

    policies: Dict[str, Any] = {}
    for row in policy_rows:
        rules = row["rules"]
        if isinstance(rules, str):
            rules = json.loads(rules)
        policies[row["id"]] = {"rules": rules}

    role_bindings: Dict[str, str] = {
        row["role_id"]: row["policy_id"]
        for row in binding_rows
    }

    return {"policies": policies, "role_bindings": role_bindings}


async def generate_and_notify(tenant_id: str):
    try:
        await _rebuild_combined_bundle()
        tenant_data = await _build_tenant_data(tenant_id)
        await _push_to_opa(tenant_id, tenant_data)
        await _push_data_update_to_opal(tenant_id)
        logger.info("OPA updated for tenant %s", tenant_id)
    except Exception as e:
        logger.error("Failed to update OPA for tenant %s: %s", tenant_id, e)


async def _rebuild_combined_bundle():
    tenants = await _list_tenants()
    combined_data: Dict[str, Any] = {"tenants": {}}

    for tenant_id in tenants:
        combined_data["tenants"][tenant_id] = await _build_tenant_data(tenant_id)

    combined_bundle = os.path.join(BUNDLES_PATH, "combined_bundle.tar.gz")
    rego = _generate_combined_rego()

    with tarfile.open(combined_bundle, "w:gz") as tar:
        manifest = json.dumps({"revision": datetime.utcnow().isoformat(), "roots": ["authz"]}).encode()
        _tar_add(tar, ".manifest", manifest)

        data_bytes = json.dumps(combined_data).encode()
        _tar_add(tar, "data.json", data_bytes)

        rego_bytes = rego.encode()
        _tar_add(tar, "authz/policy.rego", rego_bytes)


def _tar_add(tar: tarfile.TarFile, name: str, data: bytes):
    info = tarfile.TarInfo(name=name)
    info.size = len(data)
    tar.addfile(info, io.BytesIO(data))


async def _build_empty_bundle() -> str:
    bundle_file = os.path.join(BUNDLES_PATH, "combined_bundle.tar.gz")
    with tarfile.open(bundle_file, "w:gz") as tar:
        manifest = json.dumps({"revision": "empty", "roots": ["authz"]}).encode()
        _tar_add(tar, ".manifest", manifest)
    return bundle_file


def _generate_combined_rego() -> str:
    """
    单一租户无关的 Rego 策略。

    角色判断分两层：
      - super-admin / tenant-admin：基于 JWT roles[*].name（角色名字符串列表）
        super-admin 额外支持通过 master realm 判断（iss 中 /realms/master）
      - 普通用户：基于 JWT roles[*].id（UUID 列表）→ role_bindings → policy.rules 匹配
        policy.rules 是 [{resource, effect}, ...] 数组，每条规则独立配置 allow/deny
    """
    return r"""package authz

import future.keywords

default allow = false

# ---------------------------------------------------------------------------
# JWT 解码与验证
# ---------------------------------------------------------------------------

_raw_claims := claims {
    [_, claims, _] := io.jwt.decode(input.token)
}

_jwks_url := url {
    iss := _raw_claims.iss
    iss != ""
    url := concat("", [iss, "/protocol/certs"])
}

_verified_claims := claims {
    [valid, _, claims] := io.jwt.decode_verify(input.token, {"jwks_url": _jwks_url})
    valid
}

_claims := _verified_claims
_claims := _raw_claims { not _verified_claims }

# ---------------------------------------------------------------------------
# 从 JWT 提取身份信息
# ---------------------------------------------------------------------------

# 系统角色名列表：从 roles[*].name 提取（roles 为 [{id, name},...] 结构）
_sys_roles := [r.name | r := _claims.roles[_]] { _claims.roles }
_sys_roles := []                                { _claims; not _claims.roles }
_sys_roles := input.roles                       { not _claims }

# 业务角色 UUID 列表：从 roles[*].id 提取，用于 Tier-3 policy 查询
_role_ids := [r.id | r := _claims.roles[_]] { _claims.roles }
_role_ids := []                             { _claims; not _claims.roles }
_role_ids := input.role_ids                 { not _claims }

# 租户 ID（从 iss /realms/ 路径提取，回退到 tenant_id claim）
_user_tenant := t {
    iss   := _claims.iss
    contains(iss, "/realms/")
    parts := split(iss, "/realms/")
    segs  := split(parts[1], "/")
    t     := segs[0]
    t != ""
} else := _claims.tenant_id

# ---------------------------------------------------------------------------
# Super-admin 判断（满足任一条件即可）
# ---------------------------------------------------------------------------

# 条件1：JWT realm_access.roles 包含 "super-admin" 字符串
_is_super_admin { "super-admin" in _sys_roles }
# 条件2：token 由 Keycloak master realm 签发（iss 中 realm 为 master）
_is_super_admin { _user_tenant == "master" }

# ---------------------------------------------------------------------------
# 三层授权规则
# ---------------------------------------------------------------------------

# 1. super-admin：无限制跨租户访问
allow { _is_super_admin }

# 2. tenant-admin：本租户内完全访问
allow {
    _user_tenant != ""
    "tenant-admin" in _sys_roles
    input.tenant_id == _user_tenant
}

# 3. 普通用户：UUID role → role_bindings → policy.rules 匹配
#    _rule_matches 匹配逻辑：
#      - input.path 非空：用 rule.resource 做路径前缀匹配
#          精确匹配：input.path == rule.resource
#          子路径匹配：input.path 以 rule.resource + "/" 开头
#          （加 "/" 避免 /documents 误匹配 /documents-private）
#      - input.path 为空：回退到 rule.resource == input.resource 精确匹配
allow {
    _user_tenant != ""
    input.tenant_id == _user_tenant
    role_id   := _role_ids[_]
    policy_id := data.tenants[_user_tenant].role_bindings[role_id]
    policy    := data.tenants[_user_tenant].policies[policy_id]
    rule      := policy.rules[_]
    rule.effect == "allow"
    _rule_matches(rule)
}

# input.path 有值：rule.resource 精确匹配路径
_rule_matches(rule) {
    input.path != ""
    input.path == rule.resource
}

# input.path 有值：rule.resource 作为路径前缀匹配子路径
_rule_matches(rule) {
    input.path != ""
    startswith(input.path, concat("", [rule.resource, "/"]))
}

# input.path 为空（gRPC 未传路径）：回退到 resource 精确匹配
_rule_matches(rule) {
    not input.path
    rule.resource == input.resource
}

_rule_matches(rule) {
    input.path == ""
    rule.resource == input.resource
}
"""


async def _push_base_rego_to_opa():
    """启动时无条件将 Rego 策略推送到 OPA，确保冷启动时 tenant-admin 可正常鉴权。"""
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.put(
                f"{OPA_URL}/v1/policies/authz_main",
                content=_generate_combined_rego().encode(),
                headers={"Content-Type": "text/plain"},
            )
            if resp.status_code not in (200, 204):
                logger.error("OPA rejected base Rego on startup: %s – %s", resp.status_code, resp.text)
            else:
                logger.info("Base Rego policy pushed to OPA on startup")
    except Exception as e:
        logger.error("Failed to push base Rego to OPA on startup: %s", e)


async def _push_to_opa(tenant_id: str, tenant_data: Dict):
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            rego = _generate_combined_rego()
            resp = await client.put(
                f"{OPA_URL}/v1/policies/authz_main",
                content=rego.encode(),
                headers={"Content-Type": "text/plain"},
            )
            if resp.status_code not in (200, 204):
                logger.error("OPA rejected policy: %s – %s", resp.status_code, resp.text)

            data_resp = await client.put(
                f"{OPA_URL}/v1/data/tenants/{tenant_id}",
                json=tenant_data,
            )
            if data_resp.status_code not in (200, 204):
                logger.error("OPA rejected data for %s: %s", tenant_id, data_resp.text)

        logger.info("Pushed Rego + data to OPA for tenant %s", tenant_id)
    except Exception as e:
        logger.error("Failed to push to OPA for tenant %s: %s", tenant_id, e)


async def _push_data_update_to_opal(tenant_id: str):
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{OPAL_SERVER_URL}/v1/data/config",
                headers={"Authorization": f"Bearer {OPAL_SERVER_TOKEN}"},
                json={
                    "entries": [{
                        "url": f"{BUNDLE_SERVER_SELF_URL}/api/v1/data/{tenant_id}",
                        "topics": ["policy_data"],
                        "dst_path": f"/tenants/{tenant_id}",
                    }],
                    "reason": f"Policy updated for tenant {tenant_id}",
                },
            )
    except Exception as e:
        logger.error("Failed to notify OPAL Server: %s", e)
