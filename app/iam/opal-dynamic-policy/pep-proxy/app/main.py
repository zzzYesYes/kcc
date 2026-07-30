# app/main.py
from fastapi import FastAPI, HTTPException, Depends, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List, Optional
import asyncio
import httpx
import logging
import os
from datetime import datetime

_grpc_task: "asyncio.Task | None" = None

from .models import (
    PolicyCreateRequest, PolicyRule, RoleBindingRequest,
    AuthRequest, AuthResponse,
    PolicyTemplate,
)
from .auth import verify_token
from .storage import PolicyStorage
from . import grpc_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="PEP Proxy Service",
    description="Policy Enforcement Point with Dynamic Policy Management",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

OPA_URL         = os.getenv("OPA_URL", "http://localhost:8181")
BUNDLE_SERVER_URL = os.getenv("BUNDLE_SERVER_URL", "http://localhost:8001")

# storage 仅用于模板读取（in-memory after startup）
storage = PolicyStorage()

PREDEFINED_TEMPLATES = {
    "role_based": """
        package authz.templates.role_based

        default allow = false

        allow {
            input.resource == "{{resource}}"
            input.action == "{{action}}"
            input.tenant_id == "{{tenant_id}}"
            contains(input.roles[_], "{{role}}")
        }
    """,
}


@app.on_event("startup")
async def startup_event():
    global _grpc_task
    for name, content in PREDEFINED_TEMPLATES.items():
        await storage.save_template(name, content)
    logger.info("Loaded %d predefined templates", len(PREDEFINED_TEMPLATES))
    _grpc_task = asyncio.create_task(grpc_server.serve())
    _grpc_task.add_done_callback(_on_grpc_task_done)


def _on_grpc_task_done(task: "asyncio.Task") -> None:
    if task.cancelled():
        logger.warning("gRPC ext-authz task was cancelled")
    elif task.exception():
        logger.error("gRPC ext-authz task failed: %s", task.exception(), exc_info=task.exception())
    else:
        logger.info("gRPC ext-authz task finished cleanly")


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "service": "pep-proxy",
        "timestamp": datetime.utcnow().isoformat(),
    }


# ---------------------------------------------------------------------------
# Auth check（委托 OPA）
# ---------------------------------------------------------------------------

@app.post("/api/v1/auth/check", response_model=AuthResponse)
async def check_permission(
    request: AuthRequest,
    user_info: Dict = Depends(verify_token),
):
    opa_input = {
        "input": {
            "token":    user_info["token"],
            "user":     user_info["user_id"],
            "roles":    user_info["roles"],
            "role_ids": user_info["role_ids"],
            "tenant_id": request.tenant_id,
            "resource":  request.resource,
            "path":      request.path or "",
            "method":    request.method or "",
            "context":   request.context or {},
        }
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(f"{OPA_URL}/v1/data/authz/allow", json=opa_input)
            if response.status_code != 200:
                raise HTTPException(status_code=500, detail="Authorization service error")
            allowed = response.json().get("result", False)

        return AuthResponse(
            allowed=allowed,
            user=user_info["user_id"],
            tenant_id=request.tenant_id,
            resource=request.resource,
            reason="Allowed by policy" if allowed else "Denied by policy",
        )
    except httpx.RequestError as e:
        logger.error("OPA connection error: %s", e)
        raise HTTPException(status_code=503, detail="Authorization service unavailable")
    except HTTPException:
        raise
    except Exception as e:
        logger.error("Permission check error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# Policy CRUD（无 role，委托 bundle-server 持久化到 PostgreSQL）
# ---------------------------------------------------------------------------

@app.get("/api/v1/policies")
async def list_policies(user_info: Dict = Depends(verify_token)):
    """列出本租户所有 policy（任意有效 token 可调用）。"""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(
            f"{BUNDLE_SERVER_URL}/api/v1/tenants/{user_info['tenant_id']}/policies"
        )
        resp.raise_for_status()
    policies = resp.json()
    return {"policies": policies, "count": len(policies), "tenant_id": user_info["tenant_id"]}


# 固定路径（templates）必须在参数化路径（{policy_id}）之前注册，否则 FastAPI 会优先匹配参数化路由
@app.get("/api/v1/policies/templates")
async def list_templates(user_info: Dict = Depends(verify_token)):
    templates = await storage.list_templates()
    return {"templates": templates, "count": len(templates)}


@app.post("/api/v1/policies/template/{template_name}")
async def render_template(
    template_name: str,
    parameters: Dict[str, str],
    user_info: Dict = Depends(verify_token),
):
    _require_admin(user_info)
    template_content = await storage.get_template(template_name)
    if not template_content:
        raise HTTPException(status_code=404, detail=f"Template {template_name} not found")
    try:
        for key, value in parameters.items():
            template_content = template_content.replace(f"{{{{{{{key}}}}}}}", value)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Template rendering error: {e}")
    return {"status": "success", "template": template_name, "rendered_policy": template_content}


@app.get("/api/v1/policies/{policy_id}")
async def get_policy(
    policy_id: str,
    user_info: Dict = Depends(verify_token),
):
    """查看单条 policy（任意有效 token 可调用）。"""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(
            f"{BUNDLE_SERVER_URL}/api/v1/tenants/{user_info['tenant_id']}/policies/{policy_id}"
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Policy not found")
        resp.raise_for_status()
    return resp.json()


@app.post("/api/v1/policies")
async def create_policy(
    policy: PolicyCreateRequest,
    user_info: Dict = Depends(verify_token),
):
    """创建 policy（仅 tenant_admin）。"""
    _require_admin(user_info)
    _require_same_tenant(policy.tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(
            f"{BUNDLE_SERVER_URL}/api/v1/policies",
            json={
                "name":       policy.name,
                "rules":      [r.model_dump() for r in policy.rules],
                "tenant_id":  policy.tenant_id,
                "conditions": policy.conditions,
            },
        )
        resp.raise_for_status()
    return resp.json()


@app.put("/api/v1/policies/{policy_id}")
async def update_policy(
    policy_id: str,
    policy: PolicyCreateRequest,
    user_info: Dict = Depends(verify_token),
):
    """更新 policy（仅 tenant_admin）。"""
    _require_admin(user_info)
    _require_same_tenant(policy.tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.put(
            f"{BUNDLE_SERVER_URL}/api/v1/policies/{policy_id}",
            json={
                "name":       policy.name,
                "rules":      [r.model_dump() for r in policy.rules],
                "tenant_id":  policy.tenant_id,
                "conditions": policy.conditions,
            },
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Policy not found")
        resp.raise_for_status()
    return resp.json()


@app.delete("/api/v1/policies/{policy_id}")
async def delete_policy(
    policy_id: str,
    user_info: Dict = Depends(verify_token),
    tenant_id: Optional[str] = None,
):
    """删除 policy（仅 tenant_admin）。
    tenant_id 可选：不传时自动使用 token 中的租户；
    super-admin 跨租户删除时需显式传入目标 tenant_id。
    """
    _require_admin(user_info)
    tenant_id = tenant_id or user_info["tenant_id"]
    _require_same_tenant(tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.delete(
            f"{BUNDLE_SERVER_URL}/api/v1/policies/{policy_id}",
            params={"tenant_id": tenant_id},
        )
        resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Role-Policy Binding（role UUID ↔ policy，1:1 upsert，委托 bundle-server）
# ---------------------------------------------------------------------------

@app.post("/api/v1/roles/{role_id}/policy")
async def bind_policy_to_role(
    role_id: str,
    body: RoleBindingRequest,
    user_info: Dict = Depends(verify_token),
):
    """绑定（或替换）role UUID 与 policy 的 1:1 绑定（仅 tenant_admin）。"""
    _require_admin(user_info)
    _require_same_tenant(body.tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.post(
            f"{BUNDLE_SERVER_URL}/api/v1/roles/{role_id}/policy",
            json={"policy_id": body.policy_id, "tenant_id": body.tenant_id},
        )
        resp.raise_for_status()
    return resp.json()


@app.put("/api/v1/roles/{role_id}/policy")
async def update_role_policy(
    role_id: str,
    body: RoleBindingRequest,
    user_info: Dict = Depends(verify_token),
):
    """更新已有绑定的策略（角色必须已有绑定，否则 404）（仅 tenant_admin）。"""
    _require_admin(user_info)
    _require_same_tenant(body.tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.put(
            f"{BUNDLE_SERVER_URL}/api/v1/roles/{role_id}/policy",
            json={"policy_id": body.policy_id, "tenant_id": body.tenant_id},
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="No policy binding found for role")
        resp.raise_for_status()
    return resp.json()


@app.delete("/api/v1/roles/{role_id}/policy")
async def delete_role_policy(
    role_id: str,
    user_info: Dict = Depends(verify_token),
    tenant_id: Optional[str] = None,
):
    """删除 role UUID 的 policy 绑定（仅 tenant_admin）。
    tenant_id 可选：不传时自动使用 token 中的租户；
    super-admin 跨租户操作时需显式传入目标 tenant_id。
    """
    _require_admin(user_info)
    tenant_id = tenant_id or user_info["tenant_id"]
    _require_same_tenant(tenant_id, user_info)

    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.delete(
            f"{BUNDLE_SERVER_URL}/api/v1/roles/{role_id}/policy",
            params={"tenant_id": tenant_id},
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="No policy binding found for role")
        resp.raise_for_status()
    return resp.json()


@app.get("/api/v1/roles/{role_id}/policy")
async def get_role_policy(
    role_id: str,
    user_info: Dict = Depends(verify_token),
):
    """查询 role UUID 绑定的 policy（任意有效 token 可调用）。"""
    async with httpx.AsyncClient(timeout=5.0) as client:
        resp = await client.get(
            f"{BUNDLE_SERVER_URL}/api/v1/roles/{role_id}/policy",
            params={"tenant_id": user_info["tenant_id"]},
        )
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="No policy binding found for role")
        resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# External authz (agentgateway)
# ---------------------------------------------------------------------------

@app.post("/api/v1/ext-authz")
async def ext_authz_check(
    request: Request,
    user_info: Dict = Depends(verify_token),
):
    headers   = request.headers
    tenant_id = user_info["tenant_id"]
    original_path = headers.get("x-original-path", str(request.url.path))
    method    = headers.get("x-original-method", request.method)
    resource  = headers.get("x-authz-resource", "")

    if not resource:
        segments = [s for s in original_path.strip("/").split("/") if s]
        resource = segments[-1] if segments else "unknown"

    opa_input = {
        "input": {
            "token":     user_info["token"],
            "user":      user_info["user_id"],
            "roles":     user_info["roles"],
            "role_ids":  user_info["role_ids"],
            "tenant_id": tenant_id,
            "resource":  resource,
            "path":      original_path,
            "method":    method,
            "context":   {},
        }
    }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            resp = await client.post(f"{OPA_URL}/v1/data/authz/allow", json=opa_input)
            if resp.status_code != 200:
                raise HTTPException(status_code=500, detail="OPA query failed")
            allowed = resp.json().get("result", False)

        if not allowed:
            raise HTTPException(status_code=403, detail="Forbidden by policy")

        return Response(
            status_code=200,
            headers={
                "x-auth-user":    user_info["user_id"],
                "x-auth-tenant":  tenant_id,
                "x-auth-roles":   ",".join(user_info["roles"]),
                "x-auth-role-ids": ",".join(user_info["role_ids"]),
            },
        )
    except HTTPException:
        raise
    except httpx.RequestError as e:
        logger.error("OPA connection error in ext-authz: %s", e)
        raise HTTPException(status_code=503, detail="Authorization service unavailable")
    except Exception as e:
        logger.error("ext-authz error: %s", e)
        raise HTTPException(status_code=500, detail="Internal server error")


# ---------------------------------------------------------------------------
# Guard helpers
# ---------------------------------------------------------------------------

def _require_admin(user_info: Dict):
    if "tenant-admin" not in user_info["roles"] and "super-admin" not in user_info["roles"]:
        raise HTTPException(status_code=403, detail="Tenant admin access required")


def _require_same_tenant(requested_tenant: str, user_info: Dict):
    if "super-admin" in user_info["roles"]:
        return
    if requested_tenant != user_info["tenant_id"]:
        raise HTTPException(status_code=403, detail="Cannot operate on other tenant")
