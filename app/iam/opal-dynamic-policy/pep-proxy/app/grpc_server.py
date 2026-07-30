# app/grpc_server.py
#
# gRPC External Authorization server for agentgateway integration.
#
# Implements the Envoy ext-authz v3 Authorization/Check RPC on port 9000.
#
# Request flow:
#   agentgateway → gRPC Check(CheckRequest) → pep-proxy:9000
#       │
#       ├── reads dev.agentgateway.jwt gRPC metadata (pre-verified by agentgateway)
#       │      OR decodes raw Authorization: Bearer token (dev / fallback mode)
#       │
#       ├── extracts tenant_id from iss claim: {OIDC_BASE_URL}/tenants/{tenant_id}
#       ├── reads x-authz-resource HTTP header (set by routing rules)
#       │      OR derives resource from the last segment of the request path
#       │
#       ├── calls OPA: POST /v1/data/authz/allowA
#       │
#       └── returns CheckResponse:
#               ALLOW → code=0 + OkHttpResponse with x-auth-{user,tenant,roles} headers
#               DENY  → code=7 + DeniedHttpResponse with HTTP 403

import json
import logging
import os

import grpc
import httpx

# ext_authz_pb2 and ext_authz_pb2_grpc are generated during Docker build:
#   python -m grpc_tools.protoc -I/app/proto --python_out=/app --grpc_python_out=/app \
#          /app/proto/ext_authz.proto
from ext_authz_pb2 import (  # type: ignore[import]
    CheckResponse,
    DeniedHttpResponse,
    HeaderValue,
    HeaderValueOption,
    HttpStatus,
    OkHttpResponse,
    Status,
)
from ext_authz_pb2_grpc import (  # type: ignore[import]
    AuthorizationServicer,
    add_AuthorizationServicer_to_server,
)

logger = logging.getLogger(__name__)

OPA_URL = os.getenv("OPA_URL", "http://localhost:8181")
OIDC_BASE_URL = os.getenv("OIDC_BASE_URL", "")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_tenant_from_iss(iss: str) -> str:
    """
    Extract tenant_id from an issuer URL.

    Keycloak format: {base}/realms/{tenant_id}
    Example: https://auth.example.com/realms/tenant-001 → tenant-001
    Returns empty string when iss does not contain /realms/.
    """
    if "/realms/" in iss:
        tail = iss.split("/realms/", 1)[1]
        return tail.split("/")[0]
    return ""


def _ok(claims: dict, tenant_id: str, role_names: list, role_ids: list) -> CheckResponse:
    """Build an ALLOW CheckResponse with identity headers."""
    return CheckResponse(
        status=Status(code=0, message="OK"),
        ok_response=OkHttpResponse(
            headers=[
                HeaderValueOption(
                    header=HeaderValue(key="x-auth-user", value=claims.get("sub", ""))
                ),
                HeaderValueOption(
                    header=HeaderValue(key="x-auth-tenant", value=tenant_id)
                ),
                HeaderValueOption(
                    header=HeaderValue(key="x-auth-roles", value=",".join(role_names))
                ),
                HeaderValueOption(
                    header=HeaderValue(key="x-auth-role-ids", value=",".join(role_ids))
                ),
            ]
        ),
    )


def _denied(http_code: int, message: str) -> CheckResponse:
    """
    Build a DENY CheckResponse.

    gRPC code mapping:
      401 → 16 (UNAUTHENTICATED)
      403 → 7  (PERMISSION_DENIED)
      503 → 14 (UNAVAILABLE)
      other → 13 (INTERNAL)
    """
    grpc_code_map = {401: 16, 403: 7, 503: 14}
    grpc_code = grpc_code_map.get(http_code, 13)
    return CheckResponse(
        status=Status(code=grpc_code, message=message),
        denied_response=DeniedHttpResponse(
            status=HttpStatus(code=http_code),
            body=message,
        ),
    )


# ---------------------------------------------------------------------------
# AuthorizationServicer
# ---------------------------------------------------------------------------

class AuthorizationService(AuthorizationServicer):
    """
    Implements the Envoy ext-authz v3 Authorization.Check RPC.

    agentgateway verifies the JWT before calling ext-authz and injects the
    verified payload as JSON in the gRPC metadata key ``dev.agentgateway.jwt``.
    The raw Authorization header is included in the CheckRequest HTTP headers
    for use by OPA's own io.jwt.decode_verify when OIDC is configured.
    """

    async def Check(
        self,
        request,          # CheckRequest
        context,          # grpc.aio.ServicerContext
    ) -> CheckResponse:
        http = request.attributes.request.http
        headers: dict = dict(http.headers)

        # ── Step 1: obtain JWT claims ─────────────────────────────────────────
        claims: dict | None = None
        token: str = ""

        # Priority 1: pre-verified claims injected by agentgateway
        for key, value in context.invocation_metadata():
            if key == "dev.agentgateway.jwt":
                try:
                    claims = json.loads(value)
                    logger.debug("ext-authz gRPC: using pre-verified agentgateway claims")
                except Exception as e:
                    logger.warning("Failed to parse dev.agentgateway.jwt metadata: %s", e)
                break

        # Extract raw Bearer token (forwarded to OPA for its own verification)
        auth_header = headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]

        # Priority 2: decode the bearer token ourselves (dev mode / no agentgateway metadata)
        if claims is None and token:
            from .auth import _decode_unverified  # lazy import to avoid circular dep
            claims = _decode_unverified(token)
            if claims is None:
                logger.warning("ext-authz gRPC: unable to decode token")
                return _denied(401, "Unauthorized: malformed token")

        if not claims:
            logger.warning("ext-authz gRPC: no claims available, denying request")
            return _denied(401, "Unauthorized: missing token")

        # ── Step 2: derive tenant_id from iss ────────────────────────────────
        iss: str = claims.get("iss", "")
        tenant_id = _extract_tenant_from_iss(iss) or claims.get("tenant_id", "")

        # Extract role names and UUIDs from roles[{id, name}] structure
        roles_list: list = claims.get("roles", [])
        if roles_list and isinstance(roles_list[0], dict):
            role_names: list = [r.get("name", "") for r in roles_list if isinstance(r, dict)]
            role_ids:   list = [r.get("id",   "") for r in roles_list if isinstance(r, dict)]
        else:
            # fallback: plain string list (backward compat)
            role_names = roles_list
            role_ids   = []

        # super-admin has no tenant restriction; let OPA decide.
        # Only reject when tenant is absent AND the user is NOT super-admin.
        if not tenant_id and "super-admin" not in role_names:
            logger.warning("ext-authz gRPC: cannot determine tenant_id from claims")
            return _denied(401, "Unauthorized: missing tenant_id")

        # ── Step 3: resolve resource / path / method ─────────────────────────
        # 按优先级依次尝试多个 path 来源：
        #   1. gRPC CheckRequest 的 http.path（agentgateway 标准填充位置）
        #   2. HTTP/2 :path 伪头（部分实现通过此传递）
        #   3. x-forwarded-path / x-original-path（自定义转发头）
        raw_path: str = (
            http.path
            or headers.get(":path", "")
            or headers.get("x-forwarded-path", "")
            or headers.get("x-original-path", "")
            or "/"
        )
        # 去除 query string
        request_path = raw_path.split("?")[0] if raw_path else "/"
        method: str = http.method or headers.get(":method", "")
        resource: str = headers.get("x-authz-resource", "")

        if not resource:
            # Derive from the last non-empty path segment
            segments = [s for s in request_path.strip("/").split("/") if s]
            resource = segments[-1] if segments else "unknown"

        logger.info(
            "ext-authz gRPC: user=%s tenant=%s resource=%s path=%s (raw http.path=%r)",
            claims.get("sub"), tenant_id, resource, request_path, http.path,
        )

        # ── Step 4: query OPA ─────────────────────────────────────────────────
        opa_input = {
            "input": {
                "token":     token,
                "user":      claims.get("sub", ""),
                "roles":     role_names,
                "role_ids":  role_ids,
                "tenant_id": tenant_id,
                "resource":  resource,
                "path":      request_path,
                "method":    method,
                "context":   {},
            }
        }

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{OPA_URL}/v1/data/authz/allow",
                    json=opa_input,
                )
                if resp.status_code != 200:
                    logger.error("OPA returned %s: %s", resp.status_code, resp.text)
                    return _denied(503, "Authorization service error")
                allowed: bool = resp.json().get("result", False)

        except httpx.RequestError as e:
            logger.error("OPA connection error in ext-authz gRPC: %s", e)
            return _denied(503, "Authorization service unavailable")
        except Exception as e:
            logger.error("Unexpected error in ext-authz gRPC: %s", e)
            return _denied(500, "Internal error")

        if not allowed:
            logger.info(
                "ext-authz gRPC: DENIED user=%s tenant=%s resource=%s",
                claims.get("sub"), tenant_id, resource,
            )
            return _denied(403, "Forbidden by policy")

        logger.info(
            "ext-authz gRPC: ALLOWED user=%s tenant=%s resource=%s",
            claims.get("sub"), tenant_id, resource,
        )
        return _ok(claims, tenant_id, role_names, role_ids)


# ---------------------------------------------------------------------------
# Server lifecycle
# ---------------------------------------------------------------------------

async def serve() -> None:
    """
    Start the gRPC ext-authz server on port 9000.

    Call this once from the FastAPI startup event:
        asyncio.create_task(grpc_server.serve())
    """
    try:
        server = grpc.aio.server()
        add_AuthorizationServicer_to_server(AuthorizationService(), server)
        listen_addr = "[::]:9000"
        server.add_insecure_port(listen_addr)
        await server.start()
        logger.info("gRPC ext-authz server listening on %s", listen_addr)
        await server.wait_for_termination()
    except Exception as exc:
        logger.error("gRPC ext-authz server failed: %s", exc, exc_info=True)
        raise
