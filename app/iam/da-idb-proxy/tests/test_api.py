"""
Unit tests for all API v1 route handlers using FastAPI TestClient.
Each endpoint is tested with mocked KeycloakClient and OPAClient.
"""
import pytest
from unittest.mock import MagicMock, patch, ANY

from tests.conftest import MockResponse


def _ok(json_data=None, status=200, headers=None):
    """Shortcut: build a successful MockResponse."""
    return MockResponse(status_code=status, json_data=json_data or {}, headers=headers or {})


# ============================================================================
# Common / Health
# ============================================================================

class TestCommonEndpoints:
    def test_health_check(self, client):
        resp = client.get("/api/v1/common/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "healthy"
        assert "timestamp" in data

    def test_master_realm_blocked_on_delete(self, client):
        """DELETE on 'master' realm should return 403 (path-based dep check)."""
        resp = client.delete("/api/v1/tenants/master")
        assert resp.status_code == 403

    def test_master_realm_blocked_on_identity(self, client):
        resp = client.get("/api/v1/master/roles")
        assert resp.status_code == 403


# ============================================================================
# Tenants
# ============================================================================

class TestTenantEndpoints:
    def test_create_tenant_success(self, client, mock_kc):
        """POST /tenants should orchestrate realm creation and return 201."""

        def side_effect(method, path, **kwargs):
            resp = _ok({})
            # 1. POST /realms          -> 201
            # 2. POST /clients         -> 201 + Location
            if "clients" in path and "roles" not in path and "protocol-mappers" not in path and "role-mappings" not in path:
                if method == "POST":
                    return MockResponse(201, {}, {"Location": "http://x/admin/realms/testr/clients/c-uuid"})
                if method == "GET":
                    return _ok([{"id": "mgmt-uuid"}])
            # 3. POST protocol-mappers  -> 204
            if "protocol-mappers" in path:
                return MockResponse(204, {})
            # 4. GET roles             -> list of realm-management roles
            if path.endswith("/roles") and method == "GET":
                return _ok([
                    {"name": "manage-realm"}, {"name": "manage-users"},
                    {"name": "view-users"}, {"name": "query-users"},
                    {"name": "manage-identity-providers"},
                ])
            # 5. POST /roles           -> 201
            # 6. POST composites       -> 204
            if "composites" in path:
                return MockResponse(204, {})
            # 7,8. user management
            if "users" in path and method == "GET":
                return _ok([{"id": "user-uuid", "username": "tenant-admin"}])
            if "reset-password" in path:
                return MockResponse(204, {})
            # 9. auth flows
            if "executions" in path and method == "GET":
                return _ok([{"providerId": "idp-review-profile", "requirement": "REQUIRED"}])
            if "executions" in path and method == "PUT":
                return MockResponse(204, {})
            return resp

        mock_kc.request.side_effect = side_effect

        resp = client.post("/api/v1/tenants", json={
            "realm": "test-realm", "displayName": "Test Realm"
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["realm"] == "test-realm"
        assert data["admin_role"] == "tenant-admin"
        assert data["admin_user"] == "tenant-admin"

        # Reset to avoid leaking to other tests
        mock_kc.request.side_effect = None

    def test_list_tenants_filters_master(self, client, mock_kc):
        """GET /tenants should exclude the protected master realm."""
        mock_kc.request.return_value = _ok([
            {"id": "1", "realm": "master", "displayName": "Master", "enabled": True},
            {"id": "2", "realm": "tenant-a", "displayName": "Tenant A", "enabled": True},
        ])

        resp = client.get("/api/v1/tenants")
        assert resp.status_code == 200
        data = resp.json()
        realms = [r["realm"] for r in data]
        assert "master" not in realms
        assert "tenant-a" in realms

    def test_delete_tenant(self, client, mock_kc):
        """DELETE /tenants/{realm} should proxy to KC and return 204."""
        mock_kc.request.return_value = MockResponse(204, {})
        resp = client.delete("/api/v1/tenants/test-realm")
        assert resp.status_code == 204

    def test_delete_master_tenant_blocked(self, client):
        resp = client.delete("/api/v1/tenants/master")
        assert resp.status_code == 403


# ============================================================================
# IDP (Identity Provider)
# ============================================================================

class TestIDPEndpoints:
    def test_create_idp_instance_success(self, client, mock_kc):
        """POST /{realm}/idp/saml/instances creates an IDP instance."""
        call_count = {"n": 0}

        def side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if "instances" in path and method == "GET":
                if n == 1:
                    return _ok([])  # existing check: empty
                # Response must pass IDPInstanceResponse validation
                return _ok({
                    "alias": "test-saml-idp", "providerId": "saml",
                    "enabled": True, "config": {},
                })
            if "instances" in path and method == "POST":
                return MockResponse(201, {})
            return _ok({})

        mock_kc.request.side_effect = side_effect

        resp = client.post("/api/v1/test-realm/idp/saml/instances", json={
            "config": {"singleSignOnServiceUrl": "https://sso.example.com"},
        })
        assert resp.status_code == 201
        mock_kc.request.side_effect = None

    def test_create_idp_duplicate_rejected(self, client, mock_kc):
        """Creating a second IDP instance should return 400."""
        mock_kc.request.return_value = _ok([{"alias": "existing"}])

        resp = client.post("/api/v1/test-realm/idp/saml/instances", json={
            "config": {"singleSignOnServiceUrl": "https://sso.example.com"},
        })
        assert resp.status_code == 400
        assert "already has" in resp.json()["detail"]

    def test_create_idp_missing_required_config(self, client, mock_kc):
        """Missing singleSignOnServiceUrl should return 400."""
        def side_effect(method, path, **kwargs):
            return _ok([])  # empty instances list (uniqueness passes)

        mock_kc.request.side_effect = side_effect

        resp = client.post("/api/v1/test-realm/idp/saml/instances", json={
            "config": {},  # missing singleSignOnServiceUrl
        })
        assert resp.status_code == 400
        mock_kc.request.side_effect = None

    def test_update_idp_instance(self, client, mock_kc):
        """PUT /{realm}/idp/saml/instances updates an existing IDP."""
        call_count = {"n": 0}

        def side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                # GET existing: must include config dict for update logic
                return _ok({
                    "alias": "test-saml-idp", "providerId": "saml",
                    "enabled": True, "trustEmail": False,
                    "config": {"singleSignOnServiceUrl": "https://old.example.com"},
                })
            if n == 2:
                # PUT
                return MockResponse(204, {})
            # GET after update
            return _ok({
                "alias": "test-saml-idp", "providerId": "saml",
                "enabled": False, "trustEmail": False,
                "config": {"singleSignOnServiceUrl": "https://new.example.com"},
            })

        mock_kc.request.side_effect = side_effect

        resp = client.put("/api/v1/test-realm/idp/saml/instances", json={
            "enabled": False,
            "config": {"singleSignOnServiceUrl": "https://new.example.com"},
        })
        assert resp.status_code == 200
        mock_kc.request.side_effect = None

    def test_list_idp_instances(self, client, mock_kc):
        mock_kc.request.return_value = _ok([
            {"alias": "idp1", "providerId": "saml", "enabled": True, "config": {}},
        ])
        resp = client.get("/api/v1/test-realm/idp/saml/instances")
        assert resp.status_code == 200
        assert len(resp.json()) == 1

    def test_delete_idp_instance(self, client, mock_kc):
        mock_kc.request.return_value = MockResponse(204, {})
        resp = client.delete("/api/v1/test-realm/idp/saml/instances/my-alias")
        assert resp.status_code == 204

    def test_create_idp_mapper(self, client, mock_kc):
        mock_kc.request.return_value = MockResponse(
            201, {},
            {"Location": "http://kc/admin/.../mappers/mapper-uuid"},
        )
        resp = client.post("/api/v1/test-realm/idp/saml/instances/my-alias/mappers", json={
            "name": "attr-mapper",
            "attributeKey": "email",
            "attributeValue": "email",
        })
        assert resp.status_code == 201
        data = resp.json()
        assert data["id"] == "mapper-uuid"

    def test_list_idp_mappers(self, client, mock_kc):
        mock_kc.request.return_value = _ok([
            {
                "id": "m1", "name": "mapper1",
                "config": {"user.attribute": "email", "attribute.name": "email"},
            },
        ])
        resp = client.get("/api/v1/test-realm/idp/saml/instances/my-alias/mappers")
        assert resp.status_code == 200
        assert resp.json()[0]["attributeKey"] == "email"

    def test_update_idp_mapper(self, client, mock_kc):
        call_count = {"n": 0}

        def side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                return _ok({"id": "m1", "name": "old", "config": {}})
            return MockResponse(204, {})

        mock_kc.request.side_effect = side_effect
        resp = client.put("/api/v1/test-realm/idp/saml/instances/my-alias/mappers/m1", json={
            "name": "new-name",
        })
        assert resp.status_code == 204
        mock_kc.request.side_effect = None

    def test_delete_idp_mapper(self, client, mock_kc):
        mock_kc.request.return_value = MockResponse(204, {})
        resp = client.delete("/api/v1/test-realm/idp/saml/instances/my-alias/mappers/m1")
        assert resp.status_code == 204


# ============================================================================
# Identity – Roles
# ============================================================================

class TestRoleEndpoints:
    def test_list_roles_filters_client_roles(self, client, mock_kc, mock_opa):
        """GET /{realm}/roles returns only realm roles, excluding built-in ones."""
        mock_kc.request.return_value = _ok([
            {"id": "r1", "name": "custom-role", "clientRole": False},
            {"id": "r2", "name": "default-roles-realm", "clientRole": False},
            {"id": "r3", "name": "client-role", "clientRole": True},
        ])
        # OPA unavailable for policy enrichment (caught silently in route)
        mock_opa.request.side_effect = Exception("OPA unavailable")

        resp = client.get("/api/v1/test-realm/roles")
        assert resp.status_code == 200
        data = resp.json()
        names = [r["name"] for r in data]
        assert "custom-role" in names
        assert "default-roles-realm" not in names
        assert "client-role" not in names
        mock_opa.request.side_effect = None

    def test_create_role_success(self, client, mock_kc, mock_opa):
        """POST /{realm}/roles creates a role (no policy)."""
        call_count = {"n": 0}

        def side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                # POST to create role -> 201
                return MockResponse(201, {})
            # GET to fetch created role -> must have 'id' key
            return _ok({"id": "role-uuid", "name": "new-role", "clientRole": False})

        mock_kc.request.side_effect = side_effect

        resp = client.post("/api/v1/test-realm/roles", json={
            "name": "new-role", "description": "A new role",
        })
        assert resp.status_code == 201
        assert resp.json()["name"] == "new-role"
        mock_kc.request.side_effect = None

    def test_create_role_with_policy_binding(self, client, mock_kc, mock_opa):
        """Role creation with policy_id triggers OPA bind."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                return MockResponse(201, {})
            return _ok({"id": "role-uuid", "name": "new-role", "clientRole": False})

        mock_kc.request.side_effect = kc_side_effect

        # OPA: successful bind (POST) + get (GET)
        opa_call = {"n": 0}
        def opa_side_effect(method, path, **kwargs):
            opa_call["n"] += 1
            if opa_call["n"] == 2:
                return _ok({"policy": {
                    "id": "pol-1", "name": "pol", "tenant_id": "test-realm",
                    "rules": [], "created_at": "", "updated_at": "",
                }})
            return _ok({})

        mock_opa.request.side_effect = opa_side_effect

        resp = client.post("/api/v1/test-realm/roles", json={
            "name": "new-role", "policy_id": "pol-1",
        })
        assert resp.status_code == 201
        mock_kc.request.side_effect = None
        mock_opa.request.side_effect = None

    def test_create_role_opa_bind_failure_rolls_back(self, client, mock_kc, mock_opa):
        """When OPA bind fails, the KC role should be rolled back (502)."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            if n == 1:
                return MockResponse(201, {})
            return _ok({"id": "role-uuid", "name": "new-role", "clientRole": False})

        mock_kc.request.side_effect = kc_side_effect
        mock_opa.request.side_effect = Exception("OPA down")

        resp = client.post("/api/v1/test-realm/roles", json={
            "name": "new-role", "policy_id": "pol-1",
        })
        assert resp.status_code == 502
        mock_kc.request.side_effect = None
        mock_opa.request.side_effect = None

    def test_get_role(self, client, mock_kc, mock_opa):
        """GET /{realm}/roles/{role_name} returns role details."""
        mock_kc.request.return_value = _ok({
            "id": "role-uuid", "name": "my-role", "clientRole": False,
        })
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.get("/api/v1/test-realm/roles/my-role")
        assert resp.status_code == 200
        assert resp.json()["name"] == "my-role"

    def test_update_role_success(self, client, mock_kc, mock_opa):
        """PUT /{realm}/roles/{role_name} updates a role."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            n = call_count["n"]
            base = {"id": "role-uuid", "name": "my-role", "clientRole": False, "description": "old"}
            if n >= 3:
                base = {"id": "role-uuid", "name": "my-role", "clientRole": False, "description": "updated"}
            return _ok(base)

        mock_kc.request.side_effect = kc_side_effect
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.put("/api/v1/test-realm/roles/my-role", json={"description": "updated"})
        assert resp.status_code == 200
        mock_kc.request.side_effect = None

    def test_delete_role(self, client, mock_kc, mock_opa):
        """DELETE /{realm}/roles/{role_name} removes the role."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _ok({"id": "role-uuid", "name": "my-role"})
            return MockResponse(204, {})

        mock_kc.request.side_effect = kc_side_effect
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.delete("/api/v1/test-realm/roles/my-role")
        assert resp.status_code == 204
        mock_kc.request.side_effect = None

    def test_get_role_by_id(self, client, mock_kc, mock_opa):
        """GET /{realm}/roles/by-id/{role_id} returns role by UUID."""
        mock_kc.request.return_value = _ok({
            "id": "role-uuid", "name": "my-role", "clientRole": False,
        })
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.get("/api/v1/test-realm/roles/by-id/role-uuid")
        assert resp.status_code == 200

    def test_update_role_by_id(self, client, mock_kc, mock_opa):
        """PUT /{realm}/roles/by-id/{role_id} updates role by UUID."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            return _ok({"id": "role-uuid", "name": "my-role", "clientRole": False})

        mock_kc.request.side_effect = kc_side_effect
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.put("/api/v1/test-realm/roles/by-id/role-uuid", json={"description": "updated"})
        assert resp.status_code == 200
        mock_kc.request.side_effect = None

    def test_delete_role_by_id(self, client, mock_kc, mock_opa):
        """DELETE /{realm}/roles/by-id/{role_id} removes role by UUID."""
        call_count = {"n": 0}

        def kc_side_effect(method, path, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _ok({"id": "role-uuid", "name": "my-role"})
            return MockResponse(204, {})

        mock_kc.request.side_effect = kc_side_effect
        mock_opa.request.side_effect = Exception("no OPA")

        resp = client.delete("/api/v1/test-realm/roles/by-id/role-uuid")
        assert resp.status_code == 204
        mock_kc.request.side_effect = None


# ============================================================================
# Identity – Groups
# ============================================================================

class TestGroupEndpoints:
    def test_list_groups(self, client, mock_kc):
        mock_kc.request.return_value = _ok([
            {"id": "g1", "name": "engineering", "path": "/engineering", "subGroups": []},
        ])
        resp = client.get("/api/v1/test-realm/groups")
        assert resp.status_code == 200
        assert resp.json()[0]["name"] == "engineering"

    def test_create_group(self, client, mock_kc):
        """POST /{realm}/groups creates a group."""
        def side_effect(method, path, **kwargs):
            if method == "POST" and path.endswith("/groups"):
                return MockResponse(201, {})
            # GET groups list - must include the new group for next() to find it
            if method == "GET" and path.endswith("/groups"):
                return _ok([{"id": "g1", "name": "new-group", "path": "/new-group", "subGroups": []}])
            return _ok({})

        mock_kc.request.side_effect = side_effect

        resp = client.post("/api/v1/test-realm/groups", json={"name": "new-group"})
        assert resp.status_code == 201
        assert resp.json()["name"] == "new-group"
        mock_kc.request.side_effect = None

    def test_update_group(self, client, mock_kc):
        call_count = {"n": 0}

        def side_effect(method, path, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                return _ok({"id": "g1", "name": "old-name"})
            return MockResponse(204, {})

        mock_kc.request.side_effect = side_effect
        resp = client.put("/api/v1/test-realm/groups/g1", json={"name": "new-name"})
        assert resp.status_code == 204
        mock_kc.request.side_effect = None

    def test_get_group_detail(self, client, mock_kc):
        def side_effect(method, path, **kwargs):
            if "members" in path:
                return _ok([{"id": "u1", "username": "alice"}])
            if "role-mappings" in path:
                return _ok({"realmMappings": [{"id": "r1", "name": "custom-role", "clientRole": False}]})
            return _ok({"id": "g1", "name": "eng"})

        mock_kc.request.side_effect = side_effect
        resp = client.get("/api/v1/test-realm/groups/g1")
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "eng"
        assert data["members"][0]["username"] == "alice"
        mock_kc.request.side_effect = None

    def test_delete_group(self, client, mock_kc):
        mock_kc.request.return_value = MockResponse(204, {})
        resp = client.delete("/api/v1/test-realm/groups/g1")
        assert resp.status_code == 204


# ============================================================================
# Identity – Users
# ============================================================================

class TestUserEndpoints:
    def test_list_users(self, client, mock_kc):
        mock_kc.request.return_value = _ok([
            {"id": "u1", "username": "alice"},
        ])
        resp = client.get("/api/v1/test-realm/users")
        assert resp.status_code == 200
        assert resp.json()[0]["username"] == "alice"

    def test_get_user_full_context(self, client, mock_kc):
        def side_effect(method, path, **kwargs):
            if "groups" in path:
                return _ok([{"id": "g1", "name": "eng"}])
            if "role-mappings" in path:
                return _ok({"realmMappings": [{"id": "r1", "name": "reader"}]})
            return _ok({})

        mock_kc.request.side_effect = side_effect
        resp = client.get("/api/v1/test-realm/users/u1/details")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["groups"]) == 1
        assert data["roles"][0]["name"] == "reader"
        mock_kc.request.side_effect = None


# ============================================================================
# Token
# ============================================================================

class TestTokenEndpoints:
    @patch("app.api.v1.token.requests.post")
    def test_exchange_code_for_token_success(self, mock_requests_post, client):
        """POST /{realm}/token/exchange should proxy to Keycloak token endpoint."""
        mock_requests_post.return_value = MockResponse(200, {
            "access_token": "at-123",
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "rt-456",
            "scope": "openid",
        })

        resp = client.post("/api/v1/test-realm/token/exchange", json={
            "code": "auth-code",
            "redirect_uri": "https://app/callback",
            "client_id": "my-client",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["access_token"] == "at-123"
        assert data["token_type"] == "Bearer"

    @patch("app.api.v1.token.requests.post")
    def test_exchange_token_failure(self, mock_requests_post, client):
        mock_resp = MockResponse(400, {}, text="invalid_grant")
        mock_resp.ok = False
        mock_requests_post.return_value = mock_resp

        resp = client.post("/api/v1/test-realm/token/exchange", json={
            "code": "bad-code",
            "redirect_uri": "https://app/callback",
            "client_id": "my-client",
        })
        assert resp.status_code == 400


# ============================================================================
# Export Spec
# ============================================================================

class TestExportSpec:
    def test_export_spec(self, client):
        resp = client.get("/api/v1/export-spec")
        assert resp.status_code == 200
        data = resp.json()
        assert "openapi" in data or "paths" in data


# ============================================================================
# Global error handler
# ============================================================================

class TestErrorHandling:
    def test_keycloak_error_returns_json_response(self, client, mock_kc):
        """When KeycloakError is raised, the global handler returns JSON."""
        from app.core.keycloak import KeycloakError
        mock_kc.request.side_effect = KeycloakError(500, "KC internal error")

        resp = client.get("/api/v1/test-realm/roles")
        assert resp.status_code == 500
        data = resp.json()
        assert "detail" in data
        mock_kc.request.side_effect = None
