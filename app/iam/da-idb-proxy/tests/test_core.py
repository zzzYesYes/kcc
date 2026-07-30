"""
Unit tests for app.core.keycloak (KeycloakClient) and
app.core.opa_client (OPAClient).
"""
import os
import time
import pytest
from unittest.mock import MagicMock, patch, PropertyMock

from app.core.keycloak import KeycloakClient, KeycloakError, kc
from app.core.opa_client import OPAClient, OPAError, opa


# ============================================================================
# KeycloakClient
# ============================================================================

class TestKeycloakClient:
    """Tests for KeycloakClient – token acquisition and request proxying."""

    def test_initialization_defaults(self, monkeypatch):
        """Should read base_url from env or use default."""
        monkeypatch.delenv("KEYCLOAK_URL", raising=False)
        client = KeycloakClient()
        assert client.base_url == "http://localhost:8080"

    def test_initialization_from_env(self):
        client = KeycloakClient()
        # env is set by conftest autouse fixture
        assert client.base_url == "http://mock-keycloak:8080"

    def test_get_token_success(self):
        """_get_token should POST to token endpoint and return access_token."""
        client = KeycloakClient()
        mock_post_resp = MagicMock()
        mock_post_resp.ok = True
        mock_post_resp.json.return_value = {
            "access_token": "fake-jwt-token",
            "expires_in": 300,
        }
        with patch.object(client.session, "post", return_value=mock_post_resp) as mock_post:
            token = client._get_token()

        assert token == "fake-jwt-token"
        assert client._token == "fake-jwt-token"
        mock_post.assert_called_once()

    def test_get_token_failure_raises_http_exception(self):
        """When Keycloak returns non-2xx on token endpoint, raise HTTPException."""
        client = KeycloakClient()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 401
        mock_resp.text = "Unauthorized"

        with patch.object(client.session, "post", return_value=mock_resp):
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                client._get_token()
            assert exc_info.value.status_code == 401

    def test_request_injects_auth_header(self):
        """request() should fetch a token and attach Authorization header."""
        client = KeycloakClient()
        # Mock _get_token to avoid real HTTP
        mock_get_token = MagicMock(return_value="token-abc")
        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {"id": "x"}

        with patch.object(client, "_get_token", mock_get_token), \
             patch.object(client.session, "request", return_value=mock_resp) as mock_request:
            client.request("GET", "/realms/test/users")

        mock_request.assert_called_once()
        _, kwargs = mock_request.call_args
        assert kwargs["headers"]["Authorization"] == "Bearer token-abc"

    def test_request_builds_correct_url(self):
        """request() should construct URL as {base}/admin/{path} for non-admin paths."""
        client = KeycloakClient()
        mock_get_token = MagicMock(return_value="tok")

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {}

        with patch.object(client, "_get_token", mock_get_token), \
             patch.object(client.session, "request", return_value=mock_resp) as mock_req:
            client.request("GET", "/realms/r/users")

        called_url = mock_req.call_args[0][1]
        assert called_url == "http://mock-keycloak:8080/admin/realms/r/users"

    def test_request_preserves_admin_prefix(self):
        """Paths already starting with admin/ should not get doubled."""
        client = KeycloakClient()
        mock_get_token = MagicMock(return_value="tok")

        mock_resp = MagicMock()
        mock_resp.ok = True
        mock_resp.json.return_value = {}

        with patch.object(client, "_get_token", mock_get_token), \
             patch.object(client.session, "request", return_value=mock_resp) as mock_req:
            client.request("GET", "/admin/realms/r/users")

        called_url = mock_req.call_args[0][1]
        assert called_url == "http://mock-keycloak:8080/admin/realms/r/users"

    def test_request_raises_keycloak_error_on_failure(self):
        """Non-2xx responses should raise KeycloakError."""
        client = KeycloakClient()
        mock_get_token = MagicMock(return_value="tok")

        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 404
        mock_resp.text = "Not found"

        with patch.object(client, "_get_token", mock_get_token), \
             patch.object(client.session, "request", return_value=mock_resp):
            with pytest.raises(KeycloakError) as exc_info:
                client.request("GET", "/realms/r/roles/nonexistent")
            assert exc_info.value.status_code == 404

    def test_keycloak_error_attributes(self):
        """KeycloakError should store status_code and detail."""
        err = KeycloakError(500, "Internal error")
        assert err.status_code == 500
        assert err.detail == "Internal error"

    def test_global_kc_singleton(self):
        """The module-level kc singleton should be a KeycloakClient instance."""
        assert isinstance(kc, KeycloakClient)


# ============================================================================
# OPAClient
# ============================================================================

class TestOPAClient:
    """Tests for OPAClient – OPA service communication."""

    def test_base_url_from_env(self):
        """base_url property should read OPA_BASE_URL from environment."""
        client = OPAClient()
        assert client.base_url == "http://mock-opa:8000"

    def test_base_url_default_when_env_missing(self, monkeypatch):
        """base_url falls back to default when OPA_BASE_URL is not set."""
        monkeypatch.delenv("OPA_BASE_URL", raising=False)
        client = OPAClient()
        assert client.base_url.endswith(":8001")

    def test_request_sets_content_type_header(self):
        """request() should inject Content-Type: application/json."""
        client = OPAClient()
        mock_resp = MagicMock()
        mock_resp.ok = True

        with patch.object(client.session, "request", return_value=mock_resp) as mock_req:
            client.request("GET", "/api/v1/policies")

        _, kwargs = mock_req.call_args
        assert kwargs["headers"]["Content-Type"] == "application/json"

    def test_request_builds_url_correctly(self):
        """request() should join base_url and path correctly."""
        client = OPAClient()
        mock_resp = MagicMock()
        mock_resp.ok = True

        with patch.object(client.session, "request", return_value=mock_resp) as mock_req:
            client.request("GET", "/api/v1/roles/r1/policy")

        called_url = mock_req.call_args[0][1]
        assert called_url == "http://mock-opa:8000/api/v1/roles/r1/policy"

    def test_request_raises_opa_error_on_failure(self):
        """Non-2xx OPA responses should raise OPAError."""
        client = OPAClient()
        mock_resp = MagicMock()
        mock_resp.ok = False
        mock_resp.status_code = 500
        mock_resp.text = "OPA internal error"

        with patch.object(client.session, "request", return_value=mock_resp):
            with pytest.raises(OPAError) as exc_info:
                client.request("POST", "/api/v1/policies", json={})
            assert exc_info.value.status_code == 500

    def test_opa_error_attributes(self):
        """OPAError should store status_code and detail."""
        err = OPAError(404, "Policy not found")
        assert err.status_code == 404
        assert err.detail == "Policy not found"

    def test_global_opa_singleton(self):
        """The module-level opa singleton should be an OPAClient instance."""
        assert isinstance(opa, OPAClient)

    def test_request_passes_extra_kwargs(self):
        """Extra kwargs like json and params should be forwarded to requests."""
        client = OPAClient()
        mock_resp = MagicMock()
        mock_resp.ok = True

        with patch.object(client.session, "request", return_value=mock_resp) as mock_req:
            client.request("POST", "/api/v1/roles/r1/policy",
                           json={"policy_id": "p1"},
                           params={"tenant_id": "t1"})

        _, kwargs = mock_req.call_args
        assert kwargs["json"] == {"policy_id": "p1"}
        assert kwargs["params"] == {"tenant_id": "t1"}

    def test_session_trust_env_is_false(self):
        """Session should not trust proxy environment variables."""
        client = OPAClient()
        assert client.session.trust_env is False
