"""
Shared fixtures for da-idb-proxy unit tests.
Provides mocked KeycloakClient, OPAClient, and FastAPI TestClient.
"""
import os
import sys
import types
import pytest
from unittest.mock import MagicMock, patch, PropertyMock
from fastapi.testclient import TestClient


# ---------------------------------------------------------------------------
# Pre-import fix: fake python-multipart modules so FastAPI won't complain
# about missing 'python-multipart' when processing routes that use UploadFile.
#
# Starlette does:
#   import multipart
#   from multipart.multipart import parse_options_header
#
# So we must build a fake package with the right nested structure.
# ---------------------------------------------------------------------------
def _install_fake_multipart():
    """Create fake multipart / python_multipart modules to satisfy Starlette."""
    if "multipart" not in sys.modules:
        _mp = types.ModuleType("multipart")
        _mp.__version__ = "0.0.0"
        sys.modules["multipart"] = _mp
    if "multipart.multipart" not in sys.modules:
        _mp_sub = types.ModuleType("multipart.multipart")
        _mp_sub.parse_options_header = lambda x: ("form-data", {})
        sys.modules["multipart"].multipart = _mp_sub
        sys.modules["multipart.multipart"] = _mp_sub
    if "python_multipart" not in sys.modules:
        sys.modules["python_multipart"] = sys.modules["multipart"]


_install_fake_multipart()


# ---------------------------------------------------------------------------
# Environment setup -- ensure all tests run with predictable defaults
# ---------------------------------------------------------------------------
@pytest.fixture(autouse=True)
def _env_setup(monkeypatch):
    """Set environment variables for every test to avoid host-specific leakage."""
    monkeypatch.setenv("KEYCLOAK_URL", "http://mock-keycloak:8080")
    monkeypatch.setenv("KC_REALM", "master")
    monkeypatch.setenv("KC_CLIENT_ID", "test-client")
    monkeypatch.setenv("KC_CLIENT_SECRET", "test-secret")
    monkeypatch.setenv("KC_NEW_CLIENT_ID", "data-agent")
    monkeypatch.setenv("KC_SCRIPT_MAPPER", "script-test-mapper.js")
    monkeypatch.setenv("DEFAULT_IDP_ALIAS", "test-saml-idp")
    monkeypatch.setenv("DEFAULT_TENANT_ADMIN_ROLE", "tenant-admin")
    monkeypatch.setenv("DEFAULT_TENANT_ADMIN_NAME", "tenant-admin")
    monkeypatch.setenv("OPA_BASE_URL", "http://mock-opa:8000")
    monkeypatch.setenv("KC_URL", "http://mock-keycloak:8080")


# ---------------------------------------------------------------------------
# Mocked Keycloak responses
# ---------------------------------------------------------------------------
class MockResponse:
    """Simulates a requests.Response for use with mocked kc.request()."""

    def __init__(self, status_code=200, json_data=None, headers=None, text=""):
        self.status_code = status_code
        self._json = json_data or {}
        self._headers = headers or {}
        self.text = text
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._json

    @property
    def headers(self):
        return self._headers


@pytest.fixture
def mock_kc():
    """Return a MagicMock standing in for the global kc (KeycloakClient).
    
    By default, mock_kc.request() returns a MockResponse(200, {}).
    Tests customize behavior via mock_kc.request.side_effect or
    mock_kc.request.return_value.
    """
    kc_mock = MagicMock()
    kc_mock.request.return_value = MockResponse(status_code=200, json_data={})
    return kc_mock


@pytest.fixture
def mock_opa():
    """Return a MagicMock standing in for the global opa (OPAClient)."""
    opa_mock = MagicMock()
    opa_mock.request.return_value = MockResponse(status_code=200, json_data={})
    return opa_mock


# ---------------------------------------------------------------------------
# Pre-built TestClient with all routes registered
# ---------------------------------------------------------------------------

# Cache the imported app so we only import once (FastAPI route registration
# triggers python-multipart check; our fake module handles it).
_app_cache = None

def _get_app():
    global _app_cache
    if _app_cache is None:
        from app.main import app
        _app_cache = app
    return _app_cache


@pytest.fixture
def client(mock_kc, mock_opa):
    """
    FastAPI TestClient with kc and opa replaced by mocks.

    We patch:
      - app.core.keycloak.kc (source singleton)
      - Every API/utility module that imports kc or opa at module level

    This ensures every test gets its own fresh mock, even though the
    FastAPI app and its route modules are only imported once.
    """
    app = _get_app()

    with patch("app.core.keycloak.kc", mock_kc), \
         patch("app.core.opa_client.opa", mock_opa), \
         patch("app.api.v1.tenants.kc", mock_kc), \
         patch("app.api.v1.idp.kc", mock_kc), \
         patch("app.api.v1.identity.kc", mock_kc), \
         patch("app.api.v1.token.kc", mock_kc), \
         patch("app.utils.opa.opa", mock_opa):
        with TestClient(app) as tc:
            yield tc


# ---------------------------------------------------------------------------
# Common mock data factories
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_realm():
    return {
        "id": "abc-123",
        "realm": "test-realm",
        "displayName": "Test Realm",
        "enabled": True,
    }


@pytest.fixture
def sample_role():
    return {
        "id": "role-uuid-1",
        "name": "test-role",
        "description": "A test role",
        "clientRole": False,
        "containerId": "test-realm",
        "attributes": {"level": ["gold"]},
        "composite": False,
    }


@pytest.fixture
def sample_group():
    return {
        "id": "group-uuid-1",
        "name": "test-group",
        "path": "/test-group",
        "subGroups": [],
    }


@pytest.fixture
def sample_user():
    return {
        "id": "user-uuid-1",
        "username": "testuser",
        "firstName": "Test",
        "lastName": "User",
        "email": "test@example.com",
        "enabled": True,
    }


@pytest.fixture
def sample_idp_instance():
    return {
        "alias": "test-saml-idp",
        "displayName": "Test SAML IDP",
        "internalId": "idp-internal-1",
        "providerId": "saml",
        "enabled": True,
        "config": {
            "singleSignOnServiceUrl": "https://idp.example.com/sso",
        },
    }


@pytest.fixture
def sample_policy():
    return {
        "id": "policy-uuid-1",
        "name": "test-policy",
        "tenant_id": "test-realm",
        "rules": [{"action": "read", "resource": "*"}],
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2025-01-01T00:00:00Z",
    }
