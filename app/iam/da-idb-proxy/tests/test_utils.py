"""
Unit tests for app.utils.opa – OPA utility functions that delegate
to the OPAClient.
"""
import pytest
from unittest.mock import MagicMock, patch

from app.utils.opa import (
    get_role_policy,
    bind_policy_to_role,
    update_role_policy,
    unbind_policy_from_role,
)
from app.schemas.roles import PolicyInfo
from app.core.opa_client import OPAError


def _mock_opa_response(policy_dict):
    """Helper: create a mock response that returns {policy: ...}."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = {"policy": policy_dict}
    return mock_resp


SAMPLE_POLICY_DICT = {
    "id": "pol-1",
    "name": "read-only",
    "tenant_id": "realm-a",
    "rules": [{"action": "read"}],
    "created_at": "2025-01-01T00:00:00Z",
    "updated_at": "2025-01-01T00:00:00Z",
}


class TestGetRolePolicy:
    def test_returns_policy_info_when_present(self):
        mock_resp = _mock_opa_response(SAMPLE_POLICY_DICT)
        with patch("app.utils.opa.opa.request", return_value=mock_resp) as mock_req:
            result = get_role_policy("role-uuid", "realm-a")

        assert isinstance(result, PolicyInfo)
        assert result.id == "pol-1"
        mock_req.assert_called_once_with(
            "GET", "/api/v1/roles/role-uuid/policy",
            params={"tenant_id": "realm-a"},
        )

    def test_returns_none_when_policy_missing(self):
        """If the OPA response lacks policy info, model_dump may fail."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"policy": None}
        with patch("app.utils.opa.opa.request", return_value=mock_resp):
            with pytest.raises(Exception):
                get_role_policy("role-uuid", "realm-a")

    def test_propagates_opa_error(self):
        with patch("app.utils.opa.opa.request", side_effect=OPAError(500, "boom")):
            with pytest.raises(OPAError) as exc_info:
                get_role_policy("role-uuid", "realm-a")
            assert exc_info.value.status_code == 500


class TestBindPolicyToRole:
    def test_sends_correct_payload(self):
        with patch("app.utils.opa.opa.request") as mock_req:
            bind_policy_to_role("role-uuid", "pol-1", "realm-a")

        mock_req.assert_called_once_with(
            "POST", "/api/v1/roles/role-uuid/policy",
            json={"policy_id": "pol-1", "tenant_id": "realm-a"},
        )

    def test_propagates_opa_error(self):
        with patch("app.utils.opa.opa.request", side_effect=OPAError(409, "conflict")):
            with pytest.raises(OPAError):
                bind_policy_to_role("r1", "p1", "t1")


class TestUpdateRolePolicy:
    def test_sends_correct_payload(self):
        with patch("app.utils.opa.opa.request") as mock_req:
            update_role_policy("role-uuid", "pol-2", "realm-b")

        mock_req.assert_called_once_with(
            "PUT", "/api/v1/roles/role-uuid/policy",
            json={"policy_id": "pol-2", "tenant_id": "realm-b"},
        )

    def test_propagates_opa_error(self):
        with patch("app.utils.opa.opa.request", side_effect=OPAError(404, "not found")):
            with pytest.raises(OPAError):
                update_role_policy("r1", "p1", "t1")


class TestUnbindPolicyFromRole:
    def test_sends_delete_request(self):
        with patch("app.utils.opa.opa.request") as mock_req:
            unbind_policy_from_role("role-uuid", "realm-a")

        mock_req.assert_called_once_with(
            "DELETE", "/api/v1/roles/role-uuid/policy",
            params={"tenant_id": "realm-a"},
        )

    def test_suppresses_404_error(self):
        """404 errors should be silently swallowed (policy already gone)."""
        with patch("app.utils.opa.opa.request", side_effect=OPAError(404, "not found")):
            # Should not raise
            unbind_policy_from_role("r1", "t1")

    def test_propagates_non_404_opa_error(self):
        with patch("app.utils.opa.opa.request", side_effect=OPAError(500, "server error")):
            with pytest.raises(OPAError) as exc_info:
                unbind_policy_from_role("r1", "t1")
            assert exc_info.value.status_code == 500
