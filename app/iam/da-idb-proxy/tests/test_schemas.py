"""
Unit tests for Pydantic schemas – validation, serialization and defaults.
Covers schemas/roles.py, schemas/idp.py, schemas/groups.py,
schemas/token.py, schemas/realm.py, schemas/users.py.
"""
import pytest
from pydantic import ValidationError

from app.schemas.roles import (
    PolicyInfo, PolicyBindingResponse, RoleBase, RoleCreate,
    RoleUpdate, RoleResponse, RoleUpdateByIdRequest,
)
from app.schemas.idp import (
    IDPRequest, IDPInstanceResponse, SAMLMetadataImportResponse,
    IdPMapperCreate, IdPMapperUpdate, IdPMapperResponse,
)
from app.schemas.groups import (
    GroupMember, GroupDetailResponse, GroupBase, GroupCreate,
    GroupUpdate, GroupResponse,
)
from app.schemas.token import TokenExchangeRequest, TokenExchangeResponse
from app.schemas.realm import (
    TenantCreate, TenantResponse, TenantListResponse, MessageResponse,
)
from app.schemas.users import (
    UserBase, UserCreate, UserUpdate, UserResponse, UserContextResponse,
)


# ============================================================================
# Roles schemas
# ============================================================================

class TestPolicyInfo:
    def test_valid_policy_info(self):
        p = PolicyInfo(
            id="p1", name="my-policy", tenant_id="t1",
            rules=[{"action": "read"}],
            created_at="2025-01-01T00:00:00Z",
            updated_at="2025-01-02T00:00:00Z",
        )
        assert p.id == "p1"
        assert p.name == "my-policy"

    def test_missing_required_fields(self):
        with pytest.raises(ValidationError):
            PolicyInfo()


class TestRoleCreate:
    def test_minimal_create(self):
        r = RoleCreate(name="admin")
        assert r.name == "admin"
        assert r.description is None
        assert r.attributes is None
        assert r.composite is False
        assert r.policy_id is None

    def test_create_with_all_fields(self):
        r = RoleCreate(
            name="admin",
            description="Administrator role",
            attributes={"level": ["gold"]},
            composite=True,
            policy_id="pol-1",
        )
        assert r.description == "Administrator role"
        assert r.attributes == {"level": ["gold"]}
        assert r.composite is True
        assert r.policy_id == "pol-1"

    def test_name_is_required(self):
        with pytest.raises(ValidationError):
            RoleCreate(description="Missing name")


class TestRoleUpdate:
    def test_all_fields_optional(self):
        """RoleUpdate should accept an empty dict (all optional)."""
        r = RoleUpdate()
        assert r.name is None

    def test_partial_update(self):
        r = RoleUpdate(description="new desc", policy_id="p-new")
        assert r.description == "new desc"
        assert r.policy_id == "p-new"


class TestRoleResponse:
    def test_serialization(self):
        r = RoleResponse(
            id="uuid-1", name="reader", clientRole=False,
        )
        data = r.model_dump()
        assert data["id"] == "uuid-1"
        assert data["policy"] is None

    def test_with_policy(self):
        r = RoleResponse(
            id="uuid-1", name="reader", clientRole=False,
            policy=PolicyInfo(
                id="p1", name="pol", tenant_id="t1",
                rules=[], created_at="", updated_at="",
            ),
        )
        assert r.policy is not None
        assert r.policy.id == "p1"


class TestRoleUpdateByIdRequest:
    def test_empty_payload(self):
        req = RoleUpdateByIdRequest()
        assert req.name is None
        assert req.policy_id is None


# ============================================================================
# IDP schemas
# ============================================================================

class TestIDPRequest:
    def test_defaults(self):
        req = IDPRequest()
        assert req.enabled is True
        assert req.trustEmail is False
        assert req.config == {}

    def test_with_config(self):
        req = IDPRequest(config={"singleSignOnServiceUrl": "https://sso.example.com"})
        assert req.config["singleSignOnServiceUrl"] == "https://sso.example.com"


class TestIDPInstanceResponse:
    def test_minimal_response(self):
        resp = IDPInstanceResponse(alias="idp-1", providerId="saml", enabled=True)
        assert resp.alias == "idp-1"
        assert resp.providerId == "saml"

    def test_config_defaults_to_empty_dict(self):
        resp = IDPInstanceResponse(alias="idp-1", providerId="saml", enabled=False)
        assert resp.config == {}


class TestIdPMapperCreate:
    def test_required_fields(self):
        m = IdPMapperCreate(name="mapper1", attributeKey="email", attributeValue="email")
        assert m.name == "mapper1"
        assert m.attributeKey == "email"
        assert m.friendlyName is None

    def test_missing_required(self):
        with pytest.raises(ValidationError):
            IdPMapperCreate(name="m1")  # missing attributeKey, attributeValue


class TestIdPMapperUpdate:
    def test_all_optional(self):
        m = IdPMapperUpdate()
        assert m.name is None
        assert m.attributeKey is None


# ============================================================================
# Groups schemas
# ============================================================================

class TestGroupCreate:
    def test_minimal(self):
        g = GroupCreate(name="engineering")
        assert g.name == "engineering"
        assert g.users == []
        assert g.roles == []

    def test_name_required(self):
        with pytest.raises(ValidationError):
            GroupCreate()


class TestGroupResponse:
    def test_recursive_subgroups(self):
        parent = GroupResponse(id="1", name="parent")
        child = GroupResponse(id="2", name="child")
        parent.subGroups.append(child)
        assert len(parent.subGroups) == 1
        assert parent.subGroups[0].name == "child"


class TestGroupUpdate:
    def test_empty_update(self):
        u = GroupUpdate()
        assert u.name is None

    def test_with_users_and_roles(self):
        u = GroupUpdate(users=["u1"], roles=["r1"])
        assert u.users == ["u1"]


class TestGroupDetailResponse:
    def test_with_members_and_roles(self):
        member = GroupMember(id="u1", username="alice")
        detail = GroupDetailResponse(
            id="g1", name="eng",
            members=[member],
            roles=[],
        )
        assert detail.members[0].username == "alice"


# ============================================================================
# Token schemas
# ============================================================================

class TestTokenExchangeRequest:
    def test_required_fields(self):
        req = TokenExchangeRequest(
            code="auth-code-123",
            redirect_uri="https://app/callback",
            client_id="my-client",
        )
        assert req.code == "auth-code-123"

    def test_missing_code_raises(self):
        with pytest.raises(ValidationError):
            TokenExchangeRequest(redirect_uri="https://app/callback", client_id="c1")


class TestTokenExchangeResponse:
    def test_full_response(self):
        resp = TokenExchangeResponse(
            access_token="at", token_type="Bearer", expires_in=3600,
            refresh_token="rt", id_token="idt", scope="openid",
        )
        assert resp.access_token == "at"
        assert resp.scope == "openid"


# ============================================================================
# Realm schemas
# ============================================================================

class TestTenantCreate:
    def test_valid(self):
        t = TenantCreate(realm="my-realm", displayName="My Realm")
        assert t.realm == "my-realm"

    def test_missing_realm_raises(self):
        with pytest.raises(ValidationError):
            TenantCreate(displayName="No realm")


class TestTenantResponse:
    def test_serialization(self):
        t = TenantResponse(realm="r", id="abc", admin_role="admin")
        assert t.admin_role == "admin"
        assert t.admin_user is None


class TestTenantListResponse:
    def test_defaults(self):
        t = TenantListResponse(id="abc", realm="r")
        assert t.enabled is True


class TestMessageResponse:
    def test_basic(self):
        m = MessageResponse(msg="done")
        assert m.msg == "done"


# ============================================================================
# Users schemas
# ============================================================================

class TestUserCreate:
    def test_username_required(self):
        u = UserCreate(username="alice")
        assert u.username == "alice"

    def test_missing_username_raises(self):
        with pytest.raises(ValidationError):
            UserCreate()

    def test_extra_fields_ignored(self):
        u = UserCreate(username="bob", firstName="Bob", email="bob@example.com")
        assert u.firstName == "Bob"


class TestUserUpdate:
    def test_all_optional(self):
        u = UserUpdate()
        assert u.username is None


class TestUserResponse:
    def test_id_is_required(self):
        with pytest.raises(ValidationError):
            UserResponse()

    def test_full_response(self):
        u = UserResponse(id="uid", username="alice", email="a@b.com")
        assert u.id == "uid"


class TestUserContextResponse:
    def test_empty_defaults(self):
        ctx = UserContextResponse()
        assert ctx.groups == []
        assert ctx.roles == []
