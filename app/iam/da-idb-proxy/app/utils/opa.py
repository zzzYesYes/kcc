from typing import Optional
from app.schemas.roles import PolicyInfo
from app.core.opa_client import opa, OPAError


def get_role_policy(role_id: str, tenant_id: str) -> Optional[PolicyInfo]:
    """
    查询角色绑定的策略

    Args:
        role_id: 角色 UUID
        tenant_id: 租户 ID（即 realm 名称）

    Returns:
        PolicyInfo 对象，如果未绑定则返回 None

    Raises:
        OPAError: OPA 服务请求失败
    """
    params = {"tenant_id": tenant_id}
    resp = opa.request("GET", f"/api/v1/roles/{role_id}/policy", params=params)

    data = resp.json()
    return PolicyInfo(**data["policy"])


def bind_policy_to_role(role_id: str, policy_id: str, tenant_id: str) -> None:
    """
    为角色绑定策略

    Args:
        role_id: 角色 UUID
        policy_id: 策略 ID
        tenant_id: 租户 ID（即 realm 名称）

    Raises:
        OPAError: OPA 服务请求失败
    """
    payload = {
        "policy_id": policy_id,
        "tenant_id": tenant_id
    }
    opa.request("POST", f"/api/v1/roles/{role_id}/policy", json=payload)


def update_role_policy(role_id: str, policy_id: str, tenant_id: str) -> None:
    """
    为角色更换绑定的策略

    Args:
        role_id: 角色 UUID
        policy_id: 新策略 ID
        tenant_id: 租户 ID（即 realm 名称）

    Raises:
        OPAError: OPA 服务请求失败
    """
    payload = {
        "policy_id": policy_id,
        "tenant_id": tenant_id
    }
    opa.request("PUT", f"/api/v1/roles/{role_id}/policy", json=payload)


def unbind_policy_from_role(role_id: str, tenant_id: str) -> None:
    """
    为角色解绑策略

    Args:
        role_id: 角色 UUID
        tenant_id: 租户 ID（即 realm 名称）

    Raises:
        OPAError: OPA 服务请求失败（非 404 错误）
    """
    params = {"tenant_id": tenant_id}

    try:
        opa.request("DELETE", f"/api/v1/roles/{role_id}/policy", params=params)
    except OPAError as e:
        # 允许 404 Not Found（策略可能已经不存在）
        if e.status_code == 404:
            return
        raise
