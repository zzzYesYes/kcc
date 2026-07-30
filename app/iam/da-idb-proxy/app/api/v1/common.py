from fastapi import APIRouter, HTTPException, Request, status
from datetime import datetime
import os


# 从环境变量获取受保护的 Realm 名称，默认值为 "master"
PROTECTED_REALM = os.getenv("KC_REALM", "master")

def skip_master_realm(request: Request):
    """
    通用拦截器：自动从路径参数中寻找名为 realm 或 realm_name 的值并校验
    """
    # 尝试从路径参数中获取可能的键名
    path_params = request.path_params
    realm_val = path_params.get("realm") or path_params.get("realm_name")

    if realm_val and realm_val.lower() == PROTECTED_REALM.lower():
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=f"Operations on protected realm '{PROTECTED_REALM}' are not allowed."
        )
    return realm_val


router = APIRouter(prefix="/common", tags=["Common"])


@router.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    """健康检查接口 (GET /api/v1/common/health)"""
    return {
        "status": "healthy",
        "code": 200,
        "timestamp": datetime.utcnow().isoformat()
    }

