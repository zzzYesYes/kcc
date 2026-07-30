from fastapi import APIRouter, HTTPException
from app.schemas.token import TokenExchangeRequest, TokenExchangeResponse
from app.core.keycloak import kc
import requests
import os

router = APIRouter(prefix="/{realm}/token", tags=["Token"])


@router.post("/exchange", response_model=TokenExchangeResponse)
def exchange_code_for_token(realm: str, payload: TokenExchangeRequest):
    """
    通过OIDC授权码换取access token，并在响应中添加realm_id和role_ids
    """
    # 1. 调用Keycloak的token接口换取access token
    token_url = f"{os.getenv('KC_URL', 'http://localhost:8080')}/realms/{realm}/protocol/openid-connect/token"
    
    token_data = {
        "grant_type": "authorization_code",
        "code": payload.code,
        "redirect_uri": payload.redirect_uri,
        "client_id": payload.client_id
    }
    
    if payload.client_secret:
        token_data["client_secret"] = payload.client_secret
    
    token_resp = requests.post(token_url, data=token_data)
    
    if token_resp.status_code != 200:
        raise HTTPException(
            status_code=token_resp.status_code,
            detail=f"Failed to exchange token: {token_resp.text}"
        )
    
    token_result = token_resp.json()

    response = TokenExchangeResponse(
        access_token=token_result.get("access_token"),
        token_type=token_result.get("token_type", "Bearer"),
        expires_in=token_result.get("expires_in", 0),
        refresh_token=token_result.get("refresh_token"),
        scope=token_result.get("scope")
    )
    
    return response
