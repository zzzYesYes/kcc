from pydantic import BaseModel, Field
from typing import Optional


class TokenExchangeRequest(BaseModel):
    code: str = Field(..., description="OIDC授权码")
    redirect_uri: str = Field(..., description="重定向URI")
    client_id: str = Field(..., description="客户端ID")
    client_secret: Optional[str] = Field(None, description="客户端密钥（可选）")


class TokenExchangeResponse(BaseModel):
    access_token: str
    token_type: str
    expires_in: int
    refresh_token: Optional[str] = None
    id_token: Optional[str] = None
    scope: Optional[str] = None
