from pydantic import BaseModel
from typing import Optional, List
from typing import Any


class TenantCreate(BaseModel):
    realm: str
    displayName: str


class TenantResponse(BaseModel):
    realm: str
    id: str
    admin_role: str
    admin_user: Optional[str] = None


class TenantListResponse(BaseModel):
    """Response for listing all tenants (realms)"""
    id: str
    realm: str
    displayName: Optional[str] = None
    displayNameHtml: Optional[str] = None
    enabled: bool = True
    notBefore: Optional[int] = None
    defaultSignatureAlgorithm: Optional[str] = None
    sslRequired: Optional[str] = None
    registrationAllowed: Optional[bool] = None
    loginWithEmailAllowed: Optional[bool] = None
    duplicateEmailsAllowed: Optional[bool] = None
    resetPasswordAllowed: Optional[bool] = None
    editUsernameAllowed: Optional[bool] = None
    bruteForceProtected: Optional[bool] = None


class MessageResponse(BaseModel):
    """Standard message response for operations that don't return data"""
    msg: str
    alias: Optional[str] = None
    id: Optional[str] = None
