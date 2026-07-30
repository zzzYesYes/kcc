from pydantic import BaseModel, Field
from typing import Optional, List, Dict


class UserBase(BaseModel):
    username: Optional[str] = None
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    emailVerified: Optional[bool] = None
    enabled: Optional[bool] = None
    attributes: Optional[Dict[str, List[str]]] = None


class UserCreate(BaseModel):
    username: str = Field(..., description="Username is required for user creation")
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    email: Optional[str] = None
    emailVerified: Optional[bool] = None
    enabled: Optional[bool] = None
    attributes: Optional[Dict[str, List[str]]] = None


class UserUpdate(UserBase):
    """All fields optional for updating users"""
    pass


class UserResponse(UserBase):
    id: str = Field(..., description="Keycloak generated user UUID")
    createdTimestamp: Optional[int] = None
    totp: Optional[bool] = None
    federationLink: Optional[str] = None
    serviceAccountClientId: Optional[str] = None
    notBefore: Optional[int] = None


class UserContextResponse(BaseModel):
    """Response for user context endpoint (groups and roles)"""
    groups: List[dict] = Field(default_factory=list, description="Groups the user belongs to")
    roles: List[dict] = Field(default_factory=list, description="Roles assigned to the user")
