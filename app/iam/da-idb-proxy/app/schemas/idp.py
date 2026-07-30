from pydantic import BaseModel, Field
from typing import Dict, Any, Optional, List


class IDPRequest(BaseModel):
    alias: Optional[str] = None
    displayName: Optional[str] = None
    enabled: bool = True
    trustEmail: bool = False
    # 前端传来的 SAML 技术参数（如 singleSignOnServiceUrl）放在这里
    config: Dict[str, Any] = Field(default_factory=dict)


class IDPInstanceResponse(BaseModel):
    """Response for SAML IDP instance"""
    alias: str
    displayName: Optional[str] = None
    internalId: Optional[str] = None
    providerId: str
    enabled: bool
    trustEmail: Optional[bool] = None
    storeToken: Optional[bool] = None
    addReadTokenRoleOnCreate: Optional[bool] = None
    authenticateByDefault: Optional[bool] = None
    linkOnly: Optional[bool] = None
    hideOnLogin: Optional[bool] = None
    firstBrokerLoginFlowAlias: Optional[str] = None
    postBrokerLoginFlowAlias: Optional[str] = None
    config: Dict[str, str] = Field(default_factory=dict)


class SAMLMetadataImportResponse(BaseModel):
    """Response from SAML metadata import endpoint - matches Keycloak's actual response"""
    addExtensionsElementWithKeyInfo: Optional[str] = None
    artifactBindingResponse: Optional[str] = None
    artifactResolutionServiceUrl: Optional[str] = None
    enabledFromMetadata: Optional[str] = None
    idpEntityId: Optional[str] = None
    loginHint: Optional[str] = None
    metadataDescriptorUrl: Optional[str] = None
    nameIDPolicyFormat: Optional[str] = None
    postBindingAuthnRequest: Optional[str] = None
    postBindingLogout: Optional[str] = None
    postBindingResponse: Optional[str] = None
    signingCertificate: Optional[str] = None
    singleLogoutServiceUrl: Optional[str] = None
    singleSignOnServiceUrl: Optional[str] = None
    validateSignature: Optional[str] = None
    wantAuthnRequestsSigned: Optional[str] = None


class IdPMapperCreate(BaseModel):
    """简化的 IDP Mapper 创建请求"""
    name: str = Field(..., description="Mapper 名称")
    attributeKey: str = Field(..., description="Remote Attribute（SAML 属性名）")
    attributeValue: str = Field(..., description="Local Attribute（Keycloak 用户属性名）")
    friendlyName: Optional[str] = Field(None, description="Friendly Name（可选）")


class IdPMapperUpdate(BaseModel):
    """简化的 IDP Mapper 更新请求"""
    name: Optional[str] = None
    attributeKey: Optional[str] = None
    attributeValue: Optional[str] = None
    friendlyName: Optional[str] = None


class IdPMapperResponse(BaseModel):
    """简化的 IDP Mapper 响应"""
    id: str
    name: str
    attributeKey: str
    attributeValue: str
    friendlyName: Optional[str] = None
