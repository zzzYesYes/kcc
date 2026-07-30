package com.dataagent.keycloak.mapper;

import org.keycloak.models.ClientSessionContext;
import org.keycloak.models.KeycloakSession;
import org.keycloak.models.ProtocolMapperModel;
import org.keycloak.models.RoleModel;
import org.keycloak.models.UserSessionModel;
import org.keycloak.protocol.oidc.mappers.AbstractOIDCProtocolMapper;
import org.keycloak.protocol.oidc.mappers.OIDCAccessTokenMapper;
import org.keycloak.protocol.oidc.mappers.OIDCAttributeMapperHelper;
import org.keycloak.protocol.oidc.mappers.OIDCIDTokenMapper;
import org.keycloak.protocol.oidc.mappers.UserInfoTokenMapper;
import org.keycloak.provider.ProviderConfigProperty;
import org.keycloak.representations.IDToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Custom Protocol Mapper that outputs realm roles as structured objects:
 * "roles": [{"id": "uuid", "name": "role-name"}, ...]
 *
 * This allows downstream services (OPA, pep-proxy) to use role UUIDs
 * for policy binding while also having role names for display/logging.
 */
public class StructuredRoleMapper extends AbstractOIDCProtocolMapper
        implements OIDCAccessTokenMapper, OIDCIDTokenMapper, UserInfoTokenMapper {

    public static final String PROVIDER_ID = "structured-realm-role-mapper";

    private static final List<ProviderConfigProperty> CONFIG_PROPERTIES = new ArrayList<>();

    static {
        ProviderConfigProperty claimName = new ProviderConfigProperty();
        claimName.setName("claim.name");
        claimName.setLabel("Token Claim Name");
        claimName.setType(ProviderConfigProperty.STRING_TYPE);
        claimName.setDefaultValue("roles");
        claimName.setHelpText("Name of the claim to insert into the token.");
        CONFIG_PROPERTIES.add(claimName);

        OIDCAttributeMapperHelper.addIncludeInTokensConfig(CONFIG_PROPERTIES, StructuredRoleMapper.class);
    }

    @Override
    public String getId() {
        return PROVIDER_ID;
    }

    @Override
    public String getDisplayType() {
        return "Structured Realm Role Mapper";
    }

    @Override
    public String getDisplayCategory() {
        return TOKEN_MAPPER_CATEGORY;
    }

    @Override
    public String getHelpText() {
        return "Adds realm roles as [{id, name}] array to the token.";
    }

    @Override
    public List<ProviderConfigProperty> getConfigProperties() {
        return CONFIG_PROPERTIES;
    }

    @Override
    protected void setClaim(IDToken token, ProtocolMapperModel mappingModel,
                            UserSessionModel userSession, KeycloakSession keycloakSession,
                            ClientSessionContext clientSessionCtx) {

        String claimName = mappingModel.getConfig().getOrDefault("claim.name", "roles");

        List<Map<String, String>> rolesList = userSession.getUser()
                .getRealmRoleMappingsStream()
                .map(role -> {
                    Map<String, String> entry = new HashMap<>();
                    entry.put("id", role.getId());
                    entry.put("name", role.getName());
                    return entry;
                })
                .collect(Collectors.toList());

        token.getOtherClaims().put(claimName, rolesList);
    }
}
