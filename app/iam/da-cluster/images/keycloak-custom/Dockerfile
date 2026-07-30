# Keycloak with 320 Script Mapper (data-agent-mapper.jar)
FROM quay.io/keycloak/keycloak:26.5.2

# Copy the pre-built script mapper JAR
COPY data-agent-mapper.jar /opt/keycloak/providers/

RUN /opt/keycloak/bin/kc.sh build --features=scripts
