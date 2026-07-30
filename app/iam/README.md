# AIDP IAM

Multi-tenant Identity & Access Management system. Federates customer identity providers (SAML/OIDC) via Keycloak, enforces dynamic authorization policies via OPA, and unifies everything behind a single API gateway.

## Repository Structure

```
da-cluster/           Deployment system (Helm charts, scripts, offline images, docs)
da-idb-proxy/         Keycloak Proxy API (tenant/role/group/user/IDP management)
opal-dynamic-policy/  OPA Policy Engine (pep-proxy + bundle-server + OPAL)
```

## Quick Start

See [da-cluster/README.md](da-cluster/README.md) for full deployment instructions.

```bash
cd da-cluster
# download offline packages from GitHub Releases, then:
./scripts/setup.sh              # Kind (dev)
./scripts/setup.sh --no-kind    # K8s (production)
./scripts/test.sh               # 192 tests
```

## Documentation

- [Architecture](da-cluster/docs/architecture.md)
- [Deployment Guide](da-cluster/docs/deployment-guide.md)
- [Frontend API Reference](da-cluster/docs/frontend-api-reference.md)

## TODO (Priority from high to low)

1. [ ] AuthN (Who is accessing the app)
2. [ ] AuthZ (Who can access the app)
3. [ ] For LLM services, rate limit by token usage
4. [ ] Service registration (integrate with DevPortal to automate)
5. [ ] AuthZ policy as code through DevPortal
6. [ ] Cascade identity from upstream identity providers (Google/GitHub/Apple, etc.)
7. [ ] Multi-tenancy
