# da-cluster: Unified Multi-Tenant Auth System

Consolidates Keycloak + keycloak-proxy + OPAL dynamic policy behind a single AgentGateway. Supports air-gapped deployment on amd64 and arm64.

## Architecture

```
Client (browser / backend)
    |
AgentGateway Proxy (unified entry, port 80)
    |
    +-- /realms/*, /admin/*                    --> Keycloak (no auth)
    +-- /api/v1/tenants, /api/v1/common        --> keycloak-proxy (ext-authz)
    +-- /api/v1/{realm}/roles|groups|users|idp  --> keycloak-proxy (ext-authz)
    +-- /api/v1/policies, /api/v1/roles         --> pep-proxy (ext-authz)
    +-- /api/v1/auth                            --> pep-proxy (ext-authz)
    +-- /{tenant-id}/**                         --> your backend (ext-authz)
```

## Quick Start (Kind, for development)

```bash
cd da-cluster

# 1. Download offline packages from GitHub Releases
wget https://github.com/zzzYesYes/aidp-iam/releases/download/v1.0.0/aidp-iam-offline-amd64-v1.0.0.tar.gz
wget https://github.com/zzzYesYes/aidp-iam/releases/download/v1.0.0/aidp-iam-offline-common-v1.0.0.tar.gz

# 2. Extract into offline/ directory
tar xzf aidp-iam-offline-amd64-v1.0.0.tar.gz -C offline/
tar xzf aidp-iam-offline-common-v1.0.0.tar.gz -C offline/

# 3. Deploy
./scripts/setup.sh

# 4. Test
./scripts/test.sh
```

## Deploy to Production (K8s, air-gapped server)

```bash
# On your dev machine: clone and download offline packages
git clone https://github.com/zzzYesYes/aidp-iam.git
cd aidp-iam/da-cluster

# Download the correct platform package (amd64 or arm64) + common
wget .../aidp-iam-offline-arm64-v1.0.0.tar.gz
wget .../aidp-iam-offline-common-v1.0.0.tar.gz

# Transfer everything to server
scp -r ../aidp-iam user@server:/opt/

# On the server:
cd /opt/aidp-iam/da-cluster
tar xzf aidp-iam-offline-arm64-v1.0.0.tar.gz -C offline/
tar xzf aidp-iam-offline-common-v1.0.0.tar.gz -C offline/
./scripts/setup.sh --no-kind
```

## Update Code Only (no network needed)

When you only changed application code (not dependencies):

```bash
# On the server
cd /opt/aidp-iam
git pull                                    # pull latest code
cd da-cluster
./scripts/setup.sh --no-kind --fat-base     # rebuild from fat base images, no pip/apt needed
```

## Upgrade (new image version)

When images have changed:

```bash
# Download new version's offline package
wget .../aidp-iam-offline-arm64-v1.1.0.tar.gz
tar xzf aidp-iam-offline-arm64-v1.1.0.tar.gz -C offline/    # overwrites old images
git pull
./scripts/setup.sh --no-kind
```

## setup.sh Options

```bash
./scripts/setup.sh [OPTIONS]

Options:
  (default)         Create Kind cluster + deploy (development)
  --no-kind         Deploy to existing K8s cluster (production)
  --build           Rebuild custom images from source (requires network)
  --fat-base        Rebuild from fat base images (no network, code-only)
  --existing-kind   Use existing Kind cluster, skip creation

Environment:
  PLATFORM=arm64    Force platform (default: auto-detect via uname -m)
  CLUSTER_NAME=xxx  Kind cluster name (default: da-cluster)
  K8S_NODES="..."   Space-separated node IPs for multi-node K8s
  K8S_NODE_USER=xxx SSH user for nodes (default: root)
```

## Offline Package Structure

After extracting, the `offline/` directory should look like:

```
offline/
  images/
    amd64/ (or arm64/)
      keycloak-proxy_v2.tar
      opal-proxy_v1.tar
      keycloak-init_v1.tar
      keycloak-custom_26.5.2.tar
      postgres_17.tar
      nginx_alpine.tar
      cr.agentgateway.dev_controller_v2.2.0-main.tar
      cr.agentgateway.dev_agentgateway_0.11.1.tar
      permitio_opal-server_0.7.4.tar
      permitio_opal-client_0.7.4.tar
      mccutchen_go-httpbin_2.24.0.tar
      base-keycloak-proxy_v1.tar      # fat base (for --fat-base mode)
      base-opal-proxy_v1.tar
      base-keycloak-init_v1.tar
  charts/
    agentgateway-crds-v2.2.1.tgz
    agentgateway-v2.2.1.tgz
  crds/
    gateway-api-v1.4.0.yaml
```

## For Maintainers: Creating a Release

```bash
cd da-cluster

# 1. Package offline assets
tar czf aidp-iam-offline-amd64-v1.0.0.tar.gz -C offline images/amd64
tar czf aidp-iam-offline-arm64-v1.0.0.tar.gz -C offline images/arm64
tar czf aidp-iam-offline-common-v1.0.0.tar.gz -C offline charts crds

# 2. Push code + create release
git push
gh release create v1.0.0 \
  aidp-iam-offline-amd64-v1.0.0.tar.gz \
  aidp-iam-offline-arm64-v1.0.0.tar.gz \
  aidp-iam-offline-common-v1.0.0.tar.gz \
  --title "v1.0.0" \
  --notes "Initial release with offline images for amd64 and arm64"
```

## Testing

```bash
./scripts/test.sh    # 192 tests covering all API endpoints
```

## Access Services

```bash
# Gateway (all services via single entry point)
kubectl -n agentgateway-system port-forward svc/agentgateway-proxy 8080:80

# Direct access (debugging)
kubectl -n keycloak port-forward svc/keycloak 8080:8080
kubectl -n keycloak port-forward svc/keycloak-proxy 8090:8090
kubectl -n opa port-forward svc/pep-proxy 8000:8000
```

## Default Accounts

| User | Realm | Password | Role |
|------|-------|----------|------|
| super-admin | master | SuperInit@123 | super-admin |
| tenant-admin | data-agent | TenantAdmin@123 | tenant-admin |
| normal-user | data-agent | NormalUser@123 | normal-user |

## Documentation

- [Architecture](docs/architecture.md) - System design and component overview
- [Deployment Guide](docs/deployment-guide.md) - Full deployment instructions
- [Frontend API Reference](docs/frontend-api-reference.md) - Complete API documentation
- [Frontend Sync Checklist](docs/frontend-sync-checklist.md) - Preparation for frontend integration

## Cleanup

```bash
./scripts/cleanup.sh
```
