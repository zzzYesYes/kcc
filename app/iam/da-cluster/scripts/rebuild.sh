#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# rebuild.sh — Quick rebuild of keycloak-proxy and/or opal-proxy
#
# For development iteration: rebuild image(s) from source, load into cluster,
# and restart the affected deployment(s). Does NOT redeploy the full stack.
#
# Usage:
#   ./scripts/rebuild.sh                # Rebuild both images
#   ./scripts/rebuild.sh proxy          # Rebuild keycloak-proxy only
#   ./scripts/rebuild.sh opa            # Rebuild opal-proxy only
#   ./scripts/rebuild.sh --no-kind      # Target existing K8s cluster (not Kind)
#
# Environment variables:
#   CLUSTER_NAME   — Kind cluster name (default: da-cluster)
#   K8S_NODES      — space-separated node IPs (K8s mode)
#   K8S_NODE_USER  — SSH user for nodes (K8s mode, default: root)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AUTH_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"
KEYCLOAK_NS="keycloak"
OPA_NS="opa"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Parse arguments ─────────────────────────────────────────────────────
USE_KIND=true
USE_FAT_BASE=false
BUILD_PROXY=false
BUILD_OPA=false
BUILD_INIT=false
TARGET_SPECIFIED=false

for arg in "$@"; do
  case "$arg" in
    proxy)      BUILD_PROXY=true; TARGET_SPECIFIED=true ;;
    opa)        BUILD_OPA=true; TARGET_SPECIFIED=true ;;
    init)       BUILD_INIT=true; TARGET_SPECIFIED=true ;;
    --no-kind)  USE_KIND=false ;;
    --fat-base) USE_FAT_BASE=true ;;
    --help|-h)
      echo "Usage: $0 [proxy|opa|init] [--no-kind] [--fat-base]"
      echo ""
      echo "  (default)    Rebuild keycloak-proxy:v2 and opal-proxy:v1"
      echo "  proxy        Rebuild keycloak-proxy:v2 only"
      echo "  opa          Rebuild opal-proxy:v1 only"
      echo "  init         Rebuild keycloak-init:v1 only"
      echo "  --no-kind    Target existing K8s cluster (not Kind)"
      echo "  --fat-base   Build from fat base images (no network needed)"
      exit 0
      ;;
    *) err "Unknown argument: $arg (use --help)" ;;
  esac
done

# Default: build both
if [ "$TARGET_SPECIFIED" = false ]; then
  BUILD_PROXY=true
  BUILD_OPA=true
fi

K8S_NODE_USER="${K8S_NODE_USER:-root}"

image_to_filename() {
  echo "$1" | sed 's|/|_|g; s|:|_|g'
}

# ── Helper: load a single image into cluster ────────────────────────────
load_image_to_cluster() {
  local img="$1"
  if [ "$USE_KIND" = true ]; then
    log "  Loading $img into Kind cluster..."
    kind load docker-image "$img" --name "$CLUSTER_NAME" 2>/dev/null \
      || err "Failed to load $img into Kind"
  else
    local fname="$(image_to_filename "$img").tar"
    local tmptar=$(mktemp)
    docker save -o "$tmptar" "$img"

    if [ -n "${K8S_NODES:-}" ]; then
      for node in $K8S_NODES; do
        log "  Loading $img to node $node..."
        scp -q "$tmptar" "${K8S_NODE_USER}@${node}:/tmp/$fname"
        ssh "${K8S_NODE_USER}@${node}" "ctr -n k8s.io images import /tmp/$fname && rm -f /tmp/$fname" 2>/dev/null \
          || warn "  Failed to import $img on $node"
      done
    elif command -v ctr &>/dev/null; then
      log "  Loading $img via ctr..."
      ctr -n k8s.io images import "$tmptar" 2>/dev/null \
        || err "Failed to ctr-import $img"
    else
      err "'ctr' not found and K8S_NODES not set"
    fi
    rm -f "$tmptar"
  fi
}

# ── Build & load keycloak-proxy ─────────────────────────────────────────
if [ "$BUILD_PROXY" = true ]; then
  PROXY_BUILD_DIR=$(mktemp -d)
  cp -r "$AUTH_DIR/da-idb-proxy/app" "$PROXY_BUILD_DIR/app"

  if [ "$USE_FAT_BASE" = true ]; then
    log "Building keycloak-proxy:v2 (slim, from fat base)..."
    cp "$PROJECT_DIR/images/keycloak-proxy/Dockerfile.slim" "$PROXY_BUILD_DIR/Dockerfile"
    docker build -t keycloak-proxy:v2 --build-arg BASE_IMAGE=base-keycloak-proxy:v1 "$PROXY_BUILD_DIR"
  else
    log "Building keycloak-proxy:v2..."
    cp "$PROJECT_DIR/images/keycloak-proxy/Dockerfile" "$PROXY_BUILD_DIR/Dockerfile"
    docker build -t keycloak-proxy:v2 "$PROXY_BUILD_DIR"
  fi
  rm -rf "$PROXY_BUILD_DIR"

  load_image_to_cluster "keycloak-proxy:v2"

  log "Restarting keycloak-proxy deployment..."
  kubectl -n "$KEYCLOAK_NS" rollout restart deployment/keycloak-proxy
  kubectl -n "$KEYCLOAK_NS" rollout status deployment/keycloak-proxy --timeout=120s
  log "keycloak-proxy updated successfully"
fi

# ── Build & load opal-proxy ─────────────────────────────────────────────
if [ "$BUILD_OPA" = true ]; then
  OPAL_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/opal-proxy/supervisord.conf" "$OPAL_BUILD_DIR/supervisord.conf"
  cp -r "$AUTH_DIR/opal-dynamic-policy/pep-proxy" "$OPAL_BUILD_DIR/pep-proxy"
  cp -r "$AUTH_DIR/opal-dynamic-policy/bundle-server" "$OPAL_BUILD_DIR/bundle-server"
  cp -r "$AUTH_DIR/opal-dynamic-policy/data" "$OPAL_BUILD_DIR/data"

  if [ "$USE_FAT_BASE" = true ]; then
    log "Building opal-proxy:v1 (slim, from fat base)..."
    cp "$PROJECT_DIR/images/opal-proxy/Dockerfile.slim" "$OPAL_BUILD_DIR/Dockerfile"
    docker build -t opal-proxy:v1 --build-arg BASE_IMAGE=base-opal-proxy:v1 "$OPAL_BUILD_DIR"
  else
    log "Building opal-proxy:v1..."
    cp "$PROJECT_DIR/images/opal-proxy/Dockerfile" "$OPAL_BUILD_DIR/Dockerfile"
    cp "$PROJECT_DIR/images/opal-proxy/requirements.txt" "$OPAL_BUILD_DIR/requirements.txt"
    docker build -t opal-proxy:v1 "$OPAL_BUILD_DIR"
  fi
  rm -rf "$OPAL_BUILD_DIR"

  load_image_to_cluster "opal-proxy:v1"

  log "Restarting pep-proxy deployment..."
  kubectl -n "$OPA_NS" rollout restart deployment/pep-proxy
  kubectl -n "$OPA_NS" rollout status deployment/pep-proxy --timeout=180s
  log "opal-proxy (pep-proxy) updated successfully"
fi

# ── Build & load keycloak-init ──────────────────────────────────────────
if [ "$BUILD_INIT" = true ]; then
  INIT_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/keycloak-init/init-keycloak.py" "$INIT_BUILD_DIR/init-keycloak.py"

  if [ "$USE_FAT_BASE" = true ]; then
    log "Building keycloak-init:v1 (slim, from fat base)..."
    cp "$PROJECT_DIR/images/keycloak-init/Dockerfile.slim" "$INIT_BUILD_DIR/Dockerfile"
    docker build -t keycloak-init:v1 --build-arg BASE_IMAGE=base-keycloak-init:v1 "$INIT_BUILD_DIR"
  else
    log "Building keycloak-init:v1..."
    cp "$PROJECT_DIR/images/keycloak-init/Dockerfile" "$INIT_BUILD_DIR/Dockerfile"
    docker build -t keycloak-init:v1 "$INIT_BUILD_DIR"
  fi
  rm -rf "$INIT_BUILD_DIR"

  load_image_to_cluster "keycloak-init:v1"

  log "Restarting keycloak-init job..."
  kubectl -n "$KEYCLOAK_NS" delete job keycloak-init 2>/dev/null || true
  helm upgrade -i keycloak "$PROJECT_DIR/charts/keycloak" --namespace "$KEYCLOAK_NS" 2>/dev/null
  kubectl -n "$KEYCLOAK_NS" wait --for=condition=complete job/keycloak-init --timeout=300s 2>/dev/null || warn "keycloak-init job not yet complete"
  log "keycloak-init updated successfully"
fi

# ── Done ─────────────────────────────────────────────────────────────────
log ""
log "==============================================="
log "Rebuild complete!"
log "==============================================="
log ""
log "Updated pods:"
[ "$BUILD_PROXY" = true ] && kubectl -n "$KEYCLOAK_NS" get pods -l app=keycloak-proxy --no-headers 2>/dev/null | while read line; do echo "  $line"; done
[ "$BUILD_OPA" = true ]   && kubectl -n "$OPA_NS" get pods -l app=pep-proxy --no-headers 2>/dev/null | while read line; do echo "  $line"; done
[ "$BUILD_INIT" = true ]  && kubectl -n "$KEYCLOAK_NS" get pods -l job-name=keycloak-init --no-headers 2>/dev/null | while read line; do echo "  $line"; done
log ""
log "Run tests: ./scripts/test.sh"
