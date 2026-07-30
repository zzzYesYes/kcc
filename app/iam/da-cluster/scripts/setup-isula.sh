#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# setup-isula.sh — Deploy da-cluster IAM on Huawei Cloud K8s (isula runtime)
#
# Prerequisites:
#   - kubectl (configured with kubeconfig)
#   - helm 3
#   - isula (on each node, only when --load-images is used)
#
# NO internet access required. NO Kind. NO build. NO httpbin.
#
# Usage:
#   ./scripts/setup-isula.sh                          # Deploy only (images already loaded)
#   ./scripts/setup-isula.sh --load-images            # Load images from offline/ then deploy
#   ./scripts/setup-isula.sh --help                   # Show help
#
# Environment variables:
#   KC_HOSTNAME    — Keycloak external hostname (e.g. http://EIP:30080)
#   STORAGE_CLASS  — StorageClass for PVC (e.g. dorado-inner-nas, default: auto-detect)
#   PLATFORM       — amd64 or arm64 (default: auto-detect)
#   K8S_NODES      — space-separated node IPs for image loading via SSH
#   K8S_NODE_USER  — SSH user for nodes (default: root)
#   IMAGE_DIR      — remote temp dir for image tars (default: /tmp/da-images)
#   KUBECONFIG     — path to kubeconfig (default: ~/.kube/config)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OFFLINE_DIR="$PROJECT_DIR/offline"

KEYCLOAK_NS="keycloak"
OPA_NS="opa"
AGENTGATEWAY_NS="agentgateway-system"

AGENTGATEWAY_CHART_VERSION="v2.2.1"
GATEWAY_API_VERSION="v1.4.0"

K8S_NODE_USER="${K8S_NODE_USER:-root}"
IMAGE_DIR="${IMAGE_DIR:-/tmp/da-images}"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Parse arguments ─────────────────────────────────────────────────────
LOAD_IMAGES=false
for arg in "$@"; do
  case "$arg" in
    --load-images)  LOAD_IMAGES=true ;;
    --help|-h)
      echo "Usage: $0 [OPTIONS]"
      echo ""
      echo "Deploy da-cluster IAM system to an existing Huawei Cloud K8s cluster."
      echo "Designed for isula container runtime. No Kind, no build, no httpbin."
      echo ""
      echo "Options:"
      echo "  --load-images     Load images from offline/images/ via isula before deploying."
      echo "                    Without this flag, images are assumed to be already loaded."
      echo "  --help, -h        Show this help message."
      echo ""
      echo "Environment variables:"
      echo "  KC_HOSTNAME       Keycloak external URL (e.g. http://1.2.3.4:30080)"
      echo "                    Required for OIDC redirects to work correctly."
      echo "  STORAGE_CLASS     StorageClass for PostgreSQL PVC (e.g. dorado-inner-nas)"
      echo "                    Auto-detected from cluster if not set."
      echo "  PLATFORM          Force platform: amd64 or arm64 (default: auto-detect)"
      echo "  K8S_NODES         Space-separated node IPs for multi-node image loading via SSH"
      echo "                    (only used with --load-images)"
      echo "  K8S_NODE_USER     SSH user for nodes (default: root)"
      echo "  IMAGE_DIR         Remote temp dir for image tars (default: /tmp/da-images)"
      echo "  KUBECONFIG        Path to kubeconfig (default: ~/.kube/config)"
      echo ""
      echo "Examples:"
      echo "  # Deploy (images pre-loaded):"
      echo "  KC_HOSTNAME=http://80.10.79.111:30080 STORAGE_CLASS=dorado-inner-nas $0"
      echo ""
      echo "  # Load images first, then deploy:"
      echo "  KC_HOSTNAME=http://80.10.79.111:30080 $0 --load-images"
      echo ""
      echo "  # Multi-node image loading:"
      echo "  K8S_NODES=\"10.0.0.1 10.0.0.2\" KC_HOSTNAME=http://EIP:30080 $0 --load-images"
      exit 0
      ;;
    *) err "Unknown argument: $arg (use --help)" ;;
  esac
done

# ── Platform detection ────────────────────────────────────────────────────
if [ -z "${PLATFORM:-}" ]; then
  ARCH=$(uname -m)
  case "$ARCH" in
    x86_64|amd64)   PLATFORM="amd64" ;;
    aarch64|arm64)   PLATFORM="arm64" ;;
    *)               PLATFORM="amd64"; warn "Unknown arch '$ARCH', defaulting to amd64" ;;
  esac
fi
log "Platform: $PLATFORM"

# ── Pre-flight checks ─────────────────────────────────────────────────────
log "Pre-flight: checking resources..."
[ -d "$OFFLINE_DIR/charts" ] || err "Missing $OFFLINE_DIR/charts/ — run export.sh first"
[ -d "$OFFLINE_DIR/crds" ]   || err "Missing $OFFLINE_DIR/crds/ — run export.sh first"

if [ "$LOAD_IMAGES" = true ]; then
  IMAGES_DIR="$OFFLINE_DIR/images/$PLATFORM"
  [ -d "$IMAGES_DIR" ] || err "Missing $IMAGES_DIR/ — run export.sh first"
fi

for cmd in kubectl helm; do
  command -v "$cmd" &>/dev/null || err "'$cmd' not found in PATH"
done

# ── Helper: convert image name to tar filename ─────────────────────────
image_to_filename() {
  echo "$1" | sed 's|/|_|g; s|:|_|g'
}

# All application images (no httpbin)
ALL_APP_IMAGES=(
  "keycloak-proxy:v2"
  "opal-proxy:v1"
  "keycloak-init:v1"
  "keycloak-custom:26.5.2"
  "postgres:17"
  "cr.agentgateway.dev/controller:v2.2.0-main"
  "cr.agentgateway.dev/agentgateway:0.11.1"
  "permitio/opal-server:0.7.4"
  "permitio/opal-client:0.7.4"
  "nginx:alpine"
)

# ════════════════════════════════════════════════════════════════════════
# Step 1: Verify K8s cluster connectivity
# ════════════════════════════════════════════════════════════════════════
log "Step 1: Verifying K8s cluster connectivity..."
kubectl cluster-info || err "Cannot connect to K8s cluster. Check KUBECONFIG."

# ════════════════════════════════════════════════════════════════════════
# Step 2: Load images (optional, only with --load-images)
# ════════════════════════════════════════════════════════════════════════
if [ "$LOAD_IMAGES" = true ]; then
  log "Step 2: Loading images into K8s nodes (isula)..."

  if [ -n "${K8S_NODES:-}" ]; then
    # ── Multi-node: SCP tars to each node, then isula load ──────────
    for node in $K8S_NODES; do
      log "  Node: $node"
      ssh "${K8S_NODE_USER}@${node}" "mkdir -p ${IMAGE_DIR}" 2>/dev/null || true

      for img in "${ALL_APP_IMAGES[@]}"; do
        fname="$(image_to_filename "$img").tar"
        tarpath="$IMAGES_DIR/$fname"

        if [ ! -f "$tarpath" ]; then
          warn "    Image tar not found: $fname"
          continue
        fi

        log "    Loading: $img"
        scp -q "$tarpath" "${K8S_NODE_USER}@${node}:${IMAGE_DIR}/$fname"
        ssh "${K8S_NODE_USER}@${node}" "isula load -i ${IMAGE_DIR}/$fname" 2>/dev/null \
          || ssh "${K8S_NODE_USER}@${node}" "ctr -n k8s.io images import ${IMAGE_DIR}/$fname" 2>/dev/null \
          || warn "    Failed to load $img on $node"
      done

      ssh "${K8S_NODE_USER}@${node}" "rm -rf ${IMAGE_DIR}" 2>/dev/null || true
    done
  else
    # ── Single-node / local ─────────────────────────────────────────
    if command -v isula &>/dev/null; then
      for img in "${ALL_APP_IMAGES[@]}"; do
        fname="$(image_to_filename "$img").tar"
        tarpath="$IMAGES_DIR/$fname"

        if [ ! -f "$tarpath" ]; then
          warn "  Image tar not found: $fname"
          continue
        fi

        log "  Loading: $img"
        isula load -i "$tarpath" 2>/dev/null \
          || ctr -n k8s.io images import "$tarpath" 2>/dev/null \
          || warn "  Failed to load $img"
      done
    elif command -v ctr &>/dev/null; then
      for img in "${ALL_APP_IMAGES[@]}"; do
        fname="$(image_to_filename "$img").tar"
        tarpath="$IMAGES_DIR/$fname"

        if [ ! -f "$tarpath" ]; then
          warn "  Image tar not found: $fname"
          continue
        fi

        log "  Loading: $img"
        ctr -n k8s.io images import "$tarpath" 2>/dev/null \
          || warn "  Failed to load $img"
      done
    else
      err "'isula' and 'ctr' not found. Cannot load images."
    fi
  fi
else
  log "Step 2: Skipping image loading (use --load-images to load from offline/)"
fi

# ════════════════════════════════════════════════════════════════════════
# Step 3: Install Gateway API CRDs
# ════════════════════════════════════════════════════════════════════════
log "Step 3: Installing Gateway API CRDs..."
CRD_FILE="$OFFLINE_DIR/crds/gateway-api-${GATEWAY_API_VERSION}.yaml"
[ -f "$CRD_FILE" ] || err "Missing CRD file: $CRD_FILE"
kubectl apply --server-side --force-conflicts -f "$CRD_FILE"

# ════════════════════════════════════════════════════════════════════════
# Step 4: Install AgentGateway controller
# ════════════════════════════════════════════════════════════════════════
log "Step 4: Installing AgentGateway controller..."
AGENTGATEWAY_CRDS_TGZ="$OFFLINE_DIR/charts/agentgateway-crds-${AGENTGATEWAY_CHART_VERSION}.tgz"
AGENTGATEWAY_TGZ="$OFFLINE_DIR/charts/agentgateway-${AGENTGATEWAY_CHART_VERSION}.tgz"
[ -f "$AGENTGATEWAY_CRDS_TGZ" ] || err "Missing chart: $AGENTGATEWAY_CRDS_TGZ"
[ -f "$AGENTGATEWAY_TGZ" ]      || err "Missing chart: $AGENTGATEWAY_TGZ"

helm upgrade -i agentgateway-crds \
  "$AGENTGATEWAY_CRDS_TGZ" \
  --create-namespace --namespace "$AGENTGATEWAY_NS"

helm upgrade -i agentgateway \
  "$AGENTGATEWAY_TGZ" \
  --namespace "$AGENTGATEWAY_NS" \
  --set controller.image.pullPolicy=IfNotPresent \
  --set controller.image.tag=v2.2.0-main

log "  Patching AgentGateway controller for Huawei Cloud (imagePullPolicy + securityContext)..."
kubectl patch deployment agentgateway -n "$AGENTGATEWAY_NS" -p '{
  "spec":{"template":{"spec":{
    "securityContext":{"fsGroup":0,"runAsUser":0},
    "containers":[{"name":"controller","imagePullPolicy":"IfNotPresent"}]
  }}}
}' 2>/dev/null || true

log "  Waiting for AgentGateway controller to be ready..."
kubectl -n "$AGENTGATEWAY_NS" rollout status deployment/agentgateway --timeout=120s 2>/dev/null || true

# Apply Gateway resource
helm upgrade -i agentgateway-gateway \
  "$PROJECT_DIR/charts/agentgateway" \
  --namespace "$AGENTGATEWAY_NS"

# Wait for proxy pod
log "  Waiting for gateway proxy pod..."
for i in $(seq 1 30); do
  PROXY_DEPLOY=$(kubectl -n "$AGENTGATEWAY_NS" get deploy -l gateway.networking.k8s.io/gateway-name=agentgateway-proxy -o name 2>/dev/null | head -1)
  if [ -n "$PROXY_DEPLOY" ]; then
    kubectl -n "$AGENTGATEWAY_NS" patch "$PROXY_DEPLOY" \
      -p '{"spec":{"template":{"spec":{"containers":[{"name":"agentgateway","imagePullPolicy":"IfNotPresent"}]}}}}' 2>/dev/null || true
    kubectl -n "$AGENTGATEWAY_NS" rollout status "$PROXY_DEPLOY" --timeout=60s 2>/dev/null || true
    break
  fi
  sleep 2
done

# ════════════════════════════════════════════════════════════════════════
# Step 5: Install Keycloak stack
# ════════════════════════════════════════════════════════════════════════
log "Step 5: Installing Keycloak stack..."
kubectl create namespace "$KEYCLOAK_NS" --dry-run=client -o yaml | kubectl apply -f -

# Auto-detect StorageClass if not set
if [ -z "${STORAGE_CLASS:-}" ]; then
  STORAGE_CLASS=$(kubectl get sc -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [ -n "$STORAGE_CLASS" ]; then
    log "  Auto-detected StorageClass: $STORAGE_CLASS"
  else
    warn "  No StorageClass found. PostgreSQL PVC may fail to bind."
  fi
else
  log "  Using StorageClass: $STORAGE_CLASS"
fi

HELM_EXTRA_ARGS=()
if [ -n "${KC_HOSTNAME:-}" ]; then
  log "  Using KC_HOSTNAME: $KC_HOSTNAME"
  HELM_EXTRA_ARGS+=(--set "keycloak.config.hostname=$KC_HOSTNAME")
fi
if [ -n "${STORAGE_CLASS:-}" ]; then
  HELM_EXTRA_ARGS+=(--set "postgres.persistence.storageClass=$STORAGE_CLASS")
fi

helm upgrade -i keycloak \
  "$PROJECT_DIR/charts/keycloak" \
  --namespace "$KEYCLOAK_NS" \
  "${HELM_EXTRA_ARGS[@]+"${HELM_EXTRA_ARGS[@]}"}"

log "  Waiting for PostgreSQL..."
kubectl -n "$KEYCLOAK_NS" rollout status statefulset/postgres --timeout=120s

log "  Waiting for Keycloak (this may take several minutes)..."
kubectl -n "$KEYCLOAK_NS" rollout status statefulset/keycloak --timeout=600s

log "  Waiting for keycloak-init job to complete..."
kubectl -n "$KEYCLOAK_NS" wait --for=condition=complete job/keycloak-init --timeout=300s || warn "keycloak-init job not yet complete, continuing..."

log "  Restarting keycloak-proxy to pick up client secret..."
kubectl -n "$KEYCLOAK_NS" rollout restart deployment/keycloak-proxy
kubectl -n "$KEYCLOAK_NS" rollout status deployment/keycloak-proxy --timeout=120s 2>/dev/null || warn "keycloak-proxy not ready yet"

# ════════════════════════════════════════════════════════════════════════
# Step 6: Install OPA stack
# ════════════════════════════════════════════════════════════════════════
log "Step 6: Installing OPA stack..."
kubectl create namespace "$OPA_NS" --dry-run=client -o yaml | kubectl apply -f -

helm upgrade -i opa \
  "$PROJECT_DIR/charts/opa" \
  --namespace "$OPA_NS"

log "  Waiting for OPAL server..."
kubectl -n "$OPA_NS" rollout status deployment/opal-server --timeout=120s

log "  Waiting for PEP proxy..."
kubectl -n "$OPA_NS" rollout status deployment/pep-proxy --timeout=180s

# ════════════════════════════════════════════════════════════════════════
# Step 7: Apply gateway routes (no httpbin)
# ════════════════════════════════════════════════════════════════════════
log "Step 7: Applying gateway routes..."
kubectl apply -f "$PROJECT_DIR/gateway-routes/reference-grants.yaml"
kubectl apply -f "$PROJECT_DIR/gateway-routes/keycloak-routes.yaml"
kubectl apply -f "$PROJECT_DIR/gateway-routes/protected-routes.yaml"

# ════════════════════════════════════════════════════════════════════════
# Step 8: Expose gateway via NodePort
# ════════════════════════════════════════════════════════════════════════
log "Step 8: Exposing gateway on NodePort 30080..."
kubectl -n "$AGENTGATEWAY_NS" patch svc agentgateway-proxy \
  -p '{"spec":{"type":"NodePort","ports":[{"port":80,"targetPort":8080,"nodePort":30080,"protocol":"TCP"}]}}' \
  2>/dev/null || warn "Failed to patch NodePort (may already be set)"

# ── Summary ───────────────────────────────────────────────────────────────
log ""
log "==============================================="
log "da-cluster deployment complete! (Huawei Cloud K8s + isula, $PLATFORM)"
log "==============================================="
log ""
log "Pods by namespace:"
for ns in "$KEYCLOAK_NS" "$OPA_NS" "$AGENTGATEWAY_NS"; do
  log "  $ns:"
  kubectl -n "$ns" get pods --no-headers 2>/dev/null | while read line; do echo "    $line"; done
done
log ""
log "Gateway access:"
NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' 2>/dev/null || echo "<NODE_IP>")
log "  http://${NODE_IP}:30080/"
log ""
log "Quick test:"
log "  curl http://${NODE_IP}:30080/realms/master/.well-known/openid-configuration"
log ""
if [ -n "${KC_HOSTNAME:-}" ]; then
  log "KC_HOSTNAME: $KC_HOSTNAME"
else
  warn "KC_HOSTNAME not set. If Keycloak redirects break, re-run with:"
  warn "  KC_HOSTNAME=http://<EIP>:30080 STORAGE_CLASS=dorado-inner-nas $0"
fi
if [ -n "${STORAGE_CLASS:-}" ]; then
  log "StorageClass: $STORAGE_CLASS"
fi
log ""
log "Run tests:"
log "  ./scripts/test.sh"
