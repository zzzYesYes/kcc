#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# setup.sh — Air-gapped deployment of da-cluster unified auth system
#
# Prerequisites on the target Linux server:
#   - docker (with containerd)
#   - kubectl
#   - helm 3
#   - kind (only for Kind mode)
#   - ctr  (only for K8s mode, usually bundled with containerd)
#
# All images, Helm charts, and CRDs are loaded from offline/ directory.
# NO internet access required (unless --build is used).
#
# Usage:
#   ./scripts/setup.sh              # Kind mode (default): create Kind cluster
#   ./scripts/setup.sh --no-kind    # K8s mode: deploy to existing cluster
#   ./scripts/setup.sh --build      # Rebuild keycloak-proxy & opal-proxy from source
#   ./scripts/setup.sh --build --no-kind  # Combine both flags
#   CLUSTER_NAME=xxx ./scripts/setup.sh  # Use custom Kind cluster name (in Kind mode)
#
# Environment variables:
#   CLUSTER_NAME   — Kind cluster name (default: da-cluster)
#   KUBECONFIG     — path to kubeconfig (K8s mode, default: ~/.kube/config)
#   K8S_NODES      — space-separated list of node IPs for image loading
#                    (K8s mode, e.g. "192.168.1.10 192.168.1.11")
#   K8S_NODE_USER  — SSH user for nodes (K8s mode, default: root)
#   IMAGE_DIR      — remote path to copy image tars (K8s mode, default: /tmp/da-images)
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
OFFLINE_DIR="$PROJECT_DIR/offline"

CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"
KEYCLOAK_NS="keycloak"
OPA_NS="opa"
AGENTGATEWAY_NS="agentgateway-system"
HTTPBIN_NS="httpbin"

KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-}"
AGENTGATEWAY_CHART_VERSION="v2.2.1"
GATEWAY_API_VERSION="v1.4.0"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ── Parse arguments ─────────────────────────────────────────────────────
USE_KIND=true
USE_BUILD=false
EXISTING_KIND=false
USE_FAT_BASE=false
for arg in "$@"; do
  case "$arg" in
    --no-kind)        USE_KIND=false ;;
    --build)          USE_BUILD=true ;;
    --existing-kind)  EXISTING_KIND=true ;;
    --fat-base)       USE_FAT_BASE=true ;;
    --help|-h)
      echo "Usage: $0 [--no-kind] [--build] [--fat-base] [--existing-kind]"
      echo ""
      echo "  (default)         Kind mode: create Kind cluster, load images via docker, deploy"
      echo "  --no-kind         K8s mode: deploy to existing K8s cluster (no Kind)"
      echo "  --build           Rebuild custom images from source (requires docker + internet)"
      echo "  --fat-base        Use fat base images for code-only rebuild (no network needed)"
      echo "                    Base images must be pre-loaded; only copies code into them."
      echo "  --existing-kind   Use existing Kind cluster: skip cluster creation, import"
      echo "                    offline tars directly into Kind containerd (no docker build)"
      echo ""
      echo "Environment variables:"
      echo "  CLUSTER_NAME   Kind cluster name (default: da-cluster)"
      echo "  PLATFORM       Force platform: amd64 or arm64 (default: auto-detect)"
      echo "  K8S_NODES      space-separated node IPs (K8s mode, for image loading via SSH)"
      echo "  K8S_NODE_USER  SSH user for nodes (default: root)"
      echo "  IMAGE_DIR      remote temp dir for images (default: /tmp/da-images)"
      echo ""
      echo "Examples:"
      echo "  ./scripts/setup.sh                              # Create Kind cluster + deploy"
      echo "  ./scripts/setup.sh --build                      # Build images from source + deploy"
      echo "  ./scripts/setup.sh --fat-base                   # Rebuild from fat base (air-gapped)"
      echo "  ./scripts/setup.sh --existing-kind              # Deploy to existing Kind cluster"
      echo "  PLATFORM=arm64 ./scripts/setup.sh --no-kind     # Deploy arm64 to existing K8s"
      exit 0
      ;;
    *) err "Unknown argument: $arg (use --help)" ;;
  esac
done

K8S_NODE_USER="${K8S_NODE_USER:-root}"
IMAGE_DIR="${IMAGE_DIR:-/tmp/da-images}"

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
log "Pre-flight: checking offline resources..."
IMAGES_DIR="$OFFLINE_DIR/images/$PLATFORM"
[ -d "$IMAGES_DIR" ] || err "Missing $IMAGES_DIR/ — run export.sh or copy platform images first"
[ -d "$OFFLINE_DIR/charts" ] || err "Missing $OFFLINE_DIR/charts/ — run export.sh first"
[ -d "$OFFLINE_DIR/crds" ]   || err "Missing $OFFLINE_DIR/crds/ — run export.sh first"

REQUIRED_CMDS=(kubectl helm)
if [ "$EXISTING_KIND" = true ]; then
  REQUIRED_CMDS+=(docker)
  log "Mode: --existing-kind (use existing Kind cluster, import offline tars)"
elif [ "$USE_KIND" = true ]; then
  REQUIRED_CMDS+=(docker kind)
else
  log "Mode: K8s (--no-kind)"
fi
if [ "$USE_BUILD" = true ]; then
  if [ "$EXISTING_KIND" = true ]; then
    err "--build and --existing-kind are mutually exclusive"
  fi
  if [[ ! " ${REQUIRED_CMDS[*]} " =~ " docker " ]]; then
    REQUIRED_CMDS+=(docker)
  fi
  log "Mode: --build (will rebuild custom images from source)"
fi
if [ "$USE_FAT_BASE" = true ]; then
  if [[ ! " ${REQUIRED_CMDS[*]} " =~ " docker " ]]; then
    REQUIRED_CMDS+=(docker)
  fi
  log "Mode: --fat-base (code-only rebuild from base images, no network needed)"
fi
for cmd in "${REQUIRED_CMDS[@]}"; do
  command -v "$cmd" &>/dev/null || err "'$cmd' not found in PATH"
done

# ── Helper: convert image name to tar filename ─────────────────────────
image_to_filename() {
  echo "$1" | sed 's|/|_|g; s|:|_|g'
}

# All application images to load
ALL_APP_IMAGES=(
  "keycloak-proxy:v2"
  "opal-proxy:v1"
  "keycloak-init:v1"
  "keycloak-custom:26.5.2"
  "postgres:17"
  "mccutchen/go-httpbin:2.24.0"
  "cr.agentgateway.dev/controller:v2.2.0-main"
  "cr.agentgateway.dev/agentgateway:0.11.1"
  "permitio/opal-server:0.7.4"
  "permitio/opal-client:0.7.4"
  "nginx:alpine"
)

# ── Source directories (for --build mode) ────────────────────────────────
AUTH_DIR="$(cd "$PROJECT_DIR/.." && pwd)"

# Images that --build will rebuild from source (others still use offline tar)
BUILD_IMAGES=("keycloak-proxy:v2" "opal-proxy:v1" "keycloak-init:v1" "keycloak-custom:26.5.2")

FAT_BASE_IMAGES=("keycloak-proxy:v2" "opal-proxy:v1" "keycloak-init:v1")

is_build_image() {
  local img="$1"
  for bi in "${BUILD_IMAGES[@]}"; do
    [ "$img" = "$bi" ] && return 0
  done
  return 1
}

is_fat_base_image() {
  local img="$1"
  for fi in "${FAT_BASE_IMAGES[@]}"; do
    [ "$img" = "$fi" ] && return 0
  done
  return 1
}

# ════════════════════════════════════════════════════════════════════════
# Step 0a (--fat-base only): Rebuild from fat base images (no network)
# ════════════════════════════════════════════════════════════════════════

if [ "$USE_FAT_BASE" = true ]; then
  log "Step 0: Building custom images from fat base (no network)..."

  # Ensure base images are loaded
  for base_img in "base-keycloak-proxy:v1" "base-opal-proxy:v1" "base-keycloak-init:v1"; do
    base_fname="$(image_to_filename "$base_img").tar"
    base_tar="$IMAGES_DIR/$base_fname"
    if ! docker image inspect "$base_img" &>/dev/null; then
      if [ -f "$base_tar" ]; then
        log "  Loading base image: $base_img"
        docker load -i "$base_tar" 2>/dev/null || true
      else
        err "Base image '$base_img' not found and tar missing: $base_tar"
      fi
    fi
  done

  log "  Building keycloak-proxy:v2 (slim, from base)..."
  PROXY_BUILD_DIR=$(mktemp -d)
  cp -r "$AUTH_DIR/da-idb-proxy/app" "$PROXY_BUILD_DIR/app"
  cp "$PROJECT_DIR/images/keycloak-proxy/Dockerfile.slim" "$PROXY_BUILD_DIR/Dockerfile"
  docker build -t keycloak-proxy:v2 --build-arg BASE_IMAGE=base-keycloak-proxy:v1 "$PROXY_BUILD_DIR"
  rm -rf "$PROXY_BUILD_DIR"

  log "  Building opal-proxy:v1 (slim, from base)..."
  OPAL_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/opal-proxy/Dockerfile.slim" "$OPAL_BUILD_DIR/Dockerfile"
  cp "$PROJECT_DIR/images/opal-proxy/supervisord.conf" "$OPAL_BUILD_DIR/supervisord.conf"
  cp -r "$AUTH_DIR/opal-dynamic-policy/pep-proxy" "$OPAL_BUILD_DIR/pep-proxy"
  cp -r "$AUTH_DIR/opal-dynamic-policy/bundle-server" "$OPAL_BUILD_DIR/bundle-server"
  cp -r "$AUTH_DIR/opal-dynamic-policy/data" "$OPAL_BUILD_DIR/data"
  docker build -t opal-proxy:v1 --build-arg BASE_IMAGE=base-opal-proxy:v1 "$OPAL_BUILD_DIR"
  rm -rf "$OPAL_BUILD_DIR"

  log "  Building keycloak-init:v1 (slim, from base)..."
  INIT_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/keycloak-init/Dockerfile.slim" "$INIT_BUILD_DIR/Dockerfile"
  cp "$PROJECT_DIR/images/keycloak-init/init-keycloak.py" "$INIT_BUILD_DIR/init-keycloak.py"
  docker build -t keycloak-init:v1 --build-arg BASE_IMAGE=base-keycloak-init:v1 "$INIT_BUILD_DIR"
  rm -rf "$INIT_BUILD_DIR"

  log "  Fat-base build complete (keycloak-custom uses offline tar, no rebuild needed)"
fi

# ════════════════════════════════════════════════════════════════════════
# Step 0b (--build only): Rebuild custom images from source
# ════════════════════════════════════════════════════════════════════════

if [ "$USE_BUILD" = true ]; then
  log "Step 0: Building custom images from source..."

  log "  Building keycloak-proxy:v2 from $AUTH_DIR/da-idb-proxy..."
  PROXY_BUILD_DIR=$(mktemp -d)
  cp -r "$AUTH_DIR/da-idb-proxy/app" "$PROXY_BUILD_DIR/app"
  cp "$PROJECT_DIR/images/keycloak-proxy/Dockerfile" "$PROXY_BUILD_DIR/Dockerfile"
  docker build -t keycloak-proxy:v2 "$PROXY_BUILD_DIR"
  rm -rf "$PROXY_BUILD_DIR"

  log "  Building opal-proxy:v1 from $AUTH_DIR/opal-dynamic-policy..."
  OPAL_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/opal-proxy/Dockerfile" "$OPAL_BUILD_DIR/Dockerfile"
  cp "$PROJECT_DIR/images/opal-proxy/supervisord.conf" "$OPAL_BUILD_DIR/supervisord.conf"
  cp "$PROJECT_DIR/images/opal-proxy/requirements.txt" "$OPAL_BUILD_DIR/requirements.txt"
  cp -r "$AUTH_DIR/opal-dynamic-policy/pep-proxy" "$OPAL_BUILD_DIR/pep-proxy"
  cp -r "$AUTH_DIR/opal-dynamic-policy/bundle-server" "$OPAL_BUILD_DIR/bundle-server"
  cp -r "$AUTH_DIR/opal-dynamic-policy/data" "$OPAL_BUILD_DIR/data"
  docker build -t opal-proxy:v1 "$OPAL_BUILD_DIR"
  rm -rf "$OPAL_BUILD_DIR"

  log "  Building keycloak-init:v1 from $PROJECT_DIR/images/keycloak-init..."
  docker build -t keycloak-init:v1 "$PROJECT_DIR/images/keycloak-init"

  log "  Building keycloak-custom:26.5.2 from $PROJECT_DIR/images/keycloak-custom..."
  docker build -t keycloak-custom:26.5.2 "$PROJECT_DIR/images/keycloak-custom"

  log "  Custom images built successfully"
fi

# ════════════════════════════════════════════════════════════════════════
# Step 1 & 2: Cluster creation + Image loading (differs by mode)
# ════════════════════════════════════════════════════════════════════════

if [ "$USE_KIND" = true ] || [ "$EXISTING_KIND" = true ]; then
  # ── Kind Mode ────────────────────────────────────────────────────────
  CONTROL_PLANE="${CLUSTER_NAME}-control-plane"

  if [ "$EXISTING_KIND" = true ]; then
    # --existing-kind: skip cluster creation, verify it exists
    log "Step 1: Using existing Kind cluster '$CLUSTER_NAME'..."
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTROL_PLANE}$"; then
      err "Kind cluster '$CLUSTER_NAME' not found (container '${CONTROL_PLANE}' not running)"
    fi
    kubectl cluster-info --context "kind-${CLUSTER_NAME}" 2>/dev/null \
      || kubectl cluster-info \
      || err "Cannot connect to Kind cluster"
  else
    # Default Kind mode: create cluster
    log "Step 1: Creating Kind cluster '$CLUSTER_NAME'..."

    # Try loading Kind node image from offline tar
    KIND_NODE_TAR=$(find "$IMAGES_DIR" -name 'kindest_node_*.tar' 2>/dev/null | head -1 || true)
    if [ -n "$KIND_NODE_TAR" ] && [ -f "$KIND_NODE_TAR" ]; then
      log "  Loading Kind node image from $KIND_NODE_TAR..."
      docker load -i "$KIND_NODE_TAR" 2>/dev/null || true
    fi

    # Auto-detect Kind node image if not set
    if [ -z "$KIND_NODE_IMAGE" ]; then
      AVAILABLE_IMAGES=($(docker images --format '{{.Repository}}:{{.Tag}}' | grep '^kindest/node:' || true))
      # sort by version descending (fallback if sort -V not available)
      if [ ${#AVAILABLE_IMAGES[@]} -gt 1 ]; then
        AVAILABLE_IMAGES=($(printf '%s\n' "${AVAILABLE_IMAGES[@]}" | sort -t: -k2 -rV 2>/dev/null || printf '%s\n' "${AVAILABLE_IMAGES[@]}" | sort -r))
      fi
      if [ ${#AVAILABLE_IMAGES[@]} -eq 0 ]; then
        err "No kindest/node image found. Pull one first: docker pull kindest/node:v1.31.6"
      elif [ ${#AVAILABLE_IMAGES[@]} -eq 1 ]; then
        KIND_NODE_IMAGE="${AVAILABLE_IMAGES[0]}"
        log "  Using Kind node image: $KIND_NODE_IMAGE"
      else
        echo -e "${YELLOW}Available Kind node images:${NC}"
        for i in "${!AVAILABLE_IMAGES[@]}"; do
          echo "  $((i+1)). ${AVAILABLE_IMAGES[$i]}"
        done
        echo -n "Select [1]: "
        read -r CHOICE
        CHOICE=${CHOICE:-1}
        KIND_NODE_IMAGE="${AVAILABLE_IMAGES[$((CHOICE-1))]}"
        log "  Selected: $KIND_NODE_IMAGE"
      fi
    fi

    if kind get clusters 2>/dev/null | grep -q "^${CLUSTER_NAME}$"; then
      warn "Cluster '$CLUSTER_NAME' already exists, skipping creation"
    else
      kind create cluster \
        --name "$CLUSTER_NAME" \
        --image "$KIND_NODE_IMAGE" \
        --config "$PROJECT_DIR/kind-config.yaml" \
        --wait 60s
    fi
    kubectl cluster-info --context "kind-${CLUSTER_NAME}"
  fi

  log "Step 2: Loading images into Kind cluster..."
  for img in "${ALL_APP_IMAGES[@]}"; do
    # --build or --fat-base images: already in docker, load directly into Kind
    if { [ "$USE_BUILD" = true ] && is_build_image "$img"; } || \
       { [ "$USE_FAT_BASE" = true ] && is_fat_base_image "$img"; }; then
      log "  Loading (built): $img"
      kind load docker-image "$img" --name "$CLUSTER_NAME" 2>/dev/null \
        || warn "    Failed to load $img into Kind"
      continue
    fi

    fname="$(image_to_filename "$img").tar"
    tarpath="$IMAGES_DIR/$fname"
    if [ ! -f "$tarpath" ]; then
      warn "  Image tar not found: $fname"
      continue
    fi

    log "  Loading: $img"
    # Try docker load + kind load first; fallback to direct ctr import
    if docker load -i "$tarpath" 2>/dev/null | grep -q "Loaded"; then
      kind load docker-image "$img" --name "$CLUSTER_NAME" 2>/dev/null \
        || docker exec -i "$CONTROL_PLANE" ctr --namespace=k8s.io images import - < "$tarpath" 2>/dev/null \
        || warn "    Failed to load $img into Kind"
    else
      docker exec -i "$CONTROL_PLANE" ctr --namespace=k8s.io images import - < "$tarpath" 2>/dev/null \
        || warn "    Failed to ctr-import $img"
    fi
  done

else
  # ── K8s Mode ─────────────────────────────────────────────────────────
  log "Step 1: Skipping cluster creation (--no-kind, using existing K8s cluster)"
  kubectl cluster-info || err "Cannot connect to K8s cluster. Check KUBECONFIG."

  log "Step 2: Loading images into K8s nodes..."

  # For --build or --fat-base images in K8s mode, save to temp tar first
  if [ "$USE_BUILD" = true ] || [ "$USE_FAT_BASE" = true ]; then
    BUILD_TMP_DIR=$(mktemp -d)
    for img in "${BUILD_IMAGES[@]}"; do
      fname="$(image_to_filename "$img").tar"
      log "  Saving built image $img to temp tar..."
      docker save -o "$BUILD_TMP_DIR/$fname" "$img"
    done
  fi

  if [ -n "${K8S_NODES:-}" ]; then
    # ── Multi-node: SCP tars to each node, then ctr import ────────────
    for node in $K8S_NODES; do
      log "  Node: $node"
      ssh "${K8S_NODE_USER}@${node}" "mkdir -p ${IMAGE_DIR}" 2>/dev/null || true

      for img in "${ALL_APP_IMAGES[@]}"; do
        fname="$(image_to_filename "$img").tar"

        # --build images use temp tar; others use offline tar
        if [ "$USE_BUILD" = true ] && is_build_image "$img"; then
          tarpath="$BUILD_TMP_DIR/$fname"
        elif [ "$USE_FAT_BASE" = true ] && is_fat_base_image "$img"; then
          # fat-base built images: save to temp tar for SCP/ctr import
          tarpath="$BUILD_TMP_DIR/$fname"
          if [ ! -f "$tarpath" ]; then
            docker save -o "$tarpath" "$img" 2>/dev/null
          fi
        else
          tarpath="$IMAGES_DIR/$fname"
        fi

        if [ ! -f "$tarpath" ]; then
          warn "    Image tar not found: $fname"
          continue
        fi

        log "    Loading: $img"
        scp -q "$tarpath" "${K8S_NODE_USER}@${node}:${IMAGE_DIR}/$fname"
        ssh "${K8S_NODE_USER}@${node}" "ctr -n k8s.io images import ${IMAGE_DIR}/$fname" 2>/dev/null \
          || warn "    Failed to import $img on $node"
      done

      # Clean up remote temp files
      ssh "${K8S_NODE_USER}@${node}" "rm -rf ${IMAGE_DIR}" 2>/dev/null || true
    done
  else
    # ── Single-node / local: ctr import directly ──────────────────────
    if command -v ctr &>/dev/null; then
      for img in "${ALL_APP_IMAGES[@]}"; do
        fname="$(image_to_filename "$img").tar"

        if [ "$USE_BUILD" = true ] && is_build_image "$img"; then
          tarpath="$BUILD_TMP_DIR/$fname"
        elif [ "$USE_FAT_BASE" = true ] && is_fat_base_image "$img"; then
          # fat-base built images: save to temp tar for SCP/ctr import
          tarpath="$BUILD_TMP_DIR/$fname"
          if [ ! -f "$tarpath" ]; then
            docker save -o "$tarpath" "$img" 2>/dev/null
          fi
        else
          tarpath="$IMAGES_DIR/$fname"
        fi

        if [ ! -f "$tarpath" ]; then
          warn "  Image tar not found: $fname"
          continue
        fi

        log "  Loading: $img"
        ctr -n k8s.io images import "$tarpath" 2>/dev/null \
          || warn "  Failed to import $img"
      done
    else
      warn "  'ctr' not found and K8S_NODES not set."
      warn "  Please load images manually into containerd on all nodes:"
      warn "    ctr -n k8s.io images import <image>.tar"
    fi
  fi

  # Clean up temp build tars
  if { [ "$USE_BUILD" = true ] || [ "$USE_FAT_BASE" = true ]; } && [ -d "${BUILD_TMP_DIR:-}" ]; then
    rm -rf "$BUILD_TMP_DIR"
  fi
fi

# ════════════════════════════════════════════════════════════════════════
# Steps 3-8: Common for both Kind and K8s
# ════════════════════════════════════════════════════════════════════════

# ── Step 3: Install Gateway API CRDs (from local file) ───────────────────
log "Step 3: Installing Gateway API CRDs (offline)..."
CRD_FILE="$OFFLINE_DIR/crds/gateway-api-${GATEWAY_API_VERSION}.yaml"
[ -f "$CRD_FILE" ] || err "Missing CRD file: $CRD_FILE"
kubectl apply --server-side --force-conflicts -f "$CRD_FILE"

# ── Step 4: Install AgentGateway controller (from local charts) ──────────
log "Step 4: Installing AgentGateway controller (offline)..."
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

log "  Waiting for AgentGateway controller to be ready..."
kubectl -n "$AGENTGATEWAY_NS" rollout status deployment/agentgateway --timeout=120s 2>/dev/null || true

# Apply Gateway resource (local chart)
helm upgrade -i agentgateway-gateway \
  "$PROJECT_DIR/charts/agentgateway" \
  --namespace "$AGENTGATEWAY_NS"

# Wait for proxy pod to be created by the controller
log "  Waiting for gateway proxy pod..."
for i in $(seq 1 30); do
  PROXY_DEPLOY=$(kubectl -n "$AGENTGATEWAY_NS" get deploy -l gateway.networking.k8s.io/gateway-name=agentgateway-proxy -o name 2>/dev/null | head -1)
  if [ -n "$PROXY_DEPLOY" ]; then
    # Patch proxy to use local image
    kubectl -n "$AGENTGATEWAY_NS" patch "$PROXY_DEPLOY" \
      -p '{"spec":{"template":{"spec":{"containers":[{"name":"agentgateway","imagePullPolicy":"IfNotPresent"}]}}}}' 2>/dev/null || true
    kubectl -n "$AGENTGATEWAY_NS" rollout status "$PROXY_DEPLOY" --timeout=60s 2>/dev/null || true
    break
  fi
  sleep 2
done

# ── Step 5: Install Keycloak stack ────────────────────────────────────────
log "Step 5: Installing Keycloak stack..."
kubectl create namespace "$KEYCLOAK_NS" --dry-run=client -o yaml | kubectl apply -f -

helm upgrade -i keycloak \
  "$PROJECT_DIR/charts/keycloak" \
  --namespace "$KEYCLOAK_NS"

log "  Waiting for PostgreSQL..."
kubectl -n "$KEYCLOAK_NS" rollout status statefulset/postgres --timeout=120s

log "  Waiting for Keycloak (this may take several minutes)..."
kubectl -n "$KEYCLOAK_NS" rollout status statefulset/keycloak --timeout=600s

# Wait for init job
log "  Waiting for keycloak-init job to complete..."
kubectl -n "$KEYCLOAK_NS" wait --for=condition=complete job/keycloak-init --timeout=300s || warn "keycloak-init job not yet complete, continuing..."

log "  Restarting keycloak-proxy to pick up client secret..."
kubectl -n "$KEYCLOAK_NS" rollout restart deployment/keycloak-proxy
kubectl -n "$KEYCLOAK_NS" rollout status deployment/keycloak-proxy --timeout=120s 2>/dev/null || warn "keycloak-proxy not ready yet"

# ── Step 6: Install OPA stack ─────────────────────────────────────────────
log "Step 6: Installing OPA stack..."
kubectl create namespace "$OPA_NS" --dry-run=client -o yaml | kubectl apply -f -

helm upgrade -i opa \
  "$PROJECT_DIR/charts/opa" \
  --namespace "$OPA_NS"

log "  Waiting for OPAL server..."
kubectl -n "$OPA_NS" rollout status deployment/opal-server --timeout=120s

log "  Waiting for PEP proxy..."
kubectl -n "$OPA_NS" rollout status deployment/pep-proxy --timeout=180s

# ── Step 7: Deploy httpbin test backend ───────────────────────────────────
log "Step 7: Deploying httpbin test backend..."
kubectl apply -f "$PROJECT_DIR/gateway-routes/httpbin-test.yaml"
kubectl -n "$HTTPBIN_NS" rollout status deployment/httpbin --timeout=60s 2>/dev/null || warn "httpbin not ready yet"

# ── Step 8: Apply gateway routes ──────────────────────────────────────────
log "Step 8: Applying gateway routes..."
kubectl apply -f "$PROJECT_DIR/gateway-routes/reference-grants.yaml"
kubectl apply -f "$PROJECT_DIR/gateway-routes/keycloak-routes.yaml"
kubectl apply -f "$PROJECT_DIR/gateway-routes/protected-routes.yaml"

# ── Summary ───────────────────────────────────────────────────────────────
log ""
log "==============================================="
MODE_DESC=""
[ "$USE_KIND" = true ] && MODE_DESC="Kind" || MODE_DESC="K8s"
[ "$USE_BUILD" = true ] && MODE_DESC="$MODE_DESC + build"
[ "$USE_FAT_BASE" = true ] && MODE_DESC="$MODE_DESC + fat-base"
MODE_DESC="$MODE_DESC ($PLATFORM)"
log "da-cluster deployment complete! ($MODE_DESC mode)"
log "==============================================="
log ""
log "Pods by namespace:"
for ns in "$KEYCLOAK_NS" "$OPA_NS" "$AGENTGATEWAY_NS" "$HTTPBIN_NS"; do
  log "  $ns:"
  kubectl -n "$ns" get pods --no-headers 2>/dev/null | while read line; do echo "    $line"; done
done
log ""
if [ "$USE_KIND" = true ]; then
  log "To access via port-forward:"
  log "  kubectl -n $AGENTGATEWAY_NS port-forward svc/agentgateway-proxy 8080:80 &"
else
  log "To access the gateway:"
  log "  Option 1 (port-forward): kubectl -n $AGENTGATEWAY_NS port-forward svc/agentgateway-proxy 8080:80 --address 0.0.0.0 &"
  log "  Option 2 (NodePort):     kubectl -n $AGENTGATEWAY_NS patch svc agentgateway-proxy -p '{\"spec\":{\"type\":\"NodePort\",\"ports\":[{\"port\":80,\"nodePort\":30080}]}}'"
fi
log "  curl http://localhost:8080/realms/master/.well-known/openid-configuration"
log ""
log "Run tests:"
log "  ./scripts/test.sh"
