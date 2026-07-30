#!/usr/bin/env bash
# export-images.sh — 拉取/构建所有镜像并保存到 offline/images/
#
# 自动检测当前平台（amd64/arm64），拉取对应架构的镜像。
# --build 的镜像从源码构建，其他镜像从 registry pull。
#
# Usage:
#   ./scripts/export-images.sh          # pull + build 全部镜像
#   ./scripts/export-images.sh --pull   # 只 pull 第三方镜像（不 build）
#   ./scripts/export-images.sh --build  # 只 build 自定义镜像（不 pull）
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
AUTH_DIR="$(cd "$PROJECT_DIR/.." && pwd)"
OFFLINE_DIR="$PROJECT_DIR/offline/images"

mkdir -p "$OFFLINE_DIR"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log()  { echo -e "${GREEN}[INFO]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()  { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

ARCH=$(docker info --format '{{.Architecture}}' 2>/dev/null || uname -m)
case "$ARCH" in
  x86_64|amd64) PLATFORM="linux/amd64" ;;
  aarch64|arm64) PLATFORM="linux/arm64" ;;
  *) PLATFORM="linux/$ARCH" ;;
esac
log "Detected platform: $PLATFORM ($ARCH)"

# ── Image lists ──────────────────────────────────────────────────────────

# Third-party images to pull from registry
PULL_IMAGES=(
  "postgres:17"
  "mccutchen/go-httpbin:2.24.0"
  "cr.agentgateway.dev/controller:v2.2.0-main"
  "cr.agentgateway.dev/agentgateway:0.11.1"
  "permitio/opal-server:0.7.4"
  "permitio/opal-client:0.7.4"
  "nginx:alpine"
  "kindest/node:v1.35.0"
)

# Custom images to build from source
BUILD_IMAGES=(
  "keycloak-proxy:v2"
  "opal-proxy:v1"
  "keycloak-init:v1"
  "keycloak-custom:26.5.2"
)

# ── Helpers ──────────────────────────────────────────────────────────────

image_to_filename() {
  echo "$1" | sed 's|/|_|g; s|:|_|g'
}

save_image() {
  local img="$1"
  local fname
  fname="$(image_to_filename "$img").tar"
  log "  Saving $img -> $fname"
  docker save -o "$OFFLINE_DIR/$fname" "$img"
  local size
  size=$(du -h "$OFFLINE_DIR/$fname" | cut -f1)
  log "  Saved ($size)"
}

# ── Parse arguments ──────────────────────────────────────────────────────

DO_PULL=true
DO_BUILD=true
for arg in "$@"; do
  case "$arg" in
    --pull)  DO_BUILD=false ;;
    --build) DO_PULL=false ;;
    --help|-h)
      echo "Usage: $0 [--pull] [--build]"
      echo ""
      echo "  (default)  Pull third-party images + build custom images, save all to offline/"
      echo "  --pull     Only pull and save third-party images"
      echo "  --build    Only build and save custom images"
      echo ""
      echo "Images are pulled for the current platform: $PLATFORM"
      exit 0
      ;;
    *) err "Unknown argument: $arg" ;;
  esac
done

# ── Pull third-party images ─────────────────────────────────────────────

if [ "$DO_PULL" = true ]; then
  log "Pulling third-party images (platform: $PLATFORM)..."
  for img in "${PULL_IMAGES[@]}"; do
    log "  Pulling $img..."
    if docker pull --platform "$PLATFORM" "$img" 2>/dev/null; then
      save_image "$img"
    else
      warn "  Failed to pull $img (may need VPN or mirror)"
    fi
  done
fi

# ── Build custom images ─────────────────────────────────────────────────

if [ "$DO_BUILD" = true ]; then
  log "Building custom images..."

  # keycloak-proxy:v2
  log "  Building keycloak-proxy:v2..."
  PROXY_BUILD_DIR=$(mktemp -d)
  cp -r "$AUTH_DIR/da-idb-proxy/app" "$PROXY_BUILD_DIR/app"
  cp "$PROJECT_DIR/images/keycloak-proxy/Dockerfile" "$PROXY_BUILD_DIR/Dockerfile"
  docker build -t keycloak-proxy:v2 "$PROXY_BUILD_DIR"
  rm -rf "$PROXY_BUILD_DIR"
  save_image "keycloak-proxy:v2"

  # opal-proxy:v1
  log "  Building opal-proxy:v1..."
  OPAL_BUILD_DIR=$(mktemp -d)
  cp "$PROJECT_DIR/images/opal-proxy/Dockerfile" "$OPAL_BUILD_DIR/Dockerfile"
  cp "$PROJECT_DIR/images/opal-proxy/supervisord.conf" "$OPAL_BUILD_DIR/supervisord.conf"
  cp "$PROJECT_DIR/images/opal-proxy/requirements.txt" "$OPAL_BUILD_DIR/requirements.txt"
  cp -r "$AUTH_DIR/opal-dynamic-policy/pep-proxy" "$OPAL_BUILD_DIR/pep-proxy"
  cp -r "$AUTH_DIR/opal-dynamic-policy/bundle-server" "$OPAL_BUILD_DIR/bundle-server"
  cp -r "$AUTH_DIR/opal-dynamic-policy/data" "$OPAL_BUILD_DIR/data"
  docker build -t opal-proxy:v1 "$OPAL_BUILD_DIR"
  rm -rf "$OPAL_BUILD_DIR"
  save_image "opal-proxy:v1"

  # keycloak-init:v1
  log "  Building keycloak-init:v1..."
  docker build -t keycloak-init:v1 "$PROJECT_DIR/images/keycloak-init"
  save_image "keycloak-init:v1"

  # keycloak-custom:26.5.2
  log "  Building keycloak-custom:26.5.2..."
  docker build -t keycloak-custom:26.5.2 "$PROJECT_DIR/images/keycloak-custom"
  save_image "keycloak-custom:26.5.2"
fi

# ── Summary ──────────────────────────────────────────────────────────────

log ""
log "========================================="
log "Export complete! Platform: $PLATFORM"
log "========================================="
log ""
log "Offline images:"
ls -lh "$OFFLINE_DIR/" | awk 'NR>1 {printf "  %-50s %s\n", $9, $5}'
log ""
TOTAL_SIZE=$(du -sh "$OFFLINE_DIR" | cut -f1)
log "Total size: $TOTAL_SIZE"
log ""
log "To deploy on target server:"
log "  1. Copy da-cluster/ to server"
log "  2. Run: ./scripts/setup.sh            (create Kind + deploy)"
log "  3. Or:  ./scripts/setup.sh --existing-kind  (use existing cluster)"
