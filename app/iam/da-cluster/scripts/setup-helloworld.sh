#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"
KIND_NODE_IMAGE="${KIND_NODE_IMAGE:-kindest/node:v1.35.0}"
GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-v1.4.0}"
AGENTGATEWAY_CHART_VERSION="${AGENTGATEWAY_CHART_VERSION:-v2.2.1}"
AGENTGATEWAY_NS="agentgateway-system"

log() {
  printf '[INFO] %s\n' "$*"
}

fail() {
  printf '[ERROR] %s\n' "$*" >&2
  exit 1
}

for cmd in docker kind kubectl helm; do
  command -v "$cmd" >/dev/null 2>&1 || fail "'$cmd' is required"
done

docker info >/dev/null 2>&1 || fail "Docker is not running"

if kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME"; then
  log "Reusing Kind cluster '$CLUSTER_NAME'"
else
  log "Creating Kind cluster '$CLUSTER_NAME'"
  docker pull "$KIND_NODE_IMAGE"
  kind create cluster \
    --name "$CLUSTER_NAME" \
    --image "$KIND_NODE_IMAGE" \
    --config "$PROJECT_DIR/kind-config.yaml" \
    --wait 120s
fi

kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null

log "Installing Gateway API CRDs $GATEWAY_API_VERSION"
kubectl apply --server-side --force-conflicts -f \
  "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GATEWAY_API_VERSION}/standard-install.yaml"

log "Installing AgentGateway $AGENTGATEWAY_CHART_VERSION"
helm upgrade --install agentgateway-crds \
  oci://ghcr.io/kgateway-dev/charts/agentgateway-crds \
  --version "$AGENTGATEWAY_CHART_VERSION" \
  --namespace "$AGENTGATEWAY_NS" \
  --create-namespace

helm upgrade --install agentgateway \
  oci://ghcr.io/kgateway-dev/charts/agentgateway \
  --version "$AGENTGATEWAY_CHART_VERSION" \
  --namespace "$AGENTGATEWAY_NS"

kubectl -n "$AGENTGATEWAY_NS" rollout status deployment/agentgateway --timeout=180s

log "Creating the Gateway"
helm upgrade --install agentgateway-gateway \
  "$PROJECT_DIR/charts/agentgateway" \
  --namespace "$AGENTGATEWAY_NS"

log "Deploying httpbin"
kubectl apply -f "$PROJECT_DIR/gateway-routes/httpbin-test.yaml"
kubectl -n httpbin rollout status deployment/httpbin --timeout=180s

log "Creating the public /hello route"
kubectl apply -f "$PROJECT_DIR/gateway-routes/helloworld-route.yaml"

log "Waiting for the Gateway proxy"
for _ in $(seq 1 60); do
  if kubectl -n "$AGENTGATEWAY_NS" get service agentgateway-proxy >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
kubectl -n "$AGENTGATEWAY_NS" get service agentgateway-proxy >/dev/null 2>&1 \
  || fail "Gateway proxy Service was not created"

log "Deployment complete"
log "Run: $PROJECT_DIR/scripts/test-helloworld.sh"
