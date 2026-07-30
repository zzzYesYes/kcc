#!/bin/bash
# deploy.sh – Build, load, and deploy the opal-dynamic-policy stack.
#
# Usage:
#   ./deploy.sh                       # full deploy (K8s core only)
#   DEPLOY_AGENTGATEWAY=true ./deploy.sh  # also install agentgateway
#
# Environment variables:
#   NAMESPACE               – K8s namespace for the OPA stack  (default: opal-dynamic-policy)
#   VERSION                 – image tag                        (default: latest)
#   DEPLOY_AGENTGATEWAY     – install agentgateway CRDs + helm chart + routing config
#                             (default: false – requires helm + internet access)
#   AGENTGATEWAY_NAMESPACE  – namespace for the agentgateway control plane and routing
#                             (default: agentgateway-system)
#                             Set to a different value (e.g. agentgateway-system-opa) when
#                             the CRDs are already installed under another namespace.
#                             CRD installation is skipped automatically when the CRDs exist.
#   SKIP_BUILD              – skip docker build step           (default: false)

set -euo pipefail

NAMESPACE=${NAMESPACE:-"opal-dynamic-policy"}
VERSION=${VERSION:-"latest"}
DEPLOY_AGENTGATEWAY=${DEPLOY_AGENTGATEWAY:-true}
SKIP_BUILD=${SKIP_BUILD:-true}
# Namespace where the agentgateway control plane + routing config will be deployed.
# CRDs are cluster-scoped and installed only once regardless of this value.
# Set to an existing namespace if the CRDs are already installed there to avoid conflicts.
AGENTGATEWAY_NAMESPACE=${AGENTGATEWAY_NAMESPACE:-"agentgateway-system-opa"}

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

step()  { echo -e "\n${YELLOW}>>> $*${NC}"; }
info()  { echo -e "${CYAN}    $*${NC}"; }
ok()    { echo -e "${GREEN}    OK${NC}"; }

# # ---------------------------------------------------------------------------
# # Step 1: Build and load the opal-proxy image
# # ---------------------------------------------------------------------------
if [ "$SKIP_BUILD" != "true" ]; then
    step "Building opal-proxy:${VERSION} image..."
    docker build -t "opal-proxy:${VERSION}" .
    ok

    step "Loading image into Kind cluster..."
    kind load docker-image "opal-proxy:${VERSION}" 2>/dev/null \
        || info "Note: 'kind load' failed – image may already be present or cluster is not Kind."
    ok
else
    info "SKIP_BUILD=true – skipping docker build."
fi

# ---------------------------------------------------------------------------
# Step 2: Create namespace
# ---------------------------------------------------------------------------
step "Creating namespace ${NAMESPACE}..."
kubectl create namespace "${NAMESPACE}" --dry-run=client -o yaml | kubectl apply -f -
ok

# ---------------------------------------------------------------------------
# Step 3: Deploy core services (Services + Deployments)
# ---------------------------------------------------------------------------
step "Applying Services..."
kubectl apply -f k8s/service.yaml -n "${NAMESPACE}"
ok

step "Applying Deployments (PostgreSQL / OPAL Server / pep-proxy + bundle-server)..."
kubectl apply -f k8s/deployment.yaml -n "${NAMESPACE}"
ok

# ---------------------------------------------------------------------------
# Step 4: Wait for readiness
# ---------------------------------------------------------------------------
step "Waiting for PostgreSQL to be ready..."
kubectl wait --for=condition=ready pod -l app=postgres \
    -n "${NAMESPACE}" --timeout=120s
ok

step "Waiting for opal-server to be ready..."
kubectl wait --for=condition=available deployment/opal-server \
    -n "${NAMESPACE}" --timeout=90s
ok

step "Waiting for pep-proxy to be ready..."
kubectl wait --for=condition=available deployment/pep-proxy \
    -n "${NAMESPACE}" --timeout=180s
ok

# ---------------------------------------------------------------------------
# Step 5: (Optional) Deploy agentgateway
# ---------------------------------------------------------------------------
if [ "$DEPLOY_AGENTGATEWAY" = "true" ]; then

    # ── 5a: Kubernetes Gateway API CRDs (cluster-scoped, install once) ────────
    # Check whether the CRDs already exist before trying to apply them.
    # Re-applying with a different field manager causes conflicts; skipping is safe
    # because the CRD schema is the same regardless of which namespace owns the
    # agentgateway control plane.
    if kubectl get crd gateways.gateway.networking.k8s.io &>/dev/null; then
        info "Gateway API CRDs already present – skipping install."
    else
        step "Installing Kubernetes Gateway API CRDs..."
        kubectl apply -f \
            https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.4.0/standard-install.yaml
        ok
    fi

    # ── 5b: agentgateway CRDs (helm, cluster-scoped) ─────────────────────────
    # agentgateway-crds chart does ONE thing: install CRDs.
    # CRDs carry a helm ownership annotation (meta.helm.sh/release-namespace).
    # If the CRDs were installed by a release in a DIFFERENT namespace, helm will
    # refuse to "import" them into the new namespace even with --skip-crds.
    # Solution: skip the chart entirely when any agentgateway CRD already exists.
    if kubectl get crd agentgatewaybackends.agentgateway.dev &>/dev/null; then
        info "agentgateway CRDs already present in cluster (owned by another release) – skipping agentgateway-crds chart."
    else
        step "Installing agentgateway CRDs via Helm (${AGENTGATEWAY_NAMESPACE})..."
        helm upgrade -i agentgateway-crds \
            oci://ghcr.io/kgateway-dev/charts/agentgateway-crds \
            --create-namespace --namespace "${AGENTGATEWAY_NAMESPACE}" \
            --version v2.2.1
        ok
    fi

    # Ensure the target namespace exists even when agentgateway-crds was skipped
    kubectl create namespace "${AGENTGATEWAY_NAMESPACE}" --dry-run=client -o yaml \
        | kubectl apply -f -

    # ── 5c: agentgateway routing config ──────────────────────────────────────
    # Applies: ReferenceGrant + Gateway + AgentgatewayBackend +
    #          AgentgatewayPolicy + HTTPRoute.
    # The agentgateway controller sees the Gateway and creates a proxy Deployment.
    step "Applying agentgateway routing configuration..."
    kubectl apply -f k8s/agentgateway.yaml
    ok

    # ── 5d: Fix proxy Deployment image + imagePullPolicy ─────────────────────
    # The controller creates the proxy Deployment with its default image
    # (ghcr.io/kgateway-dev/agentgateway:v2.2.1) which may not exist in the
    # cluster.  Patch both image and imagePullPolicy so the kubelet uses the
    # image already present in the cluster nodes without hitting the registry.
    AGENTGATEWAY_IMAGE=${AGENTGATEWAY_IMAGE:-"cr.agentgateway.dev/agentgateway:0.11.1"}
    step "Patching proxy Deployment image → ${AGENTGATEWAY_IMAGE} (IfNotPresent)..."
    for i in $(seq 1 10); do
        kubectl get deployment agent-gateway -n "${AGENTGATEWAY_NAMESPACE}" \
            &>/dev/null && break
        sleep 3
    done
    kubectl patch deployment agent-gateway -n "${AGENTGATEWAY_NAMESPACE}" \
        --type=json \
        -p "[
          {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/image\",\"value\":\"${AGENTGATEWAY_IMAGE}\"},
          {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"IfNotPresent\"}
        ]" \
        2>/dev/null \
        || info "Note: proxy Deployment not yet created – run manually after controller reconciles:"
    info "  kubectl patch deployment agent-gateway -n ${AGENTGATEWAY_NAMESPACE} --type=json -p '[{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/image\",\"value\":\"${AGENTGATEWAY_IMAGE}\"},{\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"IfNotPresent\"}]'"
    ok

else
    info "DEPLOY_AGENTGATEWAY=false – skipping agentgateway install."
    info "To deploy agentgateway run: DEPLOY_AGENTGATEWAY=true ./deploy.sh"
    info "Prerequisites documented in k8s/agentgateway.yaml."
fi


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
step "Deployment status:"
kubectl get pods -n "${NAMESPACE}"
echo ""
kubectl get svc  -n "${NAMESPACE}"

echo ""
echo -e "${GREEN}Deployment complete!${NC}"
echo ""
echo "  PEP Proxy (HTTP)   :  http://pep-proxy.${NAMESPACE}.svc.cluster.local:8000"
echo "  PEP Proxy (gRPC)   :  pep-proxy.${NAMESPACE}.svc.cluster.local:9000"
echo "  Bundle Server      :  http://bundle-server.${NAMESPACE}.svc.cluster.local:8001"
echo "  OPAL Server        :  http://opal-server.${NAMESPACE}.svc.cluster.local:7002"
if [ "$DEPLOY_AGENTGATEWAY" = "true" ]; then
    echo "  Agent Gateway (MCP):  http://agent-gateway.${AGENTGATEWAY_NAMESPACE}.svc.cluster.local:3000"
    echo "  Agent Gateway (mgmt): http://agent-gateway.${AGENTGATEWAY_NAMESPACE}.svc.cluster.local:8080"
fi
echo ""
echo "  To run integration tests: ./test.sh"
