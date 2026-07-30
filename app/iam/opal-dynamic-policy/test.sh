#!/bin/bash
# test.sh – Integration tests for the opal-dynamic-policy stack.
#
# Usage:
#   ./test.sh [pep-proxy-url] [bundle-server-url]
#
# The script manages kubectl port-forward automatically.
# Set AUTO_PORT_FORWARD=false to use pre-existing port-forwards.
#
# Environment variables:
#   TENANT_ID               – tenant used in tests        (default: tenant-test-001)
#   JWT_SECRET              – HS256 secret matching deploy (default: jwt-secret)
#   NAMESPACE               – K8s namespace               (default: opal-dynamic-policy)
#   AUTO_PORT_FORWARD       – start port-forward auto     (default: true)
#   OIDC_BASE_URL           – set to test Keycloak iss extraction
#                             (default: "" → uses tenant_id claim fallback)
#   TEST_GATEWAY            – run Section 10 gateway end-to-end tests
#                             (default: false)
#   AGENTGATEWAY_NAMESPACE  – namespace of the agentgateway proxy
#                             (default: agentgateway-system-opa)
#   GATEWAY_URL             – agentgateway base URL (default: http://localhost:8080)

NAMESPACE=${NAMESPACE:-"opal-dynamic-policy"}
PEP_URL=${1:-"http://localhost:8000"}
BUNDLE_URL=${2:-"http://localhost:8001"}
TENANT_ID=${TENANT_ID:-"tenant-test-001"}
JWT_SECRET=${JWT_SECRET:-"jwt-secret"}
AUTO_PORT_FORWARD=${AUTO_PORT_FORWARD:-true}
TEST_GATEWAY=${TEST_GATEWAY:-false}
AGENTGATEWAY_NAMESPACE=${AGENTGATEWAY_NAMESPACE:-"agentgateway-system-opa"}
GATEWAY_URL=${GATEWAY_URL:-"http://localhost:8080"}
# Fake OIDC base for iss claim in test tokens (Keycloak format)
# auth.py HS256 fallback will extract tenant from: {FAKE_OIDC}/realms/{tenant}
FAKE_OIDC="http://keycloak.test"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

pass()    { echo -e "${GREEN}  ✓ $1${NC}"; }
fail()    { echo -e "${RED}  ✗ $1${NC}"; EXIT_CODE=1; }
section() { echo -e "\n${YELLOW}=== $1 ===${NC}"; }
info()    { echo -e "${CYAN}  i $1${NC}"; }

EXIT_CODE=0
PF_PIDS=()

# ---------------------------------------------------------------------------
# Port-forward management
# ---------------------------------------------------------------------------
_wait_for_port() {
    local port=$1 retries=15
    while [ $retries -gt 0 ]; do
        if curl -s -o /dev/null --connect-timeout 1 "http://localhost:$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        retries=$((retries - 1))
    done
    return 1
}

_wait_for_tcp() {
    # Check raw TCP (for gRPC port which speaks HTTP/2, not HTTP/1)
    local port=$1 retries=15
    while [ $retries -gt 0 ]; do
        if timeout 1 bash -c "echo >/dev/tcp/localhost/$port" 2>/dev/null; then
            return 0
        fi
        sleep 1
        retries=$((retries - 1))
    done
    return 1
}

start_port_forward() {
    info "Starting kubectl port-forward (namespace: $NAMESPACE)..."

    # Kill any stale forwarding on the target ports
    local ports="8000 8001 9000"
    [ "$TEST_GATEWAY" = "true" ] && ports="$ports 8080"
    for port in $ports; do
        lsof -ti "tcp:$port" 2>/dev/null | xargs kill -9 2>/dev/null || true
    done
    sleep 1

    kubectl port-forward svc/pep-proxy     8000:8000 -n "$NAMESPACE" \
        >/tmp/pf-pep.log 2>&1 &
    PF_PIDS+=($!)

    kubectl port-forward svc/bundle-server 8001:8001 -n "$NAMESPACE" \
        >/tmp/pf-bundle.log 2>&1 &
    PF_PIDS+=($!)

    # gRPC ext-authz port
    kubectl port-forward svc/pep-proxy 9000:9000 -n "$NAMESPACE" \
        >/tmp/pf-grpc.log 2>&1 &
    PF_PIDS+=($!)

    # agentgateway management port (only when TEST_GATEWAY=true)
    if [ "$TEST_GATEWAY" = "true" ]; then
        kubectl port-forward svc/agent-gateway 8080:8080 -n "$AGENTGATEWAY_NAMESPACE" \
            >/tmp/pf-gw.log 2>&1 &
        PF_PIDS+=($!)
    fi

    info "Waiting for HTTP port-forwards to be ready..."
    if ! _wait_for_port 8000; then
        echo -e "${RED}  ✗ pep-proxy port-forward failed:${NC}"
        cat /tmp/pf-pep.log
        exit 1
    fi
    if ! _wait_for_port 8001; then
        echo -e "${RED}  ✗ bundle-server port-forward failed:${NC}"
        cat /tmp/pf-bundle.log
        exit 1
    fi
    if [ "$TEST_GATEWAY" = "true" ]; then
        if ! _wait_for_port 8080; then
            echo -e "${RED}  ✗ agentgateway port-forward failed:${NC}"
            cat /tmp/pf-gw.log
            exit 1
        fi
        info "Port-forwards established: pep-proxy:8000, bundle-server:8001, gRPC:9000, gateway:8080"
    else
        info "Port-forwards established: pep-proxy:8000, bundle-server:8001, gRPC:9000"
    fi
}

cleanup() {
    if [ ${#PF_PIDS[@]} -gt 0 ]; then
        info "Stopping port-forwards (PIDs: ${PF_PIDS[*]})..."
        kill "${PF_PIDS[@]}" 2>/dev/null || true
        wait "${PF_PIDS[@]}" 2>/dev/null || true
    fi
    rm -f /tmp/pf-pep.log /tmp/pf-bundle.log /tmp/pf-grpc.log /tmp/pf-gw.log /tmp/http_body
}
trap cleanup EXIT

# ---------------------------------------------------------------------------
# Preflight connectivity check (manual mode)
# ---------------------------------------------------------------------------
preflight_check() {
    local url=$1 label=$2
    local code
    code=$(curl -s -o /dev/null -w "%{http_code}" --connect-timeout 3 \
        "$url" 2>/dev/null || true)
    if [ "${code:-000}" = "000" ]; then
        echo -e "${RED}  ✗ Cannot reach $label at $url${NC}"
        exit 1
    fi
}

if [ "$AUTO_PORT_FORWARD" = "true" ]; then
    start_port_forward
else
    info "AUTO_PORT_FORWARD=false – expecting pre-existing port-forwards"
    preflight_check "$PEP_URL/health"    "pep-proxy"
    preflight_check "$BUNDLE_URL/health" "bundle-server"
fi
info "Connectivity OK – starting tests"
echo ""

# ---------------------------------------------------------------------------
# JWT generation (HS256 via OpenSSL – no external dependencies)
#
# Includes an iss claim in Keycloak format so auth.py exercises the
# issuer-based tenant extraction path (_extract_tenant_from_issuer).
# ---------------------------------------------------------------------------
make_jwt() {
    local sub=$1 tenant=$2 roles_json=$3
    local header payload sig
    # roles_json 格式：[{"id":"<uuid>","name":"<role-name>"},...]
    # iss = {FAKE_OIDC}/realms/{tenant}  → auth.py extracts tenant from /realms/
    header=$(printf '{"alg":"HS256","typ":"JWT"}' \
        | base64 | tr -d '=' | tr '+/' '-_' | tr -d '\n')
    payload=$(printf \
        '{"sub":"%s","tenant_id":"%s","roles":%s,"email":"%s@example.com","iss":"%s/realms/%s"}' \
        "$sub" "$tenant" "$roles_json" "$sub" "$FAKE_OIDC" "$tenant" \
        | base64 | tr -d '=' | tr '+/' '-_' | tr -d '\n')
    sig=$(printf '%s.%s' "$header" "$payload" \
        | openssl dgst -sha256 -hmac "$JWT_SECRET" -binary \
        | base64 | tr -d '=' | tr '+/' '-_' | tr -d '\n')
    printf '%s.%s.%s' "$header" "$payload" "$sig"
}

# Fake role UUID for the "viewer" business role (used in role-policy bindings)
VIEWER_ROLE_ID="11111111-1111-1111-1111-111111111111"
# Second business role UUID for multi-role test
EDITOR_ROLE_ID="22222222-2222-2222-2222-222222222222"

SUPER_JWT=$(make_jwt "super-admin-001"  "$TENANT_ID" '[{"id":"super-admin-id","name":"super-admin"}]')
ADMIN_JWT=$(make_jwt "tenant-admin-001" "$TENANT_ID" '[{"id":"tenant-admin-id","name":"tenant-admin"}]')
USER_JWT=$(make_jwt  "normal-user-001"  "$TENANT_ID" "[{\"id\":\"$VIEWER_ROLE_ID\",\"name\":\"viewer\"}]")
# User with two roles: viewer (documents) + editor (reports)
MULTI_JWT=$(make_jwt "multi-user-001"   "$TENANT_ID" "[{\"id\":\"$VIEWER_ROLE_ID\",\"name\":\"viewer\"},{\"id\":\"$EDITOR_ROLE_ID\",\"name\":\"editor\"}]")

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------
http() {
    local method=$1; shift
    local url=$1;    shift
    > /tmp/http_body
    curl -s -o /tmp/http_body -w "%{http_code}" --connect-timeout 5 \
        -X "$method" "$url" "$@" 2>/dev/null || true
}

check_status() {
    local label=$1 expected=$2 actual=$3
    if [ "$actual" = "$expected" ]; then
        pass "$label (HTTP $actual)"
    else
        fail "$label (expected HTTP $expected, got HTTP $actual)"
        python3 -m json.tool /tmp/http_body 2>/dev/null || cat /tmp/http_body
        echo
    fi
}

json_field() {
    python3 -c "import sys,json; d=json.load(open('/tmp/http_body')); print($1)" 2>/dev/null || echo "?"
}

# ===========================================================================
# 1. Health checks
# ===========================================================================
section "1. Health Checks"

STATUS=$(http GET "$PEP_URL/health")
check_status "pep-proxy /health" "200" "$STATUS"

STATUS=$(http GET "$BUNDLE_URL/health")
check_status "bundle-server /health" "200" "$STATUS"

# ===========================================================================
# 2. gRPC ext-authz port connectivity
# ===========================================================================
section "2. gRPC Ext-Authz Server (port 9000)"

if _wait_for_tcp 9000; then
    pass "gRPC ext-authz port 9000 is open"
else
    fail "gRPC ext-authz port 9000 is not reachable"
    info "Check pep-proxy logs: kubectl logs -l app=pep-proxy -c opal-proxy -n $NAMESPACE"
fi

# Optional: full gRPC test with grpcurl
if command -v grpcurl &>/dev/null; then
    info "grpcurl found – testing Authorization/Check via reflection (if enabled)..."
    # Note: pep-proxy does not enable gRPC server reflection by default.
    # To use grpcurl with proto file:
    #   grpcurl -plaintext -proto pep-proxy/proto/ext_authz.proto \
    #     -d '{"attributes":{"request":{"http":{"method":"GET","path":"/","headers":{"authorization":"Bearer '$ADMIN_JWT'","x-authz-resource":"documents","x-authz-action":"read"}}}}}' \
    #     localhost:9000 envoy.service.auth.v3.Authorization/Check
    info "Run the grpcurl command above manually with pep-proxy/proto/ext_authz.proto"
else
    info "grpcurl not found – skipping full gRPC protocol test"
    info "Install: https://github.com/fullstorydev/grpcurl"
fi

# ===========================================================================
# 3. Bundle Server – tenant / data / bundle endpoints
# ===========================================================================
section "3. Bundle Server – Tenant / Data APIs"

STATUS=$(http GET "$BUNDLE_URL/api/v1/tenants")
check_status "List tenants" "200" "$STATUS"

STATUS=$(http POST "$BUNDLE_URL/api/v1/policies" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"documents-allow\",\"rules\":[{\"resource\":\"documents\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Create policy via bundle-server" "200" "$STATUS"
DOC_POLICY_ID=$(json_field 'd.get("policy_id","?")')
info "policy_id (UUID): $DOC_POLICY_ID"
info "name: $(json_field 'd.get("name","?")')"

# policy_id 必须是 UUID 格式，而不是策略名本身
if echo "$DOC_POLICY_ID" | grep -qE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
    pass "bundle-server: policy_id is a valid UUID"
else
    fail "bundle-server: policy_id is NOT a UUID (got: $DOC_POLICY_ID)"
fi

# Bind viewer role UUID to the documents-allow policy using UUID policy_id
STATUS=$(http POST "$BUNDLE_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$DOC_POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Bind viewer role to documents-allow via UUID policy_id (bundle-server)" "200" "$STATUS"

STATUS=$(http GET "$BUNDLE_URL/api/v1/tenants/$TENANT_ID/policies")
check_status "Get tenant policies" "200" "$STATUS"

STATUS=$(http GET "$BUNDLE_URL/api/v1/data/$TENANT_ID")
check_status "Get tenant OPA data" "200" "$STATUS"
info "role_bindings: $(json_field 'd.get("tenants",{}).get("'"$TENANT_ID"'",{}).get("role_bindings",{})')"

STATUS=$(http GET "$BUNDLE_URL/api/v1/opa-bundle")
check_status "Download OPA bundle tar.gz" "200" "$STATUS"

info "Waiting 3 s for OPA to receive the policy bundle..."
sleep 3

# ===========================================================================
# 4. PEP Proxy – policy templates
# ===========================================================================
section "4. PEP Proxy – Policy Templates"

STATUS=$(http GET "$PEP_URL/api/v1/policies/templates" \
    -H "Authorization: Bearer $USER_JWT")
check_status "List templates (any valid JWT → 200)" "200" "$STATUS"
info "templates: $(json_field 'd.get("templates",[])')"

# ===========================================================================
# 5. PEP Proxy – policy creation (tenant_admin only)
# ===========================================================================
section "5. PEP Proxy – Policy Creation"

STATUS=$(http POST "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"reports-allow\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Create policy (tenant_admin → 200)" "200" "$STATUS"
POLICY_ID=$(json_field 'd.get("policy_id","?")')
POLICY_NAME=$(json_field 'd.get("name","?")')
info "created policy_id: $POLICY_ID"
info "created name: $POLICY_NAME"

# policy_id 应为 UUID，name 应为用户传入的字符串，两者互相独立
if echo "$POLICY_ID" | grep -qE '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'; then
    pass "policy_id is a valid UUID (not the name)"
else
    fail "policy_id is NOT a UUID (got: $POLICY_ID)"
fi
if [ "$POLICY_NAME" = "reports-allow" ]; then
    pass "name field is human-readable label: $POLICY_NAME"
else
    fail "name field mismatch (expected 'reports-allow', got: $POLICY_NAME)"
fi

# 查看单条 policy（通过 UUID policy_id）
STATUS=$(http GET "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Get policy by UUID policy_id (→ 200)" "200" "$STATUS"
info "policy name in response: $(json_field 'd.get("name","?")')"

# 通过策略名（而非 UUID）查询应返回 404，因为存储以 UUID 为键
STATUS=$(http GET "$PEP_URL/api/v1/policies/reports-allow" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Get policy by name string (not UUID) → 404" "404" "$STATUS"

STATUS=$(http GET "$PEP_URL/api/v1/policies/nonexistent_policy" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Get non-existent policy (→ 404)" "404" "$STATUS"

# Bind viewer role UUID to the reports-allow policy (upsert – replaces documents-allow)
STATUS=$(http POST "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Bind viewer role to $POLICY_ID (tenant_admin → 200)" "200" "$STATUS"

STATUS=$(http POST "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $USER_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Bind role (normal_user → 403)" "403" "$STATUS"

STATUS=$(http POST "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $USER_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"reports-allow\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Create policy (normal_user → 403)" "403" "$STATUS"

STATUS=$(http POST "$PEP_URL/api/v1/policies/template/role_based" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"tenant_id\":\"$TENANT_ID\",\"role\":\"viewer\",\"resource\":\"documents\",\"action\":\"read\"}")
check_status "Render role_based template (tenant_admin → 200)" "200" "$STATUS"

info "Waiting 3 s for OPA policy update to propagate..."
sleep 3

# ===========================================================================
# 6. PEP Proxy – Policy CRUD (list / update / delete / cross-tenant isolation)
# ===========================================================================
section "6. PEP Proxy – Policy CRUD + Cross-Tenant Isolation"

STATUS=$(http GET "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $USER_JWT")
check_status "List policies (normal_user → 200)" "200" "$STATUS"
info "policy count: $(json_field 'd.get("count","?")')"

STATUS=$(http PUT "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$POLICY_ID\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Update policy (tenant_admin → 200)" "200" "$STATUS"

STATUS=$(http PUT "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $USER_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$POLICY_ID\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Update policy (normal_user → 403)" "403" "$STATUS"

# Cross-tenant: admin cannot write policy for a different tenant
STATUS=$(http PUT "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$POLICY_ID\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"other-tenant\"}")
check_status "Cross-tenant update rejected (403)" "403" "$STATUS"

STATUS=$(http DELETE "$PEP_URL/api/v1/policies/$POLICY_ID?tenant_id=other-tenant" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "Cross-tenant delete rejected (403)" "403" "$STATUS"

STATUS=$(http DELETE "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Delete policy (normal_user → 403)" "403" "$STATUS"

# PUT: 更新已有绑定（reports-allow UUID → documents-allow UUID），测试显式 update 语义
STATUS=$(http PUT "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$DOC_POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Update role binding via PUT (tenant_admin → 200)" "200" "$STATUS"
info "policy_id after PUT update: $(json_field 'd.get("policy_id","?")')"

STATUS=$(http PUT "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $USER_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$DOC_POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Update role binding via PUT (normal_user → 403)" "403" "$STATUS"

# PUT on non-existent role → 404
STATUS=$(http PUT "$PEP_URL/api/v1/roles/no-such-role/policy" \
    -H "Authorization: Bearer $ADMIN_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"documents-allow\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Update binding for unknown role via PUT (→ 404)" "404" "$STATUS"

# Verify current binding
STATUS=$(http GET "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Get role policy after PUT update (→ 200)" "200" "$STATUS"
info "role binding after update: id=$(json_field 'd.get("policy",{}).get("id","?")') name=$(json_field 'd.get("policy",{}).get("name","?")')"

# DELETE role binding: normal_user → 403
STATUS=$(http DELETE "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy?tenant_id=$TENANT_ID" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Delete role binding (normal_user → 403)" "403" "$STATUS"

# DELETE role binding: tenant_admin → 200
STATUS=$(http DELETE "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy?tenant_id=$TENANT_ID" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "Delete role binding (tenant_admin → 200)" "200" "$STATUS"

# GET after delete → 404
STATUS=$(http GET "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Authorization: Bearer $USER_JWT")
check_status "Get role policy after delete (→ 404)" "404" "$STATUS"

# DELETE non-existent → 404
STATUS=$(http DELETE "$PEP_URL/api/v1/roles/$VIEWER_ROLE_ID/policy?tenant_id=$TENANT_ID" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "Delete already-deleted role binding (→ 404)" "404" "$STATUS"

# 重建绑定以保证 section 7 auth check 继续可用
http POST "$BUNDLE_URL/api/v1/roles/$VIEWER_ROLE_ID/policy" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$DOC_POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}" >/dev/null
info "Recreated viewer role binding (DOC_POLICY_ID) for section 7"

STATUS=$(http DELETE "$PEP_URL/api/v1/policies/$POLICY_ID" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "Delete policy (tenant_admin → 200)" "200" "$STATUS"

STATUS=$(http GET "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "List policies after delete (→ 200)" "200" "$STATUS"
info "policy count after delete: $(json_field 'd.get("count","?")')"

# ===========================================================================
# 7. PEP Proxy – authorization checks via OPA (/api/v1/auth/check)
# ===========================================================================
section "7. PEP Proxy – Authorization Checks (OPA)"

_auth_check() {
    local label=$1 jwt=$2 resource=$3 tenant=$4 want_allow=$5
    STATUS=$(http POST "$PEP_URL/api/v1/auth/check" \
        -H "Authorization: Bearer $jwt" \
        -H "Content-Type: application/json" \
        -d "{\"resource\":\"$resource\",\"tenant_id\":\"$tenant\"}")
    check_status "$label (HTTP 200)" "200" "$STATUS"
    local decision
    decision=$(json_field 'd.get("allowed",False)')
    if   [ "$want_allow" = "true"  ] && [ "$decision" = "True"  ]; then
        pass "$label → ALLOW"
    elif [ "$want_allow" = "false" ] && [ "$decision" = "False" ]; then
        pass "$label → DENY"
    else
        fail "$label → expected $([ "$want_allow" = "true" ] && echo ALLOW || echo DENY), got $decision"
    fi
}

# viewer role UUID is bound to documents-allow (rebinding in section 6); reports-allow was deleted
_auth_check "viewer accesses documents (UUID role binding)"  "$USER_JWT"  "documents"   "$TENANT_ID"   "true"
_auth_check "viewer accesses reports (deleted policy)"       "$USER_JWT"  "reports"     "$TENANT_ID"   "false"
_auth_check "tenant-admin accesses anything in own tenant"   "$ADMIN_JWT" "documents"   "$TENANT_ID"   "true"
_auth_check "super-admin cross-tenant operation"             "$SUPER_JWT" "anything"    "other-tenant" "true"

# ===========================================================================
# 8. Multi-role user: multiple role_ids, each bound to a different policy
# ===========================================================================
section "8. Multi-Role User (multiple role_ids)"

# Setup: create reports-allow policy and bind EDITOR_ROLE_ID to it.
# VIEWER_ROLE_ID is already bound to documents-allow UUID (from section 6 PUT).
STATUS=$(http POST "$BUNDLE_URL/api/v1/policies" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"reports-allow\",\"rules\":[{\"resource\":\"reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Create reports-allow for multi-role test (bundle-server → 200)" "200" "$STATUS"
REPORTS_POLICY_ID=$(json_field 'd.get("policy_id","?")')
info "reports-allow policy_id (UUID): $REPORTS_POLICY_ID"

STATUS=$(http POST "$BUNDLE_URL/api/v1/roles/$EDITOR_ROLE_ID/policy" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"$REPORTS_POLICY_ID\",\"tenant_id\":\"$TENANT_ID\"}")
check_status "Bind editor role to reports-allow via UUID (bundle-server → 200)" "200" "$STATUS"

info "Waiting 3 s for OPA to receive updated data..."
sleep 3

# MULTI_JWT carries both VIEWER_ROLE_ID (→ documents:allow) and EDITOR_ROLE_ID (→ reports:allow)
# Tier-3 iterates all role_ids; either UUID can satisfy the resource check.
_auth_check "multi-role: accesses documents (via viewer role)"  "$MULTI_JWT" "documents" "$TENANT_ID" "true"
_auth_check "multi-role: accesses reports (via editor role)"    "$MULTI_JWT" "reports"   "$TENANT_ID" "true"
_auth_check "multi-role: accesses billing (no binding → DENY)"  "$MULTI_JWT" "billing"   "$TENANT_ID" "false"

# Cleanup: delete reports-allow by UUID policy_id
http DELETE "$BUNDLE_URL/api/v1/policies/${REPORTS_POLICY_ID}?tenant_id=$TENANT_ID" >/dev/null

# ===========================================================================
# 9. JWT issuer-based tenant extraction
# ===========================================================================
section "9. JWT Issuer-Based Tenant Extraction"

# The JWTs generated by make_jwt include:
#   "iss": "http://keycloak.test/realms/<tenant>"
# In HS256 dev mode auth.py runs: _extract_tenant_from_issuer(iss)
# and should resolve the same tenant_id as the claim.
info "Test JWT iss: ${FAKE_OIDC}/realms/${TENANT_ID}"
info "(HS256 dev mode: auth.py extracts tenant from /realms/ path in iss)"

# If tenant extracted from iss is correct, the following will succeed
STATUS=$(http GET "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $ADMIN_JWT")
check_status "Auth with iss-embedded tenant works (→ 200)" "200" "$STATUS"

# Attempt to forge a token whose iss points to a different tenant but
# whose tenant_id claim matches. auth.py should use the iss-derived tenant.
FORGE_JWT=$(make_jwt "attacker" "other-tenant" '[{"id":"forge-id","name":"tenant-admin"}]')
STATUS=$(http POST "$PEP_URL/api/v1/policies" \
    -H "Authorization: Bearer $FORGE_JWT" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"data-allow\",\"rules\":[{\"resource\":\"data\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
check_status "Cross-tenant write via forged token rejected (403)" "403" "$STATUS"

# ===========================================================================
# 10. Reject invalid / missing tokens
# ===========================================================================
section "10. Auth Rejection"

STATUS=$(http GET "$PEP_URL/api/v1/policies/templates")
check_status "No token → 403" "403" "$STATUS"

STATUS=$(http GET "$PEP_URL/api/v1/policies/templates" \
    -H "Authorization: Bearer invalidtoken")
check_status "Bad token → 401" "401" "$STATUS"

# ===========================================================================
# 11. Agentgateway end-to-end (TEST_GATEWAY=true)
#
# Exercises the full chain:
#   curl → gateway:8080 → gRPC ext-authz (pep-proxy:9000) → OPA → pep-proxy:8000
#
# Run with: TEST_GATEWAY=true ./test.sh
# ===========================================================================
if [ "$TEST_GATEWAY" = "true" ]; then
    section "11. Agentgateway End-to-End (gateway:8080)"

    # ── 10a: Connectivity ────────────────────────────────────────────────────
    STATUS=$(http GET "$GATEWAY_URL/api/v1/policies/templates")
    check_status "Gateway reachable (no token → 403)" "403" "$STATUS"

    # ── 10b: Auth rejection through gateway ─────────────────────────────────
    # agentgateway maps all ext-authz denials (including 401 Unauthorized) to
    # HTTP 403, so we expect 403 regardless of the underlying denial reason.
    STATUS=$(http GET "$GATEWAY_URL/api/v1/policies/templates" \
        -H "Authorization: Bearer invalidtoken")
    check_status "Bad token via gateway → 403" "403" "$STATUS"

    # ── 10c: Authenticated requests through gateway ──────────────────────────
    # The gateway enforces OPA for ALL requests.  viewer only has documents:read
    # in role_mappings; OPA denies templates:list and policies:list for viewer.
    # Use tenant_admin (unconditionally allowed by OPA within own tenant).
    STATUS=$(http GET "$GATEWAY_URL/api/v1/policies/templates" \
        -H "Authorization: Bearer $ADMIN_JWT")
    check_status "List templates via gateway (tenant_admin → 200)" "200" "$STATUS"

    STATUS=$(http GET "$GATEWAY_URL/api/v1/policies" \
        -H "Authorization: Bearer $ADMIN_JWT")
    check_status "List policies via gateway (tenant_admin → 200)" "200" "$STATUS"

    # ── 10d: Policy write through gateway (tenant_admin only) ────────────────
    STATUS=$(http POST "$GATEWAY_URL/api/v1/policies" \
        -H "Authorization: Bearer $ADMIN_JWT" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"gw-reports-allow\",\"rules\":[{\"resource\":\"gw-reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
    check_status "Create policy via gateway (tenant_admin → 200)" "200" "$STATUS"
    GW_POLICY_ID=$(json_field 'd.get("policy_id","gw-reports-allow")')
    info "created policy_id: $GW_POLICY_ID"

    STATUS=$(http POST "$GATEWAY_URL/api/v1/policies" \
        -H "Authorization: Bearer $USER_JWT" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"gw-reports-allow\",\"rules\":[{\"resource\":\"gw-reports\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
    check_status "Create policy via gateway (viewer → 403)" "403" "$STATUS"

    # ── 10e: Auth check via gateway ──────────────────────────────────────────
    STATUS=$(http POST "$GATEWAY_URL/api/v1/auth/check" \
        -H "Authorization: Bearer $ADMIN_JWT" \
        -H "Content-Type: application/json" \
        -d "{\"resource\":\"documents\",\"tenant_id\":\"$TENANT_ID\"}")
    check_status "Auth check via gateway (tenant_admin, HTTP 200)" "200" "$STATUS"
    decision=$(json_field 'd.get("allowed",False)')
    if [ "$decision" = "True" ]; then
        pass "Auth check via gateway → ALLOW"
    else
        fail "Auth check via gateway → expected ALLOW, got $decision"
    fi

    # cleanup gateway-created policy
    http DELETE "$GATEWAY_URL/api/v1/policies/${GW_POLICY_ID}" \
        -H "Authorization: Bearer $ADMIN_JWT" >/dev/null

    # ── 11f: Cold-start – OPA Rego loaded without tenant data ────────────────
    # 验证修复：bundle-server 启动时无论 DB 是否为空，都会向 OPA 推送基础 Rego 策略。
    # 若未修复，OPA 空库时没有任何策略，tenant-admin 经 gateway 也会被拒绝（死锁）。
    section "11f. Cold-Start: OPA Rego 冷启动验证"

    # Step 1: 通过 pep-proxy /auth/check 验证 OPA Rego 已加载且 tenant-admin 规则生效
    # pep-proxy 内部调用 OPA /v1/data/authz/allow，结果等价于直查 OPA
    STATUS=$(http POST "$PEP_URL/api/v1/auth/check" \
        -H "Authorization: Bearer $ADMIN_JWT" \
        -H "Content-Type: application/json" \
        -d "{\"resource\":\"policies\",\"tenant_id\":\"$TENANT_ID\"}")
    check_status "OPA Rego 已加载：tenant-admin auth/check (→ 200)" "200" "$STATUS"
    COLD_ALLOW=$(json_field 'd.get("allowed", False)')
    if [ "$COLD_ALLOW" = "True" ]; then
        pass "OPA 直查：tenant-admin 在无 tenant 数据时仍被放行（冷启动 Rego 生效）"
    else
        fail "OPA 直查：tenant-admin 被拒绝 – 冷启动修复未生效（检查 bundle-server 启动日志）"
    fi

    # Step 2: 通过 gateway 验证 tenant-admin 可在空库场景下创建策略（端到端）
    STATUS=$(http POST "$GATEWAY_URL/api/v1/policies" \
        -H "Authorization: Bearer $ADMIN_JWT" \
        -H "Content-Type: application/json" \
        -d "{\"name\":\"cold-start-test\",\"rules\":[{\"resource\":\"cold-test\",\"effect\":\"allow\"}],\"tenant_id\":\"$TENANT_ID\"}")
    check_status "tenant-admin 经 gateway 创建策略（冷启动场景 → 200）" "200" "$STATUS"
    COLD_POLICY_ID=$(json_field 'd.get("policy_id","?")')
    info "cold-start policy_id: $COLD_POLICY_ID"

    # cleanup
    if [ "$COLD_POLICY_ID" != "?" ]; then
        http DELETE "$GATEWAY_URL/api/v1/policies/$COLD_POLICY_ID" \
            -H "Authorization: Bearer $ADMIN_JWT" >/dev/null
    fi

else
    info "Gateway tests skipped – run with TEST_GATEWAY=true ./test.sh to enable."
fi

# ===========================================================================
# Summary
# ===========================================================================
echo ""
if [ "$EXIT_CODE" -eq 0 ]; then
    echo -e "${GREEN}All tests passed!${NC}"
else
    echo -e "${RED}Some tests FAILED – review output above.${NC}"
fi
exit $EXIT_CODE
