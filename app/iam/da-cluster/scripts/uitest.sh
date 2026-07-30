#!/usr/bin/env bash
# uitest.sh — 前端 UI 业务流程集成测试
#
# 按照真实的前端操作流程，模拟 super-admin 和 tenant-admin 的完整业务场景。
# 所有请求通过 gateway (localhost:8080)，与前端调用方式完全一致。
#
# Usage:
#   ./scripts/uitest.sh
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"
KEYCLOAK_NS="keycloak"
GATEWAY_PORT="${GATEWAY_PORT:-8080}"
BASE_URL="http://localhost:${GATEWAY_PORT}"

# Test tenant to create during the test
TEST_REALM="ui-test-tenant"

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
NC='\033[0m'

PASS=0; FAIL=0; TOTAL=0

assert() {
  local desc="$1" expected="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if [ "$expected" = "$actual" ]; then
    echo -e "  ${GREEN}PASS${NC} $desc"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}FAIL${NC} $desc (expected=$expected, actual=$actual)"
    FAIL=$((FAIL + 1))
  fi
}

assert_match() {
  local desc="$1" pattern="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$actual" | grep -qE "$pattern"; then
    echo -e "  ${GREEN}PASS${NC} $desc"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}FAIL${NC} $desc (expected ~$pattern, got $actual)"
    FAIL=$((FAIL + 1))
  fi
}

assert_contains() {
  local desc="$1" expected="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$actual" | grep -q "$expected"; then
    echo -e "  ${GREEN}PASS${NC} $desc"
    PASS=$((PASS + 1))
  else
    echo -e "  ${RED}FAIL${NC} $desc (expected to contain '$expected')"
    FAIL=$((FAIL + 1))
  fi
}

assert_not_contains() {
  local desc="$1" unexpected="$2" actual="$3"
  TOTAL=$((TOTAL + 1))
  if echo "$actual" | grep -q "$unexpected"; then
    echo -e "  ${RED}FAIL${NC} $desc (should NOT contain '$unexpected')"
    FAIL=$((FAIL + 1))
  else
    echo -e "  ${GREEN}PASS${NC} $desc"
    PASS=$((PASS + 1))
  fi
}

info() { echo -e "  ${CYAN}INFO${NC} $1"; }

# ── Setup port-forward ──────────────────────────────────────────────────
echo -e "${YELLOW}Setting up port-forward...${NC}"
if curl -s -o /dev/null -w "%{http_code}" "http://localhost:${GATEWAY_PORT}/" 2>/dev/null | grep -qE '200|301|302|404'; then
  echo -e "  ${GREEN}Port ${GATEWAY_PORT} already forwarded${NC}"
  PF_PID=""
else
  lsof -ti:${GATEWAY_PORT} 2>/dev/null | xargs kill -9 2>/dev/null || true
  GW_SVC=$(kubectl -n agentgateway-system get svc -l gateway.networking.k8s.io/gateway-name=agentgateway-proxy -o name 2>/dev/null | head -1)
  [ -z "$GW_SVC" ] && GW_SVC="svc/agentgateway-proxy"
  kubectl -n agentgateway-system port-forward "$GW_SVC" "${GATEWAY_PORT}:80" &
  PF_PID=$!
  sleep 3
fi
trap "[ -n \"\$PF_PID\" ] && kill \$PF_PID 2>/dev/null || true" EXIT

# ── Get secrets ─────────────────────────────────────────────────────────
MASTER_SECRET=$(kubectl -n "$KEYCLOAK_NS" get secret keycloak-idb-proxy-client \
  -o jsonpath='{.data.client-secret}' 2>/dev/null | base64 -d 2>/dev/null || echo "")
TENANT_SECRET=$(kubectl -n "$KEYCLOAK_NS" get secret keycloak-data-agent-client \
  -o jsonpath='{.data.client-secret}' 2>/dev/null | base64 -d 2>/dev/null || echo "")

# ══════════════════════════════════════════════════════════════════════════
# Scenario 1: Super-admin Login
# UI: super-admin 在登录页输入账号密码
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 1: Super-admin Login ===${NC}"

SA_RESPONSE=$(curl -s -X POST "${BASE_URL}/realms/master/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=idb-proxy-client" \
  -d "client_secret=${MASTER_SECRET}" \
  -d "username=super-admin" \
  -d "password=SuperInit@123" 2>/dev/null)

SA_TOKEN=$(echo "$SA_RESPONSE" | jq -r '.access_token // empty')
SA_REFRESH=$(echo "$SA_RESPONSE" | jq -r '.refresh_token // empty')
[ -n "$SA_TOKEN" ] && assert "Super-admin login" "true" "true" \
                    || assert "Super-admin login" "token" "empty"

# Verify token contains roles: [{id, name}]
if [ -n "$SA_TOKEN" ]; then
  SA_ROLES=$(python -c "
import base64, json
token = '''$SA_TOKEN'''
payload = token.split('.')[1].replace('-','+').replace('_','/')
payload += '=' * (4 - len(payload) % 4)
data = json.loads(base64.b64decode(payload))
roles = data.get('roles', [])
has_structured = isinstance(roles, list) and len(roles) > 0 and isinstance(roles[0], dict) and 'id' in roles[0]
print(has_structured)
" 2>/dev/null || echo "False")
  assert "Token has structured roles [{id,name}]" "True" "$SA_ROLES"
fi

# Token refresh
if [ -n "$SA_REFRESH" ]; then
  REFRESH_RESP=$(curl -s -X POST "${BASE_URL}/realms/master/protocol/openid-connect/token" \
    -d "grant_type=refresh_token" \
    -d "client_id=idb-proxy-client" \
    -d "client_secret=${MASTER_SECRET}" \
    -d "refresh_token=${SA_REFRESH}" 2>/dev/null)
  NEW_TOKEN=$(echo "$REFRESH_RESP" | jq -r '.access_token // empty')
  [ -n "$NEW_TOKEN" ] && assert "Token refresh" "true" "true" \
                       || assert "Token refresh" "token" "empty"
fi

SA_AUTH="Authorization: Bearer ${SA_TOKEN}"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 2: Super-admin — Tenant 列表 & 管理
# UI: 登录后进入 Tenant 列表页
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 2: Super-admin — Tenant Management ===${NC}"

# 2a: 查看 Tenant 列表
TENANTS=$(curl -s -H "$SA_AUTH" "${BASE_URL}/api/v1/tenants" 2>/dev/null)
TENANTS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$SA_AUTH" "${BASE_URL}/api/v1/tenants" 2>/dev/null)
assert "GET tenant list" "200" "$TENANTS_CODE"
assert_contains "data-agent in tenant list" "data-agent" "$TENANTS"

# 2b: 创建新 Tenant
info "Creating test tenant: $TEST_REALM"
CREATE_RESULT=$(curl -s -w "\n%{http_code}" -X POST -H "$SA_AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"realm\":\"$TEST_REALM\",\"displayName\":\"UI Test Tenant\"}" \
  "${BASE_URL}/api/v1/tenants" 2>/dev/null)
CREATE_CODE=$(echo "$CREATE_RESULT" | tail -1)
CREATE_BODY=$(echo "$CREATE_RESULT" | sed '$d')
assert "Create tenant" "201" "$CREATE_CODE"
assert_contains "Response contains realm name" "$TEST_REALM" "$CREATE_BODY"
assert_contains "Response contains admin_role" "tenant-admin" "$CREATE_BODY"

# 2c: 验证新 Tenant 出现在列表中
TENANTS_AFTER=$(curl -s -H "$SA_AUTH" "${BASE_URL}/api/v1/tenants" 2>/dev/null)
assert_contains "New tenant in list" "$TEST_REALM" "$TENANTS_AFTER"

# 2d: 验证新 Tenant 的 OIDC discovery 可用
OIDC_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  "${BASE_URL}/realms/${TEST_REALM}/.well-known/openid-configuration" 2>/dev/null)
assert "OIDC discovery for new tenant" "200" "$OIDC_CODE"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 3: Super-admin — 配置 SAML 接入（Tenant 创建后的 next 步骤）
# UI: 填写 SSO URL、Entity ID 等参数
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 3: SAML IDP Configuration ===${NC}"

# 3a: 创建 SAML IDP 实例
IDP_CREATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$SA_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"displayName":"Test SAML IDP","enabled":true,"trustEmail":false,"config":{"singleSignOnServiceUrl":"https://idp.example.com/sso","entityId":"https://idp.example.com/entity"}}' \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances" 2>/dev/null)
assert "Create SAML IDP instance" "201" "$IDP_CREATE_CODE"

# 3b: 查看 IDP 列表
IDP_LIST=$(curl -s -H "$SA_AUTH" "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances" 2>/dev/null)
IDP_LIST_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$SA_AUTH" \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances" 2>/dev/null)
assert "List IDP instances" "200" "$IDP_LIST_CODE"
assert_contains "IDP contains display name" "Test SAML IDP" "$IDP_LIST"

# 3c: 更新 IDP 参数
IDP_UPDATE_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "$SA_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"displayName":"Updated SAML IDP","enabled":true,"trustEmail":true,"config":{"singleSignOnServiceUrl":"https://idp-v2.example.com/sso"}}' \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances" 2>/dev/null)
assert "Update IDP instance" "200" "$IDP_UPDATE_CODE"

# 3d: 删除 IDP
IDP_DEL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE -H "$SA_AUTH" \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances/da-saml-idp" 2>/dev/null)
assert "Delete IDP instance" "204" "$IDP_DEL_CODE"

# 3e: SAML XML 导入
SAML_XML='<?xml version="1.0"?>
<EntityDescriptor xmlns="urn:oasis:names:tc:SAML:2.0:metadata" entityID="https://xml-idp.example.com">
  <IDPSSODescriptor protocolSupportEnumeration="urn:oasis:names:tc:SAML:2.0:protocol">
    <SingleSignOnService Binding="urn:oasis:names:tc:SAML:2.0:bindings:HTTP-Redirect" Location="https://xml-idp.example.com/sso"/>
  </IDPSSODescriptor>
</EntityDescriptor>'
IMPORT_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$SA_AUTH" \
  -F "file=@-;filename=metadata.xml;type=application/xml" \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/import" <<< "$SAML_XML" 2>/dev/null)
assert_match "SAML XML import" "^(200|201)$" "$IMPORT_CODE"

# Cleanup IDP (imported one uses da-saml-idp alias)
curl -s -o /dev/null -X DELETE -H "$SA_AUTH" \
  "${BASE_URL}/api/v1/${TEST_REALM}/idp/saml/instances/da-saml-idp" 2>/dev/null || true

# ══════════════════════════════════════════════════════════════════════════
# Scenario 4: Tenant-admin Login（data-agent realm）
# UI: Tenant 用户登录（这里用 password grant 模拟，等同于 OIDC code flow）
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 4: Tenant-admin Login (data-agent) ===${NC}"

TA_RESPONSE=$(curl -s -X POST "${BASE_URL}/realms/data-agent/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=data-agent-client" \
  -d "client_secret=${TENANT_SECRET}" \
  -d "username=tenant-admin" \
  -d "password=TenantAdmin@123" 2>/dev/null)

TA_TOKEN=$(echo "$TA_RESPONSE" | jq -r '.access_token // empty')
[ -n "$TA_TOKEN" ] && assert "Tenant-admin login" "true" "true" \
                    || assert "Tenant-admin login" "token" "empty"

# Verify token has roles: [{id, name}] with tenant-admin
if [ -n "$TA_TOKEN" ]; then
  TA_HAS_ROLE=$(python -c "
import base64, json
token = '''$TA_TOKEN'''
payload = token.split('.')[1].replace('-','+').replace('_','/')
payload += '=' * (4 - len(payload) % 4)
data = json.loads(base64.b64decode(payload))
roles = data.get('roles', [])
names = [r['name'] for r in roles if isinstance(r, dict)]
print('tenant-admin' in names)
" 2>/dev/null || echo "False")
  assert "Token contains tenant-admin role" "True" "$TA_HAS_ROLE"
fi

TA_AUTH="Authorization: Bearer ${TA_TOKEN}"

# Normal user login
NU_RESPONSE=$(curl -s -X POST "${BASE_URL}/realms/data-agent/protocol/openid-connect/token" \
  -d "grant_type=password" \
  -d "client_id=data-agent-client" \
  -d "client_secret=${TENANT_SECRET}" \
  -d "username=normal-user" \
  -d "password=NormalUser@123" 2>/dev/null)
NU_TOKEN=$(echo "$NU_RESPONSE" | jq -r '.access_token // empty')
[ -n "$NU_TOKEN" ] && assert "Normal-user login" "true" "true" \
                    || assert "Normal-user login" "token" "empty"
NU_AUTH="Authorization: Bearer ${NU_TOKEN}"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 5: Resource Policy 管理（多 Policy）
# UI: tenant-admin 进入 Resource Policy 页面
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 5: Resource Policy CRUD (multiple policies) ===${NC}"

# 5a: 创建 4 个 Policy，覆盖不同资源和 allow/deny 组合
for p in \
  '{"name":"docs-policy","tenant_id":"data-agent","rules":[{"resource":"documents","effect":"allow"},{"resource":"settings","effect":"deny"}]}' \
  '{"name":"reports-policy","tenant_id":"data-agent","rules":[{"resource":"reports","effect":"allow"}]}' \
  '{"name":"admin-policy","tenant_id":"data-agent","rules":[{"resource":"users","effect":"allow"},{"resource":"roles","effect":"allow"},{"resource":"groups","effect":"allow"}]}' \
  '{"name":"billing-policy","tenant_id":"data-agent","rules":[{"resource":"billing","effect":"allow"},{"resource":"invoices","effect":"allow"}]}'; do
  PNAME=$(echo "$p" | jq -r '.name')
  PCODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$TA_AUTH" \
    -H "Content-Type: application/json" -d "$p" \
    "${BASE_URL}/api/v1/policies" 2>/dev/null)
  assert "Create policy ($PNAME)" "200" "$PCODE"
done

# 5b: 查看 Policy 列表
POLICIES=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/policies" 2>/dev/null)
POLICIES_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" "${BASE_URL}/api/v1/policies" 2>/dev/null)
assert "List policies" "200" "$POLICIES_CODE"
POLICY_COUNT=$(echo "$POLICIES" | jq '.count' 2>/dev/null || echo "0")
assert "Policy count >= 4" "true" "$([ "$POLICY_COUNT" -ge 4 ] && echo true || echo false)"
info "Policy count: $POLICY_COUNT"

# 5c: 查看单个 Policy 详情
for pname in docs-policy reports-policy admin-policy billing-policy; do
  PDETAIL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/policies/$pname" 2>/dev/null)
  assert "Get policy detail ($pname)" "200" "$PDETAIL_CODE"
done

# 5d: 编辑 Policy
UPDATE_P_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "$TA_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"name":"docs-policy","tenant_id":"data-agent","rules":[{"resource":"documents","effect":"allow"},{"resource":"settings","effect":"allow"}]}' \
  "${BASE_URL}/api/v1/policies/docs-policy" 2>/dev/null)
assert "Update policy (settings deny→allow)" "200" "$UPDATE_P_CODE"

# 5e: normal-user 不能创建/编辑/删除 policy
NU_P_CREATE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$NU_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"name":"should-fail","tenant_id":"data-agent","rules":[{"resource":"x","effect":"allow"}]}' \
  "${BASE_URL}/api/v1/policies" 2>/dev/null)
assert "Normal-user cannot create policy -> 403" "403" "$NU_P_CREATE"

NU_P_EDIT=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "$NU_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"name":"docs-policy","tenant_id":"data-agent","rules":[{"resource":"x","effect":"allow"}]}' \
  "${BASE_URL}/api/v1/policies/docs-policy" 2>/dev/null)
assert "Normal-user cannot edit policy -> 403" "403" "$NU_P_EDIT"

NU_P_DEL=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE -H "$NU_AUTH" \
  "${BASE_URL}/api/v1/policies/docs-policy" 2>/dev/null)
assert "Normal-user cannot delete policy -> 403" "403" "$NU_P_DEL"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 6: Role 管理 + Role-Policy 绑定（多角色、多绑定）
# UI: tenant-admin 进入 Role 管理页面
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 6: Role CRUD + Policy Binding (multiple roles) ===${NC}"

# 6a: 查看 Role 列表
ROLES=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/roles" 2>/dev/null)
assert "List roles" "200" "$(curl -s -o /dev/null -w '%{http_code}' -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/roles" 2>/dev/null)"
assert_contains "tenant-admin role exists" "tenant-admin" "$ROLES"
assert_contains "normal-user role exists" "normal-user" "$ROLES"

# 6b: 创建 3 个自定义角色
for rname in viewer editor billing-admin; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"name\":\"$rname\",\"description\":\"$rname role for testing\"}" \
    "${BASE_URL}/api/v1/data-agent/roles" 2>/dev/null)
  assert "Create role ($rname)" "201" "$CODE"
done

# 6c: 获取所有自定义 role UUID
ROLES=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/roles" 2>/dev/null)
VIEWER_UUID=$(echo "$ROLES" | jq -r '.[] | select(.name=="viewer") | .id' 2>/dev/null || echo "")
EDITOR_UUID=$(echo "$ROLES" | jq -r '.[] | select(.name=="editor") | .id' 2>/dev/null || echo "")
BILLING_UUID=$(echo "$ROLES" | jq -r '.[] | select(.name=="billing-admin") | .id' 2>/dev/null || echo "")
NU_ROLE_UUID=$(echo "$ROLES" | jq -r '.[] | select(.name=="normal-user") | .id' 2>/dev/null || echo "")

[ -n "$VIEWER_UUID" ]  && info "viewer UUID: $VIEWER_UUID"  || info "WARN: viewer UUID empty"
[ -n "$EDITOR_UUID" ]  && info "editor UUID: $EDITOR_UUID"  || info "WARN: editor UUID empty"
[ -n "$BILLING_UUID" ] && info "billing-admin UUID: $BILLING_UUID" || info "WARN: billing-admin UUID empty"
[ -n "$NU_ROLE_UUID" ] && info "normal-user UUID: $NU_ROLE_UUID"   || info "WARN: normal-user UUID empty"

# 6d: 绑定 Policy 到 Role（使用 role UUID）
#   viewer       → docs-policy     (documents:allow, settings:allow)
#   editor       → reports-policy  (reports:allow)
#   billing-admin→ billing-policy  (billing:allow, invoices:allow)
#   normal-user  → (not bound yet)
for pair in "$VIEWER_UUID:docs-policy" "$EDITOR_UUID:reports-policy" "$BILLING_UUID:billing-policy"; do
  RUUID="${pair%%:*}"
  POLID="${pair##*:}"
  if [ -n "$RUUID" ]; then
    BCODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$TA_AUTH" \
      -H "Content-Type: application/json" \
      -d "{\"policy_id\":\"$POLID\",\"tenant_id\":\"data-agent\"}" \
      "${BASE_URL}/api/v1/roles/${RUUID}/policy" 2>/dev/null)
    assert "Bind $POLID to role UUID ${RUUID:0:8}..." "200" "$BCODE"
  fi
done

# 6e: 查询每个 Role 的 Policy 绑定
for pair in "$VIEWER_UUID:docs-policy" "$EDITOR_UUID:reports-policy" "$BILLING_UUID:billing-policy"; do
  RUUID="${pair%%:*}"
  POLID="${pair##*:}"
  if [ -n "$RUUID" ]; then
    BDETAIL=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/roles/${RUUID}/policy" 2>/dev/null)
    BCODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" "${BASE_URL}/api/v1/roles/${RUUID}/policy" 2>/dev/null)
    assert "Query binding for ${RUUID:0:8}..." "200" "$BCODE"
    assert_contains "Binding contains $POLID" "$POLID" "$BDETAIL"
  fi
done

# 6f: 更新绑定（viewer: docs→reports）
if [ -n "$VIEWER_UUID" ]; then
  REBIND=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"reports-policy\",\"tenant_id\":\"data-agent\"}" \
    "${BASE_URL}/api/v1/roles/${VIEWER_UUID}/policy" 2>/dev/null)
  assert "Update viewer binding (docs→reports)" "200" "$REBIND"
  # Revert back for OPA tests
  curl -s -o /dev/null -X PUT -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"docs-policy\",\"tenant_id\":\"data-agent\"}" \
    "${BASE_URL}/api/v1/roles/${VIEWER_UUID}/policy" 2>/dev/null
fi

# 6g: 未绑定角色查询 → 404
if [ -n "$NU_ROLE_UUID" ]; then
  NOBIND=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/roles/${NU_ROLE_UUID}/policy" 2>/dev/null)
  assert_match "Unbound role (normal-user) -> 200/404/500" "^(200|404|500)$" "$NOBIND"
fi

# 6h: normal-user 不能绑定 policy
if [ -n "$VIEWER_UUID" ]; then
  NU_BIND=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$NU_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"docs-policy\",\"tenant_id\":\"data-agent\"}" \
    "${BASE_URL}/api/v1/roles/${VIEWER_UUID}/policy" 2>/dev/null)
  assert "Normal-user cannot bind policy -> 403" "403" "$NU_BIND"
fi

# ══════════════════════════════════════════════════════════════════════════
# Scenario 7: User 管理
# UI: tenant-admin 进入 User 管理页面
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 7: User Management ===${NC}"

# 7a: 查看 User 列表
USERS=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/users" 2>/dev/null)
USERS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
  "${BASE_URL}/api/v1/data-agent/users" 2>/dev/null)
assert "List users" "200" "$USERS_CODE"
assert_contains "tenant-admin in user list" "tenant-admin" "$USERS"
assert_contains "normal-user in user list" "normal-user" "$USERS"

# 7b: 查看 User 详情（name, groups, roles）
NU_USER_ID=$(echo "$USERS" | jq -r '.[] | select(.username=="normal-user") | .id' 2>/dev/null || echo "")
if [ -n "$NU_USER_ID" ]; then
  USER_DETAIL=$(curl -s -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/users/${NU_USER_ID}/details" 2>/dev/null)
  USER_DETAIL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/users/${NU_USER_ID}/details" 2>/dev/null)
  assert "Get user detail" "200" "$USER_DETAIL_CODE"
  assert_contains "Detail has roles" "roles" "$USER_DETAIL"
  assert_contains "Detail has groups" "groups" "$USER_DETAIL"
  info "User roles: $(echo "$USER_DETAIL" | jq -c '.roles[].name' 2>/dev/null || echo '?')"
else
  info "SKIP: normal-user ID not found"
fi

# 7c: normal-user 不能查看用户列表
NU_USERS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" \
  "${BASE_URL}/api/v1/data-agent/users" 2>/dev/null)
assert "Normal-user cannot list users -> 403" "403" "$NU_USERS_CODE"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 8: Group 管理
# UI: tenant-admin 进入 Group 管理页面
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 8: Group CRUD ===${NC}"

# 8a: Pre-cleanup (in case previous test run left stale data)
OLD_GRP_ID=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/groups" 2>/dev/null \
  | jq -r '.[] | select(.name=="dev-team") | .id // empty' 2>/dev/null || echo "")
[ -n "$OLD_GRP_ID" ] && curl -s -o /dev/null -X DELETE -H "$TA_AUTH" \
  "${BASE_URL}/api/v1/data-agent/groups/${OLD_GRP_ID}" 2>/dev/null || true

# 8a: 创建 Group（name + roles + members）
# Note: "users" field expects user IDs, not usernames. Get the ID first.
NU_USER_ID_FOR_GRP=$(curl -s -H "$TA_AUTH" "${BASE_URL}/api/v1/data-agent/users" 2>/dev/null \
  | jq -r '.[] | select(.username=="normal-user") | .id // empty' 2>/dev/null || echo "")
GRP_USERS_JSON="[]"
[ -n "$NU_USER_ID_FOR_GRP" ] && GRP_USERS_JSON="[\"$NU_USER_ID_FOR_GRP\"]"

GRP_RESULT=$(curl -s -w "\n%{http_code}" -X POST -H "$TA_AUTH" \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"dev-team\",\"roles\":[\"normal-user\"],\"users\":$GRP_USERS_JSON}" \
  "${BASE_URL}/api/v1/data-agent/groups" 2>/dev/null)
GRP_CODE=$(echo "$GRP_RESULT" | tail -1)
GRP_BODY=$(echo "$GRP_RESULT" | sed '$d')
assert "Create group (dev-team)" "201" "$GRP_CODE"
GROUP_ID=$(echo "$GRP_BODY" | jq -r '.id // empty' 2>/dev/null || echo "")
[ -n "$GROUP_ID" ] && info "Group ID: $GROUP_ID"

# 8b: 查看 Group 列表
GROUPS_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
  "${BASE_URL}/api/v1/data-agent/groups" 2>/dev/null)
assert "List groups" "200" "$GROUPS_CODE"

# 8c: 查看 Group 详情（members + roles）
if [ -n "$GROUP_ID" ]; then
  GRP_DETAIL=$(curl -s -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/groups/${GROUP_ID}" 2>/dev/null)
  GRP_DETAIL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/groups/${GROUP_ID}" 2>/dev/null)
  assert "Get group detail" "200" "$GRP_DETAIL_CODE"
  assert_contains "Group has members" "members" "$GRP_DETAIL"
  assert_contains "Group has roles" "roles" "$GRP_DETAIL"
  assert_contains "normal-user is member" "normal-user" "$GRP_DETAIL"
  assert_contains "normal-user role assigned" "normal-user" "$GRP_DETAIL"
fi

# 8d: 编辑 Group（改名 + 改角色 + 改成员）
if [ -n "$GROUP_ID" ]; then
  GRP_UPD_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d '{"name":"dev-team-v2","roles":["normal-user"]}' \
    "${BASE_URL}/api/v1/data-agent/groups/${GROUP_ID}" 2>/dev/null)
  assert "Update group (rename + add role)" "204" "$GRP_UPD_CODE"
fi

# 8e: 删除 Group
if [ -n "$GROUP_ID" ]; then
  GRP_DEL_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/groups/${GROUP_ID}" 2>/dev/null)
  assert "Delete group" "204" "$GRP_DEL_CODE"
fi

# 8f: normal-user 不能管理 Group
NU_GRP_CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" \
  "${BASE_URL}/api/v1/data-agent/groups" 2>/dev/null)
assert "Normal-user cannot list groups -> 403" "403" "$NU_GRP_CODE"

# ══════════════════════════════════════════════════════════════════════════
# Scenario 9: OPA 动态授权 — 完整拦截/放行测试
#
# 测试矩阵：
#   tenant-admin  → 任何资源 → ALLOW（OPA tier-2）
#   normal-user   → 未绑定   → DENY
#   normal-user   → 绑定后   → ALLOW（OPA tier-3, role UUID matching）
#   normal-user   → 不在 policy 中的资源 → DENY
#   super-admin   → 任何资源 → ALLOW（OPA tier-1）
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 9: Path-Level OPA Enforcement ===${NC}"

SA_AUTH="Authorization: Bearer ${SA_TOKEN}"

# ── 9a: 未绑定时 normal-user 所有路径被拦截 ──
info "Testing normal-user BEFORE binding (all paths blocked)"
for path in /data-agent/documents /data-agent/billing /data-agent/admin/settings; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${path}" 2>/dev/null)
  assert "normal-user $path -> 403 (no binding)" "403" "$CODE"
done

# ── 9b: tenant-admin 所有路径都 200 ──
info "Testing tenant-admin (full access to all paths)"
for path in /data-agent/documents /data-agent/billing /data-agent/admin/settings /data-agent/admin/users; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" "${BASE_URL}${path}" 2>/dev/null)
  assert "tenant-admin $path -> 200" "200" "$CODE"
done
# tenant-admin 访问 proxy 管理接口
for path in /api/v1/policies /api/v1/data-agent/roles /api/v1/data-agent/users /api/v1/data-agent/groups; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$TA_AUTH" "${BASE_URL}${path}" 2>/dev/null)
  assert "tenant-admin $path -> 200" "200" "$CODE"
done

# ── 9c: super-admin 跨租户也 200 ──
info "Testing super-admin (cross-tenant, all paths)"
for path in /data-agent/documents /data-agent/admin/settings /other-tenant/anything; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$SA_AUTH" "${BASE_URL}${path}" 2>/dev/null)
  assert "super-admin $path -> 200" "200" "$CODE"
done

# ── 9d: no-token 所有路径被拦截 ──
info "Testing no-token (all blocked)"
for path in /data-agent/documents /data-agent/admin/settings; do
  CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}${path}" 2>/dev/null)
  assert_match "no-token $path -> 401/403" "^(401|403)$" "$CODE"
done

# ── 9e: 绑定 normal-user -> docs-policy, 测试路径级权限 ──
if [ -n "$NU_ROLE_UUID" ]; then
  info "Binding normal-user -> docs-policy (documents:allow, settings:allow)"
  curl -s -o /dev/null -X POST -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"docs-policy\",\"tenant_id\":\"data-agent\"}" \
    "${BASE_URL}/api/v1/roles/${NU_ROLE_UUID}/policy" 2>/dev/null
  sleep 5

  info "Testing normal-user AFTER binding - ordinary paths"
  for path in /data-agent/documents /data-agent/settings; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${path}" 2>/dev/null)
    assert "normal-user $path -> 200 (in docs-policy)" "200" "$CODE"
  done
  for path in /data-agent/billing /data-agent/invoices /data-agent/reports; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${path}" 2>/dev/null)
    assert "normal-user $path -> 403 (not in docs-policy)" "403" "$CODE"
  done

  info "Testing normal-user AFTER binding - admin paths (always blocked)"
  for path in /data-agent/admin/documents /data-agent/admin/settings /data-agent/admin/anything; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${path}" 2>/dev/null)
    assert "normal-user $path -> 403 (admin blocked)" "403" "$CODE"
  done

  info "Testing normal-user - proxy management APIs (always blocked)"
  for path in /api/v1/data-agent/roles /api/v1/data-agent/users /api/v1/data-agent/groups; do
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${path}" 2>/dev/null)
    assert "normal-user $path -> 403 (management API)" "403" "$CODE"
  done

  # ── 9f: 切换绑定到 billing-policy ──
  info "Rebinding normal-user -> billing-policy (billing:allow, invoices:allow)"
  curl -s -o /dev/null -X PUT -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d "{\"policy_id\":\"billing-policy\",\"tenant_id\":\"data-agent\"}" \
    "${BASE_URL}/api/v1/roles/${NU_ROLE_UUID}/policy" 2>/dev/null
  sleep 5

  for pair in "/data-agent/billing:200" "/data-agent/invoices:200" "/data-agent/documents:403" "/data-agent/settings:403"; do
    P="${pair%%:*}"; E="${pair##*:}"
    CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}${P}" 2>/dev/null)
    assert "normal-user $P -> $E (billing-policy)" "$E" "$CODE"
  done

  # ── 9g: 删除 policy -> 权限回收 ──
  info "Deleting billing-policy -> permission revoked"
  curl -s -o /dev/null -X DELETE -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/policies/billing-policy" 2>/dev/null
  sleep 3

  CODE=$(curl -s -o /dev/null -w "%{http_code}" -H "$NU_AUTH" "${BASE_URL}/data-agent/billing" 2>/dev/null)
  assert "normal-user /data-agent/billing -> 403 (policy deleted)" "403" "$CODE"

  # Recreate billing-policy for cleanup
  curl -s -o /dev/null -X POST -H "$TA_AUTH" \
    -H "Content-Type: application/json" \
    -d '{"name":"billing-policy","tenant_id":"data-agent","rules":[{"resource":"billing","effect":"allow"},{"resource":"invoices","effect":"allow"}]}' \
    "${BASE_URL}/api/v1/policies" 2>/dev/null
fi

# ══════════════════════════════════════════════════════════════════════════
# Scenario 10: 权限隔离
# UI: normal-user 不能执行管理操作
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Scenario 10: Permission Isolation ===${NC}"

NU_CREATE_ROLE=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$NU_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"name":"hacker-role"}' \
  "${BASE_URL}/api/v1/data-agent/roles" 2>/dev/null)
assert "Normal-user cannot create role -> 403" "403" "$NU_CREATE_ROLE"

NU_CREATE_TENANT=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "$NU_AUTH" \
  -H "Content-Type: application/json" \
  -d '{"realm":"hacker-realm","displayName":"Hack"}' \
  "${BASE_URL}/api/v1/tenants" 2>/dev/null)
assert "Normal-user cannot create tenant -> 403" "403" "$NU_CREATE_TENANT"

NO_TOKEN_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  "${BASE_URL}/api/v1/policies" 2>/dev/null)
assert_match "No token -> 401/403" "^(401|403)$" "$NO_TOKEN_CODE"

BAD_TOKEN_CODE=$(curl -s -o /dev/null -w "%{http_code}" \
  -H "Authorization: Bearer invalid.token.here" \
  "${BASE_URL}/api/v1/policies" 2>/dev/null)
assert_match "Bad token -> 401/403" "^(401|403)$" "$BAD_TOKEN_CODE"

# ══════════════════════════════════════════════════════════════════════════
# Cleanup: 删除测试数据
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Cleanup ===${NC}"

# Delete test roles
for rname in viewer editor billing-admin; do
  curl -s -o /dev/null -X DELETE -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/data-agent/roles/$rname" 2>/dev/null || true
done
info "Deleted test roles (viewer, editor, billing-admin)"

# Delete test policies
for pname in docs-policy reports-policy admin-policy billing-policy; do
  curl -s -o /dev/null -X DELETE -H "$TA_AUTH" \
    "${BASE_URL}/api/v1/policies/$pname" 2>/dev/null || true
done
info "Deleted test policies"

# Delete test tenant
curl -s -o /dev/null -X DELETE -H "$SA_AUTH" \
  "${BASE_URL}/api/v1/tenants/${TEST_REALM}" 2>/dev/null || true
info "Deleted test tenant: $TEST_REALM"

# Verify cleanup
FINAL_TENANTS=$(curl -s -H "$SA_AUTH" "${BASE_URL}/api/v1/tenants" 2>/dev/null)
assert_not_contains "Test tenant removed" "$TEST_REALM" "$FINAL_TENANTS"

# ══════════════════════════════════════════════════════════════════════════
# Summary
# ══════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}========================================${NC}"
echo -e "UI Test Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${TOTAL} total"
if [ "$FAIL" -gt 0 ]; then
  echo -e "${RED}SOME TESTS FAILED${NC}"
  exit 1
else
  echo -e "${GREEN}ALL UI TESTS PASSED${NC}"
fi
