#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# test-images.sh — Test Docker images without K8s
#
# Usage:
#   ./scripts/test-images.sh                  # Test images in docker
#   IMAGES_DIR=/path/to/tars ./scripts/test-images.sh  # Load from tars first
# ============================================================================

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PASS=0
FAIL=0
TOTAL=0

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

cleanup() {
  echo -e "\n${YELLOW}Cleaning up...${NC}"
  docker rm -f test-postgres test-keycloak test-proxy test-opal-proxy \
    test-opal-server test-nginx test-httpbin 2>/dev/null || true
  docker network rm test-iam-net 2>/dev/null || true
}
trap cleanup EXIT

# ── Load images from tars if IMAGES_DIR is set ──────────────────────────
if [ -n "${IMAGES_DIR:-}" ] && [ -d "$IMAGES_DIR" ]; then
  echo -e "${YELLOW}Loading images from $IMAGES_DIR ...${NC}"
  for tar in "$IMAGES_DIR"/*.tar; do
    [ -f "$tar" ] || continue
    echo -n "  $(basename "$tar")... "
    docker load -i "$tar" 2>/dev/null && echo "OK" || echo "SKIP"
  done
  echo ""
fi

# ════════════════════════════════════════════════════════════════════════
# Section 1: Architecture check
# ════════════════════════════════════════════════════════════════════════
echo -e "${YELLOW}=== Section 1: Architecture Check ===${NC}"

EXPECTED_ARCH=$(uname -m)
case "$EXPECTED_ARCH" in
  x86_64)  DOCKER_ARCH="amd64" ;;
  aarch64) DOCKER_ARCH="arm64" ;;
  *)       DOCKER_ARCH="$EXPECTED_ARCH" ;;
esac

for img in keycloak-proxy:v2 opal-proxy:v1 keycloak-init:v1 keycloak-custom:26.5.2; do
  if docker image inspect "$img" &>/dev/null; then
    ARCH=$(docker image inspect "$img" --format '{{.Architecture}}' 2>/dev/null || echo "unknown")
    assert "$img arch = $DOCKER_ARCH" "$DOCKER_ARCH" "$ARCH"
  else
    echo -e "  ${YELLOW}SKIP${NC} $img not found"
  fi
done

# ════════════════════════════════════════════════════════════════════════
# Section 2: keycloak-proxy:v2 (FastAPI app)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 2: keycloak-proxy:v2 ===${NC}"

# Check app modules can import
IMPORT_OK=$(docker run --rm keycloak-proxy:v2 python3 -c "
from app.main import app
from app.api.v1 import tenants, idp, identity, common, token
print('OK')
" 2>&1 || echo "FAIL")
assert "app imports OK" "OK" "$IMPORT_OK"

# Check FastAPI routes registered
ROUTES=$(docker run --rm keycloak-proxy:v2 python3 -c "
from app.main import app
paths = [r.path for r in app.routes]
# Check key routes exist
for p in ['/api/v1/tenants', '/api/v1/{realm}/roles', '/api/v1/{realm}/groups',
          '/api/v1/{realm}/users', '/api/v1/{realm}/idp', '/api/v1/{realm}/token/exchange',
          '/api/v1/{realm}/roles/by-id/{role_id}']:
    if not any(p in str(rp) for rp in paths):
        print(f'MISSING: {p}')
        exit(1)
print('OK')
" 2>&1 || echo "FAIL")
assert "all routes registered" "OK" "$ROUTES"

# Check schemas
SCHEMAS=$(docker run --rm keycloak-proxy:v2 python3 -c "
from app.schemas.roles import RoleCreate, RoleUpdate, RoleResponse, RoleUpdateByIdRequest
from app.schemas.groups import GroupCreate, GroupUpdate, GroupResponse, GroupDetailResponse
from app.schemas.idp import IDPRequest, IdPMapperCreate, IdPMapperUpdate, IdPMapperResponse, IDPInstanceResponse
from app.schemas.realm import TenantCreate, TenantResponse, TenantListResponse
from app.schemas.users import UserResponse, UserContextResponse
from app.schemas.token import TokenExchangeRequest, TokenExchangeResponse
print('OK')
" 2>&1 || echo "FAIL")
assert "all schemas importable" "OK" "$SCHEMAS"

# Start and test health endpoint
docker run -d --name test-proxy -e KEYCLOAK_URL=http://localhost:8080 \
  keycloak-proxy:v2 >/dev/null 2>&1
sleep 5
HEALTH=$(docker exec test-proxy python3 -c "
import urllib.request, json
try:
    resp = urllib.request.urlopen('http://localhost:8090/api/v1/common/health', timeout=3)
    print(json.loads(resp.read()).get('status',''))
except: print('fail')
" 2>/dev/null || echo "fail")
assert "health endpoint returns healthy" "healthy" "$HEALTH"
docker rm -f test-proxy >/dev/null 2>&1

# ════════════════════════════════════════════════════════════════════════
# Section 3: opal-proxy:v1 (supervisord + pep-proxy + bundle-server)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 3: opal-proxy:v1 ===${NC}"

# Check supervisord exists
SUP_CHECK=$(docker run --rm opal-proxy:v1 sh -c "which supervisord && echo OK" 2>&1 | tail -1)
assert "supervisord found" "OK" "$SUP_CHECK"

# Check app imports
PEP_IMPORT=$(docker run --rm opal-proxy:v1 python3 -c "
from pep_proxy.main import app
from bundle_server.main import app as bs_app
print('OK')
" 2>&1 || echo "FAIL")
assert "pep-proxy + bundle-server imports OK" "OK" "$PEP_IMPORT"

# Check proto stubs
PROTO_CHECK=$(docker run --rm opal-proxy:v1 python3 -c "
import ext_authz_pb2
import ext_authz_pb2_grpc
print('OK')
" 2>&1 || echo "FAIL")
assert "gRPC proto stubs OK" "OK" "$PROTO_CHECK"

# Check supervisord config
CONF_CHECK=$(docker run --rm opal-proxy:v1 sh -c "test -f /etc/supervisord.conf && echo OK" 2>&1)
assert "supervisord.conf exists" "OK" "$CONF_CHECK"

# Start and test both services
docker run -d --name test-opal-proxy \
  -e DB_URL=postgresql://test:test@localhost:5432/test \
  -e OPA_URL=http://localhost:8181 \
  -e OPAL_SERVER_URL=http://localhost:7002 \
  opal-proxy:v1 >/dev/null 2>&1
sleep 5

PEP_HEALTH=$(docker exec test-opal-proxy python3 -c "
import urllib.request, json
try:
    resp = urllib.request.urlopen('http://localhost:8000/health', timeout=3)
    print(json.loads(resp.read()).get('status',''))
except: print('fail')
" 2>/dev/null || echo "fail")
assert "pep-proxy health endpoint" "healthy" "$PEP_HEALTH"

BS_HEALTH=$(docker exec test-opal-proxy python3 -c "
import urllib.request, json
try:
    resp = urllib.request.urlopen('http://localhost:8001/health', timeout=3)
    d = json.loads(resp.read())
    print(d.get('status', 'ok') if 'error' not in str(d) else 'fail')
except: print('fail')
" 2>/dev/null || echo "fail")
assert "bundle-server health endpoint" "ok" "$BS_HEALTH"

docker rm -f test-opal-proxy >/dev/null 2>&1

# ════════════════════════════════════════════════════════════════════════
# Section 4: keycloak-init:v1 (Python init script)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 4: keycloak-init:v1 ===${NC}"

# Check script exists
SCRIPT_CHECK=$(docker run --rm keycloak-init:v1 sh -c "test -f /app/init-keycloak.py && echo OK" 2>&1)
assert "init-keycloak.py exists" "OK" "$SCRIPT_CHECK"

# Check dependencies
DEPS_CHECK=$(docker run --rm keycloak-init:v1 python3 -c "
import requests
import kubernetes
print('OK')
" 2>&1 || echo "FAIL")
assert "requests + kubernetes importable" "OK" "$DEPS_CHECK"

# Check script is parseable (no syntax errors)
SYNTAX_CHECK=$(docker run --rm keycloak-init:v1 python3 -c "
import ast
with open('/app/init-keycloak.py') as f:
    ast.parse(f.read())
print('OK')
" 2>&1 || echo "FAIL")
assert "init script syntax OK" "OK" "$SYNTAX_CHECK"

# ════════════════════════════════════════════════════════════════════════
# Section 5: keycloak-custom:26.5.2 (Keycloak + SPI)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 5: keycloak-custom:26.5.2 ===${NC}"

# Check SPI jar exists
JAR_CHECK=$(docker run --rm --entrypoint sh keycloak-custom:26.5.2 -c \
  "test -f /opt/keycloak/providers/data-agent-mapper.jar && echo OK" 2>&1)
assert "data-agent-mapper.jar exists" "OK" "$JAR_CHECK"

# Check jar contents (use python since unzip/jar not available in keycloak image)
JAR_CONTENTS=$(docker run --rm --entrypoint sh keycloak-custom:26.5.2 -c "
python3 -c \"
import zipfile
with zipfile.ZipFile('/opt/keycloak/providers/data-agent-mapper.jar') as z:
    names = z.namelist()
    has_js = any('.js' in n and 'META-INF' not in n for n in names)
    has_json = any('keycloak-scripts.json' in n for n in names)
    print('2' if has_js and has_json else '0')
\" 2>/dev/null || echo 0" 2>&1 | tail -1)
assert "jar contains js + keycloak-scripts.json" "2" "$JAR_CONTENTS"

# Check scripts feature was built
SCRIPTS_BUILT=$(docker run --rm --entrypoint sh keycloak-custom:26.5.2 -c "
grep -r 'scripts' /opt/keycloak/lib/quarkus/ 2>/dev/null | head -1 | wc -l || echo 0" 2>&1 | tail -1)
assert "scripts feature built" "1" "$SCRIPTS_BUILT"

# Check kc.sh works
KC_VERSION=$(docker run --rm --entrypoint sh keycloak-custom:26.5.2 -c \
  "/opt/keycloak/bin/kc.sh --version 2>/dev/null | head -1" 2>&1 || echo "FAIL")
assert_contains "Keycloak version" "26.5.2" "$KC_VERSION"

# ════════════════════════════════════════════════════════════════════════
# Section 6: postgres:17
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 6: postgres:17 ===${NC}"

docker run -d --name test-postgres \
  -e POSTGRES_USER=test -e POSTGRES_PASSWORD=test -e POSTGRES_DB=testdb \
  postgres:17 >/dev/null 2>&1
sleep 5

PG_READY=$(docker exec test-postgres pg_isready -U test 2>/dev/null | grep -c "accepting connections" || echo "0")
assert "postgres accepting connections" "1" "$PG_READY"

PG_QUERY=$(docker exec test-postgres psql -U test -d testdb -c "SELECT 1 AS ok;" -t 2>/dev/null | tr -d ' \n')
assert "postgres query works" "1" "$PG_QUERY"

docker rm -f test-postgres >/dev/null 2>&1

# ════════════════════════════════════════════════════════════════════════
# Section 7: Third-party images basic check
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 7: Third-party Images ===${NC}"

# nginx
NGINX_VER=$(docker run --rm nginx:alpine nginx -v 2>&1 || echo "FAIL")
assert_contains "nginx runs" "nginx" "$NGINX_VER"

# opal-server
if docker image inspect permitio/opal-server:0.7.4 &>/dev/null; then
  OPAL_S=$(docker run --rm --entrypoint python3 permitio/opal-server:0.7.4 -c "import opal_server; print('OK')" 2>&1 || echo "FAIL")
  assert "opal-server importable" "OK" "$OPAL_S"
else
  echo -e "  ${YELLOW}SKIP${NC} permitio/opal-server:0.7.4 not found"
fi

# opal-client
if docker image inspect permitio/opal-client:0.7.4 &>/dev/null; then
  OPAL_C=$(docker run --rm --entrypoint python3 permitio/opal-client:0.7.4 -c "import opal_client; print('OK')" 2>&1 || echo "FAIL")
  assert "opal-client importable" "OK" "$OPAL_C"
else
  echo -e "  ${YELLOW}SKIP${NC} permitio/opal-client:0.7.4 not found"
fi

# ════════════════════════════════════════════════════════════════════════
# Section 8: Integration test (postgres + keycloak + proxy)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 8: Integration Test (Keycloak + Proxy) ===${NC}"

# Create network
docker network create test-iam-net >/dev/null 2>&1

# Start postgres
docker run -d --name test-postgres --network test-iam-net \
  -e POSTGRES_USER=keycloak -e POSTGRES_PASSWORD=keycloak -e POSTGRES_DB=keycloak \
  postgres:17 >/dev/null 2>&1
sleep 5

# Start keycloak
docker run -d --name test-keycloak --network test-iam-net \
  -e KC_BOOTSTRAP_ADMIN_USERNAME=admin \
  -e KC_BOOTSTRAP_ADMIN_PASSWORD=admin \
  -e KC_DB=postgres \
  -e KC_DB_URL_HOST=test-postgres \
  -e KC_DB_URL_DATABASE=keycloak \
  -e KC_DB_USERNAME=keycloak \
  -e KC_DB_PASSWORD=keycloak \
  -e KC_HEALTH_ENABLED=true \
  -e KC_HTTP_ENABLED=true \
  -e KC_HOSTNAME_STRICT=false \
  -e KC_FEATURES=scripts \
  keycloak-custom:26.5.2 start-dev >/dev/null 2>&1

echo -e "  ${YELLOW}Waiting for Keycloak to start (may take 60-120s)...${NC}"
KC_READY=false
for i in $(seq 1 90); do
  KC_CHECK=$(docker exec test-keycloak python3 -c "
import urllib.request
try:
    urllib.request.urlopen('http://localhost:8080/health/ready', timeout=2)
    print('ready')
except: print('wait')
" 2>/dev/null || echo "wait")
  if [ "$KC_CHECK" = "ready" ]; then
    KC_READY=true
    break
  fi
  sleep 2
done
assert "Keycloak started" "true" "$KC_READY"

if [ "$KC_READY" = "true" ]; then
  # Get admin token
  ADMIN_TOKEN=$(docker exec test-keycloak python3 -c "
import urllib.request, urllib.parse, json
data = urllib.parse.urlencode({
    'grant_type': 'password', 'client_id': 'admin-cli',
    'username': 'admin', 'password': 'admin'
}).encode()
req = urllib.request.Request('http://localhost:8080/realms/master/protocol/openid-connect/token', data=data)
resp = urllib.request.urlopen(req, timeout=5)
print(json.loads(resp.read()).get('access_token',''))
" 2>/dev/null || echo "")

  assert "admin token acquired" "true" "$([ -n "$ADMIN_TOKEN" ] && echo true || echo false)"

  if [ -n "$ADMIN_TOKEN" ]; then
    # Check script mapper provider is available
    MAPPER_CHECK=$(docker exec test-keycloak python3 -c "
import urllib.request, json
req = urllib.request.Request('http://localhost:8080/admin/serverinfo')
req.add_header('Authorization', 'Bearer $ADMIN_TOKEN')
resp = urllib.request.urlopen(req, timeout=10)
info = json.loads(resp.read())
mappers = info.get('protocolMapperTypes',{}).get('openid-connect',[])
found = any(m['id'] == 'script-data-agent-mapper.js' for m in mappers)
print('true' if found else 'false')
" 2>/dev/null || echo "false")
    assert "script-data-agent-mapper.js provider available" "true" "$MAPPER_CHECK"
  fi

  # Start keycloak-proxy connected to keycloak
  docker run -d --name test-proxy --network test-iam-net \
    -e KEYCLOAK_URL=http://test-keycloak:8080 \
    -e KEYCLOAK_HEALTH_URL=http://test-keycloak:8080 \
    -e ADMIN_USER=admin \
    -e ADMIN_PASSWORD=admin \
    -e KC_REALM=master \
    -e KC_CLIENT_ID=admin-cli \
    -e KC_SCRIPT_MAPPER=script-data-agent-mapper.js \
    keycloak-proxy:v2 >/dev/null 2>&1
  sleep 5

  # Test proxy health (use python, no curl in image)
  PROXY_HEALTH=$(docker exec test-proxy python3 -c "
import urllib.request, json
try:
    resp = urllib.request.urlopen('http://localhost:8090/api/v1/common/health', timeout=3)
    print(json.loads(resp.read()).get('status',''))
except: print('fail')
" 2>/dev/null || echo "fail")
  assert "proxy health via network" "healthy" "$PROXY_HEALTH"

  # Test proxy can talk to keycloak (list realms)
  REALMS=$(docker exec test-proxy python3 -c "
import urllib.request
try:
    resp = urllib.request.urlopen('http://localhost:8090/api/v1/tenants', timeout=5)
    print(resp.read().decode())
except Exception as e: print(f'FAIL: {e}')
" 2>/dev/null || echo "FAIL")
  assert_contains "proxy lists realms from keycloak" "master" "$REALMS"
fi

# ════════════════════════════════════════════════════════════════════════
# Section 9: Fat base images (if present)
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}=== Section 9: Fat Base Images ===${NC}"

for base_img in "base-keycloak-proxy:v1" "base-opal-proxy:v1" "base-keycloak-init:v1"; do
  if docker image inspect "$base_img" &>/dev/null; then
    ARCH=$(docker image inspect "$base_img" --format '{{.Architecture}}' 2>/dev/null || echo "unknown")
    assert "$base_img exists (arch=$ARCH)" "$DOCKER_ARCH" "$ARCH"
  else
    echo -e "  ${YELLOW}SKIP${NC} $base_img not found"
  fi
done

# Test slim build from fat base (if base images exist)
if docker image inspect base-keycloak-proxy:v1 &>/dev/null; then
  SLIM_BUILD=$(echo 'FROM base-keycloak-proxy:v1
COPY --from=keycloak-proxy:v2 /app/app/ /app/app/' | docker build -q -t test-slim-proxy -f - . 2>&1 || echo "FAIL")
  if [ "$SLIM_BUILD" != "FAIL" ]; then
    SLIM_OK=$(docker run --rm test-slim-proxy python3 -c "from app.main import app; print('OK')" 2>&1 || echo "FAIL")
    assert "slim build from fat base works" "OK" "$SLIM_OK"
    docker rmi test-slim-proxy >/dev/null 2>&1 || true
  else
    assert "slim build from fat base works" "OK" "FAIL"
  fi
fi

# ════════════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════════════
echo -e "\n${YELLOW}========================================${NC}"
echo -e "Test Results: ${GREEN}${PASS} passed${NC}, ${RED}${FAIL} failed${NC}, ${TOTAL} total"
if [ "$FAIL" -gt 0 ]; then
  echo -e "${RED}SOME TESTS FAILED${NC}"
  exit 1
else
  echo -e "${GREEN}ALL TESTS PASSED${NC}"
fi
