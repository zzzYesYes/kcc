#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"
LOCAL_PORT="${LOCAL_PORT:-8080}"
AGENTGATEWAY_NS="agentgateway-system"

fail() {
  printf '[FAIL] %s\n' "$*" >&2
  exit 1
}

command -v kind >/dev/null 2>&1 || fail "'kind' is required"
command -v kubectl >/dev/null 2>&1 || fail "'kubectl' is required"
command -v curl >/dev/null 2>&1 || fail "'curl' is required"

kind get clusters 2>/dev/null | grep -qx "$CLUSTER_NAME" \
  || fail "Kind cluster '$CLUSTER_NAME' does not exist"
kubectl config use-context "kind-$CLUSTER_NAME" >/dev/null

kubectl wait --for=condition=Ready node --all --timeout=60s >/dev/null
kubectl -n httpbin rollout status deployment/httpbin --timeout=60s >/dev/null

accepted="$(
  kubectl -n "$AGENTGATEWAY_NS" get httproute helloworld-route \
    -o jsonpath='{.status.parents[0].conditions[?(@.type=="Accepted")].status}'
)"
resolved_refs="$(
  kubectl -n "$AGENTGATEWAY_NS" get httproute helloworld-route \
    -o jsonpath='{.status.parents[0].conditions[?(@.type=="ResolvedRefs")].status}'
)"
[ "$accepted" = "True" ] || fail "HTTPRoute is not Accepted"
[ "$resolved_refs" = "True" ] || fail "HTTPRoute references are not resolved"

kubectl -n "$AGENTGATEWAY_NS" port-forward \
  service/agentgateway-proxy "${LOCAL_PORT}:80" >/tmp/aidp-helloworld-port-forward.log 2>&1 &
port_forward_pid=$!

status=""
response_file="$(mktemp)"
expected_file="$(mktemp)"
printf 'quzihan_test' >"$expected_file"

cleanup() {
  kill "$port_forward_pid" >/dev/null 2>&1 || true
  wait "$port_forward_pid" 2>/dev/null || true
  rm -f "$response_file" "$expected_file"
}

trap cleanup EXIT

for _ in $(seq 1 30); do
  status="$(
    curl -sS -o "$response_file" -w '%{http_code}' \
      "http://localhost:${LOCAL_PORT}/hello" 2>/dev/null || true
  )"
  if [ "$status" = "200" ]; then
    break
  fi
  sleep 1
done

[ "$status" = "200" ] || fail "/hello returned HTTP ${status:-000}"
cmp -s "$response_file" "$expected_file" \
  || fail "/hello response body was not exactly 'quzihan_test'"

printf '[PASS] Kind node is Ready\n'
printf '[PASS] httpbin is Ready\n'
printf '[PASS] helloworld-route is Accepted and Resolved\n'
printf '[PASS] GET /hello returned HTTP 200 with body: quzihan_test\n'
