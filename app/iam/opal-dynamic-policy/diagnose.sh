# 开启网关的调试日志（临时）
kubectl set env deploy/agent-gateway -n agentgateway-system-opa RUST_LOG=debug

# 重启网关
kubectl rollout restart deploy/agent-gateway -n agentgateway-system-opa

# 触发一个请求
curl -H "Authorization: Bearer $ADMIN_JWT" \
  http://localhost:8080/api/v1/policies/templates

# 查看详细日志
kubectl logs -n agentgateway-system-opa deploy/agent-gateway --tail=10 | grep -i "ext_auth"




