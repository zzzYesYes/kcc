#!/bin/bash

NAMESPACE="opal-dynamic-policy"
AGENTGATEWAY_NAMESPACE="agentgateway-system-opa"

echo "清理 $NAMESPACE..."

# 删除命名空间
kubectl delete namespace $NAMESPACE --timeout=60s
kubectl delete namespace $AGENTGATEWAY_NAMESPACE --timeout=60s

# 清理端口转发
pkill -f "kubectl port-forward" 2>/dev/null || true


echo "清理完成！"