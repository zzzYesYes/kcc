#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm
require_command kubectl

echo "== Helm release =="
helm -n "$NAMESPACE" status "$RELEASE" 2>/dev/null || true
echo "== Pods =="
kubectl -n "$NAMESPACE" get pods -l "app.kubernetes.io/instance=$RELEASE" -o wide
echo "== Services =="
kubectl -n "$NAMESPACE" get services -l "app.kubernetes.io/instance=$RELEASE"
echo "== RayCluster =="
kubectl -n "$NAMESPACE" get rayclusters.ray.io -l "app.kubernetes.io/instance=$RELEASE"
echo "== NPU Deployments =="
kubectl -n "$NAMESPACE" get deployments -l "app.kubernetes.io/instance=$RELEASE" \
  -o custom-columns=NAME:.metadata.name,COMPONENT:.metadata.labels.app\\.kubernetes\\.io/component,DESIRED:.spec.replicas,READY:.status.readyReplicas
