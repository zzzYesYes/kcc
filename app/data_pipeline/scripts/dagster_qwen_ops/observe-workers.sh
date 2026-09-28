#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-dagster-qwen-demo}
RELEASE=${RELEASE:-qwen-demo}

echo "Watching Qwen Deployments and Pods in $NAMESPACE (Ctrl-C to stop)"
kubectl -n "$NAMESPACE" get deployment,pod \
  -l "app.kubernetes.io/instance=$RELEASE" --watch
