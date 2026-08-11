#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-dagster-qwen-demo}
RELEASE=${RELEASE:-qwen-demo}

kubectl -n "$NAMESPACE" get deployment,service,pod \
  -l "app.kubernetes.io/instance=$RELEASE" -o wide
