#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-dagster-qwen-demo}
RELEASE=${RELEASE:-qwen-demo}
LOCAL_PORT=${LOCAL_PORT:-3000}
SERVICE_PORT=${SERVICE_PORT:-3000}

kubectl -n "$NAMESPACE" port-forward \
  "service/$RELEASE-dagster-qwen-ops-dagster" "$LOCAL_PORT:$SERVICE_PORT"
