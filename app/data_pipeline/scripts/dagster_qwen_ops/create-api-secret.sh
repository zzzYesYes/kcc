#!/usr/bin/env bash
set -euo pipefail

NAMESPACE=${NAMESPACE:-dagster-qwen-demo}
SECRET_NAME=${SECRET_NAME:-dagster-qwen-api}
API_KEY=${API_KEY:?Set API_KEY; it is never written to a repository file}

kubectl create namespace "$NAMESPACE" --dry-run=client -o yaml | kubectl apply -f -
kubectl -n "$NAMESPACE" create secret generic "$SECRET_NAME" \
  --from-literal="api-key=$API_KEY" \
  --dry-run=client -o yaml | kubectl apply -f -
echo "Created/updated Secret $NAMESPACE/$SECRET_NAME"
