#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command helm

args=(upgrade --install "$HELM_RELEASE" "$CHART_DIR"
  --namespace "$DATA_LAKE_NAMESPACE" --create-namespace
  --set-string "credentials.existingSecret=$MINIO_EXISTING_SECRET"
  --set-string "s3.endpoint=$S3_ENDPOINT_URL"
  --set-string "minio.namespace=$DATA_LAKE_NAMESPACE"
  --set-string "minio.persistence.storageClass=$MINIO_STORAGE_CLASS"
  --set-string "minio.persistence.size=$MINIO_STORAGE_SIZE"
  --set "minio.service.apiNodePort=${MINIO_API_NODE_PORT:-30900}"
  --set "minio.service.consoleNodePort=${MINIO_CONSOLE_NODE_PORT:-31901}"
  --wait --timeout "${HELM_TIMEOUT:-10m}")
[[ -z "${HELM_VALUES:-}" ]] || args+=(--values "$HELM_VALUES")
"${HELM[@]}" "${args[@]}" "$@"
