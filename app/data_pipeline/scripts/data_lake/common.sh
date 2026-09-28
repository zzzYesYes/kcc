#!/usr/bin/env bash
set -euo pipefail

MODULE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
ENV_FILE=${ENV_FILE:-"$MODULE_DIR/.env"}
if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

: "${HELM_RELEASE:=k12-data-lake}"
: "${DATA_LAKE_NAMESPACE:=k12-lake}"
: "${MINIO_EXISTING_SECRET:=minio-k12-root}"
: "${MINIO_STORAGE_CLASS:=local-path}"
: "${MINIO_STORAGE_SIZE:=500Gi}"
: "${S3_ENDPOINT_URL:=http://minio-k12.${DATA_LAKE_NAMESPACE}.svc.cluster.local:9000}"

CHART_DIR="$MODULE_DIR/helm/data-lake"
KUBECTL=(kubectl)
HELM=(helm)
if [[ -n "${KUBE_CONTEXT:-}" ]]; then
  KUBECTL+=(--context "$KUBE_CONTEXT")
  HELM+=(--kube-context "$KUBE_CONTEXT")
fi

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    printf 'required command not found: %s\n' "$1" >&2
    exit 127
  }
}

require_value() {
  local name=$1
  [[ -n "${!name:-}" ]] || {
    printf 'required variable is empty: %s\n' "$name" >&2
    exit 2
  }
}
