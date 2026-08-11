#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command kubectl
require_value AWS_ACCESS_KEY_ID
require_value AWS_SECRET_ACCESS_KEY

"${KUBECTL[@]}" create namespace "$DATA_LAKE_NAMESPACE" --dry-run=client -o yaml |
  "${KUBECTL[@]}" apply -f -
"${KUBECTL[@]}" -n "$DATA_LAKE_NAMESPACE" create secret generic "$MINIO_EXISTING_SECRET" \
  --from-literal=MINIO_ROOT_USER="$AWS_ACCESS_KEY_ID" \
  --from-literal=MINIO_ROOT_PASSWORD="$AWS_SECRET_ACCESS_KEY" \
  --dry-run=client -o yaml | "${KUBECTL[@]}" apply -f -
printf 'Secret %s is present in namespace %s.\n' "$MINIO_EXISTING_SECRET" "$DATA_LAKE_NAMESPACE"
