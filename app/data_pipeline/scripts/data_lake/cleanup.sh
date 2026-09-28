#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command helm

"${HELM[@]}" uninstall "$HELM_RELEASE" --namespace "$DATA_LAKE_NAMESPACE" --ignore-not-found
if [[ ${DELETE_DATA:-false} != true ]]; then
  echo "MinIO PVC retained. Set DELETE_DATA=true and CONFIRM_DELETE_DATA=$HELM_RELEASE to delete it."
  exit 0
fi
[[ ${CONFIRM_DELETE_DATA:-} == "$HELM_RELEASE" ]] || {
  echo "Refusing PVC deletion without CONFIRM_DELETE_DATA=$HELM_RELEASE" >&2
  exit 2
}
require_command kubectl
"${KUBECTL[@]}" -n "$DATA_LAKE_NAMESPACE" delete pvc \
  -l "app.kubernetes.io/instance=$HELM_RELEASE" --ignore-not-found
