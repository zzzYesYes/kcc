#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command helm
require_command kubectl

"${HELM[@]}" -n "$DATA_LAKE_NAMESPACE" status "$HELM_RELEASE" 2>/dev/null || true
"${KUBECTL[@]}" -n "$DATA_LAKE_NAMESPACE" get statefulset,pod,svc,pvc,job \
  -l "app.kubernetes.io/instance=$HELM_RELEASE" -o wide
