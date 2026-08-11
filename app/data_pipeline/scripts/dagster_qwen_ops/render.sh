#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
HELM_BIN=${HELM_BIN:-helm}
RELEASE=${RELEASE:-qwen-demo}
NAMESPACE=${NAMESPACE:-dagster-qwen-demo}
PROFILE=${PROFILE:-$ROOT/helm/dagster-qwen-ops/values.yaml}

"$HELM_BIN" template "$RELEASE" "$ROOT/helm/dagster-qwen-ops" \
  --namespace "$NAMESPACE" -f "$PROFILE"
