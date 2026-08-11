#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm
load_profile_args

output=${1:-"/tmp/${RELEASE}-${PROFILE}.yaml"}
helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" \
  "${HELM_PROFILE_ARGS[@]}" >"$output"
echo "$output"
