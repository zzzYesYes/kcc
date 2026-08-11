#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm

helm lint "$CHART_DIR"
helm lint "$CHART_DIR" -f "$CHART_DIR/values-smoke.yaml"
helm lint "$CHART_DIR" -f "$CHART_DIR/values-production.yaml"
for profile in default smoke production; do
  args=()
  [[ "$profile" == default ]] || args=(-f "$CHART_DIR/values-$profile.yaml")
  output="/tmp/${RELEASE}-${profile}.yaml"
  helm template "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" "${args[@]}" >"$output"
  python3 "$SCRIPT_DIR/validate_helm_render.py" "$output"
done
echo "Helm lint/template passed for default, smoke, and production profiles."
