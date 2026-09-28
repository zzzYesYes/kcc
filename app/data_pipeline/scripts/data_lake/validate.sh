#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command helm
require_command python3

find "$(dirname "$0")" -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
python3 -m compileall -q "$MODULE_DIR/src/data_lake"
"${HELM[@]}" lint "$CHART_DIR"
"${HELM[@]}" template test "$CHART_DIR" --namespace "$DATA_LAKE_NAMESPACE" >/dev/null
echo "Data-lake shell, Python, helm lint, and helm template validation passed."
