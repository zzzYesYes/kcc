#!/usr/bin/env bash
set -euo pipefail

HELM_BIN=${HELM_BIN:-helm}
RELEASE=${RELEASE:-qwen-demo}
NAMESPACE=${NAMESPACE:-dagster-qwen-demo}

"$HELM_BIN" uninstall "$RELEASE" --namespace "$NAMESPACE"
