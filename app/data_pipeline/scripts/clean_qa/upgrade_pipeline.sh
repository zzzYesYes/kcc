#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm
load_profile_args

secret_args=()
[[ -z ${S3_SECRET_NAME:-} ]] || secret_args=(--set-string externalS3.credentialsSecret.name="$S3_SECRET_NAME")
helm upgrade "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" \
  "${HELM_PROFILE_ARGS[@]}" "${secret_args[@]}" "$@"
