#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm
require_command kubectl
load_profile_args

kubectl get crd rayclusters.ray.io >/dev/null
test -n "${S3_SECRET_NAME:-}" || {
  echo "Set S3_SECRET_NAME to the existing external S3 credential Secret." >&2
  exit 2
}
kubectl -n "$NAMESPACE" get secret "$S3_SECRET_NAME" >/dev/null
helm install "$RELEASE" "$CHART_DIR" --namespace "$NAMESPACE" --create-namespace \
  "${HELM_PROFILE_ARGS[@]}" \
  --set-string externalS3.credentialsSecret.name="$S3_SECRET_NAME" "$@"
