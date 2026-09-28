#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command helm

if [[ ${CONFIRM_UNINSTALL:-} != "$RELEASE" ]]; then
  echo "Refusing to uninstall without CONFIRM_UNINSTALL=$RELEASE" >&2
  exit 2
fi
helm uninstall "$RELEASE" --namespace "$NAMESPACE"
echo "External S3 data, credentials Secret, model volume, and KubeRay CRDs were not deleted."
