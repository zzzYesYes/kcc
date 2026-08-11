#!/usr/bin/env bash
set -euo pipefail

snapshot_dir=${1:?"usage: $0 SNAPSHOT_DIR"}
namespace=${NAMESPACE:-k12}
read -r -a kubectl_command <<<"${KUBECTL_COMMAND:-kubectl}"

[[ -d "$snapshot_dir/manifests" ]] || {
  echo "snapshot manifests directory not found: $snapshot_dir/manifests" >&2
  exit 2
}

if [[ -f "$snapshot_dir/SHA256SUMS" ]]; then
  (cd "$snapshot_dir" && sha256sum --check SHA256SUMS)
fi

apply_group() {
  local group=$1
  [[ -d "$snapshot_dir/manifests/$group" ]] || return 0
  while IFS= read -r -d '' manifest; do
    "${kubectl_command[@]}" -n "$namespace" apply -f "$manifest"
  done < <(find "$snapshot_dir/manifests/$group" -type f -name '*.json' -print0 | sort -z)
}

# RBAC and launchers must exist before the RayCluster controllers create Pods.
apply_group rbac
apply_group configmaps
apply_group services
apply_group deployments
apply_group rayclusters

"${kubectl_command[@]}" -n "$namespace" get deploy,raycluster,pod,svc
