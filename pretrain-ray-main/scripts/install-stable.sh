#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 3 || $# -gt 4 ]]; then
  printf '%s\n' 'usage: install-stable.sh BUNDLE_DIR NAMESPACE HELM_RELEASE [VALUES.yaml]' >&2
  exit 2
fi

bundle=$1
namespace=$2
release=$3
values=${4:-$bundle/values.yaml}

for command_name in helm sha256sum; do
  command -v "$command_name" >/dev/null || {
    printf 'missing command: %s\n' "$command_name" >&2
    exit 1
  }
done
if [[ ! -f "$bundle/SHA256SUMS" || ! -f "$values" ]]; then
  printf '%s\n' 'bundle checksum manifest or values file is missing' >&2
  exit 1
fi
(
  cd "$bundle"
  sha256sum --check SHA256SUMS
)
mapfile -t charts < <(find "$bundle/helm" -maxdepth 1 -type f -name 'kcc-training-*.tgz' -print)
if [[ ${#charts[@]} -ne 1 ]]; then
  printf '%s\n' 'bundle must contain exactly one stable Helm package' >&2
  exit 1
fi

helm upgrade --install "$release" "${charts[0]}" \
  --namespace "$namespace" \
  --create-namespace \
  --values "$values" \
  --wait

