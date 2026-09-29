#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  printf '%s\n' 'usage: load-images.sh BUNDLE_DIR' >&2
  printf '%s\n' 'Set IMAGE_ENGINE=docker (default) or nerdctl. Run once on every image-cache node.' >&2
  exit 2
fi

bundle=$1
engine=${IMAGE_ENGINE:-docker}
archive="$bundle/images/kcc-training-images.tar"

if [[ ! -f "$bundle/SHA256SUMS" || ! -f "$archive" ]]; then
  printf '%s\n' 'bundle checksum manifest or image archive is missing' >&2
  exit 1
fi
(
  cd "$bundle"
  sha256sum --check SHA256SUMS
)

case "$engine" in
  docker)
    command -v docker >/dev/null || {
      printf '%s\n' 'docker is not installed' >&2
      exit 1
    }
    docker image load --input "$archive"
    ;;
  nerdctl)
    command -v nerdctl >/dev/null || {
      printf '%s\n' 'nerdctl is not installed' >&2
      exit 1
    }
    nerdctl --namespace "${NERDCTL_NAMESPACE:-k8s.io}" image load --input "$archive"
    ;;
  *)
    printf 'unsupported IMAGE_ENGINE: %s (supported: docker, nerdctl)\n' "$engine" >&2
    exit 2
    ;;
esac

printf '%s\n' 'Images loaded. Repeat on each node that may run controller, head, or worker Pods.'
printf '%s\n' 'Alternatively import the archive into an internal registry and update values/Profile to its returned digests.'

