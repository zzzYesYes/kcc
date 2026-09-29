#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: build-images.sh REGISTRY VERSION CONTROLLER_BASE HEAD_BASE WORKER_BASE KUBECTL_IMAGE [CONTROL_PLATFORM] [WORKER_PLATFORM]' \
    'Every base image and KUBECTL_IMAGE must be pinned with @sha256.' \
    'Platforms default to CONTROL_PLATFORM/WORKER_PLATFORM, then PLATFORM, then linux/amd64.' \
    'CANN_ASCEND_DIR is forwarded to the worker build and defaults to the standard toolkit path.'
}

if [[ $# -lt 6 || $# -gt 8 ]]; then
  usage >&2
  exit 2
fi

registry=$1
version=$2
controller_base=$3
head_base=$4
worker_base=$5
kubectl_image=$6
control_platform=${7:-${CONTROL_PLATFORM:-${PLATFORM:-linux/amd64}}}
worker_platform=${8:-${WORKER_PLATFORM:-${PLATFORM:-$control_platform}}}
cann_ascend_dir=${CANN_ASCEND_DIR:-/usr/local/Ascend/cann/ascend-toolkit/latest}
kubectl_source=${KUBECTL_SOURCE:-/bin/kubectl}

if [[ -n ${VCS_REF:-} ]]; then
  vcs_ref=$VCS_REF
elif command -v git >/dev/null && vcs_candidate=$(git rev-parse --verify HEAD 2>/dev/null); then
  vcs_ref=$vcs_candidate
else
  vcs_ref=unknown
fi
if [[ ! "$vcs_ref" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  printf 'VCS_REF contains unsupported characters: %s\n' "$vcs_ref" >&2
  exit 2
fi

for value in "$controller_base" "$head_base" "$worker_base" "$kubectl_image"; do
  if [[ ! "$value" =~ @sha256:[0-9a-f]{64}$ ]]; then
    printf 'base image is not digest pinned: %s\n' "$value" >&2
    exit 2
  fi
done
if [[ -z "$registry" || ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  printf 'registry/version is invalid; use an explicit semantic version\n' >&2
  exit 2
fi
for platform in "$control_platform" "$worker_platform"; do
  if [[ ! "$platform" =~ ^linux/(amd64|arm64)$ ]]; then
    printf 'unsupported platform: %s\n' "$platform" >&2
    exit 2
  fi
done
if [[ "$cann_ascend_dir" != /* ]]; then
  printf 'CANN_ASCEND_DIR must be absolute: %s\n' "$cann_ascend_dir" >&2
  exit 2
fi
command -v docker >/dev/null || {
  printf '%s\n' 'docker with buildx support is required' >&2
  exit 1
}
docker buildx version >/dev/null

docker buildx build --load --pull --platform "$control_platform" \
  --build-arg "BASE_IMAGE=$controller_base" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --tag "$registry/kcc-training-controller:$version" \
  --file docker/Dockerfile.controller .

docker buildx build --load --pull --platform "$control_platform" \
  --build-arg "BASE_IMAGE=$head_base" \
  --build-arg "KUBECTL_IMAGE=$kubectl_image" \
  --build-arg "KUBECTL_SOURCE=$kubectl_source" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --tag "$registry/kcc-training-head:$version" \
  --file docker/Dockerfile.head .

docker buildx build --load --pull --platform "$worker_platform" \
  --build-arg "BASE_IMAGE=$worker_base" \
  --build-arg "CANN_ASCEND_DIR=$cann_ascend_dir" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --tag "$registry/kcc-training-worker:$version" \
  --file docker/Dockerfile.worker .

printf 'Images built locally (controller/head=%s, worker=%s). Push and publish registry digests.\n' \
  "$control_platform" "$worker_platform"

