#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command docker

image_repository=${IMAGE_REPOSITORY:-data-pipeline}
image_tag=${IMAGE_TAG:-dev}
docker build \
  --build-arg "BASE_IMAGE=${BASE_IMAGE:-python:3.11-slim}" \
  --tag "$image_repository:$image_tag" \
  "$PIPELINE_ROOT"
if [[ -n ${IMAGE_PUSH:-} ]]; then
  docker push "$image_repository:$image_tag"
fi
