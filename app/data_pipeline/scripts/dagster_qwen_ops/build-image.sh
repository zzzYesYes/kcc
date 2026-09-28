#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
IMAGE=${IMAGE:?Set IMAGE, for example registry.example.com/ai/dagster-qwen-ops:0.1.0}
BASE_IMAGE=${BASE_IMAGE:-python:3.11-slim}

docker build \
  --build-arg "BASE_IMAGE=$BASE_IMAGE" \
  -f "$ROOT/src/dagster_qwen_ops/Dockerfile" \
  -t "$IMAGE" \
  "$ROOT"

if [[ "${PUSH:-false}" == "true" ]]; then
  docker push "$IMAGE"
fi
