#!/bin/bash
# build-image.sh – Build the combined opal-proxy image and load it into the Kind cluster.

set -e

NAMESPACE="opal-dynamic-policy"
IMAGE="opal-proxy"
VERSION=${VERSION:-"latest"}

echo "📦 Building combined opal-proxy image (pep-proxy + bundle-server)..."
docker build \
  -t ${IMAGE}:${VERSION} \
  -f Dockerfile \
  .

echo "📤 Loading image into Kind cluster nodes..."
for NODE in da-cluster-control-plane da-cluster-worker da-cluster-worker2; do
  echo "  → ${NODE}"
  docker save ${IMAGE}:${VERSION} | \
    docker exec --privileged -i ${NODE} \
      ctr --namespace=k8s.io images import \
        --platform linux/amd64 --local -
done

echo "✅ Image build and load completed: ${IMAGE}:${VERSION}"
