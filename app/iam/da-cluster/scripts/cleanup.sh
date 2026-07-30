#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${CLUSTER_NAME:-da-cluster}"

echo "Deleting Kind cluster '$CLUSTER_NAME'..."
kind delete cluster --name "$CLUSTER_NAME"
echo "Done."
