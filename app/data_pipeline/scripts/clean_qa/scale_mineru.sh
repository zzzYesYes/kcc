#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command kubectl
require_command python3

replicas=${1:?Usage: scale_mineru.sh REPLICAS}
deployment=$(kubectl -n "$NAMESPACE" get deployment \
  -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=mineru-worker" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$deployment" ]]; then
  kubectl -n "$NAMESPACE" scale deployment "$deployment" --replicas="$replicas"
  exit 0
fi

cluster=$(kubectl -n "$NAMESPACE" get raycluster \
  -l "app.kubernetes.io/instance=$RELEASE" -o jsonpath='{.items[0].metadata.name}')
index=$(kubectl -n "$NAMESPACE" get raycluster "$cluster" -o json | python3 -c '
import json, sys
groups=json.load(sys.stdin)["spec"]["workerGroupSpecs"]
print(next(i for i,g in enumerate(groups) if "mineru" in g["groupName"]))
')
kubectl -n "$NAMESPACE" patch raycluster "$cluster" --type=json \
  -p="[{\"op\":\"replace\",\"path\":\"/spec/workerGroupSpecs/$index/replicas\",\"value\":$replicas}]"
echo "Ray autoscaling may subsequently adjust this group according to resource demand."
