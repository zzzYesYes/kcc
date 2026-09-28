#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command kubectl
require_command python3

replicas=${1:?Usage: scale_qwen.sh REPLICAS [GROUP_OR_all]}
group_filter=${2:-all}
deployment=$(kubectl -n "$NAMESPACE" get deployment \
  -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=qwen-worker" \
  -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
if [[ -n "$deployment" ]]; then
  kubectl -n "$NAMESPACE" scale deployment "$deployment" --replicas="$replicas"
  exit 0
fi

cluster=$(kubectl -n "$NAMESPACE" get raycluster \
  -l "app.kubernetes.io/instance=$RELEASE" -o jsonpath='{.items[0].metadata.name}')
indexes=$(kubectl -n "$NAMESPACE" get raycluster "$cluster" -o json | \
  GROUP_FILTER="$group_filter" python3 -c '
import json, os, sys
groups=json.load(sys.stdin)["spec"]["workerGroupSpecs"]
wanted=os.environ["GROUP_FILTER"]
for i,g in enumerate(groups):
    name=g["groupName"]
    if name.startswith("qa-") and (wanted == "all" or wanted == name): print(i)
')
test -n "$indexes" || {
  echo "No matching Qwen worker group: $group_filter" >&2
  exit 1
}
while read -r index; do
  kubectl -n "$NAMESPACE" patch raycluster "$cluster" --type=json \
    -p="[{\"op\":\"replace\",\"path\":\"/spec/workerGroupSpecs/$index/replicas\",\"value\":$replicas}]"
done <<<"$indexes"
echo "Ray autoscaling may subsequently adjust these groups according to resource demand."
