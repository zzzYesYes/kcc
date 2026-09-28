#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command kubectl

dagster_deployment=$(kubectl -n "$NAMESPACE" get deployment \
  -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=dagster" \
  -o jsonpath='{.items[0].metadata.name}')
module=${DAGSTER_MODULE:-clean_qa.mineru_dagster.definitions}
config_root=${PIPELINE_CONFIG_ROOT:-/opt/data-pipeline/config/helm}

run_job() {
  local job=$1 config=$2
  echo "Launching $job with $config"
  kubectl -n "$NAMESPACE" exec "deployment/$dagster_deployment" -c webserver -- \
    dagster job execute -m "$module" -j "$job" -c "$config"
}

if [[ ${RUN_MINERU_SMOKE:-0} == 1 ]]; then
  run_job mineru_smoke_10_job /opt/data-pipeline/config/clean_qa/dagster_jobs/mineru_smoke_10.yaml
fi
run_job cleanjopbstage1_10 "$config_root/stage1.yaml"
run_job qajobstage2_10 "$config_root/stage2.yaml"
