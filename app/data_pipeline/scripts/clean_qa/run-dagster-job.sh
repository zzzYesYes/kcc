#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command kubectl

job_name="${1:?usage: run-dagster-job.sh JOB_NAME [RUN_CONFIG_YAML]}"
config_file="${2:-}"
pod="$(kubectl -n "$NAMESPACE" get pod \
  -l "app.kubernetes.io/instance=$RELEASE,app.kubernetes.io/component=dagster" \
  -o jsonpath='{.items[0].metadata.name}')"
cmd=(dagster job execute -m clean_qa.mineru_dagster.definitions -j "${job_name}")
if [[ -n "${config_file}" ]]; then
  config_name="$(basename "${config_file}")"
  kubectl -n "$NAMESPACE" cp "$config_file" "$pod:/tmp/$config_name" -c webserver
  cmd+=(-c "/tmp/${config_name}")
fi
kubectl -n "$NAMESPACE" exec "$pod" -c webserver -- "${cmd[@]}"
