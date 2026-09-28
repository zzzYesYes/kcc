#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PIPELINE_ROOT=$(cd "$SCRIPT_DIR/../.." && pwd)
CHART_DIR=${CHART_DIR:-"$PIPELINE_ROOT/helm/k12-clean-qa-pipeline"}
RELEASE=${RELEASE:-k12-pipeline}
NAMESPACE=${NAMESPACE:-k12}
PROFILE=${PROFILE:-smoke}
HELM_PROFILE_ARGS=()

load_profile_args() {
  HELM_PROFILE_ARGS=()
  case "$PROFILE" in
    default) ;;
    smoke|production) HELM_PROFILE_ARGS=(-f "$CHART_DIR/values-$PROFILE.yaml") ;;
    *)
      test -f "$PROFILE" || {
        echo "PROFILE must be default, smoke, production, or a values file" >&2
        exit 2
      }
      HELM_PROFILE_ARGS=(-f "$PROFILE")
      ;;
  esac
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || {
    echo "Required command not found: $1" >&2
    exit 127
  }
}
