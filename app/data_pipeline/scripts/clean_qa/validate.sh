#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/pipeline-helm-common.sh"
require_command python3

find "$SCRIPT_DIR" -type f -name '*.sh' -print0 | xargs -0 -n1 bash -n
python3 -m compileall -q \
  "$PIPELINE_ROOT/src/clean_qa" \
  "$PIPELINE_ROOT/src/runtime" \
  "$PIPELINE_ROOT/src/legacy"

export PYTHONPATH="$PIPELINE_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"
python3 -m unittest discover \
  -s "$PIPELINE_ROOT/src/clean_qa/k12_clean_qa_pipeline/common/tests"
python3 -m unittest discover \
  -s "$PIPELINE_ROOT/src/clean_qa/k12_clean_qa_pipeline/stage1_clean/tests"
python3 -m unittest discover \
  -s "$PIPELINE_ROOT/src/clean_qa/k12_clean_qa_pipeline/stage2_qa/tests"

if command -v helm >/dev/null 2>&1; then
  "$SCRIPT_DIR/helm_lint.sh"
else
  echo "helm not found; skipped local Chart validation" >&2
fi
echo "Cleaning/QA shell, Python, unit, and available Helm validation passed."
