#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
HELM_BIN=${HELM_BIN:-helm}
PYTHON_BIN=${PYTHON_BIN:-python3}
TMP_DIR=$(mktemp -d)
trap 'rm -rf "$TMP_DIR"' EXIT

"$PYTHON_BIN" -m compileall -q "$ROOT/src/dagster_qwen_ops"
PYTHONPATH="$ROOT/src" "$PYTHON_BIN" - <<'PY'
from dagster_qwen_ops.definitions import defs

repository = defs.get_repository_def()
names = sorted(job.name for job in repository.get_all_jobs())
expected = sorted([
    "qwen_vllm_lifecycle_job",
    "qwen_vllm_8npulifecycle_job",
    "qwen_chat_job",
])
assert names == expected, (names, expected)
print("Dagster Definitions loaded; jobs:", ", ".join(names))

from dagster import validate_run_config
from dagster_qwen_ops.config import CHAT_CONFIG, LIFECYCLE_8NPU_CONFIG, LIFECYCLE_CONFIG
from dagster_qwen_ops.jobs import (
    qwen_chat_job,
    qwen_vllm_8npulifecycle_job,
    qwen_vllm_lifecycle_job,
)
for job, config in (
    (qwen_vllm_lifecycle_job, LIFECYCLE_CONFIG),
    (qwen_vllm_8npulifecycle_job, LIFECYCLE_8NPU_CONFIG),
    (qwen_chat_job, CHAT_CONFIG),
):
    validate_run_config(job, config)
    print("Dagster run config valid:", job.name)
PY

while IFS= read -r script; do bash -n "$script"; done < <(
  find "$ROOT/scripts/dagster_qwen_ops" -type f -name '*.sh' -print
)

"$HELM_BIN" lint "$ROOT/helm/dagster-qwen-ops"
"$HELM_BIN" template qwen-demo "$ROOT/helm/dagster-qwen-ops" \
  --namespace dagster-qwen-demo >"$TMP_DIR/default.yaml"
"$HELM_BIN" template qwen-demo "$ROOT/helm/dagster-qwen-ops" \
  --namespace dagster-qwen-demo \
  -f "$ROOT/helm/dagster-qwen-ops/values-smoke.yaml" >"$TMP_DIR/smoke.yaml"
"$PYTHON_BIN" "$ROOT/scripts/dagster_qwen_ops/validate_render.py" "$TMP_DIR/default.yaml"
"$PYTHON_BIN" "$ROOT/scripts/dagster_qwen_ops/validate_render.py" "$TMP_DIR/smoke.yaml"
echo "dagster-qwen-ops static validation passed"
