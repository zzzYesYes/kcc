#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command python3
require_value AWS_ACCESS_KEY_ID
require_value AWS_SECRET_ACCESS_KEY

export S3_ENDPOINT_URL
export PYTHONPATH="$MODULE_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
python3 -m data_lake.data_pipeline_tools.s3_smoke --write-probe
