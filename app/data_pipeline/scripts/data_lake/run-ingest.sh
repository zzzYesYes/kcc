#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$0")/common.sh"
require_command python3
require_value K12_ACCESS_TOKEN
require_value K12_BATCH_ID
require_value AWS_ACCESS_KEY_ID
require_value AWS_SECRET_ACCESS_KEY

export PYTHONPATH="$MODULE_DIR/src${PYTHONPATH:+:$PYTHONPATH}"
python3 -m data_lake.k12_ingest.full_ingest "$@"
