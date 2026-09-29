#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
printf '%s\n' 'DEPRECATED: release-preflight.sh delegates to stable-preflight.sh.' >&2
exec "$script_dir/stable-preflight.sh" "$@"

