#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
printf '%s\n' 'DEPRECATED: build-offline-bundle.sh delegates to build-stable-bundle.sh.' >&2
exec "$script_dir/build-stable-bundle.sh" "$@"

