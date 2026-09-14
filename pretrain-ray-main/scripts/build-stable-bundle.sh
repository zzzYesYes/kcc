#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: build-stable-bundle.sh [--metadata-only] OUTPUT_DIR VERSION CONTROLLER_IMAGE HEAD_IMAGE WORKER_IMAGE' \
    'All image references must be pinned by sha256. --metadata-only is for CI packaging smoke tests.'
}

metadata_only=false
if [[ ${1:-} == --metadata-only ]]; then
  metadata_only=true
  shift
fi
if [[ $# -ne 5 ]]; then
  usage >&2
  exit 2
fi

invocation_dir=$PWD
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
project_root=$(cd "$script_dir/.." && pwd)
output=$1
version=$2
controller_image=$3
head_image=$4
worker_image=$5
images=("$controller_image" "$head_image" "$worker_image")

if [[ "$output" != /* ]]; then
  output="$invocation_dir/$output"
fi
if [[ ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  printf 'invalid semantic version: %s\n' "$version" >&2
  exit 2
fi
if [[ -e "$output" ]]; then
  printf 'output path already exists: %s\n' "$output" >&2
  exit 2
fi
if [[ ! -d "$(dirname "$output")" ]]; then
  printf 'output parent directory does not exist: %s\n' "$(dirname "$output")" >&2
  exit 2
fi
for image in "${images[@]}"; do
  if [[ ! "$image" =~ @sha256:[0-9a-f]{64}$ ]]; then
    printf 'image is not digest pinned: %s\n' "$image" >&2
    exit 2
  fi
done
for command_name in helm python3 sha256sum; do
  command -v "$command_name" >/dev/null || {
    printf 'missing command: %s\n' "$command_name" >&2
    exit 1
  }
done
if [[ "$metadata_only" == false ]]; then
  command -v docker >/dev/null || {
    printf '%s\n' 'missing command: docker' >&2
    exit 1
  }
fi

cd "$project_root"
project_version=$(python3 -c 'import tomllib; print(tomllib.load(open("pyproject.toml", "rb"))["project"]["version"])')
module_version=$(PYTHONPATH=src python3 -c 'import kcc_training; print(kcc_training.__version__)')
chart_version=$(awk '$1 == "version:" {print $2; exit}' deploy/helm/kcc-training-stable/Chart.yaml)
chart_app_version=$(awk '$1 == "appVersion:" {gsub(/"/, "", $2); print $2; exit}' deploy/helm/kcc-training-stable/Chart.yaml)
for actual in "$project_version" "$module_version" "$chart_version" "$chart_app_version"; do
  if [[ "$actual" != "$version" ]]; then
    printf 'release version mismatch: requested=%s source=%s\n' "$version" "$actual" >&2
    exit 1
  fi
done

staging=$(mktemp -d "$(dirname "$output")/.kcc-stable-bundle.XXXXXX")
cleanup() {
  if [[ -n "${staging:-}" && -d "$staging" ]]; then
    rm -rf -- "$staging"
  fi
}
trap cleanup EXIT
mkdir -p \
  "$staging/images" \
  "$staging/python" \
  "$staging/helm" \
  "$staging/examples" \
  "$staging/contracts" \
  "$staging/docs" \
  "$staging/docs/release" \
  "$staging/scripts"

python3 -m pip wheel --no-build-isolation --wheel-dir "$staging/python" .
helm package deploy/helm/kcc-training-stable --destination "$staging/helm" >/dev/null
cp examples/runtime-profile.yaml examples/recipe.yaml examples/training-run.yaml "$staging/examples/"
cp scripts/install-stable.sh scripts/load-images.sh scripts/stable-preflight.sh "$staging/scripts/"
cp LICENSE "$staging/"
cp deploy/helm/kcc-training-stable/values.yaml "$staging/values.yaml"

python3 - "$staging/values.yaml" "$controller_image" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()
for index, line in enumerate(lines):
    if line.startswith("  image:"):
        lines[index] = f"  image: {sys.argv[2]}"
        break
else:
    raise SystemExit("controller image field was not found in values.yaml")
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

python3 - "$staging/examples/runtime-profile.yaml" "$head_image" "$worker_image" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
lines = path.read_text(encoding="utf-8").splitlines()
replacements = {"    head:": sys.argv[2], "    worker:": sys.argv[3]}
seen = set()
for index, line in enumerate(lines):
    for prefix, image in replacements.items():
        if prefix not in seen and line.startswith(prefix):
            lines[index] = f"{prefix} {image}"
            seen.add(prefix)
if seen != set(replacements):
    raise SystemExit("runtime profile image fields were not found")
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY

python3 - "$staging/images.lock.json" "$version" "$controller_image" "$head_image" "$worker_image" <<'PY'
import json
from pathlib import Path
import sys

payload = {
    "schemaVersion": "kcc-images/v1",
    "version": sys.argv[2],
    "images": {
        "controller": sys.argv[3],
        "head": sys.argv[4],
        "worker": sys.argv[5],
    },
}
Path(sys.argv[1]).write_text(
    json.dumps(payload, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

printf '%s\n' "$version" >"$staging/VERSION"
printf '%s\n' "${images[@]}" >"$staging/IMAGES"
if [[ "$metadata_only" == true ]]; then
  printf '%s\n' 'CI metadata-only bundle: OCI image archive intentionally omitted.' >"$staging/images/README.txt"
else
  docker image save --output "$staging/images/kcc-training-images.tar" "${images[@]}"
fi

(
  cd "$staging"
  find . -type f ! -name SHA256SUMS -print0 | sort -z | xargs -0 sha256sum >SHA256SUMS
)
mv "$staging" "$output"
staging=
printf 'stable bundle created: %s\n' "$output"

