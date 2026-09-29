#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: build-lighteval-worker-image-on-cluster.sh REGISTRY VERSION WORKER_BASE NODE [NAMESPACE]' \
    'WORKER_BASE must be a digest-pinned arm64 MindSpeed/KCC worker image.' \
    'The target node packages the verified LightEval source and Python dependencies.' \
    'The derived image is loaded locally and is not pushed.' \
    'Optional environment:' \
    '  KCC_KUBECTL                         kubectl command' \
    '  KCC_LIGHTEVAL_PYTHON                worker Python command (default: python3)' \
    '  KCC_LIGHTEVAL_EXPECTED_VERSION       source/import version (default: 0.13.1.dev0)' \
    '  KCC_LIGHTEVAL_SOURCE_HOST_PATH       source hostPath (default: /home/ywj/lighteval)' \
    '  KCC_LIGHTEVAL_SITE_PACKAGES_HOST_PATH dependency hostPath'
}

if [[ $# -lt 4 || $# -gt 5 ]]; then
  usage >&2
  exit 2
fi

registry=$1
version=$2
worker_base=$3
node=$4
namespace=${5:-kcc-training}
image="$registry/kcc-training-worker-lighteval:$version"
lighteval_python=${KCC_LIGHTEVAL_PYTHON:-python3}
lighteval_version=${KCC_LIGHTEVAL_EXPECTED_VERSION:-0.13.1.dev0}
source_host_path=${KCC_LIGHTEVAL_SOURCE_HOST_PATH:-/home/ywj/lighteval}
site_packages_host_path=${KCC_LIGHTEVAL_SITE_PACKAGES_HOST_PATH:-/home/ywj/lighteval-megatron-work/runtime/lighteval-venv/lib/python3.10/site-packages}

if [[ ! "$worker_base" =~ ^[^[:space:]@]+@sha256:[0-9a-f]{64}$ ]]; then
  printf 'worker base image is not digest pinned: %s\n' "$worker_base" >&2
  exit 2
fi
if [[ -z "$registry" || ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  printf '%s\n' 'registry/version is invalid; use an explicit semantic version' >&2
  exit 2
fi
if [[ ! "$node" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
  printf 'invalid Kubernetes node name: %s\n' "$node" >&2
  exit 2
fi
if [[ ! "$namespace" =~ ^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$ ]]; then
  printf 'invalid Kubernetes namespace: %s\n' "$namespace" >&2
  exit 2
fi
if [[ "$source_host_path" != /* || "$site_packages_host_path" != /* ]]; then
  printf '%s\n' 'LightEval source and site-packages host paths must be absolute' >&2
  exit 2
fi
if [[ "$lighteval_python" == */* && "$lighteval_python" != /* ]]; then
  printf 'KCC_LIGHTEVAL_PYTHON must be a command name or absolute path: %s\n' \
    "$lighteval_python" >&2
  exit 2
fi
if [[ ! "$lighteval_version" =~ ^[0-9A-Za-z]+([._+-][0-9A-Za-z]+)*$ ]]; then
  printf 'invalid expected LightEval version: %s\n' "$lighteval_version" >&2
  exit 2
fi

command -v docker >/dev/null || {
  printf '%s\n' 'docker with buildx support is required on the assembly host' >&2
  exit 1
}
docker buildx version >/dev/null

if [[ -n ${KCC_KUBECTL:-} ]]; then
  read -r -a kubectl_command <<<"$KCC_KUBECTL"
elif command -v kubectl >/dev/null; then
  kubectl_command=(kubectl)
elif [[ -x /usr/local/bin/k3s ]]; then
  kubectl_command=(sudo /usr/local/bin/k3s kubectl)
else
  printf '%s\n' 'kubectl was not found; set KCC_KUBECTL' >&2
  exit 1
fi
"${kubectl_command[@]}" version --client >/dev/null

architecture=$("${kubectl_command[@]}" get node "$node" \
  -o jsonpath='{.status.nodeInfo.architecture}')
if [[ "$architecture" != arm64 ]]; then
  printf 'node %s is %s, expected arm64\n' "$node" "$architecture" >&2
  exit 1
fi
node_ready=$("${kubectl_command[@]}" get node "$node" \
  -o jsonpath='{range .status.conditions[?(@.type=="Ready")]}{.status}{end}')
if [[ "$node_ready" != True ]]; then
  printf 'node %s is not Ready\n' "$node" >&2
  exit 1
fi

if [[ -n ${VCS_REF:-} ]]; then
  vcs_ref=$VCS_REF
elif command -v git >/dev/null && vcs_candidate=$(git rev-parse --verify HEAD 2>/dev/null); then
  vcs_ref=$vcs_candidate
else
  vcs_ref=unknown
fi
if [[ ! "$vcs_ref" =~ ^[A-Za-z0-9._/-]+$ ]]; then
  printf 'VCS_REF contains unsupported characters: %s\n' "$vcs_ref" >&2
  exit 2
fi

temporary=$(mktemp -d /tmp/kcc-lighteval-worker-build.XXXXXX)
pod="kcc-lighteval-build-${version//./-}-$$"
cleanup() {
  set +e
  "${kubectl_command[@]}" -n "$namespace" delete pod "$pod" \
    --ignore-not-found --wait=false >/dev/null 2>&1
  rm -rf -- "$temporary"
}
trap cleanup EXIT

overrides=$(python3 - \
  "$node" "$pod" "$worker_base" "$source_host_path" \
  "$site_packages_host_path" <<'PY'
import json
import sys

node, pod, image, source, site_packages = sys.argv[1:]
print(json.dumps({
    "spec": {
        "nodeName": node,
        "terminationGracePeriodSeconds": 0,
        "volumes": [
            {
                "name": "lighteval-source",
                "hostPath": {"path": source, "type": "Directory"},
            },
            {
                "name": "lighteval-site-packages",
                "hostPath": {"path": site_packages, "type": "Directory"},
            },
        ],
        "containers": [{
            "name": pod,
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["/bin/bash", "-lc", "sleep 3600"],
            "volumeMounts": [
                {
                    "name": "lighteval-source",
                    "mountPath": "/mnt/kcc-lighteval/source",
                    "readOnly": True,
                },
                {
                    "name": "lighteval-site-packages",
                    "mountPath": "/mnt/kcc-lighteval/site-packages",
                    "readOnly": True,
                },
            ],
        }],
    },
}))
PY
)
"${kubectl_command[@]}" -n "$namespace" run "$pod" \
  "--image=$worker_base" \
  --image-pull-policy=IfNotPresent \
  --restart=Never \
  "--overrides=$overrides" \
  --command -- /bin/bash -lc 'sleep 3600' >/dev/null
"${kubectl_command[@]}" -n "$namespace" wait \
  --for=condition=Ready "pod/$pod" --timeout=300s >/dev/null

pod_image_id=$("${kubectl_command[@]}" -n "$namespace" get pod "$pod" \
  -o jsonpath='{.status.containerStatuses[0].imageID}')
printf 'Base worker requested: %s\nBase worker running:   %s\n' \
  "$worker_base" "$pod_image_id"

resolved_python=$("${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  /bin/bash -c 'command -v -- "$1"' -- "$lighteval_python")
if [[ "$resolved_python" != /* ]]; then
  printf 'unable to resolve KCC_LIGHTEVAL_PYTHON: %s\n' "$resolved_python" >&2
  exit 1
fi

python_version=$("${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  "$resolved_python" -c 'import platform, sys; print(f"{sys.version_info.major}.{sys.version_info.minor} {platform.machine()}")')
if [[ "$python_version" != '3.10 aarch64' ]]; then
  printf 'LightEval payload requires Python 3.10/aarch64, found %s\n' \
    "$python_version" >&2
  exit 1
fi

source_info=$("${kubectl_command[@]}" -n "$namespace" exec -i "$pod" -- \
  /bin/bash -s -- "$lighteval_version" <<'REMOTE'
set -euo pipefail
expected_version=$1
source_dir=/mnt/kcc-lighteval/source
site_packages=/mnt/kcc-lighteval/site-packages
[[ -r "$source_dir/pyproject.toml" && -d "$source_dir/src/lighteval" ]] || {
  printf '%s\n' 'LightEval source tree is incomplete' >&2
  exit 1
}
[[ -d "$site_packages" ]] || {
  printf '%s\n' 'LightEval site-packages directory is missing' >&2
  exit 1
}
source_version=$(sed -n 's/^version = "\([^"]*\)"/\1/p' \
  "$source_dir/pyproject.toml" | head -n 1)
if [[ "$source_version" != "$expected_version" ]]; then
  printf 'LightEval source version is %s, expected %s\n' \
    "$source_version" "$expected_version" >&2
  exit 1
fi
git_home=/tmp/kcc-lighteval-git-home
rm -rf -- "$git_home"
install -d -m 0700 "$git_home"
HOME=$git_home git config --global --add safe.directory "$source_dir"
source_ref=$(HOME=$git_home git --no-optional-locks -C "$source_dir" \
  rev-parse --verify HEAD)
if [[ -n $(HOME=$git_home git --no-optional-locks -C "$source_dir" \
  status --porcelain --untracked-files=normal) ]]; then
  source_dirty=true
else
  source_dirty=false
fi
printf '%s\t%s\t%s\n' "$source_version" "$source_ref" "$source_dirty"
REMOTE
)
IFS=$'\t' read -r detected_version lighteval_ref lighteval_dirty <<<"$source_info"
if [[ "$detected_version" != "$lighteval_version" || \
      ! "$lighteval_ref" =~ ^[0-9a-f]{40,64}$ || \
      ! "$lighteval_dirty" =~ ^(true|false)$ ]]; then
  printf 'invalid LightEval source metadata: %s\n' "$source_info" >&2
  exit 1
fi
printf 'LightEval source: version=%s ref=%s dirty=%s\n' \
  "$detected_version" "$lighteval_ref" "$lighteval_dirty"
if [[ "$lighteval_dirty" == true ]]; then
  printf '%s\n' 'LightEval dirty files (recorded, source remains read-only):'
  "${kubectl_command[@]}" -n "$namespace" exec -i "$pod" -- /bin/bash -s <<'REMOTE'
set -euo pipefail
source_dir=/mnt/kcc-lighteval/source
git_home=/tmp/kcc-lighteval-git-home
HOME=$git_home git --no-optional-locks -C "$source_dir" \
  status --short --untracked-files=normal
REMOTE
fi

"${kubectl_command[@]}" -n "$namespace" exec -i "$pod" -- \
  /bin/bash -s -- "$resolved_python" "$lighteval_version" <<'REMOTE'
set -euo pipefail
python_command=$1
expected_version=$2
source_dir=/mnt/kcc-lighteval/source
site_packages=/mnt/kcc-lighteval/site-packages
work_dir=/tmp/kcc-lighteval-build
output_dir=$work_dir/lighteval-python

rm -rf -- "$work_dir"
install -d -m 0755 "$output_dir"
cp -a "$site_packages"/. "$output_dir"/

# Replace the installed package with the exact current source tree.  Keep the
# matching dist-info so importlib.metadata and lighteval.__version__ work.
rm -rf -- "$output_dir/lighteval"
find "$output_dir" -maxdepth 1 -type d -name 'lighteval-*.dist-info' \
  -exec rm -rf -- {} +
cp -a "$source_dir/src/lighteval" "$output_dir/lighteval"
metadata_dir=$(find "$site_packages" -maxdepth 1 -type d \
  -name "lighteval-${expected_version}.dist-info" -print -quit)
if [[ -z "$metadata_dir" ]]; then
  printf 'missing lighteval-%s.dist-info\n' "$expected_version" >&2
  exit 1
fi
cp -a "$metadata_dir" "$output_dir/"

# MindSpeed and the legacy lighteval-mindspeed adapter were editable installs
# pointing at host paths.  Evaluation uses the run's own MindSpeed source; do
# not package stale finders or claim those editable distributions are present.
find "$output_dir" -maxdepth 1 -type f \
  \( -name '__editable__*.pth' -o -name '__editable__*_finder.py' \
     -o -name '*.egg-link' \) -delete
rm -rf -- "$output_dir/mindspeed" "$output_dir/lighteval_mindspeed"
find "$output_dir" -maxdepth 1 -type d \
  \( -name 'mindspeed-*.dist-info' \
     -o -name 'lighteval_mindspeed-*.dist-info' \) \
  -exec rm -rf -- {} +
find "$output_dir" -type f -name direct_url.json -print0 | \
  while IFS= read -r -d '' direct_url; do
    if grep -Eq '"editable"[[:space:]]*:[[:space:]]*true|"url"[[:space:]]*:[[:space:]]*"file:///' \
      "$direct_url"; then
      rm -f -- "$direct_url"
    fi
  done
find "$output_dir" -type d -name __pycache__ -prune -exec rm -rf -- {} +
find "$output_dir" -type f \( -name '*.pyc' -o -name '*.pyo' \) -delete

if find "$output_dir" \
  \( -name '__editable__*' -o -name '*.egg-link' \) \
  -print -quit | grep -q .; then
  printf '%s\n' 'editable install artifacts remain in LightEval payload' >&2
  exit 1
fi
while IFS= read -r -d '' pth; do
  if grep -Eq '(^|[[:space:]])(/home/|/mnt/|/inspect/)|__editable__' "$pth"; then
    printf 'host path remains in %s\n' "$pth" >&2
    exit 1
  fi
done < <(find "$output_dir" -type f -name '*.pth' -print0)
if find "$output_dir" -type l -lname '/*' -print -quit | grep -q .; then
  printf '%s\n' 'absolute symlink remains in LightEval payload' >&2
  exit 1
fi

binary_extension=$(find "$output_dir" -type f \
  -name '*-aarch64-linux-gnu.so' -print -quit)
if [[ -z "$binary_extension" ]]; then
  printf '%s\n' 'no aarch64 Python binary dependency found in payload' >&2
  exit 1
fi
if command -v readelf >/dev/null; then
  while IFS= read -r extension; do
    readelf -h "$extension" | grep -q 'Machine:.*AArch64' || {
      printf 'non-aarch64 extension in payload: %s\n' "$extension" >&2
      exit 1
    }
  done < <(find "$output_dir" -type f -name '*-aarch64-linux-gnu.so')
fi

LIGHTEVAL_EXPECTED_VERSION="$expected_version" \
LIGHTEVAL_PAYLOAD_ROOT="$output_dir" \
PYTHONNOUSERSITE=1 \
PYTHONPATH="$output_dir${PYTHONPATH:+:$PYTHONPATH}" \
  "$python_command" -c \
  'import os; import lighteval; from lighteval.models.megatron.megatron_runner import scan_checkpoints; assert lighteval.__version__ == os.environ["LIGHTEVAL_EXPECTED_VERSION"], lighteval.__version__; assert lighteval.__file__.startswith(os.environ["LIGHTEVAL_PAYLOAD_ROOT"] + "/"); print(f"LightEval payload import: version={lighteval.__version__} source={lighteval.__file__}")'
chmod -R a+rX "$output_dir"
du -sh "$output_dir"
REMOTE

install -d -m 0755 "$temporary/lighteval-python"
"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  tar -cf - -C /tmp/kcc-lighteval-build/lighteval-python . | \
  tar -xf - -C "$temporary/lighteval-python"

docker buildx build --load --pull --platform linux/arm64 \
  --build-arg "BASE_IMAGE=$worker_base" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --build-arg "LIGHTEVAL_VERSION=$lighteval_version" \
  --build-arg "LIGHTEVAL_VCS_REF=$lighteval_ref" \
  --build-arg "LIGHTEVAL_VCS_DIRTY=$lighteval_dirty" \
  --tag "$image" \
  --file docker/Dockerfile.worker-lighteval-prebuilt "$temporary"

docker image inspect "$image" --format '{{.Architecture}}' | grep -qx arm64
docker image inspect "$image" \
  --format '{{index .Config.Labels "io.kcc.training.base-image"}}' | \
  grep -Fxq "$worker_base"
docker image inspect "$image" \
  --format '{{index .Config.Labels "io.kcc.training.lighteval.version"}}' | \
  grep -Fxq "$lighteval_version"
docker image inspect "$image" \
  --format '{{index .Config.Labels "io.kcc.training.lighteval.revision"}}' | \
  grep -Fxq "$lighteval_ref"
if docker image inspect "$image" \
  --format '{{range .Config.Env}}{{println .}}{{end}}' | \
  grep -E '^PYTHONPATH=.*(/opt/kcc/lighteval-python)' >/dev/null; then
  printf '%s\n' 'derived image unexpectedly enables LightEval globally' >&2
  exit 1
fi

printf 'LightEval worker image built locally: %s\n' "$image"
printf 'Evaluation-only import: PYTHONPATH=/opt/kcc/lighteval-python:%s %s -m lighteval ...\n' \
  '${PYTHONPATH:-}' "$resolved_python"
