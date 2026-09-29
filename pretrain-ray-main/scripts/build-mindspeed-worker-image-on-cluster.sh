#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: build-mindspeed-worker-image-on-cluster.sh REGISTRY VERSION WORKER_BASE NODE [NAMESPACE]' \
    'WORKER_BASE must be pinned with @sha256 and already contain the KCC worker runtime.' \
    'The dependency environment is assembled on the target arm64 node, then packaged locally.'
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
image="$registry/kcc-training-worker-mindspeed:$version"

if [[ ! "$worker_base" =~ @sha256:[0-9a-f]{64}$ ]]; then
  printf 'worker base image is not digest pinned: %s\n' "$worker_base" >&2
  exit 2
fi
if [[ -z "$registry" || ! "$version" =~ ^[0-9]+\.[0-9]+\.[0-9]+([.-][0-9A-Za-z.-]+)?$ ]]; then
  printf '%s\n' 'registry/version is invalid; use an explicit semantic version' >&2
  exit 2
fi

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

command -v docker >/dev/null
docker buildx version >/dev/null

temporary=$(mktemp -d /tmp/kcc-mindspeed-worker-build.XXXXXX)
pod="kcc-mindspeed-build-${version//./-}-$$"
cleanup() {
  set +e
  "${kubectl_command[@]}" -n "$namespace" delete pod "$pod" \
    --ignore-not-found --wait=false >/dev/null 2>&1
  rm -rf -- "$temporary"
}
trap cleanup EXIT

overrides=$(printf \
  '{"spec":{"nodeName":"%s","terminationGracePeriodSeconds":0,"containers":[{"name":"%s","image":"%s","command":["/bin/bash","-lc"],"args":["sleep 3600"],"volumeMounts":[{"name":"models","mountPath":"/mnt/models","readOnly":true}]}],"volumes":[{"name":"models","hostPath":{"path":"/mnt/models","type":"Directory"}}]}}' \
  "$node" "$pod" "$worker_base")
"${kubectl_command[@]}" -n "$namespace" run "$pod" \
  "--image=$worker_base" \
  --image-pull-policy=IfNotPresent \
  --restart=Never \
  "--overrides=$overrides" \
  --command -- /bin/bash -lc 'sleep 3600' >/dev/null
"${kubectl_command[@]}" -n "$namespace" wait \
  --for=condition=Ready "pod/$pod" --timeout=300s >/dev/null

"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- /bin/bash -lc '
set -euo pipefail
archive=/mnt/models/CODE/env/conda_env/ms_env.tar.gz
[[ -r "$archive" ]] || { printf "missing MindSpeed environment archive: %s\n" "$archive" >&2; exit 1; }
rm -rf /tmp/mindspeed-extract /tmp/mindspeed-python
install -d -m 0755 /tmp/mindspeed-extract /tmp/mindspeed-python
tar -xzf "$archive" -C /tmp/mindspeed-extract \
  ms/lib/python3.10/site-packages/datasets \
  ms/lib/python3.10/site-packages/datasets-3.6.0.dist-info \
  ms/lib/python3.10/site-packages/transformers \
  ms/lib/python3.10/site-packages/transformers-4.57.1.dist-info \
  ms/lib/python3.10/site-packages/huggingface_hub \
  ms/lib/python3.10/site-packages/huggingface_hub-0.35.3.dist-info \
  ms/lib/python3.10/site-packages/fsspec \
  ms/lib/python3.10/site-packages/fsspec-2025.3.0.dist-info \
  ms/lib/python3.10/site-packages/dill \
  ms/lib/python3.10/site-packages/dill-0.3.8.dist-info \
  ms/lib/python3.10/site-packages/multiprocess \
  ms/lib/python3.10/site-packages/multiprocess-0.70.16.dist-info
cp -a /tmp/mindspeed-extract/ms/lib/python3.10/site-packages/. /tmp/mindspeed-python/
find /tmp/mindspeed-python -type d -name __pycache__ -prune -exec rm -rf {} +
python -m pip install --no-cache-dir --no-deps --target /tmp/mindspeed-python xxhash==3.5.0
PYTHONPATH=/tmp/mindspeed-python:${PYTHONPATH:-} python -c \
  "import datasets, transformers, xxhash; assert datasets.__version__ == '\''3.6.0'\''; assert transformers.__version__ == '\''4.57.1'\''"
'

install -d -m 0755 "$temporary/site-packages"
"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  tar -cf - -C /tmp/mindspeed-python . | tar -xf - -C "$temporary/site-packages"

if [[ -n ${VCS_REF:-} ]]; then
  vcs_ref=$VCS_REF
else
  vcs_ref=$(git rev-parse --verify HEAD 2>/dev/null || printf unknown)
fi

docker buildx build --load --pull --platform linux/arm64 \
  --build-arg "BASE_IMAGE=$worker_base" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --tag "$image" \
  --file docker/Dockerfile.worker-mindspeed-prebuilt "$temporary"

docker image inspect "$image" --format '{{.Architecture}}' | grep -qx arm64
printf 'MindSpeed worker image built: %s\n' "$image"
