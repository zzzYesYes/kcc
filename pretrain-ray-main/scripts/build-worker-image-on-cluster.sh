#!/usr/bin/env bash
set -euo pipefail

usage() {
  printf '%s\n' \
    'usage: build-worker-image-on-cluster.sh REGISTRY VERSION WORKER_BASE NODE [NAMESPACE]' \
    'WORKER_BASE must be pinned with @sha256.' \
    'The target node must be arm64 and able to pull WORKER_BASE.' \
    'Set KCC_KUBECTL when kubectl is not on PATH; for example:' \
    '  KCC_KUBECTL="sudo /usr/local/bin/k3s kubectl" scripts/build-worker-image-on-cluster.sh ...' \
    'KCC_WORKER_RUNTIME_BIN defaults to the directory containing KCC_WORKER_PYTHON.' \
    'CANN_ASCEND_DIR may override automatic CANN include/lib discovery inside the base image.'
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
image="$registry/kcc-training-worker:$version"
worker_python=${KCC_WORKER_PYTHON:-python3}
runtime_bin=${KCC_WORKER_RUNTIME_BIN:-}

if [[ ! "$worker_base" =~ @sha256:[0-9a-f]{64}$ ]]; then
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
if [[ -n ${CANN_ASCEND_DIR:-} && ${CANN_ASCEND_DIR} != /* ]]; then
  printf 'CANN_ASCEND_DIR must be absolute: %s\n' "$CANN_ASCEND_DIR" >&2
  exit 2
fi
if [[ "$worker_python" == */* && "$worker_python" != /* ]]; then
  printf 'KCC_WORKER_PYTHON must be a command name or absolute path: %s\n' "$worker_python" >&2
  exit 2
fi
if [[ -n "$runtime_bin" && "$runtime_bin" != /* ]]; then
  printf 'KCC_WORKER_RUNTIME_BIN must be absolute: %s\n' "$runtime_bin" >&2
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

temporary=$(mktemp -d /tmp/kcc-worker-cluster-build.XXXXXX)
pod="kcc-worker-build-${version//./-}-$$"
cleanup() {
  set +e
  "${kubectl_command[@]}" -n "$namespace" delete pod "$pod" \
    --ignore-not-found --wait=false >/dev/null 2>&1
  rm -rf -- "$temporary"
}
trap cleanup EXIT

overrides=$(python3 - "$node" "$pod" "$worker_base" "$worker_python" \
  "${CANN_ASCEND_DIR:-}" <<'PY'
import json
import sys

node, pod, image, worker_python, ascend_dir = sys.argv[1:]
environment = [{"name": "KCC_WORKER_PYTHON", "value": worker_python}]
if ascend_dir:
    environment.append({"name": "CANN_ASCEND_DIR", "value": ascend_dir})
print(json.dumps({
    "spec": {
        "nodeName": node,
        "terminationGracePeriodSeconds": 0,
        "volumes": [{
            "name": "ascend-driver",
            "hostPath": {
                "path": "/usr/local/Ascend/driver",
                "type": "Directory",
            },
        }],
        "containers": [{
            "name": pod,
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "command": ["/bin/bash", "-lc", "sleep 3600"],
            "env": environment,
            "volumeMounts": [{
                "name": "ascend-driver",
                "mountPath": "/usr/local/Ascend/driver",
                "readOnly": True,
            }],
        }],
    },
}))
PY
)
run_args=(
  -n "$namespace" run "$pod"
  "--image=$worker_base"
  --image-pull-policy=IfNotPresent
  --restart=Never
  "--overrides=$overrides"
)

"${kubectl_command[@]}" "${run_args[@]}" >/dev/null
"${kubectl_command[@]}" -n "$namespace" wait \
  --for=condition=Ready "pod/$pod" --timeout=300s >/dev/null

resolved_worker_python=$("${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  /bin/bash -lc 'command -v -- "$KCC_WORKER_PYTHON"')
if [[ "$resolved_worker_python" != /* ]]; then
  printf 'unable to resolve KCC_WORKER_PYTHON to an absolute path: %s\n' \
    "$resolved_worker_python" >&2
  exit 1
fi
if [[ -z "$runtime_bin" ]]; then
  runtime_bin=${resolved_worker_python%/*}
fi
"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  /bin/bash -c 'test -x "$1/ray"' -- "$runtime_bin" || {
    printf 'Ray CLI is not executable in KCC_WORKER_RUNTIME_BIN: %s/ray\n' \
      "$runtime_bin" >&2
    exit 1
  }

"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- /bin/bash -lc \
  'rm -rf /tmp/kcc-build && install -d -m 0755 /tmp/kcc-build/src /tmp/kcc-build/out/site-packages /tmp/kcc-build/out/kcc-hccl/bin'
tar -cf - pyproject.toml src ray_startup_bundle/hccl_runtime | \
  "${kubectl_command[@]}" -n "$namespace" exec -i "$pod" -- \
    tar -xf - -C /tmp/kcc-build/src

"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- /bin/bash -lc '
set -euo pipefail
python_command=${KCC_WORKER_PYTHON:-python}
ascend_dir=${CANN_ASCEND_DIR:-}
if [[ -z "$ascend_dir" ]]; then
  for candidate in \
    "${ASCEND_TOOLKIT_HOME:-}/aarch64-linux" \
    "${ASCEND_HOME_PATH:-}/aarch64-linux" \
    "${ASCEND_TOOLKIT_HOME:-}" \
    "${ASCEND_HOME_PATH:-}" \
    /usr/local/Ascend/cann/ascend-toolkit/latest; do
    if [[ -f "$candidate/include/hccl/hccl.h" || -f "$candidate/include/hccl.h" ]] && \
       [[ -e "$candidate/lib64/libhccl.so" ]] && \
       [[ -e "$candidate/lib64/libascendcl.so" ]]; then
      ascend_dir=$candidate
      break
    fi
  done
fi
if [[ -z "$ascend_dir" ]]; then
  printf "%s\n" "unable to locate CANN headers and libraries" >&2
  exit 1
fi
cd /tmp/kcc-build/src
"$python_command" -m pip install --no-cache-dir --no-deps --no-build-isolation \
  --target /tmp/kcc-build/out/site-packages .
make -C ray_startup_bundle/hccl_runtime/native ASCEND_DIR="$ascend_dir"
install -m 0755 \
  ray_startup_bundle/hccl_runtime/native/bin/ranktable_allreduce_probe \
  /tmp/kcc-build/out/kcc-hccl/bin/ranktable_allreduce_probe
cp -a ray_startup_bundle/hccl_runtime/hccl_check \
  /tmp/kcc-build/out/kcc-hccl/hccl_check
readelf -h /tmp/kcc-build/out/kcc-hccl/bin/ranktable_allreduce_probe | \
  grep -q "Machine:.*AArch64"
ldd /tmp/kcc-build/out/kcc-hccl/bin/ranktable_allreduce_probe | \
  grep -q "libhccl.so =>"
PYTHONPATH=/tmp/kcc-build/out/site-packages "$python_command" -c \
  "import kcc_training, ray; print(f\"worker artifacts: kcc={kcc_training.__version__} ray={ray.__version__}\")"
'

"${kubectl_command[@]}" -n "$namespace" exec "$pod" -- \
  tar -cf - -C /tmp/kcc-build/out . | tar -xf - -C "$temporary"

docker buildx build --load --pull --platform linux/arm64 \
  --build-arg "BASE_IMAGE=$worker_base" \
  --build-arg "VERSION=$version" \
  --build-arg "VCS_REF=$vcs_ref" \
  --build-arg "RUNTIME_BIN=$runtime_bin" \
  --tag "$image" \
  --file docker/Dockerfile.worker-prebuilt "$temporary"

docker image inspect "$image" --format '{{.Architecture}}' | grep -qx arm64
docker image inspect "$image" --format '{{range .Config.Env}}{{println .}}{{end}}' | \
  grep -Fq "PATH=$runtime_bin:"
printf 'Worker image built locally on %s with artifacts compiled on %s.\n' \
  "$image" "$node"
