#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 || $# -gt 4 ]]; then
  printf '%s\n' 'usage: stable-preflight.sh VALUES.yaml [NAMESPACE] [HELM_RELEASE] [RUNTIME_PROFILE.yaml]' >&2
  exit 2
fi

values=$1
namespace=${2:-kcc-training}
release=${3:-kcc}
profile=${4:-}
script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
project_root=$(cd "$script_dir/.." && pwd)
if [[ -n ${KCC_CHART:-} ]]; then
  chart=$KCC_CHART
elif [[ -d "$project_root/deploy/helm/kcc-training-stable" ]]; then
  chart="$project_root/deploy/helm/kcc-training-stable"
else
  mapfile -t bundled_charts < <(
    find "$project_root/helm" -maxdepth 1 -type f -name 'kcc-training-*.tgz' -print 2>/dev/null
  )
  if [[ ${#bundled_charts[@]} -ne 1 ]]; then
    printf '%s\n' 'unable to resolve exactly one stable Chart; set KCC_CHART explicitly' >&2
    exit 1
  fi
  chart=${bundled_charts[0]}
fi

for command_name in python3 helm; do
  command -v "$command_name" >/dev/null || {
    printf 'missing command: %s\n' "$command_name" >&2
    exit 1
  }
done
if [[ ! -f "$values" ]]; then
  printf 'values file does not exist: %s\n' "$values" >&2
  exit 2
fi

temporary=$(mktemp -d /tmp/kcc-stable-preflight.XXXXXX)
trap 'rm -rf -- "$temporary"' EXIT
contract_python_path="$project_root/src"
if [[ ! -d "$contract_python_path/kcc_training" ]]; then
  if [[ ! -d "$project_root/python" ]]; then
    printf '%s\n' 'bundle Python wheelhouse is missing; cannot run contract validation' >&2
    exit 1
  fi
  python3 -m pip install --disable-pip-version-check --no-index \
    --find-links "$project_root/python" \
    --target "$temporary/python" \
    kcc-training >/dev/null
  contract_python_path="$temporary/python"
fi

PYTHONPATH="$contract_python_path" python3 - "$values" <<'PY'
import sys
import yaml

value = yaml.safe_load(open(sys.argv[1], encoding="utf-8")) or {}
image = value.get("controller", {}).get("image", "")
gateway = value.get("artifactGateway", {}).get("endpoint", "")
if "example.invalid" in image or "sha256:" + "0" * 64 in image:
    raise SystemExit("controller image is still a placeholder")
if "invalid" in gateway or not gateway.startswith(("http://", "https://")):
    raise SystemExit("artifact gateway is missing or still a placeholder")
PY
helm lint "$chart" -f "$values"
helm template "$release" "$chart" \
  --namespace "$namespace" \
  --include-crds \
  -f "$values" >"$temporary/rendered.yaml"
if [[ -n "$profile" ]]; then
  if [[ ! -f "$profile" ]]; then
    printf 'runtime profile does not exist: %s\n' "$profile" >&2
    exit 2
  fi
  PYTHONPATH="$contract_python_path" python3 -m kcc_training.cli validate "$profile"
fi
if [[ ${KCC_PREFLIGHT_RENDER_ONLY:-false} == true ]]; then
  printf '%s\n' 'stable preflight render PASS (cluster discovery intentionally skipped)'
  exit 0
fi
command -v kubectl >/dev/null || {
  printf '%s\n' 'missing command: kubectl' >&2
  exit 1
}

mapfile -t settings < <(
  PYTHONPATH="$contract_python_path" python3 - "$temporary/rendered.yaml" "$values" <<'PY'
import sys
import yaml

documents = [item for item in yaml.safe_load_all(open(sys.argv[1], encoding="utf-8")) if item]
values = yaml.safe_load(open(sys.argv[2], encoding="utf-8")) or {}
deployment = next(item for item in documents if item.get("kind") == "Deployment")
runtime_binding = next(
    item
    for item in documents
    if item.get("kind") == "RoleBinding"
    and item.get("metadata", {}).get("name", "").endswith("-runtime")
)
print(f"deployment={deployment['metadata']['name']}")
print(f"controller={deployment['spec']['template']['spec']['serviceAccountName']}")
print(f"runtime={runtime_binding['subjects'][0]['name']}")
print(f"controllerCreate={'true' if values['controller']['serviceAccount']['create'] else 'false'}")
print(f"runtimeCreate={'true' if values['runtimeServiceAccount']['create'] else 'false'}")
exporter = values.get("npuExporter", {})
print(f"exporterEnabled={'true' if exporter.get('enabled', False) else 'false'}")
print(f"exporterNamespace={exporter.get('namespace', 'npu-exporter')}")
print(f"exporterRbacCreate={'true' if exporter.get('rbac', {}).get('create', True) else 'false'}")
token = values.get("artifactGateway", {}).get("tokenSecretName", "")
if token:
    print(f"tokenSecret={token}")
for item in values.get("imagePullSecrets", []):
    print(f"secret={item['name']}")
for item in values.get("runtimeServiceAccount", {}).get("imagePullSecrets", []):
    print(f"secret={item['name']}")
PY
)
for item in "${settings[@]}"; do
  case "$item" in
    deployment=*) deployment_name=${item#deployment=} ;;
    controller=*) controller_sa=${item#controller=} ;;
    runtime=*) runtime_sa=${item#runtime=} ;;
    controllerCreate=*) controller_create=${item#controllerCreate=} ;;
    runtimeCreate=*) runtime_create=${item#runtimeCreate=} ;;
    exporterEnabled=*) exporter_enabled=${item#exporterEnabled=} ;;
    exporterNamespace=*) exporter_namespace=${item#exporterNamespace=} ;;
    exporterRbacCreate=*) exporter_rbac_create=${item#exporterRbacCreate=} ;;
  esac
done

kubectl get crd rayclusters.ray.io >/dev/null
kubectl get nodes >/dev/null

current_can_i() {
  local verb=$1
  local resource=$2
  local target_namespace=${3:-}
  local answer
  if [[ -n "$target_namespace" ]]; then
    answer=$(kubectl auth can-i "$verb" "$resource" --namespace "$target_namespace")
  else
    answer=$(kubectl auth can-i "$verb" "$resource")
  fi
  if [[ "$answer" != yes ]]; then
    printf 'installer RBAC denied: %s %s\n' "$verb" "$resource" >&2
    return 1
  fi
}

installed=false
if kubectl -n "$namespace" get deployment "$deployment_name" >/dev/null 2>&1; then
  installed=true
fi

if [[ "$installed" == true ]]; then
  kubectl -n "$namespace" get serviceaccount "$controller_sa" >/dev/null
  kubectl -n "$namespace" get serviceaccount "$runtime_sa" >/dev/null

  can_i() {
    local service_account=$1
    local verb=$2
    local resource=$3
    local target_namespace=${4:-}
    local answer
    if [[ -n "$target_namespace" ]]; then
      answer=$(kubectl auth can-i \
        --as="system:serviceaccount:$namespace:$service_account" \
        "$verb" "$resource" --namespace "$target_namespace")
    else
      answer=$(kubectl auth can-i \
        --as="system:serviceaccount:$namespace:$service_account" \
        "$verb" "$resource")
    fi
    if [[ "$answer" != yes ]]; then
      printf 'RBAC denied for %s: %s %s\n' "$service_account" "$verb" "$resource" >&2
      return 1
    fi
  }

  can_i "$controller_sa" list trainingruns.training.kcc.io "$namespace"
  can_i "$controller_sa" create rayclusters.ray.io "$namespace"
  can_i "$controller_sa" list nodes
  can_i "$runtime_sa" create configmaps "$namespace"
  can_i "$runtime_sa" create pods/exec "$namespace"
else
  printf '%s\n' 'INFO: release is not installed; checking installer permissions instead of future ServiceAccounts.'
  if ! kubectl get namespace "$namespace" >/dev/null 2>&1; then
    current_can_i create namespaces
  fi
  current_can_i create deployments.apps "$namespace"
  if [[ "$controller_create" == true || "$runtime_create" == true ]]; then
    current_can_i create serviceaccounts "$namespace"
  fi
  current_can_i create roles.rbac.authorization.k8s.io "$namespace"
  current_can_i create rolebindings.rbac.authorization.k8s.io "$namespace"
  current_can_i create clusterroles.rbac.authorization.k8s.io
  current_can_i create clusterrolebindings.rbac.authorization.k8s.io
  current_can_i create customresourcedefinitions.apiextensions.k8s.io
  if [[ "$controller_create" == false ]]; then
    kubectl -n "$namespace" get serviceaccount "$controller_sa" >/dev/null
  fi
  if [[ "$runtime_create" == false ]]; then
    kubectl -n "$namespace" get serviceaccount "$runtime_sa" >/dev/null
  fi
fi

if [[ "$exporter_enabled" == true ]]; then
  kubectl get namespace "$exporter_namespace" >/dev/null
  if [[ "$installed" == true ]]; then
    can_i "$controller_sa" get pods "$exporter_namespace"
  elif [[ "$exporter_rbac_create" == true ]]; then
    current_can_i create roles.rbac.authorization.k8s.io "$exporter_namespace"
    current_can_i create rolebindings.rbac.authorization.k8s.io "$exporter_namespace"
  else
    printf '%s\n' 'INFO: exporter RBAC is externally managed; rendered resources are not required.'
  fi
fi

for item in "${settings[@]}"; do
  case "$item" in
    tokenSecret=*)
      token_secret=${item#tokenSecret=}
      has_token=$(kubectl -n "$namespace" get secret "$token_secret" -o json | python3 -c \
        'import json,sys; print("yes" if "token" in (json.load(sys.stdin).get("data") or {}) else "no")')
      if [[ "$has_token" != yes ]]; then
        printf 'artifact Gateway Secret %s does not contain a token key\n' "$token_secret" >&2
        exit 1
      fi
      ;;
    secret=*)
      kubectl -n "$namespace" get secret "${item#secret=}" >/dev/null
      ;;
  esac
done

if [[ -n "$profile" ]]; then
  mapfile -t profile_values < <(
    PYTHONPATH="$contract_python_path" python3 - "$profile" <<'PY'
import sys
import yaml

value = yaml.safe_load(open(sys.argv[1], encoding="utf-8"))
spec = value["spec"]
print(spec["workspace"]["claimName"])
print(spec["accelerator"].get("runtimeClassName", ""))
print(spec["accelerator"]["resourceName"])
print(spec["accelerator"]["devicesPerNode"])
print(spec["integrations"]["healthProvider"])
print(f"activeCount={len(spec['scheduling']['activeNodes'])}")
for node in spec["scheduling"]["activeNodes"] + spec["scheduling"]["spareNodes"]:
    print(f"node={node}")
for secret in spec["images"].get("pullSecrets", []):
    print(f"secret={secret}")
PY
  )
  claim=${profile_values[0]}
  runtime_class=${profile_values[1]}
  resource_name=${profile_values[2]}
  devices_per_node=${profile_values[3]}
  health_provider=${profile_values[4]}
  active_count=${profile_values[5]#activeCount=}

  phase=$(kubectl -n "$namespace" get pvc "$claim" -o jsonpath='{.status.phase}')
  if [[ "$phase" != Bound ]]; then
    printf 'workspace PVC is not Bound: %s (%s)\n' "$claim" "$phase" >&2
    exit 1
  fi
  access_modes=$(kubectl -n "$namespace" get pvc "$claim" -o json | python3 -c \
    'import json,sys; print(" ".join(json.load(sys.stdin).get("spec",{}).get("accessModes",[])))')
  if [[ "$active_count" -gt 1 && " $access_modes " != *" ReadWriteMany "* ]]; then
    printf 'workspace PVC %s must include ReadWriteMany for %s active nodes (has: %s)\n' \
      "$claim" "$active_count" "$access_modes" >&2
    exit 1
  fi
  if [[ -n "$runtime_class" ]]; then
    kubectl get runtimeclass "$runtime_class" >/dev/null
  fi
  for item in "${profile_values[@]:6}"; do
    case "$item" in
      node=*)
        node=${item#node=}
        allocatable=$(kubectl get node "$node" -o json | python3 -c \
          'import json,sys; value=json.load(sys.stdin); print(value.get("status",{}).get("allocatable",{}).get(sys.argv[1],"0"))' \
          "$resource_name")
        if [[ ! "$allocatable" =~ ^[0-9]+$ || "$allocatable" -lt "$devices_per_node" ]]; then
          printf 'node %s exposes %s=%s; expected at least %s\n' \
            "$node" "$resource_name" "$allocatable" "$devices_per_node" >&2
          exit 1
        fi
        ;;
      secret=*)
        kubectl -n "$namespace" get secret "${item#secret=}" >/dev/null
        ;;
    esac
  done
  if [[ "$health_provider" == npu-exporter && "$exporter_enabled" != true ]]; then
    printf '%s\n' 'RuntimeProfile uses npu-exporter but values npuExporter.enabled is false' >&2
    exit 1
  fi
fi

printf '%s\n' 'stable preflight PASS (render plus read-only discovery/storage/device/RBAC checks)'

