#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
project_root=$(cd "$script_dir/.." && pwd)
cd "$project_root"

for command_name in python3 helm; do
  command -v "$command_name" >/dev/null || {
    printf 'missing command: %s\n' "$command_name" >&2
    exit 1
  }
done

chart=deploy/helm/kcc-training-stable
temporary=$(mktemp -d /tmp/kcc-stable-audit.XXXXXX)
trap 'rm -rf -- "$temporary"' EXIT

PYTHONPATH=src python3 -m unittest discover -s tests
python3 -m compileall -q src tests ray_startup_bundle
for document in examples/runtime-profile.yaml examples/recipe.yaml examples/training-run.yaml; do
  PYTHONPATH=src python3 -m kcc_training.cli validate "$document"
done
python3 -m json.tool deploy/helm/kcc-training-stable/values.schema.json >/dev/null
for schema in contracts/*.schema.json; do
  python3 -m json.tool "$schema" >/dev/null
done

python3 <<'PY'
import pathlib
import re
import tomllib

root = pathlib.Path(".")
project = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
version = project["project"]["version"]
module = (root / "src/kcc_training/__init__.py").read_text(encoding="utf-8")
match = re.search(r'__version__\s*=\s*"([^"]+)"', module)
if match is None or match.group(1) != version:
    raise SystemExit("Python package/module versions differ")
chart = (root / "deploy/helm/kcc-training-stable/Chart.yaml").read_text(encoding="utf-8")
if f"version: {version}\n" not in chart or f'appVersion: "{version}"' not in chart:
    raise SystemExit("Python and stable Chart versions differ")
for path in (root / "contracts").glob("*.json"):
    if "v1alpha1" in path.read_text(encoding="utf-8"):
        raise SystemExit(f"legacy apiVersion remains in canonical contract: {path}")
for path in (root / "examples").glob("*.yaml"):
    if "training.kcc.io/v1beta1" not in path.read_text(encoding="utf-8"):
        raise SystemExit(f"canonical example is not v1beta1: {path}")
PY

helm lint "$chart"
helm template kcc "$chart" --namespace kcc-training --include-crds >"$temporary/default.yaml"
helm template kcc "$chart" --namespace kcc-training \
  --set npuExporter.enabled=true \
  --include-crds >"$temporary/exporter.yaml"
helm template kcc "$chart" --namespace kcc-training \
  --set controller.serviceAccount.create=false \
  --set controller.serviceAccount.name=external-controller \
  --set runtimeServiceAccount.create=false \
  --set runtimeServiceAccount.name=external-runtime \
  >"$temporary/external-serviceaccounts.yaml"

python3 - "$temporary/default.yaml" "$temporary/exporter.yaml" "$temporary/external-serviceaccounts.yaml" <<'PY'
import sys
import yaml

def load(path):
    return [item for item in yaml.safe_load_all(open(path, encoding="utf-8")) if item]

default, exporter, external = map(load, sys.argv[1:])
deployment = next(item for item in default if item.get("kind") == "Deployment")
container = deployment["spec"]["template"]["spec"]["containers"][0]
if container["command"] != ["python", "-m", "kcc_training.controller_stable"]:
    raise SystemExit("stable Chart does not launch controller_stable")
if any(
    item.get("kind") in {"Role", "RoleBinding"}
    and item.get("metadata", {}).get("namespace") == "npu-exporter"
    for item in default
):
    raise SystemExit("default kubernetes health deployment still requires exporter namespace")
if not any(
    item.get("kind") == "Role"
    and item.get("metadata", {}).get("namespace") == "npu-exporter"
    for item in exporter
):
    raise SystemExit("enabled exporter RBAC was not rendered")
if any(item.get("kind") == "ServiceAccount" for item in external):
    raise SystemExit("external ServiceAccounts were unexpectedly created")
external_deployment = next(item for item in external if item.get("kind") == "Deployment")
if external_deployment["spec"]["template"]["spec"]["serviceAccountName"] != "external-controller":
    raise SystemExit("external controller ServiceAccount was not selected")
pdb = next((item for item in default if item.get("kind") == "PodDisruptionBudget"), None)
if pdb is None or str(pdb["spec"].get("minAvailable")) != "1":
    raise SystemExit("default controller PodDisruptionBudget is missing")
cluster_roles = [item for item in default if item.get("kind") == "ClusterRole"]
if len(cluster_roles) != 1:
    raise SystemExit("stable Chart must render one minimal ClusterRole")
cluster_resources = {
    resource
    for rule in cluster_roles[0]["rules"]
    for resource in rule.get("resources", [])
}
if cluster_resources != {"nodes"}:
    raise SystemExit("ClusterRole contains permissions beyond node reads")
controller_role = next(
    item
    for item in default
    if item.get("kind") == "Role"
    and item.get("metadata", {}).get("namespace") == "kcc-training"
    and item.get("metadata", {}).get("name") == deployment["metadata"]["name"]
)
namespaced_resources = {
    resource
    for rule in controller_role["rules"]
    for resource in rule.get("resources", [])
}
if not {"trainingruns", "trainingruns/status", "rayclusters"} <= namespaced_resources:
    raise SystemExit("controller namespaced Role is incomplete")
runtime_binding = next(
    item
    for item in default
    if item.get("kind") == "RoleBinding"
    and item.get("metadata", {}).get("name", "").endswith("-runtime")
)
runtime_name = runtime_binding["subjects"][0]["name"]
if runtime_name == "kcc-training-runtime" or not runtime_name.endswith("-runtime"):
    raise SystemExit("default runtime ServiceAccount is not release-scoped")
external_runtime = next(
    item
    for item in external
    if item.get("kind") == "RoleBinding"
    and item.get("metadata", {}).get("name", "").endswith("-runtime")
)
if external_runtime["subjects"][0]["name"] != "external-runtime":
    raise SystemExit("external runtime ServiceAccount was not selected")
PY

python3 -m pip wheel --no-deps --no-build-isolation -w "$temporary" .
wheel=$(find "$temporary" -maxdepth 1 -type f -name '*.whl' -print -quit)
if [[ -z "$wheel" ]]; then
  printf '%s\n' 'wheel build produced no artifact' >&2
  exit 1
fi
python3 -m pip install --no-deps --target "$temporary/install" "$wheel" >/dev/null
expected_version=$(PYTHONPATH=src python3 -c 'import kcc_training; print(kcc_training.__version__)')
PYTHONPATH="$temporary/install" python3 - "$expected_version" <<'PY'
import importlib.metadata
import sys
import kcc_training.controller_stable
import kcc_training.runtime.coordinator_stable

if importlib.metadata.version("kcc-training") != sys.argv[1]:
    raise SystemExit("installed wheel metadata version differs")
PY
helm package "$chart" --destination "$temporary" >/dev/null
chart_archive=$(find "$temporary" -maxdepth 1 -type f -name 'kcc-training-*.tgz' -print -quit)
if [[ -z "$chart_archive" ]]; then
  printf '%s\n' 'stable Chart package was not produced' >&2
  exit 1
fi
if tar -tzf "$chart_archive" | grep -Eq '(\.orig|\.rej|~)$'; then
  printf '%s\n' 'stable Chart package contains editor or patch backup files' >&2
  exit 1
fi
bash -n scripts/*.sh
for script in scripts/*.sh scripts/shadow-compare.py; do
  if [[ ! -x "$script" ]]; then
    printf 'release script is not executable: %s\n' "$script" >&2
    exit 1
  fi
done
if ! grep -Fq 'ENTRYPOINT ["python", "-m", "kcc_training.controller_stable"]' docker/Dockerfile.controller; then
  printf '%s\n' 'controller image default entrypoint is not stable' >&2
  exit 1
fi
printf '%s\n' 'stable static release audit PASS (image build and cluster acceptance are separate)'

