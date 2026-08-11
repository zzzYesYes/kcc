from __future__ import annotations

import json
import os
import ssl
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request, urlopen

from dagster import ConfigurableResource


class QwenKubernetesResource(ConfigurableResource):
    """Minimal in-cluster Kubernetes client for the dedicated Qwen Deployment."""

    namespace: str = os.environ.get("QWEN_NAMESPACE", "k12")
    deployment_name: str = os.environ.get(
        "QWEN_DEPLOYMENT_NAME", "qwen36-35b-a3b-worker-14-15"
    )
    config_map_name: str = os.environ.get("QWEN_CONFIGMAP_NAME", "qwen36-35b-launcher")
    service_name: str = os.environ.get("QWEN_SERVICE_NAME", "qwen36-35b-a3b")
    pod_label_key: str = os.environ.get("QWEN_POD_LABEL_KEY", "app.kubernetes.io/name")
    pod_label: str = os.environ.get("QWEN_POD_LABEL", "qwen36-35b-a3b-worker")
    service_count: int = int(os.environ.get("QWEN_SERVICE_COUNT", "1"))
    device_pairs: str = os.environ.get("QWEN_DEVICE_PAIRS", "14,15")
    endpoint_specs: str = os.environ.get("QWEN_ENDPOINT_SPECS", "")
    data_parallel_size: int = int(os.environ.get("QWEN_DATA_PARALLEL_SIZE", "1"))
    tensor_parallel_size: int = int(os.environ.get("QWEN_TENSOR_PARALLEL_SIZE", "1"))
    model_path: str = os.environ.get(
        "QWEN_MODEL_PATH", "/models/Qwen3.6-35B-A3B-w8a8"
    )
    model_name: str = os.environ.get("QWEN_MODEL_NAME", "qwen3.6-35b-a3b")
    base_port: int = int(os.environ.get("QWEN_BASE_PORT", "8000"))

    _token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
    _ca_path = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
    _api_base = "https://kubernetes.default.svc"

    def _request(self, method: str, path: str, body: dict | None = None) -> dict:
        token = self._token_path.read_text().strip()
        data = json.dumps(body).encode() if body is not None else None
        request = Request(
            f"{self._api_base}{path}",
            data=data,
            headers={
                "Authorization": f"Bearer {token}",
                "Accept": "application/json",
                "Content-Type": "application/merge-patch+json",
            },
            method=method,
        )
        context = ssl.create_default_context(cafile=self._ca_path)
        try:
            with urlopen(request, context=context, timeout=30) as response:
                payload = response.read()
        except HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:2000]
            raise RuntimeError(
                f"Kubernetes {method} {path} failed: {exc.code} {detail}"
            ) from exc
        return json.loads(payload) if payload else {}

    def deployment(self) -> dict:
        return self._request(
            "GET",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{self.deployment_name}",
        )

    def endpoints(self) -> list[tuple[str, str, int, str]]:
        if self.endpoint_specs.strip():
            endpoints = []
            for raw_spec in self.endpoint_specs.split(";"):
                fields = [value.strip() for value in raw_spec.split("|")]
                if len(fields) != 4 or not all(fields):
                    raise ValueError(f"Invalid QWEN_ENDPOINT_SPECS entry: {raw_spec!r}")
                endpoint_id, devices, port, cpu_set = fields
                endpoints.append((endpoint_id, devices, int(port), cpu_set))
        else:
            device_groups = [
                value.strip() for value in self.device_pairs.split(";") if value.strip()
            ]
            if len(device_groups) != self.service_count:
                raise ValueError(
                    f"Expected {self.service_count} device groups, found "
                    f"{len(device_groups)}"
                )
            endpoints = [
                (f"qwen-{index}", devices, self.base_port + index, "0-255")
                for index, devices in enumerate(device_groups)
            ]
        if len(endpoints) != self.service_count:
            raise ValueError(
                f"Expected {self.service_count} endpoints, found {len(endpoints)}"
            )
        return endpoints

    def api_urls(self) -> list[str]:
        host = f"{self.service_name}.{self.namespace}.svc.cluster.local"
        return [f"http://{host}:{port}" for _, _, port, _ in self.endpoints()]

    def pod_status(self) -> dict:
        listing = self._request(
            "GET",
            f"/api/v1/namespaces/{self.namespace}/pods?labelSelector="
            f"{quote(self.pod_label_key, safe='')}%3D{quote(self.pod_label, safe='')}",
        )
        items = listing.get("items", [])
        if not items:
            return {"phase": "Stopped", "ready": False}
        pod = items[0]
        statuses = {
            status["name"]: bool(status.get("ready"))
            for status in pod.get("status", {}).get("containerStatuses", [])
        }
        return {
            "name": pod["metadata"]["name"],
            "phase": pod.get("status", {}).get("phase", "Unknown"),
            "node": pod.get("spec", {}).get("nodeName"),
            "ready": statuses.get("vllm-ascend", False)
            and statuses.get("ray-bridge", False),
            "containers": statuses,
        }

    def status(self) -> dict:
        deployment = self.deployment()
        spec = deployment.get("spec", {})
        state = deployment.get("status", {})
        api_urls = self.api_urls()
        return {
            "deployment": self.deployment_name,
            "desired_replicas": int(spec.get("replicas", 0)),
            "available_replicas": int(state.get("availableReplicas", 0)),
            "updated_replicas": int(state.get("updatedReplicas", 0)),
            "pod": self.pod_status(),
            "api_url": api_urls[0],
            "api_urls": api_urls,
            "data_parallel_size": self.data_parallel_size,
            "tensor_parallel_size": self.tensor_parallel_size,
        }

    def render_launcher(self, config: dict) -> str:
        endpoints = self.endpoints()
        launches = "\n".join(
            f'launch_service "{endpoint_id}" "{devices}" {port} "{cpu_set}"'
            for endpoint_id, devices, port, cpu_set in endpoints
        )
        expected_devices = len(
            {
                device.strip()
                for _, devices, _, _ in endpoints
                for device in devices.split(",")
                if device.strip()
            }
        )
        return f'''#!/usr/bin/env bash
set -euo pipefail
export PYTORCH_NPU_ALLOC_CONF=expandable_segments:True
export HCCL_OP_EXPANSION_MODE=AIV
export HCCL_BUFFSIZE=1024
export OMP_NUM_THREADS=1
export TASK_QUEUE_ENABLE=1
python - <<'PY'
import torch
import torch_npu
count = torch.npu.device_count()
if count != {expected_devices}:
    raise SystemExit(f"Expected {expected_devices} visible NPU devices, found {{count}}")
print(f"Validated {{count}} visible NPU devices")
PY
pids=()
stop_all() {{
  if ((${{#pids[@]}})); then
    kill "${{pids[@]}}" 2>/dev/null || true
    wait "${{pids[@]}}" 2>/dev/null || true
  fi
}}
trap stop_all EXIT INT TERM
launch_service() {{
  local endpoint_id="$1"
  local devices="$2"
  local port="$3"
  local cpu_set="$4"
  (
    export ASCEND_VISIBLE_DEVICES="$devices"
    export ASCEND_RT_VISIBLE_DEVICES="$devices"
    export VLLM_CACHE_ROOT="/tmp/vllm-cache-${{port}}"
    export XDG_CACHE_HOME="/tmp/xdg-cache-${{port}}"
    exec taskset -c "$cpu_set" vllm serve {self.model_path} \
      --host 0.0.0.0 \
      --port "$port" \
      --served-model-name {self.model_name} \
      --data-parallel-size {self.data_parallel_size} \
      --tensor-parallel-size {self.tensor_parallel_size} \
      --enable-expert-parallel \
      --quantization ascend \
      --dtype bfloat16 \
      --max-model-len {config["max_model_len"]} \
      --max-num-seqs {config["max_num_seqs"]} \
      --max-num-batched-tokens {config["max_num_batched_tokens"]} \
      --gpu-memory-utilization {config["gpu_memory_utilization"]:.2f} \
      --trust-remote-code \
      --no-enable-prefix-caching \
      --compilation-config '{{"cudagraph_mode":"FULL_DECODE_ONLY"}}' \
      --additional-config '{{"enable_cpu_binding":true,"ascend_compilation_config":{{"fuse_norm_quant":false}}}}'
  ) >"/tmp/qwen-vllm-${{port}}.log" 2>&1 &
  pids+=("$!")
  echo "Started Qwen endpoint=${{endpoint_id}} port=${{port}} devices=${{devices}} pid=${{pids[-1]}}"
}}
{launches}
set +e
wait -n "${{pids[@]}}"
status=$?
set -e
echo "A Qwen endpoint exited with status $status; stopping remaining endpoints" >&2
exit "$status"
'''

    def configure_and_restart(self, config: dict, replicas: int, restart: bool) -> dict:
        self._request(
            "PATCH",
            f"/api/v1/namespaces/{self.namespace}/configmaps/{self.config_map_name}",
            {"data": {"start_qwen.sh": self.render_launcher(config)}},
        )
        template_patch: dict = {"spec": {"replicas": replicas}}
        if restart:
            template_patch["spec"]["template"] = {
                "metadata": {
                    "annotations": {
                        "k12.ai/qwen-restarted-at": datetime.now(timezone.utc).isoformat(),
                    }
                }
            }
        self._request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{self.deployment_name}",
            template_patch,
        )
        return self.status()

    def stop(self) -> dict:
        self._request(
            "PATCH",
            f"/apis/apps/v1/namespaces/{self.namespace}/deployments/{self.deployment_name}",
            {"spec": {"replicas": 0}},
        )
        return self.status()

    def wait_until_ready(self, timeout_seconds: int) -> dict:
        deadline = time.monotonic() + timeout_seconds
        last_status: dict = {}
        while time.monotonic() < deadline:
            last_status = self.status()
            if last_status["available_replicas"] == 1 and last_status["pod"]["ready"]:
                return last_status
            time.sleep(5)
        raise TimeoutError(f"Qwen did not become ready: {last_status}")
