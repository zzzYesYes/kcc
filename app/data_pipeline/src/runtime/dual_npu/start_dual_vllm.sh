#!/usr/bin/env bash
set -euo pipefail

state_dir=${1:-/tmp/mineru-dual}
mapping_file="$state_dir/npu-mapping.json"
mkdir -p "$state_dir"

python3 /opt/mineru-dual/discover_npu_mapping.py --output "$mapping_file"

mapping_value() {
  local physical_id=$1
  local field=$2
  python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["devices"][sys.argv[2]][sys.argv[3]])' \
    "$mapping_file" "$physical_id" "$field"
}

wait_healthy() {
  local port=$1
  local pid=$2
  for _ in $(seq 1 240); do
    if ! kill -0 "$pid" 2>/dev/null; then
      return 1
    fi
    if curl --noproxy '*' -fsS --max-time 3 "http://127.0.0.1:$port/health" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

start_service() {
  local name=$1
  local physical_id=$2
  local port=$3
  local cpu_set=$4
  local logical_id
  logical_id=$(mapping_value "$physical_id" logical_id)
  local log_file="$state_dir/vllm-$name.log"
  local pid_file="$state_dir/vllm-$name.pid"

  if test -f "$pid_file" && kill -0 "$(cat "$pid_file")" 2>/dev/null; then
    echo "service $name already running"
    return 0
  fi

  nohup setsid taskset -c "$cpu_set" env \
    ASCEND_VISIBLE_DEVICES="$logical_id" \
    ASCEND_RT_VISIBLE_DEVICES="$logical_id" \
    ASCEND_DEVICE_ID="$logical_id" \
    HTTP_PROXY= HTTPS_PROXY= ALL_PROXY= http_proxy= https_proxy= all_proxy= \
    mineru-vllm-server \
      --host 127.0.0.1 \
      --port "$port" \
      --gpu-memory-utilization 0.5 \
      --max-num-seqs 288 \
      --max-num-batched-tokens 2560 \
      >"$log_file" 2>&1 &
  local service_pid=$!
  echo "$service_pid" >"$pid_file"
  if ! wait_healthy "$port" "$service_pid"; then
    tail -100 "$log_file"
    return 1
  fi
  echo "$name physical=$physical_id logical=$logical_id port=$port cpu_set=$cpu_set pid=$service_pid"
}

start_service A 14 30001 0-31
start_service B 15 30002 32-63

curl --noproxy '*' -fsS http://127.0.0.1:30001/health >/dev/null
curl --noproxy '*' -fsS http://127.0.0.1:30002/health >/dev/null
echo "dual MinerU vLLM services are healthy"
