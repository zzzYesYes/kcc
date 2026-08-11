#!/usr/bin/env bash
set -euo pipefail

state_dir=${1:-/tmp/mineru-dual}

for service_name in A B; do
  pid_file="$state_dir/vllm-$service_name.pid"
  test -f "$pid_file" || continue
  service_pid=$(cat "$pid_file")
  if kill -0 "$service_pid" 2>/dev/null; then
    kill -TERM -- "-$service_pid" 2>/dev/null || kill -TERM "$service_pid"
    for _ in $(seq 1 30); do
      kill -0 "$service_pid" 2>/dev/null || break
      sleep 1
    done
    kill -KILL -- "-$service_pid" 2>/dev/null || true
  fi
  rm -f "$pid_file"
done
