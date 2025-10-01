#!/usr/bin/env bash
# Stop the demo UI + agents started by run_testnet_demo.sh
set -euo pipefail
cd "$(dirname "$0")/.."

stop_pid() {
  local file="$1"
  if [[ -f "$file" ]]; then
    local pid
    pid=$(cat "$file" 2>/dev/null || echo "")
    if [[ -n "$pid" ]]; then
      kill -TERM "$pid" 2>/dev/null || true
    fi
    rm -f "$file"
  fi
}

stop_pid ui_demo.pid
stop_pid demo_agents.pid

echo "Stopped demo processes (if any)."

