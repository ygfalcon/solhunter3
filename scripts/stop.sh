#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT_DIR"

PIDS=$(pgrep -f "scripts/live_runtime_controller.py" || true)
if [[ -z ${PIDS:-} ]]; then
  echo "No running live_runtime_controller processes found." >&2
  exit 0
fi

declare -a TARGETS=()
for pid in $PIDS; do
  if ps -p "$pid" >/dev/null 2>&1; then
    TARGETS+=("$pid")
  fi
fi

if [[ ${#TARGETS[@]} -eq 0 ]]; then
  echo "No active runtime processes detected." >&2
  exit 0
fi

for pid in "${TARGETS[@]}"; do
  pgid=$(ps -o pgid= -p "$pid" | tr -d ' ' || true)
  if [[ -n ${pgid:-} ]]; then
    echo "Stopping runtime process group $pgid (leader $pid)" >&2
    kill -TERM "-$pgid" 2>/dev/null || kill -TERM "$pid" 2>/dev/null || true
  else
    echo "Stopping runtime process $pid" >&2
    kill -TERM "$pid" 2>/dev/null || true
  fi
done

# Give processes a short window to exit gracefully
sleep 2

for pid in "${TARGETS[@]}"; do
  if ps -p "$pid" >/dev/null 2>&1; then
    pgid=$(ps -o pgid= -p "$pid" | tr -d ' ' || true)
    if [[ -n ${pgid:-} ]]; then
      kill -KILL "-$pgid" 2>/dev/null || kill -KILL "$pid" 2>/dev/null || true
    else
      kill -KILL "$pid" 2>/dev/null || true
    fi
  fi
done

echo "Runtime processes stopped. Redis and shared services were left untouched." >&2
