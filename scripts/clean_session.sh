#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SESSION_ROOT="$ROOT_DIR/artifacts/prelaunch"

if [[ ! -d $SESSION_ROOT ]]; then
  echo "No prelaunch artifacts found to clean." >&2
  exit 0
fi

rm -rf "$SESSION_ROOT"/* "$SESSION_ROOT"/.??* 2>/dev/null || true
mkdir -p "$SESSION_ROOT/logs"

echo "Cleared artifacts/prelaunch/. Redis and persistent state were left untouched." >&2
