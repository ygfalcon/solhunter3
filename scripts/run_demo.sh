#!/usr/bin/env bash
set -e

# Change to repository root
cd "$(dirname "$0")/.."

EXTRA=".[uvloop]"
for arg in "$@"; do
  if [[ "$arg" == "--full-system" ]]; then
    EXTRA=".[full]"
    break
  fi
done

pip install "$EXTRA"

python demo.py "$@"

echo "Summary tail:"
tail reports/summary.json

echo "Highlights tail:"
tail reports/highlights.json
