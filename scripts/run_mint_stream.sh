#!/usr/bin/env bash
set -euo pipefail

if [[ -f ".venv/bin/activate" ]]; then
  source .venv/bin/activate
fi

exec python -m solhunter_zero.rpc_mint_stream "$@"
