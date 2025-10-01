#!/usr/bin/env bash
# macOS launcher for SolHunter Zero on Solana devnet.
# Uses the dedicated devnet config and starts the bot in --testnet mode.

set -euo pipefail
cd "$(dirname "$0")"

# Prefer project venv if present
if [[ -d .venv ]]; then
  PY=".venv/bin/python"
else
  PY="python3"
fi

# Point to the devnet config; auto-select the single keypair if present
export SOLHUNTER_CONFIG="$(pwd)/config/devnet.toml"
export AUTO_SELECT_KEYPAIR=1

# Make sure we’re clearly not using MEV bundles or mainnet-only settings
export USE_MEV_BUNDLES=0
export SOLANA_TESTNET_RPC_URL=${SOLANA_TESTNET_RPC_URL:-https://api.devnet.solana.com}

exec "$PY" -m solhunter_zero.main --auto --testnet --min-delay 10 --max-delay 120 "$@"

