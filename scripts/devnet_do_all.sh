#!/usr/bin/env bash
# One-shot: mint + seed + swap + launch agents pointed at your mint.
# Safe to re-run; uses latest mint in minted_mint.txt.

set -euo pipefail
cd "$(dirname "$0")/.."

# Ensure Solana CLIs are on PATH for non-interactive shells
SOL_RELEASE_BIN="$HOME/.local/share/solana/install/active_release/bin"
LOCAL_BIN="$HOME/.local/bin"
export PATH="$SOL_RELEASE_BIN:$LOCAL_BIN:$PATH"

if [[ -z "${KEYPAIR_PATH:-}" ]]; then
  echo "Set KEYPAIR_PATH to your wallet JSON (e.g. keypairs/cli.json)" >&2
  exit 2
fi

# 1) End-to-end mint + initial seed
bash scripts/devnet_all_in_one.sh

# 2) Use the minted mint for a focused pool swap (idempotent)
MINT=$(cat minted_mint.txt)
echo "==> Using mint: $MINT"
KEYPAIR_PATH="$KEYPAIR_PATH" bash scripts/devnet_pool_swap.sh --mint "$MINT"

# 3) Launch agents on devnet using this mint
export SOLHUNTER_CONFIG="$(pwd)/config/devnet.toml"
# Use a non-conflicting local event-bus port to avoid 'address already in use'
# Avoid local event-bus port conflicts; run without a local broker for the quick check
unset EVENT_BUS_URL || true
export EVENT_BUS_DISABLE_LOCAL=1
echo "==> Launching agents on devnet with file discovery"
python -m solhunter_zero.main --auto --testnet --discovery-method file --token-list devnet_tokens.txt --iterations 1 --min-delay 5 --max-delay 10 || true

echo "==> Done"
