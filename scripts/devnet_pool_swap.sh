#!/usr/bin/env bash
# Use an existing pool + mint to perform a simple devnet swap cycle:
#  - Seed pool with your token (user -> pool ATA)
#  - Send SOL to pool (user -> pool)
#  - Send token back to user (pool -> user ATA)
#
# Usage:
#   KEYPAIR_PATH=keypairs/cli.json scripts/devnet_pool_swap.sh --mint <MINT> \
#       [--rpc https://api.devnet.solana.com] [--sol 0.01] [--amount 100000]

set -euo pipefail

cd "$(dirname "$0")/.."

# Ensure Solana CLIs are on PATH for non-interactive shells
SOL_RELEASE_BIN="$HOME/.local/share/solana/install/active_release/bin"
LOCAL_BIN="$HOME/.local/bin"
export PATH="$SOL_RELEASE_BIN:$LOCAL_BIN:$PATH"

RPC_URL="https://api.devnet.solana.com"
SOL_AMOUNT="0.01"
TOKEN_AMOUNT="100000"
MINT=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mint) MINT="$2"; shift 2 ;;
    --rpc) RPC_URL="$2"; shift 2 ;;
    --sol) SOL_AMOUNT="$2"; shift 2 ;;
    --amount) TOKEN_AMOUNT="$2"; shift 2 ;;
    *) echo "Unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ -z "$MINT" ]] && { echo "--mint is required" >&2; exit 2; }

if [[ -z "${KEYPAIR_PATH:-}" ]]; then
  echo "KEYPAIR_PATH is required (e.g. keypairs/cli.json)" >&2; exit 2
fi

if ! command -v solana >/dev/null 2>&1 || ! command -v spl-token >/dev/null 2>&1; then
  echo "solana and spl-token CLIs must be on PATH" >&2; exit 2
fi

USER_KP="$KEYPAIR_PATH"
USER_PUB=$(solana-keygen pubkey "$USER_KP")
POOL_KP="keypairs/pool.json"
[[ -f "$POOL_KP" ]] || { echo "Missing $POOL_KP (create with solana-keygen new -o keypairs/pool.json)" >&2; exit 2; }
POOL_PUB=$(solana-keygen pubkey "$POOL_KP")

echo "==> Using RPC: $RPC_URL"
echo "==> User: $USER_PUB"
echo "==> Pool: $POOL_PUB"
echo "==> Mint: $MINT"

echo "==> Funding pool system account if needed"
solana transfer "$POOL_PUB" "$SOL_AMOUNT" -u "$RPC_URL" --allow-unfunded-recipient --with-compute-unit-price 0 >/dev/null

echo "==> Ensuring pool ATA exists and seeding tokens"
spl-token --url "$RPC_URL" create-account "$MINT" --owner "$POOL_PUB" --fee-payer "$USER_KP" >/dev/null 2>&1 || true
spl-token --url "$RPC_URL" transfer "$MINT" "$TOKEN_AMOUNT" "$POOL_PUB" \
  --allow-unfunded-recipient --fund-recipient --fee-payer "$USER_KP" >/dev/null

echo "==> Pool returning tokens to user"
SOLANA_KEYPAIR_SAVE="${SOLANA_KEYPAIR:-}"
export SOLANA_KEYPAIR="$POOL_KP"
spl-token --url "$RPC_URL" transfer "$MINT" "$TOKEN_AMOUNT" "$USER_PUB" --fee-payer "$USER_KP" >/dev/null
if [ -n "$SOLANA_KEYPAIR_SAVE" ]; then
  export SOLANA_KEYPAIR="$SOLANA_KEYPAIR_SAVE"
else
  unset SOLANA_KEYPAIR || true
fi

echo "==> Balances"
echo -n "User SOL: "; solana balance "$USER_PUB" -u "$RPC_URL" || true
echo -n "Pool SOL: "; solana balance "$POOL_PUB" -u "$RPC_URL" || true
echo -n "User token: "; spl-token --url "$RPC_URL" balance "$MINT" --owner "$USER_PUB" || true
echo -n "Pool token: "; spl-token --url "$RPC_URL" balance "$MINT" --owner "$POOL_PUB" || true

echo "==> Done"
