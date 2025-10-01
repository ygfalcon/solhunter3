#!/usr/bin/env bash
# Create a devnet SPL token mint, mint supply to your wallet, and print details.
#
# Requirements:
# - solana CLI and spl-token CLI installed and on PATH
# - KEYPAIR_PATH set to your JSON keypair (defaults to keypairs/default.json)
#
# Usage:
#   KEYPAIR_PATH=keypairs/default.json scripts/devnet_spl_mint.sh
#   # or just run without vars if defaults are fine

set -euo pipefail

cd "$(dirname "$0")/.."

KEYPAIR_PATH=${KEYPAIR_PATH:-keypairs/default.json}
if [[ ! -f "$KEYPAIR_PATH" ]]; then
  echo "KEYPAIR_PATH not found: $KEYPAIR_PATH" >&2
  exit 1
fi

export SOLANA_KEYPAIR="$KEYPAIR_PATH"

echo "==> Setting Solana cluster to devnet"
solana config set --url https://api.devnet.solana.com >/dev/null

echo "==> Airdropping devnet SOL if needed"
BAL=$(solana balance | awk '{print $1}' || echo 0)
NEEDED=0.5
awk "BEGIN {exit !($BAL < $NEEDED)}" || NEEDED=0
if [[ $NEEDED != 0 ]]; then
  solana airdrop 1 >/dev/null || true
fi
solana balance

DECIMALS=${DECIMALS:-6}
MINT_AMOUNT=${MINT_AMOUNT:-100000000}  # 100 tokens when DECIMALS=6

echo "==> Creating new SPL token mint (decimals=$DECIMALS)"
MINT=$(spl-token create-token --decimals "$DECIMALS" | awk '/Creating token/ {print $3}')
if [[ -z "$MINT" ]]; then
  echo "Failed to parse mint address from spl-token output" >&2
  exit 1
fi
echo "Mint: $MINT"

echo "==> Creating associated token account"
ATA=$(spl-token create-account "$MINT" | awk '/Creating account/ {print $3}')
echo "ATA: $ATA"

echo "==> Minting supply to wallet"
spl-token mint "$MINT" "$MINT_AMOUNT"

spl-token balance "$MINT" || true

echo "$MINT" > minted_mint.txt
echo "==> Saved mint address to minted_mint.txt"

echo "==> (Optional) Wrap 0.1 SOL to get WSOL"
if spl-token wrap 0.1 >/dev/null 2>&1; then
  echo "Wrapped 0.1 SOL into WSOL ATA"
else
  echo "Skipping wrap (command not available or failed)"
fi

echo "==> Done. Mint: $MINT"
echo "Use this in a token list for discovery:"
echo "  echo $MINT > devnet_tokens.txt"
echo "  SOLHUNTER_CONFIG=$(pwd)/config/devnet.toml \\
       python -m solhunter_zero.main --auto --testnet --discovery-method file --token-list devnet_tokens.txt"

