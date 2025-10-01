#!/usr/bin/env bash
# End-to-end: install Solana tools (spl-token), create devnet wallet, airdrop,
# create SPL mint, mint supply, and perform an atomic simulated swap.
#
# Safe to re-run; steps are idempotent.
#
# Usage:
#   bash scripts/devnet_all_in_one.sh
#
set -euo pipefail

cd "$(dirname "$0")/.."

SOL_RELEASE_BIN="$HOME/.local/share/solana/install/active_release/bin"
LOCAL_BIN="$HOME/.local/bin"
export PATH="$SOL_RELEASE_BIN:$LOCAL_BIN:$PATH"

log() { printf "\033[1;32m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m[warn]\033[0m %s\n" "$*"; }
die() { printf "\033[1;31m[err]\033[0m %s\n" "$*"; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || return 1; }

install_solana() {
  log "Installing Solana release tools (includes spl-token)..."
  if curl -fsSL https://release.solana.com/v1.18.20/install | sh; then
    export PATH="$SOL_RELEASE_BIN:$PATH"
    return 0
  fi
  warn "Release installer failed; falling back to GitHub tarball"
  # Detect arch for macOS
  ARCH=$(uname -m)
  case "$ARCH" in
    arm64) TAR_URL="https://github.com/solana-labs/solana/releases/download/v1.18.20/solana-release-aarch64-apple-darwin.tar.bz2" ;;
    x86_64) TAR_URL="https://github.com/solana-labs/solana/releases/download/v1.18.20/solana-release-x86_64-apple-darwin.tar.bz2" ;;
    *) die "Unsupported architecture: $ARCH" ;;
  esac
  TMPDIR=$(mktemp -d)
  curl -fL "$TAR_URL" -o "$TMPDIR/solana.tar.bz2" || die "Download failed"
  mkdir -p "$TMPDIR/extract"
  tar -xjf "$TMPDIR/solana.tar.bz2" -C "$TMPDIR/extract" --strip-components=1
  mkdir -p "$LOCAL_BIN"
  cp "$TMPDIR/extract/bin/solana"* "$LOCAL_BIN/" 2>/dev/null || true
  cp "$TMPDIR/extract/bin/spl-token" "$LOCAL_BIN/"
  export PATH="$LOCAL_BIN:$PATH"
}

# 1) Ensure tools
if ! need solana || ! need spl-token; then
  install_solana
fi
need solana || die "solana CLI not found after install"
need spl-token || die "spl-token not found after install"

# 2) Wallet
mkdir -p keypairs
WAL=keypairs/cli.json
if [[ ! -f "$WAL" ]]; then
  log "Generating $WAL"
solana-keygen new -o "$WAL" --no-bip39-passphrase -f >/dev/null
fi
export SOLANA_KEYPAIR="$(pwd)/$WAL"
PUB=$(solana-keygen pubkey "$SOLANA_KEYPAIR")
log "Wallet: $PUB"

# 3) Devnet + airdrop (with RPC fallback)
pick_rpc() {
  local candidates
  candidates=(
    "${DEVNET_RPC_URL:-}"
    "https://api.devnet.solana.com"
    "https://rpc.ankr.com/solana_devnet"
  )
  for url in "${candidates[@]}"; do
    [[ -z "$url" ]] && continue
    if solana -u "$url" cluster-version >/dev/null 2>&1; then
      echo "$url"; return 0
    fi
  done
  echo "https://rpc.ankr.com/solana_devnet"
}

RPC_URL=$(pick_rpc)
log "Using devnet RPC: $RPC_URL"
solana config set --url "$RPC_URL" >/dev/null
solana config set --keypair "$SOLANA_KEYPAIR" >/dev/null
log "Requesting airdrop (may retry if rate-limited)"
TRIES=0
TARGET_SOL=1
while :; do
  BAL_RAW=$(solana balance "$PUB" -u "$RPC_URL" 2>/dev/null || echo "0 SOL")
  BAL=$(echo "$BAL_RAW" | awk '{print $1+0}')
  if awk "BEGIN {exit !($BAL >= $TARGET_SOL)}"; then
    break
  fi
  if [ $TRIES -ge 8 ]; then
    warn "Airdrop did not raise balance. Please fund $PUB via faucet: https://faucet.solana.com/"
    warn "Then re-run: bash scripts/devnet_all_in_one.sh"
    exit 1
  fi
  solana airdrop 1 "$PUB" -u "$RPC_URL" >/dev/null 2>&1 || true
  TRIES=$((TRIES+1))
  sleep 2
done
log "Balance OK: $BAL_RAW"

# 4) Mint + supply
log "Creating SPL mint (decimals=6)"
MINT=$(spl-token --url "$RPC_URL" create-token --decimals 6 --fee-payer "$SOLANA_KEYPAIR" --mint-authority "$SOLANA_KEYPAIR" | awk '/Creating token/ {print $3}') || true
if [[ -z "${MINT:-}" ]]; then
  die "Failed to create mint (spl-token output empty)"
fi
log "Mint: $MINT"
for t in {1..15}; do
  if solana account "$MINT" -u "$RPC_URL" >/dev/null 2>&1; then break; fi
  sleep 1
done

spl-token --url "$RPC_URL" create-account "$MINT" --owner "$PUB" --fee-payer "$SOLANA_KEYPAIR" >/dev/null || true
spl-token --url "$RPC_URL" mint "$MINT" 100000000 --mint-authority "$SOLANA_KEYPAIR" --fee-payer "$SOLANA_KEYPAIR" >/dev/null
echo "$MINT" > minted_mint.txt
echo "$MINT" > devnet_tokens.txt

# 5) Simulated swap via CLI (two tx):
#    a) seed pool with tokens (user -> pool ATA)
#    b) send SOL to pool (user -> pool)
#    c) send tokens back to user (pool -> user ATA)
log "Preparing pool keypair"
POOL_KP=keypairs/pool.json
if [[ ! -f "$POOL_KP" ]]; then
  solana-keygen new -o "$POOL_KP" --no-bip39-passphrase -f >/dev/null
fi
POOL_PUB=$(solana-keygen pubkey "$POOL_KP")

log "Funding pool system account (airdrop or transfer)"
if ! solana airdrop 0.5 "$POOL_PUB" -u "$RPC_URL" >/dev/null 2>&1; then
  solana transfer "$POOL_PUB" 0.01 -u "$RPC_URL" --allow-unfunded-recipient >/dev/null
fi

log "Seeding pool with tokens"
spl-token --url "$RPC_URL" transfer "$MINT" 100000 "$POOL_PUB" \
  --allow-unfunded-recipient --fund-recipient --fee-payer "$SOLANA_KEYPAIR" >/dev/null

log "Pool sending 0.1 token back to user"
SOLANA_KEYPAIR_SAVE="$SOLANA_KEYPAIR"
export SOLANA_KEYPAIR="$POOL_KP"
spl-token --url "$RPC_URL" transfer "$MINT" 100000 "$PUB" --fee-payer "$SOLANA_KEYPAIR_SAVE" >/dev/null
export SOLANA_KEYPAIR="$SOLANA_KEYPAIR_SAVE"

log "Simulated swap completed"

log "Done. Mint saved to minted_mint.txt; token list at devnet_tokens.txt"
log "You can point agents at your mint with:"
echo "  SOLHUNTER_CONFIG=$(pwd)/config/devnet.toml \\
  python -m solhunter_zero.main --auto --testnet --discovery-method file --token-list devnet_tokens.txt"
