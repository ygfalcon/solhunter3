#!/usr/bin/env bash
set -euo pipefail

# Live-data devnet dry run with all agents enabled.
# Usage: bash scripts/devnet_dryrun.sh [extra-args]

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG_FILE="$LOG_DIR/devnet_dryrun_${STAMP}.log"

# Environment tuned for safe, portable dry run
export DEPTH_SERVICE=false
export USE_RUST_EXEC=false
export EVENT_BUS_DISABLE_LOCAL=1
export BROKER_URLS=''
export PYTORCH_ENABLE_MPS_FALLBACK=1

# Optional: bypass price API warm-up if needed
: "${OFFLINE_PRICE_DEFAULT:=}"

CONFIG_PATH="$ROOT_DIR/config/devnet.toml"
if [[ ! -f "$CONFIG_PATH" ]]; then
  echo "[WARN] Missing config/devnet.toml; falling back to config.toml" | tee -a "$LOG_FILE"
  CONFIG_PATH="$ROOT_DIR/config.toml"
fi

echo "[INFO] Writing log to $LOG_FILE"
set -x
python -m solhunter_zero.main \
  --config "$CONFIG_PATH" \
  --testnet \
  --dry-run \
  --iterations 1 \
  --portfolio-path "$ROOT_DIR/tmp_portfolio.json" \
  --discovery-method onchain \
  "$@" | tee -a "$LOG_FILE"
set +x

echo "[INFO] Dry run complete. Log: $LOG_FILE"
echo "[INFO] Recent trades summary:"
python "$ROOT_DIR/scripts/summarize_memory.py" --limit 20 || true

