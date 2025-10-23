#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

export ENVIRONMENT=production
export SOLHUNTER_MODE=live
export GOLDEN_PIPELINE=1
export EVENT_BUS_URL="ws://127.0.0.1:8779"
export BROKER_CHANNEL="solhunter-events-v3"
export REDIS_URL="redis://localhost:6379/1"
export PRICE_PROVIDERS="pyth,dexscreener,birdeye,synthetic"
export SEED_TOKENS="So11111111111111111111111111111111111111112,EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9,Es9vMFrzaCERzSi1jS6t4G8iKrrf5gkP8KkP4dDLf2N9"

ARTIFACT_DIR="$ROOT_DIR/artifacts/demo"
mkdir -p "$ARTIFACT_DIR"

pytest -q tests/golden_pipeline/test_golden_demo.py "$@"

echo
echo "Artifacts written to $ARTIFACT_DIR/"
ls -1 "$ARTIFACT_DIR" || echo "(no artifacts found)"
