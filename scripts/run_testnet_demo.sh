#!/usr/bin/env bash
# Launch UI + agents on testnet with a non-conflicting event bus.
# - Starts UI (port 5000) and local WS bus on 8872
# - Starts agents with --testnet using config/devnet.toml
# - Writes PIDs and logs to repo root

set -euo pipefail
cd "$(dirname "$0")/.."

# Ensure Solana CLIs on PATH for any helper flows
export PATH="$HOME/.local/share/solana/install/active_release/bin:$HOME/.local/bin:$PATH"

EVENT_BUS_URL="ws://127.0.0.1:8872"
export EVENT_BUS_URL
export PYTORCH_ENABLE_MPS_FALLBACK=1
export SOLHUNTER_CONFIG="$(pwd)/config/devnet.toml"

# Start UI if not already running
if ! lsof -iTCP:5000 -sTCP:LISTEN -Pn >/dev/null 2>&1; then
  echo "Starting UI on http://127.0.0.1:5000 (bus: $EVENT_BUS_URL)"
  nohup python -m solhunter_zero.ui > ui_demo.log 2>&1 & echo $! > ui_demo.pid
else
  echo "UI port 5000 already in use; skipping UI start"
fi

# Small delay so the bus/UI can spin up
sleep 2

# Start agents (continuous)
echo "Starting agents on testnet (using $SOLHUNTER_CONFIG)"
nohup python -m solhunter_zero.main --auto --testnet > demo_agents.log 2>&1 & echo $! > demo_agents.pid

echo "\nDemo running:" 
echo "  UI:    http://127.0.0.1:5000 (PID $(cat ui_demo.pid 2>/dev/null || echo -))"
echo "  Agents PID: $(cat demo_agents.pid 2>/dev/null || echo -)"
echo "  Logs: ui_demo.log, demo_agents.log"
echo "  Event bus: $EVENT_BUS_URL"

