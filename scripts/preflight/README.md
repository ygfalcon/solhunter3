# Preflight Smoke Suite

This directory contains a drop-in preflight harness that exercises the live Solhunter stack via Redis, Websocket streams, and Solana RPC.  Each script is designed to be idempotent and to leave the stack unchanged beyond ephemeral stream messages.

## Scripts

- `env_doctor.sh` – Verifies required CLI tooling is present, the key environment variables are exported, and that Solana RPC, Helius DAS, and Redis are reachable.
- `bus_smoke.sh` – Smoke tests Redis stream append/read behaviour as well as basic key-value TTL semantics used by the swarm.
- `preflight_smoke.sh` – Runs the end-to-end checklist: golden mint emission, suggestion→decision idempotency, virtual fills, must-exit reflex, and micro-mode liquidity gates.
- `run_all.sh` – Convenience wrapper that executes the scripts in the correct order.

## Usage

1. Ensure your paper stack is running and export the staging-safe environment described in the preflight plan:

   ```bash
   export MODE=paper
   export NEW_DAS_DISCOVERY=1
   export EXIT_FEATURES_ON=1
   export RL_WEIGHTS_DISABLED=1
   export MICRO_MODE=1           # toggle between 1 and 0 to cover both modes
   export REDIS_URL=redis://127.0.0.1:6379/0
   export SOLANA_RPC_URL=...     # your RPC endpoint
   export HELIUS_API_KEY=...     # for DAS
   ```

2. From anywhere inside the repo, run:

   ```bash
   scripts/preflight/run_all.sh
   ```

   or invoke the individual scripts if you only need a specific check.

3. Repeat the `preflight_smoke.sh` run with `MICRO_MODE=0` and `MICRO_MODE=1` to confirm both gating modes behave as expected before going live.

All scripts emit green (`PASS`) / red (`FAIL`) signals and exit non-zero on failure, making them suitable for CI hooks or manual validation prior to enabling live trading.
