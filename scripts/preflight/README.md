# Preflight Smoke Suite

This directory contains a drop-in preflight harness that exercises the live Solhunter stack via Redis, Websocket streams, and Solana RPC. Each script is designed to be idempotent and now cleans up its own Redis mutations after each probe. Every run emits a timestamped JSON audit artifact under `artifacts/preflight/` (configurable via `PREFLIGHT_AUDIT_DIR`) so operators and CI can ingest structured results.

## Scripts

- `env_doctor.sh` – Verifies required CLI tooling is present, the key environment variables are exported, and that Solana RPC, Helius DAS, Redis, and the optional UI `/healthz` endpoint are reachable. A machine-readable summary of every check is written alongside the console output.
- `bus_smoke.sh` – Smoke tests Redis stream append/read behaviour and key-value TTL semantics while leaving the bus tidy. Results are captured as JSON.
- `preflight_smoke.sh` – Runs the end-to-end checklist: golden mint emission, suggestion→decision idempotency (including duplicate flood protection), virtual fills, must-exit reflex, micro-mode liquidity gates, latency SLO enforcement, and paper PnL validation. All synthetic Redis writes are deleted at exit and a rich audit payload (hashes, latencies, gating status, etc.) is produced.
- `run_all.sh` – Orchestrates the full suite, running `env_doctor` and `bus_smoke` once and `preflight_smoke` twice (with `MICRO_MODE=1` and `MICRO_MODE=0`). A JSON history is appended to `artifacts/preflight/history.jsonl` on each invocation and a readiness marker is stamped once two spaced green runs are observed.

## Usage

1. Ensure your paper stack is running and export the staging-safe environment described in the preflight plan:

   ```bash
   export MODE=paper
   export NEW_DAS_DISCOVERY=1
   export EXIT_FEATURES_ON=1
   export RL_WEIGHTS_DISABLED=1
   export MICRO_MODE=1           # toggle between 1 and 0 to cover both modes
   export REDIS_URL=redis://127.0.0.1:6379/1
   export SOLANA_RPC_URL=...     # your RPC endpoint
   export HELIUS_API_KEY=...     # for DAS
   ```

2. Run the automated dual-mode suite:

   ```bash
   scripts/preflight/run_all.sh
   ```

   This command:

   - Executes `env_doctor.sh` and `bus_smoke.sh` once, capturing `*.json` audit summaries.
   - Runs `preflight_smoke.sh` with `MICRO_MODE=1` and `MICRO_MODE=0`, storing console logs as `artifacts/preflight/preflight_micro_{on,off}_*.log` and JSON results as `preflight_smoke_*.json`.
   - Appends a history entry to `artifacts/preflight/history.jsonl` that tracks the commit, per-script status, and artifact paths.
   - Updates `artifacts/preflight/READY` once two passing runs are recorded at least `PREFLIGHT_READYNESS_SPACING_SEC` seconds apart (default 10 minutes). Delete this marker or re-run the suite if a later run fails.

3. Inspect the emitted JSON artifacts to confirm latencies, duplicate suppression, and paper PnL alignment. Each record contains the measured SLOs, hashes, and gating decisions so you can diff runs over time.

All scripts emit green (`PASS`) / red (`FAIL`) signals and exit non-zero on failure, making them suitable for CI hooks or manual validation prior to enabling live trading. The JSON artifacts can be forwarded to dashboards, on-call bots, or GitHub status checks for automated gating.
