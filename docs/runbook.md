# Operations Runbook

This runbook is designed so that a new operator can take SolHunter Zero from zero to live in a
controlled manner.  All commands are executed from the repository root unless noted.

## 1. Environment Doctor

```bash
./env_doctor.sh
```

The script validates required environment variables, connectivity to Solana RPC/WS endpoints, the
Helius API, wallet keypaths, and Redis reachability.  The command exits non-zero on failure and is
safe to run repeatedly.

## 2. Launch Order

1. Ensure Redis and external dependencies are running.
2. Export the desired environment file (`.env.staging` or `.env.prod`).
3. Run the startup launcher:
   ```bash
   poetry run python -m scripts.startup --non-interactive
   ```
   The launcher blocks until Redis, DAS, and the RL service health endpoints are reachable before
   bringing components online in the following order:
   Discovery → Enrichment → Tape/Depth → Snapshotter → Agents → Vote → Shadow Executor → UI.
4. Inspect `start_all.out` for the dependency checkmarks before confirming startup.

## 3. Bus Smoke Tests

After startup, validate the event bus.

```bash
poetry run python -m scripts.bus_smoke ping
poetry run python -m scripts.bus_smoke keys
```

Both commands must succeed.  Failures indicate Redis is not writable/readable or TTL semantics are
not applied correctly.

## 4. End-to-End Drill

Trigger the automated drill once discovery and enrichment are producing data.

```bash
poetry run python -m scripts.preflight --drill
```

The drill populates `preflight.json` with a stage-by-stage checklist.  Promotion is blocked until
each section passes with recorded latency.

## 5. UI Verification

Open the dashboard and confirm that each panel reports fresh data and no red staleness badges.  Use
the topic map in `docs/ui_topic_map.md` to cross-reference stream subscriptions.

## 6. Kill Switches

Kill switches are exposed in the UI and backed by persisted configuration flags:

- **Global Pause:** stops vote to execution propagation.
- **Paper Mode:** keeps the live executor offline.
- **Family Budgets:** slider per agent family that propagates within a vote window.
- **Spread/Depth Gates:** updates `MAX_SPREAD_BPS` and `MIN_DEPTH1PCT_USD` live.
- **RL Toggle:** flips between shadow and applied weights (`RL_WEIGHTS_DISABLED`).
- **Blacklist / Cooldown:** edit list of paused mints; persists to Redis and config storage.

All toggles must apply within a single vote window (<400ms) or the deployment is rolled back.

## 7. Pre-flight Suite

Run the automated suite prior to any promotion:

```bash
poetry run python -m scripts.preflight
```

Review `preflight.json` and the console output.  Any failure is a no-go; file an incident in the
tracking board before retrying.

## 8. Go / No-Go Checklist

Proceed to live only when the following are green:

- Environment doctor and bus smoke tests.
- Discovery → Golden pipeline, with no stale panels.
- Paper PnL stable and shadow RL uplift non-negative.
- Pre-flight suite passes and `docs/preflight.json` stored for audit.

## 9. Live Toggle Procedure

1. Set guardrails to conservative defaults (`MAX_SPREAD_BPS=80`, `MIN_DEPTH1PCT_USD=8000`).
2. Toggle **Paper-only** off to allow live executor, but keep notional caps tiny (≤ $25/order,
   $200/day) using budget sliders.
3. Monitor quote→fill latency, slippage, and UI alerts for one hour.
4. If any SLO is breached, hit **Global Pause**, re-enable paper-only, and investigate.
5. If stable for 24–48 hours, double notional caps incrementally.

## 10. Incident Response

- Red tile / alert on the dashboard → consult the corresponding Grafana panel.
- If Redis or DAS becomes unavailable, pause the pipeline via the global switch.
- Document remediation in the ops log and backfill discovery cursors using the DAS cursor key.

## 11. Shutdown

```bash
poetry run python -m scripts.startup --stop
```

Verify all components exit cleanly and that Redis keys are persisted for the next run.
