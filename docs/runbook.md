# Operations Runbook

This runbook is designed so that a new operator can take SolHunter Zero from zero to live in a
controlled manner.  All commands are executed from the repository root unless noted.

> **Generated chaos guidance:** Failure drills captured by the test suite are summarised in the
> [Chaos Remediation Guide](runbook_generated.md). Refresh the guide with
> `python scripts/generate_chaos_runbook.py` whenever new remediation artefacts are recorded.

## 0. Populate Environment Secrets

Before touching any automation, ensure the production environment file is
complete. Copy `etc/solhunter/env.production` to the host path that will be
mounted in production (or edit it in place) and replace **every** placeholder
string with the live credential or endpoint. All of the following keys must be
populated with production-grade values:

| Credential bucket | Environment keys that require live secrets |
| --- | --- |
| Solana RPC/Websocket access | `SOLANA_RPC_URL`, `SOLANA_WS_URL`, `SOLANA_KEYPAIR`, `KEYPAIR_PATH` |
| Helius (RPC, price service, and auth) | `HELIUS_API_KEY`, `HELIUS_API_KEYS`, `HELIUS_API_TOKEN`, `HELIUS_RPC_URL`, `HELIUS_WS_URL`, `HELIUS_PRICE_RPC_URL`, `HELIUS_PRICE_REST_URL`, `HELIUS_PRICE_BASE_URL` |
| Market data & quoting partners | `BIRDEYE_API_KEY`, `SOLSCAN_API_KEY`, `DEX_BASE_URL`, `DEX_TESTNET_URL`, `ORCA_API_URL`, `RAYDIUM_API_URL`, `PHOENIX_API_URL`, `METEORA_API_URL`, `JUPITER_WS_URL` |
| Persistence and bus connectivity | `REDIS_URL`, `EVENT_BUS_URL` |
| Jito bundle submission | `JITO_RPC_URL`, `JITO_AUTH`, `JITO_WS_URL`, `JITO_WS_AUTH` |
| Notification/alerting hooks | `NEWS_FEEDS`, `TWITTER_FEEDS`, `DISCORD_FEEDS` |

If your production deployment relies on additional third-party providers,
include their credentials in the environment file as well. Audit the template
for any entry that contains `YOUR_`, `REDACTED`, `XXXX`, empty strings, or other
obvious placeholders and replace them with secrets from your vault.

For the funded production wallet distributed with this deployment, set both
`SOLANA_KEYPAIR` and `KEYPAIR_PATH` to `/workspace/solhunter3/keypairs/default.json`
so the launcher inherits the correct signing key.

Run the placeholder audit to confirm nothing was missed:

```bash
make audit-placeholders ARGS="etc/solhunter/env.production"
```

Only proceed once the audit passes and the file is backed by a secure secret
store.

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

### First live test command

Once the go/no-go checklist is green, run the guarded launcher to execute the first
live test with micro-sizing enabled. Replace the sample paths with the environment
file and configuration you intend to promote.

```bash
bash scripts/launch_live.sh \
  --env etc/solhunter/env.production \
  --micro 1 \
  --canary \
  --budget 25 \
  --risk 0.25 \
  --preflight 2 \
  --soak 300 \
  --config config.toml
```

The script performs two full preflight passes (micro on/off), validates environment
secrets, ensures Redis is reachable, and starts both paper and live runtime controllers
before flipping the live executor on. Watch `artifacts/prelaunch/logs/live_runtime.log`
for the `RUNTIME_READY` marker and the console summary before lifting notional caps.

Reviewers can reference `scripts/launch_live.sh` for the full argument contract and
verify the command above matches the required flags.

> **Note:** The launcher hard-fails if any environment variables still contain
> template placeholders (for example `SOLANA_RPC_URL=YOUR_RPC_URL`). Copy the
> template file, replace every placeholder with production credentials, and rerun
> `./env_doctor.sh` before invoking the live launcher.

## 10. Incident Response

- Red tile / alert on the dashboard → consult the corresponding Grafana panel.
- If Redis or DAS becomes unavailable, pause the pipeline via the global switch.
- Document remediation in the ops log and backfill discovery cursors using the DAS cursor key.

## 11. Shutdown

```bash
poetry run python -m scripts.startup --stop
```

Verify all components exit cleanly and that Redis keys are persisted for the next run.
