# Quick Start

## Canonical runtime launch

1. Copy `etc/solhunter/env.production` and fill in the three required API keys.
   The file is wired for the shared event bus (`ws://127.0.0.1:8779`), Redis
   (`redis://localhost:6379/1`), and the production Solana providers.  Seed
   tokens (SOL, USDC, USDT) are preloaded with their Pyth price IDs so depth and
   prices hydrate immediately.

2. From the repository root, run the golden-path launcher.  It provisions
   `.venv/`, installs runtime and test dependencies, pins the JSON schema
   validator, performs Redis and event-bus sanity checks, runs the golden schema
   smoke tests, and then boots the paper runtime followed by live mode:

   ```bash
   source .venv/bin/activate && \
   bash scripts/launch_live.sh \
     --env etc/solhunter/env.production \
     --micro 1 --canary --budget 25 --risk 0.25 \
     --preflight 2 --soak 180
   ```

   The launcher prints a consolidated manifest such as
   `RUNTIME_MANIFEST channel=solhunter-events-v3 redis=redis://localhost:6379/1 …`
   so you can confirm every component shares the same broker.  If any Redis URL
   or broker channel diverges, the launcher aborts with guidance on which
   environment variables to align.

3. During startup the console will surface the readiness handshake in order:

   - `UI_READY url=http://127.0.0.1:XXXX …`
   - `Event bus: connected redis broker redis://localhost:6379/1 channel=solhunter-events-v3`
   - `runtime.stage_changed stage=wallet:balance ok=true …` (live wallet verified before trading)
   - `GOLDEN_READY topic=x:mint.golden providers=<n>`
   - `RUNTIME_READY mode=paper` / `RUNTIME_READY mode=live`

   As soon as all lines appear the launcher issues a `GET /ui/meta` request to
   verify the UI is serving and prints a bold banner with the URL.

4. To stop the runtime without disturbing Redis, run `bash scripts/stop.sh`.
   Use `bash scripts/clean_session.sh` to remove the previous session’s
   `artifacts/prelaunch/` data if you want a pristine restart.  Both scripts are
   idempotent and safe to run multiple times.

The UI will display **Event Bus: connected**, **Trading Loop: running**, and the
lag metrics as soon as the pipeline warms up.  Golden snapshots begin with the
seed tokens and expand as discovery finds new candidates.

## Investor Demo

For an instant teaser covering all strategies, double-click `demo.command` (or
run `python demo.py`). This executes the demo with the full preset and writes
summary files to the default `reports/` directory.

Run a small rolling backtest and produce reports:

```bash
python demo.py --reports reports
```

This writes `summary.json`, `trade_history.csv` and `highlights.json` to the
specified directory and prints brief snippets to the console.

Enable a lightweight reinforcement‑learning stub:

```bash
python demo.py --rl-demo --reports reports
```

Exercise the full system with heavier dependencies:

```bash
python demo.py --full-system --reports reports
```

All modes emit the same report files and console snippets. The `reports/`
directory is ignored by Git so these generated files remain local.

## Run a Golden Demo (offline)

Validate the discovery → golden snapshot → agent suggestion flow without any
external network calls by running the synthetic demo harness:

```bash
bash scripts/run_golden_demo.sh
```

The script forces paper-mode defaults, executes
`tests/golden_pipeline/test_golden_demo.py`, and writes JSONL plus a compact
markdown summary under `artifacts/demo/`. Inspect
`artifacts/demo/summary.md` to confirm counts, medians, and the top
suggestions before promoting a new environment to live trading.

## Paper Trading Simulation

Run the investor demo against live prices or bundled presets:

```bash
python paper.py --reports reports --url https://example.com/prices.json
```

Omit ``--url`` or supply ``--preset`` to use packaged datasets offline.

## Strategy Showcase Test

Run the default strategies against a fixed price feed:

```bash
pytest tests/staging/test_investor_showcase.py
```

## Troubleshooting Preflight Checks

- **RPC unreachable** — ensure `SOLANA_RPC_URL` points to a healthy endpoint and that your network allows outbound requests.
- **Wallet balance too low** — fund the default keypair or lower `min_portfolio_value` in `config.toml`.
- **Event bus port busy** — the launcher now exits if `ws://127.0.0.1:8779` is still claimed. Stop the previous run (for example,
  `pkill -f event_bus`) and run `bash scripts/clean_session.sh` to clear stale locks before restarting.

## Devnet Demo (Live Signing on Testnet)

- Use the dedicated devnet config and launcher to exercise discovery and sign real devnet swaps without touching mainnet:

  - Fund the active wallet on devnet:
    - If you have Solana CLI: `solana config set --url https://devnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY && solana airdrop 2` (re-run until balance ≥ 1 SOL)
    - Or fund the public key from a faucet. To print the bot wallet pubkey: `python -m solhunter_zero.wallet_cli list` then `python -m solhunter_zero.wallet show` (or open `keypairs/default.json`).

  - Launch on devnet (macOS): double‑click `devnet.command`.
    - This runs `python -m solhunter_zero.main --auto --testnet` with `config/devnet.toml`.
    - Agents discover tokens, build routes, and sign/broadcast transactions to the devnet cluster.

  - Launch via CLI (any OS):
    ```bash
    SOLHUNTER_CONFIG=$(pwd)/config/devnet.toml \
    USE_MEV_BUNDLES=0 \
    python -m solhunter_zero.main --auto --testnet --min-delay 10 --max-delay 120
    ```

- Notes
  - Jito/MEV is disabled in `config/devnet.toml`.
  - `AUTO_SELECT_KEYPAIR=1` is honored when there’s a single key in `keypairs/`.
  - Ensure `SOLANA_TESTNET_RPC_URL` points to `https://devnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY`.

## Live Token Discovery

Enable live discovery of freshly minted tokens from the mempool so agents can react immediately:

```bash
export SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY
python -m solhunter_zero.main --auto --live-discovery
```

You can also toggle via environment: set `LIVE_DISCOVERY=1`.

Optional environment variables:
- `MEMPOOL_SCORE_THRESHOLD`: minimum signal score to act on
- `TOKEN_BLACKLIST`: comma-separated list of mints to ignore