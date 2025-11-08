# Quick Start

 - A default keypair (`keypairs/default.json`) and configuration (`config.toml`) are bundled for immediate runs.
 - To customize, copy `config/default.toml` to `config.toml` and edit the values.
- `start_live --config configs/live_recommended.toml` runs the event-driven orchestrator, forcing the new pipeline (`NEW_PIPELINE=1`) and is the default live entry point.
- `make start` runs `scripts/startup.py` for guided setup and launches `depth_service` automatically.
- Use `solhunter-start` to launch the same startup routine with `--one-click` by default while still accepting additional flags.
- Pass `--min-delay` or `--max-delay` to enforce minimum or maximum delay between trade iterations when running the bot.
- Run `python -m solhunter_zero.launcher --auto` for a fully automated launch. On macOS, double-click `start.command` for the same effect. It ensures the `solhunter-wallet` CLI is present, auto-selects the sole keypair and active config, verifies RPC endpoints, and warns if the wallet balance is below `min_portfolio_value`.
- `scripts/quick_setup.py --auto` populates `config.toml` with defaults. Use `--non-interactive` to rely on environment variables only. Set `AUTO_SELECT_KEYPAIR=1` to have the sole keypair chosen without prompts.
- Setup scripts write the `.env` file to your current working directory, keeping environment settings alongside the source tree.
- `EVENT_BUS_URL` sets a single websocket broker. `BROKER_WS_URLS` accepts a
  comma‑separated list for clustering. If `BROKER_WS_URLS` is unset,
  `EVENT_BUS_URL` is used. Both default to `ws://127.0.0.1:8769`.
- On macOS, run `scripts/mac_setup.py` to install the Xcode command line tools if needed. The script exits after starting the installation; rerun it once the tools are installed before continuing.
- Launch the Web UI with `python -m solhunter_zero.ui`.
- Toggle **Full Auto Mode** in the UI to start trading with the active config.
- Or start everything at once with `python scripts/start_all.py` (includes `depth_service`).
- Programmatic consumers can call `solhunter_zero.bootstrap.bootstrap()` to
  perform the same setup steps before interacting with the library.

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

## Devnet Demo (Live Signing on Testnet)

- Use the dedicated devnet config and launcher to exercise discovery and sign real devnet swaps without touching mainnet:

  - Fund the active wallet on devnet:
    - If you have Solana CLI: `solana config set --url https://devnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d && solana airdrop 2` (re-run until balance ≥ 1 SOL)
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
  - Ensure `SOLANA_TESTNET_RPC_URL` points to `https://devnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d`.

## Live Token Discovery

Enable live discovery of freshly minted tokens from the mempool so agents can react immediately:

```bash
export SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d
python -m solhunter_zero.main --auto --live-discovery
```

You can also toggle via environment: set `LIVE_DISCOVERY=1`.

Optional environment variables:
- `MEMPOOL_SCORE_THRESHOLD`: minimum signal score to act on
- `TOKEN_BLACKLIST`: comma-separated list of mints to ignore
