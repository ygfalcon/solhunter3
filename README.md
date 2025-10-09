# SolHunter Zero

SolHunter Zero is an autonomous AI-driven trading bot for the Solana blockchain.
See [QUICK_START.md](QUICK_START.md) for a condensed setup guide.

This project is targeted towards being the greatest Solana bot ever created and built by a mad genius known only as zero

## Current Status & Next Steps (Sep 21 2025)

- ✅ `scripts/start_all.py` now wires the Redis→RL health pipeline: it auto-starts Redis, polls the RL health endpoint with port rollover, and records broker state in `redis_server.log` before unblocking the rest of the stack.
- ✅ `scripts/run_rl_daemon.py` runs GPU-backed DQN+PPO workers behind the new `RLHealth` HTTP server (`/health`), publishes heartbeats/weights, and keeps GPU learning always on.
- ⚠️ RL daemon logs still spam `coroutine … never awaited`, TorchScript export failures, and Redis reconnect loops whenever the broker/event bus comes up late; training completes but health flaps and weights don’t broadcast reliably.
- ⚠️ Event bus/websocket stack is noisy (invalid websocket URL fallback, port 8769 collisions, Redis `no message received`), leaving stray tasks on shutdown and tripping the `--auto` launcher.
- ⚠️ Offline data sync scheduler continues to raise TaskGroup/network errors and leaves `offline_data.db` stale when offline mode is toggled.
- ⚠️ `python -m solhunter_zero.main --auto` still crashes on exit (`Event loop is closed`, lingering websocket tasks) and spews DEX metric errors when offline; needs cleanup or a lighter entrypoint.

Pick up from here to tighten the runtime loop, quiet the websocket subsystem, and harden the auto launcher.

For a quick preview across all bundled strategies, double-click `demo.command`
(or run `python demo.py`) to generate sample reports without additional
setup. The sample metrics in [docs/demo_full_sample.md](docs/demo_full_sample.md)
show the expected output, and the integration test
[`tests/test_demo_full_preset_output.py`](tests/test_demo_full_preset_output.py)
verifies that the demo produces those values.

## Unified Startup Pipeline

`python scripts/start_all.py` now drives a single, auditable startup flow. The
script builds a `StartupPipeline` that walks through four ordered stages:

1. **prepare-environment** — loads `.env`, ensures the virtualenv, resolves the
   active config/keypair, normalises env vars, and downgrades risky options
   (for example disabling flash loans when protocol details are missing).
2. **ui-selftest** — runs the UI preflight health gate so configuration or
   dependency issues surface before any services spawn.
3. **launch-services** — starts Redis (auto-spawned when localhost brokers are
   requested), the Rust depth service, the RL daemon, and the trading runtime.
   The RL daemon exposes a local health check (`RL_HEALTH_URL`, default
   `http://127.0.0.1:7070/health`); startup now waits for this endpoint before
   progressing, so issues surface immediately.
4. **launch-ui** — spins up websocket threads, optional HTTP server, and records
   the final port.

Each stage is logged to `startup.log` and echoed to the console at exit as a
succinct checklist (OK/FAIL with timing). Re-running the command reuses the
prepared context, and `SOLHUNTER_OFFLINE=1` keeps the pipeline paper-safe while
still exercising every gate.

## Primary Entry Point (New Runtime)

This repo now has a single, obvious entry for the new, event‑driven runtime.

- Quick start (preferred):
  - `python -m solhunter_zero.primary_entry_point --config configs/live_recommended.toml`
  - This sets `NEW_RUNTIME=1` and `EVENT_DRIVEN=1` and runs the orchestrator.

- Top‑level marker for discoverability:
  - See `PRIMARY_ENTRYPOINT` (in repo root) for the exact command.

- Flow (sequential):
  1. Start UI (passive, shows status, emits controls)
  2. Start Event Bus (brokers + local WS)
  3. Start Agents
     - Discovery publishes `token_discovered`
     - Agents evaluate and publish `action_decision` (includes price/size)
     - Executor subscribes and trades, logging to `Memory` and updating `Portfolio`
     - RL trains in background and publishes `rl_weights`; AgentManager continuously evolves (mutates/culls) based on success

- Observability (key topics/endpoints):
  - Bus topics: `runtime.stage_changed`, `action_decision`, `decision_summary`, `decision_metrics`, `rl_weights`, `trade_logged`
  - UI endpoints: `/runtime/stages`, `/decisions/recent`, `/metrics/decision`, `/status`
  - Verbose stages: `ORCH_VERBOSE=1` or `--verbose-stages`

Legacy path remains available via `SOLHUNTER_LEGACY=1` when using `python -m solhunter_zero.launcher`.

## Chain Readiness Toolkit

- `./env_doctor.sh` — one-line environment doctor that validates required variables, probes Solana
  RPC/WS endpoints, verifies Redis reachability, and checks Helius authentication.
- `python -m scripts.bus_smoke <ping|keys>` — Redis smoke tool that appends/reads test messages
  across the required streams and key TTLs.
- `.env.example`, `.env.staging`, `.env.prod` — canonical environment baselines for local, staging,
  and production deployments.
- `docs/ui_topic_map.md` — authoritative UI wiring map with staleness badges and endpoints.
- `docs/runbook.md` — end-to-end operational guide including launch order, kill switches, and go/no-go
  criteria.
- `docs/data_contracts.md` — machine-readable schemas for stream and key-value payloads.

### Live Ops Prep (Recommended)

- Config: Start from `configs/live_recommended.toml` and tune risk/sizing to taste.
- Broker: Local Redis is recommended; the orchestrator auto-starts `redis-server` when `broker_urls` include localhost but no server is reachable.
- Wallet: Default keypair selection is used if already configured.
- RPC: Default to the public Solana mainnet RPC. Override as needed in the config.
- Verbose stages: `ORCH_VERBOSE=1` or `--verbose-stages` toggles detailed stage logs.

## Table of Contents

- [Setup](docs/setup.md)
- [Configuration](#configuration)
- [Usage](docs/usage.md)
- [Architecture](docs/architecture.md)
- [Event Pipeline Blueprint](docs/blueprint.md)
- [Environment Variables](docs/environment.md)
- [Testing](TESTING.md)

## Primary User Preferences

The default workflow is intentionally simple:

1. Send SOL to the desired wallet. A default keypair (`keypairs/default.json`) **and** configuration (`config.toml`) are bundled for out-of-the-box runs and can be funded directly.
2. Run `python -m solhunter_zero.launcher` from any directory for a fully
   automated launch. The launcher resolves its own location, adjusts the
   working directory and `sys.path`, and delegates to
   `solhunter_zero.launcher.main`. On macOS double-click `start.command` for a
   one-click start.
   The launcher auto-selects the sole keypair and active configuration,
   validates RPC endpoints, and warns if the wallet balance is below
   `min_portfolio_value`.
   All startup output is also appended to `startup.log` in the project directory for later inspection.
   Output from environment preflight checks is written to `preflight.log`, which is truncated
   before each run and rotated to `preflight.log.1` once it exceeds 1 MB so the previous run
   remains available.
   Preflight also verifies that the compiled artifacts `solhunter_zero/libroute_ffi.*`
   and `target/release/depth_service` are present before startup.
   A machine-readable diagnostics summary is written to `diagnostics.json` after the bot exits
   unless `--no-diagnostics` is supplied.
3. Load the keypair in the SolHunter GUI if running manually, then press **Start**.

### UI-first preflight (fastest way to surface config/deps)
```bash
python -m solhunter_zero.ui --selftest
```
This runs the same checks the full orchestrator will rely on, but isolated—so failures are clean and actionable.

### Full-stack smoke (paper-safe)
Run the on-demand smoke locally:
```bash
USE_REDIS=1 CHECK_UI_HEALTH=1 RL_HEALTH_URL=http://127.0.0.1:7070/health \
SELFTEST_SKIP_ARTIFACTS=1 CI=true \
python scripts/smoke_fullstack.py
```

### Runtime Soak (on-demand)
Prove the whole system keeps rolling in paper mode:
```bash
RL_HEALTH_URL=http://127.0.0.1:7070/health \
CHECK_UI_HEALTH=1 UI_HEALTH_URL=http://127.0.0.1:3000/healthz \
USE_REDIS=1 EVENT_BUS_URL=redis://127.0.0.1:6379/0 \
SELFTEST_SKIP_ARTIFACTS=1 CI=true DURATION_SEC=180 \
python scripts/soak_runtime.py
```
Or trigger **Runtime Soak (on-demand)** in GitHub Actions and set inputs.

### E2E in GitHub Actions (paper-safe)
An on-demand workflow **E2E — All Systems Go** spins up Redis + mock RL + mock UI, runs the UI self-test, the full-stack smoke, and `start_all.py` (exits after gates). Trigger it from the Actions tab or push to any branch.

Use `--min-delay` or `--max-delay` from the CLI to bound the delay between trade iterations during manual runs.

The mandatory Rust `depth_service` is already enabled and starts automatically, so no extra step is required. All optional agents are enabled by default and wallet selection is always manual. Offline data (around two to three days of history, capped at 50 GB by default) downloads automatically. Set `OFFLINE_DATA_LIMIT_GB` to adjust the size limit. The bot begins with an initial $20 balance linked to [`min_portfolio_value`](#minimum-portfolio-value).
Control how often snapshots and trades are flushed to disk with `OFFLINE_BATCH_SIZE` and `OFFLINE_FLUSH_INTERVAL`.
`OFFLINE_FLUSH_MAX_BATCH` caps the number of entries written per transaction.
Executemany batching yields roughly 25–30% faster logging.
Trade logs use the same mechanism via `MEMORY_BATCH_SIZE` and `MEMORY_FLUSH_INTERVAL`.

**Suggested hardware upgrades for future expansion**

- Increase RAM to 32 GB or more.
- Use an external SSD for larger datasets.
- Consider a workstation-grade GPU for model training.

## Setup
See [docs/setup.md](docs/setup.md) for comprehensive setup instructions, including quick start, macOS details, Docker Compose, environment configuration, paper trading, and the Rust depth service.

### Redis
The default event bus uses Redis for messaging. Install Redis and ensure it is running before launching the application:

```bash
sudo apt-get install redis-server  # Debian/Ubuntu
redis-server &
```

If you prefer to use a remote instance, set the `EVENT_BUS_URL` environment variable to its address.

## Configuration

The bot reads its settings from `config.toml`. Ensure the following keys are defined before starting:

- `solana_rpc_url` – RPC endpoint for Solana
- `dex_base_url` – base URL of the DEX API
- `agents` – list of agent names to enable
- `agent_weights` – table mapping each agent to a weight
- `decision_thresholds` – optional table keyed by regime that tunes buy
  thresholds (success probability, ROI, Sharpe, liquidity floor, gas cost).
  When omitted the runtime falls back to `min_success = 0.6`,
  `min_roi = 0.05`, `min_sharpe = 0.05`, with all other thresholds at zero.

A minimal example looks like:

```toml
solana_rpc_url = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
dex_base_url = "https://swap.helius.dev"
agents = ["simulation"]

[agent_weights]
simulation = 1.0
```

The default configuration prefers the [Helius swap gateway](https://swap.helius.dev) and
falls back through any partner URLs defined under `dex_partner_urls` before returning to
Jupiter's public API. To add an additional partner without breaking this cascade, extend
`dex_partner_urls` instead of overriding `dex_base_url`:

```toml
[dex_partner_urls]
my_partner = "https://example.swap.partner"
```

The partner key (for example, `my_partner`) is used in `dex_priorities`. When you append a
partner, keep the Helius entry first so that `load_dex_config()` can fall back to
`jupiter` as the last resort automatically. If you supply `dex_priorities`, include the
new partner alongside `helius` and `jupiter` to preserve the cascade.

You can optionally tailor buy criteria for each detected market regime by
defining a `decision_thresholds` table. Provide a `default` section for shared
values and override individual fields such as `min_success`, `min_roi`,
`min_liquidity`, or `gas_cost` per regime:

```toml
[decision_thresholds.default]
min_success = 0.6
min_roi = 0.1
min_sharpe = 0.1
min_liquidity = 50000.0
gas_cost = 0.02

[decision_thresholds.bear]
min_success = 0.75
min_roi = 0.18
min_sharpe = 0.15
gas_cost = 0.06
min_liquidity = 150000.0
```

`AgentManager` forwards the detected regime to agents like the simulation
module so their buy decisions automatically adapt to these profiles.

The `scripts/setup_one_click.py` helper validates these fields after writing `config.toml` and exits with guidance if any are missing.

## Flash-Loan Arbitrage

Flash loans are enabled by default. Disable them by setting
`use_flash_loans` to `false` in your configuration. The bot will borrow up
to `max_flash_amount` of the trading token. When `flash_loan_ratio` is set the
maximum amount is computed from your current portfolio value as
`portfolio_value * flash_loan_ratio`. The bot uses supported protocols
(e.g. Solend) to borrow, execute the swap chain and repay
the loan within the same transaction.  You must supply the required protocol
accounts and understand that failed repayment reverts the entire transaction.
The arbitrage path search now factors this flash-loan amount into the expected
profit calculation so routes are ranked based on the borrowed size.

   The `AgentManager` loads the agents listed under `agents` and applies any
   weights defined in the `agent_weights` table.  When `dynamic_weights` is set
   to `true` a `SwarmCoordinator` derives weights dynamically from each
   agent's historical ROI. The coordinator normalizes ROI values into a
   confidence score and feeds these weights to the `AgentSwarm` at run time.
   To control how much each agent influences trades manually, add an
   `agent_weights` table mapping agent names to weights.  Set
   `use_attention_swarm = true` and specify `attention_swarm_model` to load
   a trained transformer that predicts these weights from ROI history and
   market volatility:

   ```toml
   [agent_weights]
   "simulation" = 1.0
   "conviction" = 1.0
   "arbitrage" = 1.0
   "exit" = 1.0
   ```

   Environment variables with the same names override values from the file.
   You can specify an alternative file with the `--config` command line option
   or by setting the `SOLHUNTER_CONFIG` environment variable.

   ```bash
   export SOLHUNTER_CONFIG=/path/to/config.yaml
   ```

4. **Configure API access**
   The scanner uses the BirdEye API when `BIRDEYE_API_KEY` is set.  If the key
   is missing, it will fall back to scanning the blockchain directly using the
   RPC endpoint specified by `SOLANA_RPC_URL` and will query on-chain volume
   and liquidity metrics for discovered tokens.
   To use BirdEye, export the API key:

   ```bash
   export BIRDEYE_API_KEY=b1e60d72780940d1bd929b9b2e9225e6
   ```
   If this variable is unset, the bot logs a warning and automatically falls back
   to on-chain scanning.
   To scan the blockchain yourself, provide a Solana RPC endpoint instead:

   ```bash
   export SOLANA_RPC_URL=https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d
   ```
   If `SOLANA_WS_URL` is unset, it is automatically derived from
   `SOLANA_RPC_URL` by switching the scheme from `http` to `ws` (and `https`
   to `wss`).

5. **Configure DEX endpoints**
   Set the base URL of the DEX API for mainnet and (optionally) the testnet
   endpoint. You can also override URLs for individual venues:
   ```bash
   export DEX_BASE_URL=https://quote-api.jup.ag
   export DEX_TESTNET_URL=https://quote-api.jup.ag
   export ORCA_DEX_URL=https://dex.orca.so
   export RAYDIUM_DEX_URL=https://dex.raydium.io
   ```
6. **Set the metrics API endpoint**
    Specify the base URL used by the simulator to fetch historical return
    metrics:
    ```bash
    export METRICS_BASE_URL=https://api.coingecko.com/api/v3
    ```
7. **Export RL metrics**
    Provide a URL that receives ``rl_metrics`` events:
    ```bash
    export METRICS_URL=http://localhost:9000/metrics
    ```
8. **Configure news feeds for sentiment**
   Sentiment scores influence RL training. Provide comma-separated RSS URLs via `NEWS_FEEDS` and optional social feeds:
   ```bash
   export NEWS_FEEDS=https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml,https://www.coindesk.com/arc/outboundfeeds/rss/
   export TWITTER_FEEDS=https://nitter.net/solana/rss
   export DISCORD_FEEDS=https://discord.com/api/guilds/613425648685547541/widget.json
   ```
9. **Provide a keypair for signing**
    Generate a keypair with `solana-keygen new` if you don't already have one and
    point the bot to it using `KEYPAIR_PATH`, `SOLANA_KEYPAIR` or the `--keypair`
    flag:
    ```bash
    export KEYPAIR_PATH=/path/to/your/keypair.json
    ```
    The path can also be supplied via `SOLANA_KEYPAIR` for the Rust depth
    service.  Placing the file in the `keypairs/` directory (for example
    `keypairs/main.json`) lets the bot discover it automatically:
    ```bash
    mkdir -p keypairs
    cp ~/my-keypair.json keypairs/main.json
    ```
    Set `AUTO_SELECT_KEYPAIR=1` so the launcher and the Web UI pick the only
    available keypair automatically. When there is just one keypair in the
    `keypairs/` directory it will be selected on start. The `--one-click`
    option for `scripts/startup.py` sets `AUTO_SELECT_KEYPAIR=1` automatically.

    You can also recover a keypair from a BIP‑39 mnemonic using the
    `solhunter-wallet` utility and activate it:
    ```bash
    solhunter-wallet derive mywallet "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about" --passphrase ""
    solhunter-wallet select mywallet
    ```
    Placing the resulting file in `keypairs/` and setting `AUTO_SELECT_KEYPAIR=1`
      lets the launcher load it automatically.

    To set up a default wallet non-interactively, export `MNEMONIC` (and
    optional `PASSPHRASE`) then run:
    ```bash
    scripts/setup_default_keypair.sh
    ```
    Set `ENCRYPT_MNEMONIC=1` and provide `MNEMONIC_ENCRYPTION_KEY` to store
    `default.mnemonic` in encrypted form.
10. **Priority RPC endpoints**
    Specify one or more RPC URLs used for high-priority submission:
    ```bash
export PRIORITY_RPC=https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d,https://rpc.helius.dev/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d
    ```
11. **Priority fee multipliers**
    Configure compute unit price multipliers used when the mempool is busy:
    ```bash
    export PRIORITY_FEES="0,1,2"
    ```
12. **Cache TTL overrides**
    Adjust in-memory cache lifetimes:
    ```bash
    export PRICE_CACHE_TTL=10
    export TOKEN_METRICS_CACHE_TTL=60
    export SIM_MODEL_CACHE_TTL=600
    export TREND_CACHE_TTL=120
    export LISTING_CACHE_TTL=120
    export DEX_METRICS_CACHE_TTL=45
    export TOKEN_VOLUME_CACHE_TTL=45
    export TOP_VOLUME_TOKENS_CACHE_TTL=90
    export DEPTH_CACHE_TTL=1
    export EDGE_CACHE_TTL=60
    ```
13. **HTTP connector limits**
   Tune the aiohttp connector:
   ```bash
   export HTTP_CONNECTOR_LIMIT=0
   export HTTP_CONNECTOR_LIMIT_PER_HOST=0
   ```
14. **Auto-execution**
    Register tokens and pre-signed transactions so the depth service
    dispatches them when thresholds are crossed:
    ```bash
    export AUTO_EXEC='{"TOKEN":{"threshold":1.0,"txs":["BASE64"]}}'
    ```
15. **Run the bot**
   ```bash
   python -m solhunter_zero.launcher --auto
   ```
16. **External event bus**
    Set `EVENT_BUS_URL` to automatically connect to a remote websocket bus:
    ```bash
   export EVENT_BUS_URL=ws://0.0.0.0:8787
   ```
   Alternatively specify `event_bus_url` in the configuration file. If the
   address points to `localhost`, `127.0.0.1` or `0.0.0.0`, the autopilot will
   automatically launch a websocket event bus server and verify it is reachable
   before starting other services.
17. **Message broker**
   Connect to one or more brokers so multiple instances share events:
   ```bash
   export BROKER_URLS=redis://localhost:6379,nats://localhost:4222
   ```
   Use `broker_urls` (or legacy `broker_url`) in the configuration file for persistent settings.
18. **Event compression**
   Choose a compression algorithm for protobuf messages with
   `EVENT_COMPRESSION`:
   ```bash
   export EVENT_COMPRESSION=zstd  # or lz4, zlib, none
   ```
   A typical `depth_update` event (~2.2 KB) becomes ~1.8 KB with zlib
   (~0.27 ms), ~1.9 KB with lz4 (~0.004 ms) and ~1.7 KB with
   zstd (~0.012 ms). When either `zstandard` or `lz4` is installed,
   compression is enabled automatically with `COMPRESS_EVENTS=1` and
   `EVENT_COMPRESSION=zstd` when available. Set `COMPRESS_EVENTS=0` to
   disable compression or `USE_ZLIB_EVENTS=1` if older nodes expect zlib
   compressed messages.
   Install the optional compression libraries with:
   ```bash
   pip install .[fastcompress]
   ```
   Then verify compression is active by running
   `python scripts/compression_demo.py`, which prints the message size and
   latency with and without zstd compression.
19. **Shared memory event bus**
   Export `EVENT_BUS_MMAP` so protobuf events are written to a local ring buffer
   instead of a socket. Increase `EVENT_BUS_MMAP_SIZE` if the default 1 MB
   buffer is too small:
   ```bash
   export EVENT_BUS_MMAP=/tmp/events.mmap
   export EVENT_BUS_MMAP_SIZE=$((1<<20))
   ```
   Start all producers and consumers with these variables for lower latency.
   Adjust `EVENT_MMAP_BATCH_MS` and `EVENT_MMAP_BATCH_SIZE` to batch writes and
   reduce IPC overhead.
20. **Full system startup**
   Launch the Rust service, RL daemon and trading loop together:
   ```bash
   python scripts/start_all.py
   ```
   The script waits for the depth websocket and forwards `--config`, `EVENT_BUS_URL` and `SOLANA_RPC_URL` to all subprocesses.

Running `scripts/startup.py` handles these steps interactively and forwards any options to the cross-platform entry point
`python -m solhunter_zero.launcher --auto`, which performs a fully automated launch using the bundled defaults.

   The script ensures the `solhunter-wallet` command-line tool is available, loads the active configuration (falling back to `config/default.toml` when none is chosen) and automatically selects the sole keypair in `keypairs/`. It checks RPC endpoints and prints a warning if the wallet balance is below `min_portfolio_value`. Set `AUTO_SELECT_KEYPAIR=1` so the Web UI matches this behaviour.

   You can still run the bot manually with explicit options:
   ```bash
    python -m solhunter_zero.launcher --dry-run
    # or
    python -m solhunter_zero.launcher

   ```
## Usage
See [docs/usage.md](docs/usage.md) for usage examples, the investor demo, MEV bundle configuration, and more.

## Platform Coordination

Agents and services communicate via a lightweight event bus. Three topics are
published by default:

- `action_executed` whenever the `AgentManager` finishes an order
- `weights_updated` after agent weights change
- `rl_weights` when the RL daemon publishes new weights
- `system_metrics_combined` aggregated CPU and memory usage from
  `metrics_aggregator`
- `risk_updated` when the risk multiplier is modified
- `config_updated` when a configuration file is saved
- `risk_metrics` whenever portfolio risk metrics are recalculated
- `trade_logged` after any trade is written to a memory database

Handlers can subscribe using :func:`subscribe` or the :func:`subscription`
context manager:

```python
from solhunter_zero.event_bus import subscription

async def on_action(event):
    print("executed", event)

with subscription("action_executed", on_action):
    ...  # run trading loop
```

Other processes such as the RL daemon listen to these events to train models and
adjust configuration in real time.

When `replicate_trades` is enabled, `AdvancedMemory` also listens for
`trade_logged` events to mirror trades from other nodes.
Set `MEMORY_SYNC_INTERVAL` to control how often these sync requests are sent
(defaults to 5 s).

When the Web UI is running, these events are also forwarded over a simple
WebSocket endpoint at `ws://localhost:8770/ws` (configurable via the
`EVENT_WS_PORT` environment variable). Clients can subscribe and react
to updates directly in the browser. Use the `topics` query parameter to limit
events to a subset:

```javascript
const ws = new WebSocket('ws://localhost:8770/ws?topics=weights_updated');
ws.onmessage = (ev) => {
  const msg = JSON.parse(ev.data);
  console.log(msg.topic, msg.payload);
};
```
Subscriptions can be changed later by calling `subscribe_ws_topics(ws, topics)`.

Memory databases automatically synchronise so replicated nodes share the same
trade history. The default delay between sync requests is five seconds. Adjust
this with ``MEMORY_SYNC_INTERVAL`` or the ``memory_sync_interval`` config
option:

```bash
export MEMORY_SYNC_INTERVAL=10
```

WebSocket connections stay alive through periodic pings. Change the interval and
timeout with ``WS_PING_INTERVAL`` and ``WS_PING_TIMEOUT``:

```bash
export WS_PING_INTERVAL=20
export WS_PING_TIMEOUT=8
```

To send transactions via multiple nodes, specify a comma‑separated list of RPC
endpoints using ``priority_rpc`` (or the ``PRIORITY_RPC`` environment
variable):

```bash
export PRIORITY_RPC=https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d,https://rpc.helius.dev/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d
```
### Running in a Cluster

Set `BROKER_URL` to a Redis or NATS server so multiple bots share market
metrics, agent weights and trade intents. Launch each instance with the same
URL and they will automatically exchange events.

The helper script `scripts/cluster_setup.py` reads a TOML file describing your
nodes and launches `scripts/start_all.py` for each entry. A minimal
configuration might look like:

```toml
event_bus_url = "ws://0.0.0.0:8787"
broker_url = "redis://localhost:6379"

[[nodes]]
solana_rpc_url = "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
solana_keypair = "keypairs/node1.json"

[[nodes]]
solana_rpc_url = "https://rpc.helius.dev/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
solana_keypair = "keypairs/node2.json"
```

Run the script with:

```bash
python scripts/cluster_setup.py cluster.toml
```

This starts a local process for each node using the shared broker and event bus
settings.
## Architecture
See [docs/architecture.md](docs/architecture.md) for in-depth architecture and agent details.

## Backtesting and Datasets

### Offline Data Collection

`data_sync.start_scheduler()` runs a background task that periodically calls
`sync_recent()`. This helper downloads order‑book snapshots from the configured
`metrics_base_url` and keeps `offline_data.db` under the
`OFFLINE_DATA_LIMIT_GB` limit by pruning old rows. When running live the
`start_depth_snapshot_listener()` helper subscribes to `depth_update` events and
logs each update automatically.

The repository includes a simple backtesting framework. Tick level depth
data can be exported from `offline_data.db` using `scripts/build_tick_dataset.py`:

```bash
python scripts/build_tick_dataset.py --db offline_data.db --out datasets/tick_history.json
```

The backtester can also be launched via the `solhunter-backtest` command. A
synthetic price history with hundreds of daily points is provided at
`tests/data/prices.json`:

```bash
solhunter-backtest tests/data/prices.json -c config.toml
```

This wrapper invokes `solhunter_zero.backtest_cli`, so the available
arguments are the same. For example:

```bash
solhunter-backtest prices.json -c config.toml --optimize --iterations 30
```

For faster reinforcement learning training you can export the offline tables to
a compressed NumPy archive and memory‑map it:

```bash
python scripts/build_mmap_dataset.py --db offline_data.db --out datasets/offline_data.npz
```

When present, `TradeDataModule` loads this archive instead of querying SQLite.
If it is missing it will be created automatically when `RLTraining` or
`RLDaemon` starts. Set `rl_build_mmap_dataset = false` or export
`RL_BUILD_MMAP_DATASET=0` to disable this behaviour. When disabled the datamodule
can prefetch rows asynchronously. The buffer size is controlled by
`rl_prefetch_buffer` or `RL_PREFETCH_BUFFER`. On a small dataset memory mapping
lowered preparation time from around 3 s to roughly 0.2 s thanks to
``numpy.fromiter`` and memory mapping.
A simple arithmetic dataset located at `solhunter_zero/data/artifact_math.json` powers the
`ArtifactMathAgent` and can be loaded with `load_artifact_math`.
An alien cipher dataset at `solhunter_zero/data/alien_cipher.json` powers the
`AlienCipherAgent` and can be loaded with `load_alien_cipher`.

Offline snapshots can also be used to train a transformer-based price model:

```bash
python scripts/train_transformer_agent.py --db sqlite:///offline_data.db --out models/price.pt
```

To train the activity detection model run:

```bash
python scripts/train_activity_model.py --db offline_data.db --out models/activity.pt
```
To train the VaR forecasting model run:

```bash
python scripts/train_var_forecaster.py --db offline_data.db --out models/var.pt
```


Set the `PRICE_MODEL_PATH` environment variable to this file so agents and
`predict_price_movement()` can load it automatically. Both LSTM and transformer
models are supported. Use `GRAPH_MODEL_PATH` when loading a saved
`GraphPriceModel`.
Set `ACTIVITY_MODEL_PATH` to `models/activity.pt` to enable early activity scoring.
Set `VAR_MODEL_PATH` to `models/var.pt` to enable VaR-based risk scaling.

You can train a soft actor-critic policy from the same dataset:

```bash
python scripts/train_sac_agent.py --db sqlite:///offline_data.db --out sac_model.pt
```

### Continuous Training

Running `online_train_transformer.py` keeps the transformer model up to date by
periodically fitting new snapshots on GPU and saving checkpoints. On a CUDA
machine launch it with daemon mode enabled:

```bash
python scripts/online_train_transformer.py \
  --db sqlite:///offline_data.db \
  --model models/price.pt --device cuda \
  --daemon --log-progress
```

For quick adjustments during live trading use `scripts/live_finetune_transformer.py`.
It reloads the latest checkpoints, performs a few gradient steps and writes the
updated model back to disk so `ConvictionAgent` picks up the new weights.

Set the `PRICE_MODEL_PATH` environment variable to `models/price.pt` so trading
agents reload each checkpoint automatically. This can reference a
`TransformerModel`, `DeepTransformerModel`, `XLTransformerModel` or
`GraphPriceModel`. Use `GRAPH_MODEL_PATH` in the same way for graph-based
models.
Use `ACTIVITY_MODEL_PATH` in the same way to reload the activity model.

To continuously retrain the RL models on GPU run `scripts/train_rl_gpu.py`:

```bash
python scripts/train_rl_gpu.py --db sqlite:///offline_data.db --model ppo_model.pt --interval 3600
```

Training now also writes a TorchScript version of the checkpoint next to the
regular file (`ppo_model.ptc`). Agents and the RL daemon load this compiled
module automatically when present for faster inference. Existing models can be
converted using `scripts/export_model.py`:

```bash
python scripts/export_model.py --model ppo_model.pt --out ppo_model.ptc
python scripts/export_model.py --model models/price.pt --out models/price.onnx --format onnx
```

Loading the compiled variant reduces inference latency by roughly 15%.

You can also launch the built-in RL daemon directly with GPU acceleration:

```bash
  python -m solhunter_zero.launcher --daemon --device cuda
```

To forward events to a remote bus use the `--event-bus` option when running
`scripts/run_rl_daemon.py`:

```bash
python scripts/run_rl_daemon.py --event-bus ws://0.0.0.0:8787
```

Hierarchical RL training now runs by default and stores its policy in
`hier_policy.json`. Use `--no-hierarchical-rl` with the daemon script to
disable this behaviour.

Alternatively start the trainer manually using the dedicated CLI:

```bash
solhunter-train --daemon --device cuda
```

The dataloader now chooses the worker count automatically based on the dataset
size. When more than one CPU core is available `rl_dynamic_workers` is enabled
by default so the trainer scales workers with CPU load. You can still override
this using the `RL_NUM_WORKERS` environment
variable or the `--num-workers` flag on `solhunter-train` and
`python -m solhunter_zero.multi_rl`.
On a 4‑core test machine throughput increased from about 250 samples/s with a
single worker to around 700 samples/s with four workers.

When `multi_rl = true` the RL daemon maintains a small population of models
and trains each one on the most recent trades. After every update the model
with the highest score publishes its policy via the `rl_weights` event. The
size of this population is controlled by `rl_population_size`.

Enable `rl_live = true` to train directly from streamed `trade_logged` and
`depth_update` events. The daemon uses the new `LiveTradeDataset` to buffer
recent updates and performs incremental training every few seconds when this
mode is active.

Enable `distributed_rl = true` so multiple bots share a single training loop.
Set `BROKER_URL` to a common Redis or NATS instance and launch each bot with
`--distributed-rl` or the configuration flag. The RL daemon collects incoming
`trade_logged` and `depth_update` events from all peers before every update.

Torch 2 adds the `torch.compile` API which can speed up both training and
inference. Models are compiled automatically when this feature is available.
Set `USE_TORCH_COMPILE=0` to disable this optimization.

Set `rl_auto_train = true` in `config.toml` to enable automatic hyperparameter
tuning. When enabled the RL daemon starts automatically with the trading loop.
You can also toggle the lightweight daemon ad-hoc by exporting
`RL_DAEMON=true`, which is useful for testing without touching configuration.
It spawns `scripts/auto_train_rl.py` which periodically retrains the PPO model
from `offline_data.db`. Control how often tuning runs via `rl_tune_interval`
(seconds).
The RL agents also take the current market regime as an additional input.
Adjust the influence of this indicator with the new `regime_weight` setting
(defaults to `1.0`).
When an `AdvancedMemory` instance is provided to `_TradeDataset` each trade is
assigned a cluster, and the normalised cluster id is appended as an additional
feature.

Regime detection now supports clustering over rolling windows of price
returns and volatility. Set `regime_cluster_window` to the desired window
length and `regime_cluster_method` to either `"kmeans"` (default) or
`"dbscan"`. When there isn't enough history the simple trend detector is
used instead.

`solhunter_zero.backtest_cli` now supports Bayesian optimisation of agent
weights. Optimisation runs the backtester repeatedly while a Gaussian process
searches the weight space:

```bash
python -m solhunter_zero.backtest_cli prices.json -c config.toml --optimize --iterations 30
```

The best weight configuration found is printed as JSON.

## Protobuf Generation

Protocol buffer classes are generated with `grpcio-tools`. If you modify
`proto/event.proto`, run:

```bash
python scripts/gen_proto.py
# or
make gen-proto
```

This updates `solhunter_zero/event_pb2.py`, which is required for the event bus.

## Troubleshooting

### Preflight checks

- **RPC unreachable** — ensure `SOLANA_RPC_URL` points to a healthy endpoint and that outbound network access is available.
- **Wallet balance too low** — fund the default keypair or lower `min_portfolio_value` in `config.toml`.

### General issues

- **Permission denied when connecting to the socket** — check that
  `DEPTH_SERVICE_SOCKET` points to a writable location and that the Rust
  service owns the file. Delete any stale socket file and restart the
  service.
- **Missing keypair** — ensure a valid Solana keypair file is available.
  Set `KEYPAIR_PATH` or `SOLANA_KEYPAIR` to its path or place it in the
    `keypairs/` directory. Use `AUTO_SELECT_KEYPAIR=1` so the launcher
   (`python -m solhunter_zero.launcher --auto`) or the Web UI select the
    sole available key automatically.
  - **Service not running** — `depth_service` starts automatically with `make start` or `python -m solhunter_zero.launcher --auto`. If it isn't responding, check logs and ensure `USE_SERVICE_EXEC`, `USE_RUST_EXEC` and `USE_DEPTH_STREAM` are all set to `True`.
- **Slow routing** — the Rust service computes paths much faster. Leave
  `USE_SERVICE_ROUTE` enabled unless debugging the Python fallback.

## Testing

See [TESTING.md](TESTING.md) for the full testing guide. Install dependencies
with `pip install -e .[dev]` before running the tests. The complete test suite
requires heavy packages such as `torch`, `transformers` and `faiss`.

After setting up the environment you can run a short paper-trading simulation
which mirrors the investor demo and can source live prices:

```bash
python paper.py --reports reports --url https://example.com/prices.json
```

Omit ``--url`` to reuse one of the bundled preset datasets.

For a quick pre-flight smoke check before enabling live trading run:

```bash
python paper.py --test
# or
make paper-test
```

This command fetches a small slice of live market data, executes the trading
loop in dry-run mode and writes reports to ``reports/`` by default.

## License

This project is licensed under the [MIT License](LICENSE).