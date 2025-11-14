# Setup

## Quick Start

1. **Install Python 3.11+**
   Ensure that Python 3.11 or newer is installed. Tools like
   [pyenv](https://github.com/pyenv/pyenv) or your system package manager can help
   with installation.

2. **Install dependencies**
 ```bash
  python -m solhunter_zero.launcher --one-click
  ```
  The one-click launcher automatically installs any missing Python packages by
  invoking the internal `ensure_deps` helper.
  If you already have an active virtual environment, the setup script will use it
  instead of creating a `.venv` directory. Activate your preferred environment
  first to supply a custom name.

  > **Note:** The UI websocket servers require the [`websockets`](https://pypi.org/project/websockets/)
  > package. Startup now fails fast when it is missing so install it before
  > launching the runtime or UI.

For a guided setup you can run `scripts/startup.py` which checks dependencies, verifies that the `solhunter-wallet` CLI is installed, prompts for configuration and wallet details, then launches the bot live. `make start` runs the same script with `--one-click` for unattended startup. The `solhunter-start` command provides the same non-interactive flow by default while still accepting the standard flags for customization.

On macOS, double-click `start.command` to launch, or run `python -m solhunter_zero.launcher` from Terminal for the same entry point.

Developers embedding SolHunter Zero can initialize the environment
programmatically:

```python
from solhunter_zero.bootstrap import bootstrap

bootstrap(one_click=True)
```

The helper wraps the dependency checks and keypair/setup logic used by
`scripts/startup.py` and the `solhunter_zero.main` entry point.


### One-Click macOS M1 Setup

1. **Launch** — From Terminal, run `python -m solhunter_zero.launcher --one-click` to begin the automated setup.
   Alternatively, run `make setup` from Terminal to invoke the same command directly.
2. **Prompts** — The script verifies Python 3.11+, Homebrew and `rustup`.  
   Missing components trigger guided installers that may prompt for your password or the Xcode Command Line Tools.
3. **GPU detection** — The launcher runs `solhunter_zero.device --check-gpu` and sets `TORCH_DEVICE=mps` when an Apple GPU is available.
    `PYTORCH_ENABLE_MPS_FALLBACK=1` is exported so unsupported operations transparently fall back to the CPU.
4. **Logs** — All output is appended to `startup.log` in the project directory.
    A diagnostics summary is written to `diagnostics.json` for accessibility tools.
    Output from environment preflight checks is written to `preflight.log`, which rotates to
    `preflight.log.1` once it exceeds 1 MB so you can review the previous run.
    Older logs rotate with timestamps for easy troubleshooting.
5. **Troubleshooting** — If the script exits early, open Terminal and run `python -m solhunter_zero.launcher --one-click` to view errors.
   Common issues include missing network access, Homebrew not on `PATH`, or stale permissions on the script.

3. **Create a configuration file**
   Run `scripts/quick_setup.py --auto` to generate a `config.toml` with public
   endpoint defaults. The script populates required keys and writes the file
   without prompting. Edit the resulting `config.toml` or `config.yaml` to add
   your API keys, RPC URL and DEX endpoints:

   ```yaml
birdeye_api_key: b1e60d72780940d1bd929b9b2e9225e6
solana_rpc_url: https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY
dex_base_url: https://swap.helius.dev
dex_testnet_url: https://quote-api.jup.ag
dex_partner_urls:
  jupiter: https://quote-api.jup.ag
orca_api_url: https://api.orca.so
raydium_api_url: https://api.raydium.io
orca_ws_url: ""
raydium_ws_url: ""
orca_dex_url: https://dex.orca.so
raydium_dex_url: https://dex.raydium.io
metrics_base_url: https://api.coingecko.com/api/v3
risk_tolerance: 0.1
max_allocation: 0.2
max_risk_per_token: 0.05
stop_loss: 0.1
take_profit: 0.2
trailing_stop: 0.1
max_drawdown: 0.5
volatility_factor: 1.0
risk_multiplier: 1.0
arbitrage_threshold: 0.05
arbitrage_amount: 1.0
use_flash_loans: true
max_flash_amount: 0.02
flash_loan_ratio: 0.1
mempool_threshold: 0.0
bundle_size: 1
use_mev_bundles: true
learning_rate: 0.1
dex_priorities: "helius,orca,raydium,jupiter"
dex_fees: "{}"
dex_gas: "{}"
dex_latency: "{}"
epsilon: 0.1
discount: 0.95
agents:
  - simulation
  - conviction
  - arbitrage
  - exit
agent_weights:
  simulation: 1.0
  conviction: 1.0
  arbitrage: 1.0
dynamic_weights: true
weight_step: 0.05
evolve_interval: 1
mutation_threshold: 0.0
strategy_rotation_interval: 0
weight_config_paths: []
```

`load_dex_config()` seeds `dex_partner_urls` with known aggregators so the bot first
queries the Helius swap gateway, then partners such as Birdeye before falling back to
Jupiter. Add your own partners under `dex_partner_urls` and append their keys to
`dex_priorities` instead of overriding `dex_base_url` so the default cascade remains in
place.

To adapt buy decisions to market regimes, define a `decision_thresholds` table
with per-regime overrides. Each regime inherits the values from the
`default` section, letting you tighten liquidity floors or increase gas cost
deductions when markets turn bearish. When no table is provided the bot uses
`min_success = 0.6`, `min_roi = 0.05` and `min_sharpe = 0.05` with other
thresholds at zero:

```toml
[decision_thresholds.default]
min_success = 0.6
min_roi = 0.1
min_sharpe = 0.1
gas_cost = 0.02

[decision_thresholds.bear]
min_success = 0.8
min_roi = 0.18
min_sharpe = 0.15
min_liquidity = 200000.0
gas_cost = 0.08
```

`AgentManager` passes the active regime into the evaluation swarm so agents that
support regime-aware tuning (such as the simulation agent) automatically pick up
these profiles.

Key discovery options:

- `mempool_score_threshold` sets the minimum score for tokens observed in the
  mempool before they are considered by discovery agents.
- `trend_volume_threshold` filters out tokens with on-chain volume below this
  value when ranking new opportunities.
- `max_concurrency` limits how many discovery tasks run in parallel. The
  environment variable `MAX_CONCURRENCY` overrides this value.

   A base configuration file named `config/default.toml` is provided in the
   project root. Copy it to `config.toml` (or `config.yaml`) and edit the values
   to get started. The default configuration loads several built‑in
   **agents** that replace the previous static strategy modules.

## macOS Setup

macOS users can launch the bot using `python -m solhunter_zero.launcher`.
Double‑clicking it opens a terminal, installs any missing dependencies and
forwards to the Python launcher for a fully automated start.

### Required dependencies

1. Install Homebrew packages with the helper script (installs the Xcode command line tools if needed). The script exits after starting the Xcode installation; re-run it once the tools finish installing:
   ```bash
   ./scripts/mac_setup.py
   ```
   The run writes a machine-readable summary to `macos_setup_report.json` so you can review each step's result without scanning the full log.
2. The startup script automatically installs the Metal-enabled PyTorch build on Apple Silicon. To install manually, run:
    ```bash
    pip install torch==2.8.0 torchvision==0.23.0 \
      --extra-index-url https://download.pytorch.org/whl/metal
    ```
    Supported version pairs:

    | torch | torchvision |
    | ----- | ----------- |
    | 2.8.0 | 0.23.0 |

    The default `config.toml` sets `torch_metal_version = "2.8.0"` and
    `torchvision_metal_version = "0.23.0"`. Adjust these in the `[torch]`
    section or via the `TORCH_METAL_VERSION` and `TORCHVISION_METAL_VERSION`
    environment variables to match a supported pair if newer builds are
    available.

Note: `solhunter_zero.device.initialize_gpu` automatically exports `PYTORCH_ENABLE_MPS_FALLBACK=1` so unsupported MPS operations fall back to the CPU. Set `PYTORCH_ENABLE_MPS_FALLBACK=0` before calling `initialize_gpu` to disable the fallback.

### Troubleshooting

  right-click and choose *Open*.
- **`python` not found** – ensure Python 3.11 is installed and on your
  `PATH`.
- **Torch missing MPS backend** – confirm the Metal wheel installed and that
  macOS 13+ is in use. Validate with `python -c "import torch; print(torch.backends.mps.is_available())"`.
- **Permission denied** – Gatekeeper may block the script; allow it via
  System Preferences or run from Terminal.

## Docker Compose

Build and run inside containers without installing Python or Rust locally. The
compose file mounts `config.toml` and the `keypairs` directory so changes persist
across runs. Environment variables such as `SOLANA_RPC_URL` and `BIRDEYE_API_KEY`
are loaded from a `.env` file.

```bash
make compose
```

The `compose` target creates `.env` automatically if missing before running
`docker-compose up`.

Logs stream to the terminal. To run in the background and follow logs:

```bash
docker-compose up -d
docker-compose logs -f
```

## .env configuration

The startup scripts and Docker Compose setup read environment variables from a
`.env` file in the project root. Each non-empty line must use `KEY=value`
syntax. Lines starting with `#` or blank lines are ignored. Values already
defined in the environment take precedence over entries in the file, allowing
you to override them when invoking commands or in container settings.

An [`.env.example`](.env.example) file documents common variables and provides
sample values to copy or adapt.

See [docs/environment.md](docs/environment.md) for a complete list of
environment variables, their default values and purposes.

When `config.toml` defines a value that is missing from the environment,
`configure_environment` now appends the corresponding `KEY=value` pair to
`.env`. The file is created automatically if needed and existing content is
preserved.

## Paper Trading

Before committing real SOL, you can evaluate strategies using the paper
trading CLI. It mirrors the investor demo workflow but can source real market
data via HTTP. By default a bundled preset is used, however passing ``--url``
allows the run to pull live prices.

```bash
python paper.py --reports reports --url https://example.com/prices.json
```

The command above generates the standard demo reports using prices fetched
from the given URL. Omit ``--url`` or use ``--preset`` to replay the bundled
datasets without touching the network.

## Rust Depth Service

The project includes a small Rust daemon that aggregates order‑book depth and
exposes it over a Unix socket for the Python components.  It is optional when
working entirely with simulated data, but must be running for live trading.

### Build

`solhunter_zero.service_launcher.start_depth_service` automatically compiles the
Rust depth service when the binary is missing, so most setups do not require
manual steps.

#### Advanced/Custom build

If you need to build manually (for custom paths or optimizations):

1. Install the Rust toolchain if it is not already available.  The service is
   built with [`cargo`](https://doc.rust-lang.org/cargo/).
2. From the project root run:
   ```bash
   cargo build --release --manifest-path depth_service/Cargo.toml
   ```
   The compiled binary is written to `target/release/depth_service`.

### Run

Execute the binary directly after building:

```bash
./target/release/depth_service --config config.toml
```

The service reads optional environment variables to customise behaviour.  The
Unix socket used for IPC defaults to `/tmp/depth_service.sock` and can be
overridden with `DEPTH_SERVICE_SOCKET`.

### Integrate with Python

Most users should start the service from Python so that it is rebuilt
automatically when missing.  The helper `start_depth_service` from
`solhunter_zero.service_launcher` launches the binary and returns the subprocess
handle:

```python
from solhunter_zero.service_launcher import start_depth_service

proc = start_depth_service("config.toml")
```

The process is monitored and restarted if it exits unexpectedly.

### Troubleshooting

* **`cargo` not found** – Install Rust via
  [rustup](https://rustup.rs/) and ensure `cargo` is on your `PATH`.
* **Socket path errors** – If the service fails to bind
  `DEPTH_SERVICE_SOCKET`, remove any stale socket file and verify the directory
  exists and is writable.
* **Restart limits** – The Python watchdog only restarts the service a limited
  number of times.  Adjust `DEPTH_MAX_RESTARTS` or fix the underlying issue if
  the service keeps exiting.