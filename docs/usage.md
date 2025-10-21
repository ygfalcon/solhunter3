# Usage

## Autopilot
Automatically selects the only keypair and active configuration, then launches all services and the trading loop. It ensures the
`solhunter-wallet` CLI is installed and will provision a default keypair automatically, falling back to a non-interactive quick
setup if necessary:

```bash
python scripts/start_all.py autopilot
```
Use the `--testnet` flag to submit orders to a testnet DEX endpoint, `--dry-run` to skip order submission entirely, `--offline` to avoid network requests and use a static token list, or `--token-list <file>` to load token addresses from a file. Use `--strategy-rotation-interval N` with one or more `--weight-config` files to automatically test and switch weight presets every `N` iterations.

## Investor Demo

Run a small rolling backtest and generate lightweight reports:

```bash
python demo.py --reports reports
```

This writes `summary.json`, `trade_history.csv` and `highlights.json` to the
specified reports directory and prints brief snippets to the console.

Enable a lightweight reinforcement‑learning stub:

```bash
python demo.py --rl-demo --reports reports
```

Run a canned learning loop that rotates strategy weights:

```bash
python demo.py --learn --reports reports
```

Exercise the full system with heavier dependencies:

```bash
python demo.py --full-system --reports reports
```

All modes emit the same report files along with console summaries.

## Event capture and replay

Operators can archive the runtime event bus and rehydrate the Golden UI
offline using the bundled CLIs. Capture sessions stream ordered JSONL
records with sequence numbers, wall-clock timestamps, and the elapsed
monotonic offset used for deterministic playback.

Record a focused session (token discovery, price, and depth topics only)
for five minutes:

```bash
solhunter-capture \
  --topic token_discovered \
  --topic price_update \
  --topic depth_update \
  --duration 300 \
  --output artifacts/golden-session.jsonl
```

The resulting file begins with a metadata line describing topics, limits,
and optional labels (add context with repeated `--label key=value`).

Rehydrate the stream against a local runtime, preserving the original
timing by default. Use `--speed` to accelerate playback (0 disables
delays entirely) or restrict to specific topics/sequences:

```bash
solhunter-replay \
  artifacts/golden-session.jsonl \
  --speed 4.0 \
  --topic token_discovered --topic price_update --topic depth_update
```

During replay the messages are published to the shared event bus, so the
Golden pipeline, UI, and downstream analytics receive the same payloads
observed during the capture window.

### Paper Trading

Mirror the investor demo while optionally fetching live price data:

```bash
python paper.py --reports reports --url https://example.com/prices.json
```

Omit ``--url`` or pass ``--preset`` to use one of the bundled datasets.

## MEV Bundles

When `use_mev_bundles` is enabled (the default), swaps are submitted
through the [Jito block-engine](https://jito.network/). The toolkit now
expects you to provide existing credentials; new block-engine access is
not being issued or processed through the public authentication
challenge. You must already operate a Solana keypair that has been
whitelisted with Jito. During normal operation the client reuses the
Solhunter trading keypair you have configured (for example via
`KEYPAIR_PATH`) to sign the block engine's challenge and obtain short-lived JWT
tokens. If `JITO_AUTH` is missing, the launcher will continue to look for
a cached token in `.env`, but it will not be able to mint a new one on
your behalf. The same credentials can also be used to subscribe to
Jito's searcher websocket for real-time pending transactions. To use a
custom token, set `JITO_AUTH` in the environment or your own
configuration file before starting. Provide the block-engine and
websocket endpoints and authentication token:

```bash
export JITO_RPC_URL=https://mainnet.block-engine.jito.wtf/api/v1/bundles
export JITO_AUTH=your_token
export JITO_WS_URL=wss://mainnet.block-engine.jito.wtf/api/v1/ws
export JITO_WS_AUTH=your_token
```

The sniper and sandwich agents automatically pass these credentials to
`MEVExecutor` and will read pending transactions from the Jito stream
when both variables are set. A warning is logged if either variable is
missing while MEV bundles are enabled. The authentication request signs
Jito's challenge with your trading keypair to obtain the JWT; editing
`.env` or exporting `JITO_AUTH` lets you substitute a token retrieved
elsewhere.

### Jito Authentication Overview

1. Load the Solana keypair already configured for Solhunter—the same
   signing key used for trading and bundle submission.
2. Submit the public key to Jito and wait for it to be whitelisted for
   block-engine access.
3. During startup the client performs the challenge–response flow with
   that keypair to mint the short-lived JWT used for gRPC requests.
4. Once the RPC and websocket parameters are set, authorization happens
   automatically; no extra API calls are required in your scripts.

