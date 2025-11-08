# Usage

## Autopilot
Automatically selects the only keypair and active configuration, then launches all services and the trading loop. It ensures the
`solhunter-wallet` CLI is installed and will provision a default keypair automatically, falling back to a non-interactive quick
setup if necessary:

```bash
python scripts/start_all.py autopilot
```
Use the `--testnet` flag to submit orders to a testnet DEX endpoint, `--dry-run` to skip order submission entirely, `--offline` to avoid network requests and use a static token list, or `--token-list <file>` to load token addresses from a file. Use `--strategy-rotation-interval N` with one or more `--weight-config` files to automatically test and switch weight presets every `N` iterations.

## Dashboard

The SolHunter Zero dashboard served from the UI module refreshes itself without reloading the page. A lightweight JavaScript loop
fetches the aggregate `/?format=json` endpoint every five seconds (falling back to the individual JSON feeds when needed) and
patches the existing DOM so counters, charts and tables update in place. Because the page no longer uses a `<meta http-equiv="refresh">` tag the header, open `<details>`
sections and scroll positions stay put instead of flickering on each update. The Discovery and Logs panels remember their
expanded/collapsed state across refreshes using session storage, letting you keep frequently used panels open while watching live
data.

Administrative POST actions, such as updating the discovery method via `/discovery`, must originate from a loopback address. If a
reverse proxy or remote automation needs to reach these endpoints it should relay requests locally or add its own authentication
layer before forwarding them to the UI server.

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

### Paper Trading

Mirror the investor demo while optionally fetching live price data:

```bash
python paper.py --reports reports --url https://example.com/prices.json
```

Omit ``--url`` or pass ``--preset`` to use one of the bundled datasets.

## MEV Bundles

When `use_mev_bundles` is enabled (the default), swaps are submitted
through the [Jito block-engine](https://jito.network/). On first launch
the toolkit checks for `JITO_AUTH`; if it is missing, a new token is
requested from Jito's authentication service using your wallet keypair
and saved to `.env`. The same credentials can also be used to subscribe
to Jito's searcher websocket for real‑time pending transactions. To use
a custom token instead, set `JITO_AUTH` in the environment or your own
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

## Discovery Metrics

Discovery now emits lightweight StatsD-style metrics so operators can
observe token sourcing and emission behaviour without scraping logs. If
``STATSD_HOST`` is set (and the optional ``statsd`` package is installed)
these metrics are sent directly to the configured endpoint; otherwise
they fall back to a no-op client so the agent can still run locally.

### `discover_tokens`

Each invocation of ``DiscoveryAgent.discover_tokens`` produces:

* ``discovery.method.invocations`` – counter tagged with the discovery
  ``method`` and whether the call was ``offline``.
* ``discovery.method.result`` – counter tracking ``result`` outcomes
  (``success``, ``fallback``, ``cached``, ``empty``, ``error``).
* ``discovery.method.error`` – counter tagged with the ``error`` class
  name when discovery raises.
* ``discovery.method.duration_seconds`` – timer tagged with the final
  ``result`` so dashboards can plot per-method latency.
* ``discovery.method.tokens`` – gauge of the number of addresses yielded
  for that invocation.

### `DiscoveryService._emit_tokens`

The discovery service publishes queue-level metrics whenever batches are
considered for downstream consumers:

* ``discovery.emit.batch_size`` – gauge of candidate batch size.
* ``discovery.emit.queue_depth`` – gauge of ``stage`` (``before``,
  ``after``, ``skipped``) queue depth tagged with ``fresh`` and
  ``metadata_refresh`` to highlight blocking consumers.
* ``discovery.emit.metadata_refresh`` – counter indicating metadata-only
  refreshes, alongside ``discovery.emit.metadata_changed_tokens`` which
  gauges how many tokens changed.
* ``discovery.emit.skipped`` – counter tracking skipped emissions with a
  ``reason`` tag (``empty``, ``unchanged``, ``queue_full``, ``stopping``).

Dashboards can now alert on stalled discovery queues, unexpected error
types, or missing metadata refreshes directly from these signals.

