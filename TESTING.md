# Testing

Before running the test suite make sure all dependencies are installed.
Install the package in editable mode with the development extras:

```bash
pip install -e .[dev]
```
The dependency list in `pyproject.toml` pins each package to a
minimum/maximum version range so tests run against consistent builds.

Heavy packages such as `torch`, `transformers` and `faiss-cpu` are
required for the full test suite. Install them along with their
dependencies using the ``full`` extra:

```bash
pip install .[full]
```

Then run the tests from the project root:

```bash
pytest
```

## Continuous integration matrix

GitHub Actions runs a multi-stage workflow that mirrors the local entry
points so failures are isolated to the relevant suite:

* **`unit`** installs the development extras, regenerates protobufs, runs
  lint/bytecode checks and executes the CLI-focused unit tests used in the
  smoke pipeline.
* **`golden`** provisions the heavier `full` dependencies and runs the
  regression suite under `tests/golden_pipeline` to validate the offline
  data/golden signal generation.
* **`ui-smoke`** runs the Flask UI API tests (`tests/test_ui.py`,
  `tests/test_ui_websockets.py`, `tests/test_ui_meta_ws.py`) in headless
  mode to ensure the HTTP/websocket shims continue to work without a
  browser.
* **`chaos`** executes the short `@pytest.mark.chaos` scenarios with
  remediation output written to `artifacts/chaos` before regenerating the
  published runbook.

Each job uploads its diagnostics on failure or success. The unit job always
archives the `docs/observability` bundle, while the chaos job publishes both
`artifacts/chaos` and the generated runbook so investigators can download the
latest remediation bundle from the Actions UI even when tests fail.

## Canary promotion flow

Once the test jobs pass, the `staging-canary` job builds a staging wheel via
`python -m build --wheel --outdir dist` and uploads it as an artifact. The
same job invokes `scripts/bus_smoke.py ping` and `scripts/bus_smoke.py keys`
against the staging Redis bus URL (stored in the `STAGING_REDIS_URL` secret)
to ensure streams and TTL-bound keys behave as expected before promotion. A
failed canary blocks downstream promotion until the bus smoke check passes.

## Demo and paper CLI

`demo.py` and `paper.py` now delegate to the same
`solhunter_zero.simple_bot.run` helper so both workflows share a compact
reporting pipeline.  The demo consumes bundled price presets while the paper
CLI can optionally fetch live data before running the identical investor
engine.

Run the demo against the bundled dataset:

```bash
python demo.py --reports reports --preset short
```

Run the paper CLI and fetch recent prices from Codex:

```bash
python paper.py --reports reports --fetch-live
```

The `--fetch-live` flag downloads SOL/USD candles from a public Codex
endpoint and falls back to bundled samples when the request fails.  Set
`SOLHUNTER_PATCH_INVESTOR_DEMO=1` when heavy dependencies such as `torch`
are unavailable so that lightweight stubs are used instead.  Offline
environments may omit `--fetch-live` or supply `--ticks`/`--preset` to run
entirely on local data.

The test suite can fetch a tiny slice of these candles via
``solhunter_zero.datasets.live_ticks.load_live_ticks``.  When the network is
unreachable the loader returns an empty list and dependent tests skip
automatically.

## Investor demo

The investor demo performs a small rolling backtest and writes lightweight
reports for each strategy. Run its test directly to generate these files:

```bash
pytest tests/test_investor_demo.py
```

The test stores `summary.json`, `summary.csv`, `trade_history.json` and
`highlights.json` in a temporary reports directory. Each entry lists the
configuration name along with metrics such as ROI, Sharpe ratio, maximum
drawdown and final capital for strategies like `buy_hold`, `momentum` and
`mixed`. Inspect any of the files to compare strategy performance.

After running the CLI, inspect the generated reports as needed:

```bash
head reports/trade_history.json
python -m json.tool reports/highlights.json
```

## Paper Trading

Execute the lightweight paper trading workflow which wraps the investor demo:

```bash
pytest tests/test_paper.py
```

Run the CLI directly with live prices:

```bash
python paper.py --reports reports --fetch-live
```

For a quick pre-flight smoke test before enabling live trading run:

```bash
python paper.py --test
# or
make paper-test
```

This downloads a slice of live data, exercises the trading loop in dry-run
mode and writes reports to ``reports/`` by default.

## Startup integration flow

Verify the launcher and startup script integration without invoking the full
stack by running:

```bash
pytest tests/test_startup_sequence.py
```

To run the static analysis checks used in CI, execute:

```bash
python -m compileall .
flake8
```
