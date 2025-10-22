# Data Contracts

This document defines the canonical payloads that flow across the SolHunter event bus.  All
messages are JSON objects serialized into Redis streams unless stated otherwise.

## Stream Payloads

### `x:mint.discovered` — `DiscoveryCandidate`

`x:mint.discovered` carries v1 `DiscoveryCandidate` payloads. For backward
compatibility the identical JSON body is also emitted on
`x:discovery.candidates`.

| Field | Type | Description |
| ----- | ---- | ----------- |
| `mint` | `str` | Candidate mint address. |
| `asof` | `float` | Epoch timestamp (seconds) when the mint was observed. |
| `source` | `str` | Primary discovery source slug (lower-case, e.g. `das`, `pumpfun`). |
| `sources` | `list[str]` | Normalised list of contributing sources for display/tooling. |
| `v` | `str` | Schema version identifier (`"1.0"`). |

### `x:token.snap` — `TokenSnapshot`

| Field | Type | Description |
| `mint` | `str` | Mint address. |
| `symbol` | `str` | Token symbol. |
| `name` | `str` | Human readable token name. |
| `decimals` | `int` | SPL decimals. |
| `token_program` | `str` | Owning token program. |
| `flags` | `list[str]` | Classification flags (mint authority, freeze state, etc.). |
| `venues` | `list[str]` | Enabled venues. |
| `asof` | `str` | Snapshot timestamp. |

### `x:market.ohlcv.5m` — `OhlcvBar`

| Field | Type | Description |
| `mint` | `str` | Mint address. |
| `open` | `float` | Opening price (USD). |
| `high` | `float` | High price (USD). |
| `low` | `float` | Low price (USD). |
| `close` | `float` | Close price (USD). (`c` alias) |
| `volume` | `float` | USD notional volume (legacy alias for `vol_usd`). |
| `volume_usd` | `float` | USD notional volume. |
| `volume_base` | `float` | Base asset volume. |
| `schema_version` | `str` | Contract version identifier (`x:market.ohlcv.5m@v2`). |
| `content_hash` | `str` | Canonical hash of the published payload. |
| `asof_open` | `str` | Opening timestamp. |
| `asof_close` | `str` | Close timestamp. |

### `x:market.depth` — `DepthSnapshot`

| Field | Type | Description |
| `mint` | `str` | Mint address. |
| `mid_usd` | `float` | Mid price. |
| `spread_bps` | `float` | Quoted spread. |
| `depth_pct` | `dict[str, float]` | Depth available at 1/2/5% bands. |
| `venues` | `list[DepthLevel]` | Venue level book stats. |
| `asof` | `str` | Depth timestamp. |

The `depth_pct` map uses canonicalised string percentage keys (for example `"1"`,
`"2"`, `"5"`) to represent executable depth across slippage bands. Producers may
emit numeric keys or include a trailing percent sign; the Golden pipeline will
normalise those keys before publishing snapshots. When `depth_pct` is omitted the
service synthesises the standard 1/2/5% buckets using the aggregate `depth`,
`bids`, and `asks` values so downstream consumers always see usable depth bands.

### `x:mint.golden` — `GoldenSnapshot`

| Field | Type | Description |
| `mint` | `str` | Mint address. |
| `hash` | `str` | Deterministic hash of the aggregated snapshot. |
| `px` | `float` | Golden fair value. |
| `liq` | `float` | Liquidity score. |
| `ohlcv5m` | `OhlcvBar` | Last 5m bar used for the snapshot. |
| `meta` | `TokenSnapshot` | Token metadata reference. |
| `asof` | `str` | Publication timestamp. |
| `px_mid_usd` | `float` | Flattened mid price (`px.mid_usd`). |
| `px_bid_usd` | `float` | Flattened executable bid (`px.bid_usd`). |
| `px_ask_usd` | `float` | Flattened executable ask (`px.ask_usd`). |
| `liq_depth_0_1pct_usd` | `float` | Liquidity within 10 bps (`liq.depth_usd_by_pct["0.1"]`). |
| `liq_depth_0_5pct_usd` | `float` | Liquidity within 50 bps (`liq.depth_usd_by_pct["0.5"]`). |
| `liq_depth_1_0pct_usd` | `float` | Liquidity within 100 bps (`liq.depth_usd_by_pct["1.0"]`). |
| `liq_depth_1pct_usd` | `float` | Flattened 1% depth (`liq.depth_usd_by_pct["1"]`). |
| `degraded` | `bool` | True when falling back to synthetic (Pyth-derived) spreads. |
| `source` | `str` | Depth source identifier (`"jup_route"` or `"pyth_synthetic"`). |
| `staleness_ms` | `float` | Milliseconds since depth was observed. |
| `schema_version` | `str` | Contract version identifier (`x:mint.golden@v2`). |
| `content_hash` | `str` | Canonical hash of the payload contents. |

When `liq.route_meta` is present it includes the routed hop count, the ordered list of DEX venues, the adapter latency (`latency_ms`), and a compact `sweeps` array describing the most recent notional probes with their observed slippage.

### Field alias map

- `x:market.ohlcv.5m`: `close` mirrors `c`; `volume` and `volume_usd` mirror `vol_usd`; `volume_base` mirrors `vol_base`.
- `x:mint.golden`: `px_mid_usd` mirrors `px.mid_usd`; `px_bid_usd` mirrors `px.bid_usd`; `px_ask_usd` mirrors `px.ask_usd`; liquidity aliases mirror the corresponding `liq.depth_usd_by_pct` entries.

### `x:trade.suggested` — `TradeSuggestion`

| Field | Type | Description |
| `agent` | `str` | Agent identifier. |
| `mint` | `str` | Target mint. |
| `side` | `str` | `"buy"` or `"sell"`. |
| `notional_usd` | `float` | Suggested order notional. |
| `edge` | `float` | Edge proxy / expected alpha. |
| `risk` | `dict[str, float]` | Risk metrics (kelly, drawdown, etc.). |
| `max_slippage_bps` | `float` | Max tolerated slippage. |
| `inputs_hash` | `str` | Golden snapshot hash used as input. |
| `ttl_sec` | `int` | Suggestion validity horizon. |
| `asof` | `str` | Timestamp. |

### `x:vote.decisions` — `VoteDecision`

| Field | Type | Description |
| `mint` | `str` | Mint address. |
| `side` | `str` | Trade direction. |
| `clientOrderId` | `str` | Deterministic identifier for dedupe. |
| `snapshot_hash` | `str` | Golden snapshot hash used in the vote. |
| `score` | `float` | Weighted vote score. |
| `agents` | `list[str]` | Participating agents. |
| `notional_usd` | `float` | Approved notional. |
| `ts` | `str` | Decision timestamp. |

### `x:virt.fills` — `VirtualFill`

| Field | Type | Description |
| `order_id` | `str` | Synthetic order id. |
| `mint` | `str` | Mint address. |
| `side` | `str` | Fill direction. |
| `qty_base` | `float` | Filled base quantity. |
| `price_usd` | `float` | Execution price. |
| `fees_usd` | `float` | Estimated fees. |
| `slippage_bps` | `float` | Observed slippage. |
| `snapshot_hash` | `str` | Snapshot hash backing the execution. |
| `ts` | `str` | Fill timestamp. |

### `x:live.fills` — `LiveFill`

`LiveFill` extends `VirtualFill` with execution metadata.

| Field | Type | Description |
| `sig` | `str` | On-chain signature. |
| `route` | `str` | Venue / route used for execution. |

## Key-Value Records

### `discovery:seen:{mint}`
Stores the last discovery timestamp per mint.  TTL: 24h.

### `discovery:cursor`
Stores the current DAS pagination cursor.  TTL: 24h.

### `snap:golden:{mint}`
Caches the most recent golden snapshot hash per mint.  TTL: 90s.

### `vote:dupe:{clientOrderId}`
Deduplication flag for votes.  TTL: 5 minutes.

## Pipeline Metrics

The `GoldenPipeline.metrics_snapshot()` helper exposes rolling summaries for
published snapshots and stage health. Discovery counters are tracked as
monotonic totals and surface under the following metric names (retrieve the
current total via the `max` field in the snapshot output):

| Metric | Description |
| ------ | ----------- |
| `discovery.success_total` | Number of discovery candidates accepted and emitted. |
| `discovery.failure_total` | Upstream discovery failures recorded via `mark_failure`. |
| `discovery.dedupe_drops` | Candidates dropped because they were recently seen. |
| `discovery.breaker_openings` | Count of circuit-breaker open events triggered by failures. |
