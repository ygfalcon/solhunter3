# Data Contracts

This document defines the canonical payloads that flow across the SolHunter event bus.  All
messages are JSON objects serialized into Redis streams unless stated otherwise.

## Stream Payloads

### `x:discovery.candidates` — `DiscoveryCandidate`

| Field | Type | Description |
| ----- | ---- | ----------- |
| `mint` | `str` | Mint address under evaluation. |
| `asof` | `str` (ISO-8601) | Timestamp when the mint was observed. |
| `score` | `float` | Optional score produced by upstream scanners. |
| `source` | `str` | Discovery source identifier. |
| `cursor` | `str` | Pagination cursor for DAS backfills. |

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
| `close` | `float` | Close price (USD). |
| `volume` | `float` | Base asset volume. |
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
