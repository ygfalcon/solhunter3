# UI Topic Map

This guide maps each UI panel to its backing stream or API, the fields that must be rendered, and
staleness rules.

## Swarm Summary

- **Endpoint:** `/summary`
- **Fields:** `evaluation`, `execution`, `paper_pnl`, `golden`, `seeded`.
- **Seeded tokens:** `seeded.tokens` surfaces each configured mint with `canonical_mint`,
  `status`, and the resolved `pyth_feed_id`/`pyth_account` (when available). Tokens without a
  configured Pyth identifier are marked `status=missing` so the header can highlight gaps.
- **Validation:** The list is built from `SEED_TOKENS` and normalised using the configured Pyth
  overrides (`PYTH_PRICE_IDS` plus defaults). Missing or malformed identifiers are logged during
  startup and called out in the summary for quick remediation.

## Discovery & Enrichment

### Discovery Panel
- **Topic:** `x:discovery.candidates`
- **Fields:** `mint`, `asof`, `score`
- **Staleness:** Badge if `now - asof > 120s`
- **Interaction:** Click row → request enrichment details.

### Token Facts Panel
- **Topic:** `x:token.snap`
- **Fields:** `mint`, `symbol`, `name`, `decimals`, `token_program`, `flags`, `venues`, `asof`
- **Join:** `mint` from the discovery panel.
- **Staleness:** Badge if `now - asof > 5m` or token metadata missing.

## Market State

### OHLCV (5m)
- **Topic:** `x:market.ohlcv.5m`
- **Fields:** Candle chart by `mint`, `open`, `high`, `low`, `close`, `volume` (USD) and `volume_base`, `asof_close`
- **Staleness:** Badge if `now - asof_close > 2m`.

### Depth & Spread
- **Topic:** `x:market.depth`
- **Fields:** `mid_usd`, `spread_bps`, depth at 1/2/5%, per-venue table, `asof`
- **Staleness:** Badge if `now - asof > 30s` or spread > guardrail.

## Golden Snapshot

### Golden Panel
- **Topic:** `x:mint.golden`
- **Fields:** `hash`, `meta`, `px_mid_usd`, `px_bid_usd`, `px_ask_usd`, `liq_depth_0_1pct_usd`, `liq_depth_0_5pct_usd`, `liq_depth_1_0pct_usd`, nested `px`/`liq` (with depth bands + `route_meta`), embedded `ohlcv5m`, `asof`, `source`, `degraded`
- **Route badge tooltip:** When `source=jup_route`, the badge hover shows recent sweep notionals and their observed impact from `liq.route_meta.sweeps`; synthetic (`source=pyth_synthetic`) rows show the confidence-derived spread. `liq.route_meta.latency_ms` is rendered in the detail line to surface adapter latency.
- **Badges:** Publish cadence stats (emit rate, change ratio).  Flag if `now - asof > 60s`.

## Agents

### Agent Suggestions
- **Topic:** `x:trade.suggested`
- **Fields:** `agent`, `mint`, `side`, `notional_usd`, `edge`, `risk`, `max_slippage_bps`, `inputs_hash`, `ttl_sec`, `asof`
- **Filtering:** Highlight rows where `inputs_hash` differs from latest Golden hash.
- **Metrics:** Display suggestions/sec (rolling 5m) and acceptance rate from vote decisions.
- **Staleness:** Badge if `now - asof > ttl_sec`.

## Swarm Vote

### Vote Windows
- **Topics:** `x:trade.suggested` (grouped by `mint`, `side`, `inputs_hash`) and `x:vote.decisions`
- **Fields:** quorum size, normalized score, threshold line, countdown (derived from `VOTE_WINDOW_MS`), contributing agents.
- **Staleness:** Badge if no decision within `VOTE_WINDOW_MS` of the first suggestion.

### Decisions
- **Topic:** `x:vote.decisions`
- **Fields:** `mint`, `side`, `notional_usd`, `score`, `snapshot_hash`, `clientOrderId`, `agents`, `ts`
- **Idempotency:** Show duplicate indicator when same `clientOrderId` re-appears.
- **Staleness:** Badge if `now - ts > 3m`.

## Execution & PnL

### Shadow Fills (Paper)
- **Topic:** `x:virt.fills`
- **Fields:** `order_id`, `mint`, `side`, `qty_base`, `price_usd`, `fees_usd`, `slippage_bps`, `snapshot_hash`, `ts`
- **Staleness:** Badge if no fill for active mint >15m.

### Paper Positions
- **Endpoint:** `/api/paper/positions` (Redis backed KV)
- **Fields:** `mint`, `side`, `qty_base`, `avg_cost`, `realized_usd`, `unrealized_usd`, `total_pnl_usd`
- **Staleness:** Badge if API response older than 1m.

### Live Fills
- **Topic:** `x:live.fills`
- **Fields:** same as virtual plus `sig`, `route`
- **Staleness:** Badge if live trading enabled but no fill in 10m.

### PnL Summary
- **Endpoint:** `/api/pnl/summary`
- **Fields:** Aggregated PnL by agent, family, and total, including turnover, Sharpe proxy, drawdown.
- **Staleness:** Badge if summary older than 5m.

### Exit Diagnostics
- **Endpoint:** `/swarm/exits` (served by `TradingRuntime._collect_exit_panel`).
- **Topic:** `x:swarm.exits` / `x:exit.panel` when orchestrated via `RuntimeEventCollectors`.
- **Fields:** `hot_watch`, `diagnostics`, `queue`, `closed`, `missed_exits` (all derived from `ExitManager.summary()`).
- **Staleness:** Snapshot of the latest runtime state; highlight if unchanged for >2m while positions are open.

## Reinforcement Learning

### RL Weights
- **Topic:** `rl:weights.applied`
- **Fields:** `mint`, `window_hash`, per-agent multipliers, `asof`
- **Staleness:** Badge if >2 windows without update.

### RL Uplift
- **Topic:** Derived metric using `x:vote.decisions` and `/api/paper/pnl`
- **Fields:** Plain vs RL scores, rolling paper PnL uplift.
- **Staleness:** Badge if uplift computation older than 10m.

## Kill Switch Controls

Each control issues a POST to `/api/control/*` and persists to Redis.

| Control | Endpoint | Backing Key | Expected Propagation |
| ------- | -------- | ----------- | -------------------- |
| Global Pause | `/api/control/pause` | `control:execution:paused` | < 1 vote window |
| Paper-only | `/api/control/paper` | `control:execution:paper_only` | Immediate |
| Family Budget | `/api/control/budget/{family}` | `control:budget:{family}` | < 1 window |
| Spread Gate | `/api/control/spread` | `MAX_SPREAD_BPS` env override | < 1 window |
| Depth Gate | `/api/control/depth` | `MIN_DEPTH1PCT_USD` env override | < 1 window |
| RL Toggle | `/api/control/rl` | `RL_WEIGHTS_DISABLED` | < 1 window |
| Blacklist | `/api/control/blacklist` | `control:blacklist` | < 1 window |

## Backpressure & Websocket Guidance

- **Websocket endpoint:** `wss://<host>/ws/bus`
- **Backpressure:** client must acknowledge each message; server drops clients that are >200ms
  behind.
- **Historical fetch:** use `/api/history/{stream}?mint=...&limit=...` for cold start.
