# SolHunter Blueprint 1.0

This document captures the end-to-end event pipeline for SolHunter's discovery, inference, and execution loop. Every stage produces append-only events, honours idempotent keys, and keeps TTL caches short to suppress duplicate emissions. Golden Snapshots are the single source of truth for inference; agents never perform network I/O and only read these snapshots.

## High-Level Flow

1. **Discovery** → surfaces active, tradable mints from Helius DAS.
2. **Enrichment** → enriches mints with token metadata and flags.
3. **Market Data** → normalises on-chain swap flow into 5-minute OHLCV bars.
4. **Depth & Spread** → publishes executable liquidity metrics per venue.
5. **Golden Snapshot** → coalesces token metadata, price, depth, and candles.
6. **Agents** → produce deterministic trade suggestions from Golden Snapshots.
7. **Swarm Voting** → builds consensus and emits at most one decision per side.
8. **Execution** → shadow (paper) and live channels turn decisions into fills.
9. **PnL & Learning** → paper PnL drives reinforcement learning weight updates.

---

## 0. Ground Rules

- Golden Snapshots (token identity, closed 5-minute OHLCV, depth/spread) are the canonical input to agents.
- Agents never perform outbound I/O; they read Golden Snapshots and emit `TradeSuggestion` events.
- Decisions are intent only—executors (shadow/live) translate them into fills.
- Reinforcement learning consumes paper PnL exclusively.
- All event streams are append-only. Keys are idempotent and caches (with short TTLs) suppress duplicate processing.

## 1. Discovery (Helius DAS)

**Goal:** Continuously surface newly active, tradable mints without running raw RPC scans.

**Input:** `searchAssets` results from Helius DAS, sorted by `recentAction`.

**Process:**

- Page size 100–120, target ~3 requests/second with up to a 6-request burst for cursor advancement.
- Normalise candidate addresses (base58 length check) and drop known program/router prefixes (Jupiter router, Tokenkeg, Phoenix, Orca whirlpools, Vote111, etc.).
- Validate via `getAssetBatch` keeping only `FungibleToken` or `FungibleAsset` entries, with `0 ≤ decimals ≤ 12`. Record the `token_program` (Tokenkeg vs Token-2022).
- De-duplicate using `discovery:seen:{mint}` in Redis with a 24h TTL.
- Persist the cursor (`discovery:cursor`) for resume on restart.
- Apply a circuit breaker after 5 consecutive failures within 30 seconds, cooling down for 60 seconds.

**Output Event:**
```json
DiscoveryCandidate {
  mint,
  asof
}
```

**SLOs:** `429` rate < 0.5%; candidate latency < 2s p95.

## 2. Enrichment (Token Facts)

**Goal:** Convert a raw mint into a tradable identity.

**Input:** `DiscoveryCandidate` events.

**Process:** Batch `getAssetBatch` lookups (cache-aware) to retrieve symbol, name, decimals, `token_program`, optional venue hints, and safety flags.

**Output Event:**
```json
TokenSnapshot {
  mint,
  symbol,
  name,
  decimals,
  token_program,
  venues?: [],
  flags?: {},
  asof
}
```

**SLO:** `DiscoveryCandidate` → `TokenSnapshot` < 1.5s p95.

## 3. Market Tape (Real Swaps Only)

**Goal:** Build flow and price context from chain activity while excluding self trades.

**Input:** Enhanced webhooks (preferred) or light polling for swaps on tracked venues.

**Process:** Normalise each swap into a `TapeEvent`:
```json
{
  mint_base,
  mint_quote,
  amount_base,
  amount_quote,
  route,
  program_id,
  pool,
  signer,
  signature,
  slot,
  ts,
  fees_base,
  price_usd,
  fees_usd,
  is_self
}
```
Exclude `is_self == true` from analytics (retain for audit). Roll 5-minute closed bars into `OHLCV` records:
```json
{
  mint,
  o,
  h,
  l,
  c,
  vol_usd,
  trades,
  buyers,
  zret,
  zvol,
  asof_close
}
```

**SLOs:** Emit candle close ≤ 2s after window end; maintain zero self-trade contamination.

## 4. Depth & Spread (Real Liquidity)

**Goal:** Publish executable liquidity signals per venue.

**Input:** Rust depth sampler or AMM reserve math.

**Output Event:**
```json
Depth {
  mint,
  venue,
  mid_usd,
  spread_bps,
  depth_pct: {"1": USD, "2": USD, "5": USD},
  asof
}
```
Optionally publish venue-aggregated summaries per mint.

**SLO:** Depth staleness < 6s p95.

## 5. Snapshot Coalescer (Golden Snapshot)

**Goal:** Assemble the only input that agents consume.

Join, per mint, the latest `TokenSnapshot`, closed `OHLCV` bar, and freshest `Depth` sample. Emit a Golden Snapshot only when content changes:
```json
{
  mint,
  asof,
  meta: { symbol, decimals, token_program },
  px:   { mid_usd, spread_bps },
  liq:  { depth_pct: {"1": USD, "2": USD, "5": USD} },
  ohlcv5m: { o, h, l, c, vol_usd, buyers, zret, zvol },
  hash
}
```

- Coalesce on ~250 ms intervals.
- Cache last hash in `snap:golden:{mint}` with a 90s TTL.

**SLO:** Input change → new Golden Snapshot < 300 ms p95; at least 40% fewer emissions than naïve broadcasting.

## 6. Agents (Pure Inference)

**Goal:** Deterministically convert Golden Snapshots into `TradeSuggestion` events.

**Input:** Golden Snapshot stream.

**Gates:** Ensure `spread_bps ≤ MAX_SPREAD_BPS`, `depth_pct["1"] ≥ MIN_DEPTH1PCT_USD`, respect blacklists/cooldowns.

**Styles:** Momentum/trend, arbitrage (cross-venue mids/spreads/depth), anomaly/flow detection.

**Simulation:** Local simulator (no HTTP) to estimate expected fill given depth curve, latency bps, and fee bps. Enforce max slippage.

**Output Event:**
```json
TradeSuggestion {
  agent,
  mint,
  side,
  notional_usd,
  max_slippage_bps,
  risk: { stop_bps, take_bps, time_stop_sec },
  confidence,
  inputs_hash,
  ttl_sec
}
```
Same Golden Snapshot must always produce the same suggestions.

## 7. Swarm Voting (Windowed Consensus)

**Goal:** Select at most one action per (mint, side, snapshot) window.

**Input:** `TradeSuggestion` events.

- Window 300–500 ms per (mint, side, `inputs_hash`).
- Drop suggestions with stale snapshot hashes; enforce global gates and per-family budgets.
- Require quorum (≥ 2 agents by default).
- Score suggestions via normalised weighted confidence (base equal weights, optionally incorporate RL and performance weights).
- Accept only if score ≥ `SWARM_MIN_SCORE` (e.g., 0.04).

**Output Event:**
```json
Decision {
  mint,
  side,
  notional_usd,
  score,
  snapshot_hash,
  clientOrderId,
  agents[],
  ts
}
```

Use `clientOrderId = sha256(mint|side|rounded_notional|snapshot_hash|sorted_agents|time_bucket)` and cache for 5 minutes to ensure idempotency.

## 8. Execution Paths

### 8a. Shadow (Paper / Virtual)

- **Input:** `Decision` events.
- **Fill Model:** Estimate fill price as `mid ± (half-spread + impact + latency + fees)` bps respecting `max_slippage_bps`.
- **Output Event:**
```json
VirtualFill {
  order_id,
  mint,
  side,
  qty_base,
  price_usd,
  fees_usd,
  slippage_bps,
  snapshot_hash,
  route: "VIRTUAL",
  ts
}
```
- Maintain paper positions (moving-average cost) and mark-to-market using `mid ± half-spread`.
- Feed paper PnL to RL and agent scoring. Shadow fills never feed back into OHLCV.

### 8b. Live (On-Chain)

- **Input:** `Decision` events.
- **Flow:** Re-quote (short TTL), sign, submit via RPC, confirm via websocket.
- **Output Event:**
```json
LiveFill {
  sig,
  mint,
  side,
  qty_base,
  price_usd,
  fees_usd,
  slippage_bps,
  route,
  snapshot_hash
}
```
- Maintain live positions & PnL. When swaps appear on-chain, mark them `is_self = true` and exclude from OHLCV.

**Guardrails:** `MAX_SPREAD_BPS`, `MIN_DEPTH1PCT_USD`, `DEPTH_FRACTION_CAP`, per-mint cooldowns, `Kelly × 0.15` cap, daily budgets, global drawdown brakes.

## 9. RL Alignment (Optional Weighting Role)

- **Goal:** Adjust agent voting weights using paper PnL without influencing execution directly.
- **Observations:** Golden features plus optional per-agent stats (Sharpe ratio, drawdown, freshness).
- **Action:** Publish non-negative weights per agent (contextual bandit formulation).
- **Reward:** Paper PnL from decisions during the weight application window, penalising turnover/slippage.
- **Transport:** Subscribe to Golden Snapshots and `VirtualFill`; publish weights tagged to the active window.
- **Safety:** Run in shadow mode first. Compare `score_plain` vs `score_rl` and ensure sustained paper PnL uplift before enabling.

## 10. Observability & SLOs

Monitor each stage:

- **Discovery:** DAS requests/sec, 429 rates, page latency, cursor resumes.
- **Enrichment:** Batch latency, failure rate.
- **Tape/Depth:** Candle close latency, depth staleness, mid/slippage bias.
- **Golden:** Publish rate, coalesce ratio, change→publish latency.
- **Agents:** Suggestions/sec, p95 compute time, acceptance rate.
- **Voting:** Window hit ratio, quorum misses, dedupe rate, score distribution.
- **Execution:** Quote→fill latency, slippage vs expected, failure/timeout rates.
- **PnL:** Paper/live per agent & family, turnover, drawdown, exposure heatmap.
- **RL:** Shadow uplift, calibration drift, weight freshness, kill-switch status.

## 11. Operating Cadences (Defaults)

- Discovery: 45s sweep; DAS RPS ≈ 3; batch 100–120; 24h dedupe TTL.
- Enrichment: Batch 75; warm cache TTL 60s.
- Tape: Webhooks preferred; fallback polling every 5–10s.
- Depth: Sample every 2–5s.
- Coalescer: 250 ms coalesce interval; 90s hash TTL.
- Voting: 300–500 ms window, quorum ≥ 2, threshold tuned to realised paper outcomes.

## 12. Acceptance Checklist

Pipeline is production-ready when:

- Discovery emits validated mints; 429 rate < 0.5%.
- Token facts appear ≤ 1.5s after discovery.
- 5-minute OHLCV closes on time with no self-trade contamination.
- Depth staleness < 6s and spreads match venue expectations.
- Golden Snapshots publish only on change and end-to-end latency < 300 ms p95.
- At least one agent emits suggestions from Golden without external I/O.
- Voting produces ≤ 1 decision per (mint, side, snapshot); idempotency verified.
- Shadow executor generates `VirtualFill` events; paper PnL matches buys/sells/partials.
- RL (if shadowed) delivers weights in-window ≥ 90% of the time with positive, stable paper uplift.

## TL;DR

- DAS discovery → normalised, validated mints.
- Enrichment → token facts (symbol, decimals, program).
- Tape (excluding self) → closed 5-minute OHLCV + flow context.
- Depth → mid price, spread, depth @ 1/2/5%.
- Coalescer → Golden Snapshot (hash dedupe, publish on change).
- Agents → deterministic suggestions from Golden only.
- Vote → consensus decision with quorum, threshold, optional RL weighting.
- Shadow → virtual fills and paper PnL (learning loop).
- Live → real fills, positions, risk checks; tape excludes self trades.

