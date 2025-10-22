# SolHunter Operations UI Blueprint

This specification documents the SolHunter operations console as a contract-driven, single-page WebSocket application. Every panel derives its structure and subscriptions from the `UI_META` handshake before rendering, guaranteeing that any engineer can reproduce the UI pixel-for-pixel and behavior-for-behavior.

---

## 1. Contract-Driven Boot Sequence

1. On load, the application stays in a handshake state. A centered card lists the broker URL, channel, client version, and server version. The spinner inside the card rotates **only** when frames are queued.
2. No panel renders runtime content until `UI_META` has been received and validated. The handshake supplies:
   - `schemas`: topic → schema hash mappings for every stream.
   - `endpoints`: RL, events, and logs WebSocket URLs plus the Redis broker address.
   - `features`: current toggles (test mode, paper mode, RL availability, etc.).
   - `lags`: latest bus lag, depth lag, and golden lag readings.
   - `providers`: list of provider statuses for health displays.
3. Incoming payloads that do not match the negotiated schema hash are queued per topic until a compatible `UI_META` arrives. After a short grace period, the queue is dropped and a red banner explains the mismatch.
4. The shared handshake object feeds every panel, so a runtime contract change (for example, promoting `solhunter-events-v2` to `v3`) is applied everywhere without manual intervention.

---

## 2. Global Frame

### Layout and Skeleton

- The frame combines a fixed top system bar, a left scrollable navigation rail, a flexible multi-column content grid, and a compact bottom status strip.
- Until the handshake completes, the grid is hidden behind the handshake card described above.

### Top System Bar

- Displays the product name, the active environment badge (`PRODUCTION` or `PAPER`), a connectivity pill, and three live metrics: Bus Lag, Depth Lag, and Golden Lag.
- The connectivity pill colors green when connected, amber when degraded, and red when disconnected. A small dot inside the pill pulses each time a WebSocket frame is received.

### Navigation Rail

The rail pins sections in the exact order below and remains scrollable for smaller viewports:

1. Overview
2. Discovery
3. Market
4. Golden
5. Agents
6. Execution
7. Diagnostics
8. Settings

### Bottom Status Strip

- Mirrors the most recent toast message, shows the active schema hash negotiated during the handshake, and displays the operator’s local time.
- The strip updates in real time and never goes blank; when no toast is active it repeats the most recent informative status.

---

## 3. Shared Real-Time Patterns and Formatting

- **Time:** All timestamps render as `x s ago`, adjusted using a client-side clock-drift estimate derived from server heartbeats. Hover tooltips display ISO-8601 timestamps.
- **Numerics:** All numeric columns are right-aligned. Currencies clamp to four significant digits with suffixes (`K`, `M`, `B`). Spreads and percentages show up to two decimal places.
- **Identifiers:** Mints, hashes, and idempotency keys use a fixed-width font and copy-on-click behavior.
- **Staleness:** Any metric older than its TTL fades to 60% opacity and gains an amber dot indicator.
- **Tooling:** Each data table exposes a right-aligned toolbar containing Auto-scroll (default on while streaming), Freeze header, and Export CSV (exports the last 500 cached rows).
- **Sorting:** Numeric columns support sorting. The active direction shows a slim caret and rows animate into place with a 150–200 ms easing; reduced-motion preferences disable the animation.
- **Virtualization & Back-pressure:** Tables virtualize beyond 100 visible rows. If a panel falls behind, it fast-forwards to the latest 250 frames and displays a three-second “fast-forwarded” badge.
- **Tooltips & Empty States:** Tooltips are concise and never repeat the visible label. Empty states repeat runtime terminology (e.g., `Discovery stream idle`, `Awaiting golden stream`).
- **Error Toasts:** Error messages include the provider and action (`Birdeye trending: 521; retrying in 12 s`). Clicking the toast deep-links to the Diagnostics screen filtered to the relevant log entry.

---

## 4. Overview Screen

The Overview screen uses a three-column grid with a 1.2 : 1 : 1 width ratio on large displays and collapses into a single column with sticky tabbed navigation on narrow screens.

### Column 1

- **Session Summary** displays the following metrics as large badges with micro-labels: Suggestions/5m (sparkline plus current value), Acceptance Rate, Golden Hashes, Shadow Fills, Open Vote Windows, Paper Unrealized, Bus Lag, OHLCV Lag, Depth Lag, and Golden Lag. Missing metrics show `—` in muted gray.
- **SolHunter Swarm Lifecycle** exposes toggle badges for Test Mode, Paper Mode, Pause, and RL Shadow. Active toggles are solid; inactive toggles are outlined.

### Column 2

- **Event Bus** reads `Event Bus: connected` when healthy or shows the current error reason.
- **Trading Loop** displays `Trading Loop: running` with a heartbeat count. When paused, the label turns amber, swaps to a pause icon, and reports the paused state.

### Column 3

- **Provider Health** grids Pyth, Helius, Jupiter WS, Dexscreener, Birdeye, Redis, and Depth Service. Each provider line shows status (`ok`, `degraded`, or `down`), the last error message or HTTP status, and the time of last success. Hovering reveals the retry backoff countdown and circuit breaker state when relevant.

---

## 5. Discovery Screen

1. A filter row spans the top with a text search (mint or symbol), a source multi-select, and a minimum-score slider.
2. The **Discovery Console** table subscribes to `x:mint.discovered` and includes columns for Mint (monospace with a middle ellipsis), Score (right-aligned), Source (rounded chip such as `pump.fun`, `raydium`, or `trending:birdeye`), and Observed (relative time).
3. The table header shows `candidates • ⏳` while idle. Once streaming, it updates to `candidates • <n> live` using the live row count.
4. Source chips expose a tooltip with the upstream endpoint, request window, and provider latency.
5. Clicking a row slides in the **Token Facts** drawer from the right. The drawer covers normalized facts: mint, optional symbol, detected venues, discovery sources with timestamps, supply hints, and the initial price snapshot.
6. The drawer’s **Events** tab lists raw bus events tied to the mint, ordered newest to oldest for auditability.

---

## 6. Market Screen

1. The **Market · OHLCV & Depth** table always includes SOL, USDC, and USDT plus any token currently in Golden or with an open vote.
2. Columns cover Mint, Close, Volume, Spread, Depth 1 %, Lag (ms), and Updated.
3. Spread cells include micro green/red badges indicating whether the spread is tightening or widening relative to the previous tick.
4. Depth 1 % and Lag values are right-aligned. When depth data exceeds TTL, the value gains an amber dot and tooltip `stale (age = X s, ttl = Y s)`.
5. Row menus provide `Open in snapshot inspector`, `Pin to Hot Watch`, and `Copy mint` actions.
6. Directly beneath the table, a single-row strip of mini charts renders sparklines for Close, Volume, and order-book imbalance of the highlighted asset. Changing the selected row triggers a 150 ms fade transition.

---

## 7. Golden Screen

1. The **Golden Snapshot Inspector** lists hashes with columns Mint, Hash (monospace and truncated), Price, Liquidity, Lag (ms), and Published (relative time).
2. When Golden is idle, the header reads `0 hashes • ⏳` and a short paragraph underneath explains the missing prerequisite (for example, “awaiting fresh depth and price”).
3. Selecting a row opens a drawer containing the full normalized snapshot payload: decision thresholds in effect, contributing providers for price and depth, alias resolution summary for OHLCV fields, and a verification line confirming `payload hash matches Golden Hash`.
4. The drawer footer hosts a `Promote to Agent Input` badge. It lights when the snapshot satisfies configured thresholds; otherwise it stays gray and lists the unmet criterion (e.g., `min_liquidity`, `min_volume_spike`).

---

## 8. Agents Screen

1. The **Agent Suggestions** panel streams live cards. Each card presents Mint & Symbol on the left, a BUY or SELL lozenge, and key metrics: Notional, Edge, Breakeven, Edge Buffer, Gate, TTL, Age, Inputs Hash, Golden Hash, and Integrity.
2. When guardrails demand action, a narrow red `Must` tag appears.
3. The header reports `0 live • ⏳` whenever the stream is empty.
4. A `Hash mismatch only` toggle filters to cards where `inputs_hash != golden_hash`.
5. Clicking a card opens a detailed suggestion inspector with input features, model score breakdown, expected slippage, and the associated golden snapshot. Field mismatches render a side-by-side diff with amber highlights.
6. If the runtime is in paper mode, the card footer shows a small `PAPER` stamp. In live mode, it instead lists the target venue and bundle strategy.

---

## 9. Execution Screen

Three stacked tables power the execution view:

1. **Vote Window Visualiser** displays Mint, Side, Quorum (numerator/denominator and percentage), Score, Countdown, and Idempotency key. The score cell includes a narrow green gradient bar to convey relative strength.
2. **Shadow Execution** lists virtual fills with Mint, Side, Quantity, Price, Slippage, Snapshot hash short-ID, and Time.
3. **Paper Positions** shows simulated holdings with Side, Quantity, Average Cost, Unrealized, and Total PnL. Positive PnL glows softly in green, negative PnL is muted red, and zero balances remain neutral gray.

An `Exit Queue` pill in the top-right corner tracks queued exits. Clicking it reveals a small list detailing reasons (e.g., `breach: drawdown`, `gate: liquidity decay`) and the ETA for each exit vote.

---

## 10. Diagnostics Screen

1. **Hot Watch** starts as an explanatory empty state until an operator pins a mint. Pinned rows show trend arrows, the most recent provider errors (if any), and a quick `clear` action.
2. **Exit Diagnostics** lists exit rules that have triggered recently, with timestamps and the exact rule text.
3. **Missed Exits** is a paginated audit trail describing opportunities where an exit signal fired but no execution followed within SLA.
4. A collapsible **Logs** viewer subscribes to the logs WebSocket, groups lines by severity, and supports filters for `schema`, `provider`, `execution`, and `bus`. Selecting a log opens a modal that renders the structured payload to debug schema mismatches or stale caches.

---

## 11. Settings Screen

1. **Runtime Controls** appear as switches that dispatch optimistic WebSocket commands. The following toggles are surfaced alongside their negotiated contracts (bus topic and TTL) and a copy-topic icon: Golden Stream, Suggestions Stream, Vote Decisions, Test Mode, Pause, and RL Shadow.
2. A read-only **Contracts & Schemas** section prints the exact topic → schema hash mapping from `UI_META`. Clicking a schema hash reveals only the JSON Schema reference (ref + version), never the full schema.
3. **WS Endpoints** lists the RL, events, and logs sockets (for example, `ws://127.0.0.1:55365/ws/rl`). Each endpoint shows a blinking dot on inbound frames. If the server changes ports during a restart, the UI reconnects automatically, updates the URLs, and surfaces a toast: `Reconnected to runtime (ports changed)`.

---

## 12. Theming and Visual Language

- The default theme is dark with high-contrast neutrals.
- Color assignments stay consistent: green for positive PnL and general success, red for negative PnL and SELL tags, amber for staleness or warnings (including paused states), and blue for neutral informational affordances such as tooltips and links.
- BUY and SELL lozenges adhere to the same palette, and Test Mode / Paper Mode toggles reuse the theme-specific accents.

---

## 13. Responsiveness, Performance, and Real-Time Discipline

- On wide screens the Overview grid honors the 1.2 : 1 : 1 ratio; on narrow screens panels collapse into a single column with sticky tabbed navigation for quick switching.
- Animations default to 150–200 ms easing but respect reduced-motion preferences by disabling transitions.
- WebSocket back-pressure buffers are bounded. If a panel falls behind, it leaps forward (as described earlier) instead of freezing.
- Staleness cues, timestamps, and formatting rules are applied consistently across all panels so operators never see conflicting representations of the same data.

---

## 14. Operability Summary

The SolHunter operations console must never leave the operator guessing. Every stage—from discovery through golden snapshots, suggestions, votes, and fills—presents explicit status, diagnostics, and controls that are locked to the negotiated `UI_META` contracts. Implementing the blueprint above ensures a faithful, professional UI that stays synchronized with the runtime in real time.

