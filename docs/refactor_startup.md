# Startup Refactor Proposal — SolHunter Zero

Goal: streamline and harden startup into a clear, observable pipeline that decouples UI, event bus, and agent runtime while preserving current capabilities (discovery, deliberation, arbitrage/MEV, RL training, evolution).

## Current State — Findings

- Startup logic is spread across multiple modules: `solhunter_zero.ui`, `solhunter_zero.main`, `solhunter_zero.loop`, `scripts/startup.py`, `start_all.py`.
- Event bus init/checks are duplicated (UI `start`, `main.perform_startup`, `loop.trading_loop`), and UI attempts to manage Redis locally.
- UI starts its own websockets (logs/rl/events) and can also kick off trading; the trading loop may also start the event bus WS if not reachable.
- Health gates exist (`start_all.py`, UI self-test), but they are not the single source of truth for runtime lifecycle.
- Agent execution runs inside the main loop, but the intended “swarm via event bus” model is not yet the primary path.

Impact: race-prone boot, interleaved responsibilities, harder observability, and more complex failure handling.

## Target Startup Pipeline

1) Start UI
   - UI boots fast and remains passive: subscribes to status/events and exposes controls, but does not own core runtime startup.
   - Health panels show staged progress as the rest of the system comes up.

2) Start Event Bus/Server
   - Initialize broker(s) and start the local bus websocket once (single owner).
   - Expose `healthz`/`/bus/status` endpoints and emit periodic heartbeats.

3) Start Agents Runtime
   - Load `AgentManager` and sub‑agents; register with bus; subscribe to discovery/price/rl topics.
   - Wire RL daemon as an optional service that publishes `rl_weights` to the bus.

4) Agents Communicate on Bus
   - Agents publish `token_discovered`, `price_update`, `signals` and `action_proposals`; subscribe to `rl_weights`, `risk_updated`, `portfolio_updated`.

5) Agents Connect to Chain & Discover Tokens
   - Discovery workers connect to Solana WS/RPC and/or BirdEye; publish `token_discovered`.

6) Agents Deliberate
   - A Swarm Coordinator collects `action_proposal` events over a short quorum window and produces weighted decisions.

7) Swarm Makes Buy/Sell Decisions
   - Decision events are published to `action_decision`; the Trade Executor places orders or submits bundles.

8) Continuous Debate/Evolution
   - Periodic evaluation rotates weights, mutates/culls underperforming agents, and persists evolution state; debates continue as new tokens arrive.

## Architecture Changes (Minimal, Focused)

- New `RuntimeOrchestrator` (single owner of lifecycle)
  - Module: `solhunter_zero/runtime/orchestrator.py`
  - Methods: `start_ui()`, `start_bus()`, `start_agents()`, `start_rl_daemon()`, `stop_all()`.
  - Provides staged events on the bus: `runtime.stage_changed`, `runtime.ready`.

- Event Bus as a Service
  - Keep all bus start/stop/verify inside `event_bus` (already has `start_ws_server`, `verify_broker_connection`).
  - Remove/avoid bus init from `ui.start`, `loop.trading_loop`, and `main.perform_startup` once orchestrator owns this.
  - Add a tiny `/bus/status` UI route that reads `event_bus` health only.

- UI Simplification
  - `ui.create_app` remains, but it no longer spawns trading nor bus; it publishes control messages: `control.start`, `control.stop`, `control.reload_config`.
  - UI websockets stay (logs/events/rl), but their startup is delegated by orchestrator for ordering consistency.

- Agents Runtime (event‑driven first)
  - Introduce `solhunter_zero/agents/runtime/agent_runtime.py` (exposed via `solhunter_zero.agents.runtime.AgentRuntime`):
    - Subscribes to `token_discovered`, `price_update`, `rl_weights`, `risk_updated`.
    - Publishes `action_proposal` (buy/sell/size/score).
  - Swarm Coordinator consolidates proposals into `action_decision` using weights from RL/heuristics.
  - Trade Executor handles chain interactions, portfolio updates, and emits `trade_logged`.

- Execution/MEV
  - Wrap existing execution paths into `TradeExecutor` and `MEVExecutor` services.
  - Topics: `bundle_submitted`, `bundle_failed`, `action_executed` (already present).

- Health & Observability
  - Keep `start_all.py` as pre‑startup gates; orchestrator subscribes to `/healthz` from components and emits `runtime.stage_changed`.
  - Standardize heartbeats: `heartbeat:{service}` or `heartbeat` payload with `service`.

## Concrete Step‑By‑Step Refactor

Phase 1 — Orchestrator skeleton (no behaviour change by default)
- Add `solhunter_zero/runtime/orchestrator.py` with staged methods calling existing functions:
  - `start_bus()` → `config.initialize_event_bus()`, `event_bus.verify_broker_connection()`, `event_bus.start_ws_server()`.
  - `start_ui()` → `ui.create_app()` and UI websockets via `ui.start_websockets()`.
  - `start_agents()` → reuse `main.perform_startup` for connectivity/depth_service, then launch `loop.trading_loop` with current `AgentManager`.
  - Wire a CLI `python -m solhunter_zero.runtime` that executes stages in order.
- Gate behind `NEW_RUNTIME=1` so the default path remains unchanged.

Phase 2 — Responsibility cleanup
- Remove bus init and redis management from `ui.start`; replace with bus health checks and control messages.
- Remove bus WS bootstrap from `loop.trading_loop`; assume orchestrator owns it.
- Keep `start_all.py` as a guard; orchestrator runs it or replicates checks via `health_runtime` helpers.

Phase 3 — Event‑driven agents
- Add `agents/runtime/agent_runtime.py` (re-exported via `agents.runtime`) that:
  - Spins token discovery workers and publishes `token_discovered`.
  - Converts `AgentManager.execute(token, portfolio)` into handlers that produce `action_proposal` messages.
  - Collects proposals in a small time window and emits `action_decision`.
  - For backward compat, still supports current per‑iteration call path.

Phase 4 — Evolution loop & MEV service
- Lift existing `AgentManager.evolve()` cadence into a dedicated periodic task.
- Create `execution/service.py` with `TradeExecutor` (spot) and `MEVExecutor` (bundles via Jito) and subscribe to `action_decision`.

## Bus Topics (proposed additions)

- `control.start`, `control.stop`, `control.reload_config`
- `runtime.stage_changed` {stage, ok, detail}
- `action_proposal` {token, side, size, score, agent}
- `action_decision` {token, side, size, rationale}
- `bundle_submitted` / `bundle_failed` {txids, error}

Existing topics continue to be used: `token_discovered`, `price_update`, `rl_weights`, `risk_updated`, `trade_logged`, `rl_metrics`, `heartbeat`.

- `token_discovered` now carries structured payloads such as `{ "tokens": [...], "method": "helius", "details": {...} }` so subscribers can reason about discovery provenance while still accepting legacy plain lists.
- Emit lightweight telemetry on `telemetry.discovery_method` capturing the active discovery method selected during startup.

## Migration & Rollout Plan

1) Land orchestrator + CLI behind `NEW_RUNTIME=1` (read‑only integration).
2) Gradually switch `scripts/startup.py` and `launcher` to call the orchestrator when the flag is set.
3) Remove duplicated bus init from UI and loop; keep compatibility shims for one release.
4) Convert the agent loop to publish `action_proposal` and add Swarm Coordinator; wire executor to `action_decision`.
5) Turn the flag on by default once health checks and soak tests pass.

## Testing Strategy

- Unit: orchestrator stages emit `runtime.stage_changed` with correct ordering, error paths surface as failures.
- Integration: start with `EVENT_BUS_URL=redis://…` and `UI_DISABLE_HTTP_SERVER=1` to verify bus/agents without HTTP.
- Soak: reuse `scripts/soak_runtime.py` with `NEW_RUNTIME=1` to validate stability.
- E2E: ensure UI still presents activity and control via the bus.

## Notes

- This plan prioritizes decoupling and observability without large rewrites: it reuses the existing loop, agents, and services, but moves ownership of startup into a single place and shifts agent interaction onto the bus over time.

