# Service-Level Objectives

This document lists the primary latency and reliability targets that must hold
before promoting a deployment beyond paper trading. Each item maps to the
instrumentation already present in the runtime.

## Discovery & Enrichment

* **DAS search latency** – <2s p95 from `DiscoveryCandidate` to
  `TokenSnapshot`. Tracked via the `discovery.pipeline_latency_ms` metric emitted
  by `solhunter_zero.discovery.pipeline` tests.
* **Circuit breaker compliance** – DAS failures >5 within 30 seconds trigger the
  breaker and a 60 second cooldown. Verified by `tests/test_discovery` coverage.

## Market State

* **Candle close latency** – <=2s p95 from interval end to `OHLCV5m` publish.
* **Depth staleness** – <6s p95; measured by depth sampler metrics.
* **Self-contamination** – `self_contamination` gauge remains zero to confirm
  self-trade suppression.

## Golden Snapshot

* **Change-to-publish** – <300ms p95 from merged update to bus publish.
* **Emission reduction** – ≥40% reduction vs naive broadcast. Validated by the
  `golden_snapshot` integration tests.

## Agents & Swarm

* **Agent determinism** – hashed inputs match hashed outputs across repeated
  runs (`tests/test_agents.py`).
* **Compute time** – p95 compute <=200ms per suggestion.
* **Swarm dupes** – zero duplicate `Decision` events per `(mint, side)` group,
  recorded by swarm coordinator metrics.

## Execution & Exit

* **PnL math** – buy/partial sell/close tests (`tests/test_exit_management.py`)
  assert inventory accounting matches theory.
* **Exit rail latency** – must-exit triggers fire within 2–10s after the guard
  trips; enforced by the exit engine tests.
* **Missed fills diagnostics** – `missed_fill` counters remain zero under normal
  shadow operations.

## RL & Micro Mode

* **RL weight freshness** – RL weights applied only when `RL_WEIGHTS_DISABLED=0`
  and the published timestamp is <60s old.
* **MICRO_MODE gates** – when `MICRO_MODE=1`, max spread, depth minimums, risk
  sizing, and kill conditions from the Home-Run playbook must be satisfied before
  enabling live execution.

## UI & Observability

* **Panel staleness** – UI chips highlight when OHLCV >120s, depth >6s, or
  Golden snapshots >2× coalesce interval +5s.
* **Pre-flight** – the UI “Pre-flight” button runs the readiness sweep and
  surfaces ✅/❌ per panel.
* **Dashboards & Alerts** – Grafana dashboards track DAS, enrichment, candle
  latency, depth staleness, Golden coalesce, suggestion rate, vote timing,
  slippage vs expected, paper drawdown, and RL freshness with alerts for each
  breaching SLO.
