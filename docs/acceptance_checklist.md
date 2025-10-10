# Acceptance Checklist

Use this page to verify the launch criteria from the Home-Run playbook. Each
row points to the code, docs, or tests that satisfy the requirement.

| Area | Requirement | Evidence |
| --- | --- | --- |
| Contracts | Schemas for `DiscoveryCandidate`, `TokenSnapshot`, `OHLCV5m`, `Depth`, `GoldenSnapshot`, `TradeSuggestion`, `Decision`, `VirtualFill`, `LiveFill`, `RLWeights`. | `docs/data_contracts.md` and generated protos in `proto/`.
| Bus | Streams + KV keys initialised on boot; TTL smoke verified. | `scripts/bus_smoke.py` CLI; `docs/runbook.md`.
| Env | Single settings surface documented; env doctor green. | `docs/config.md`, `docs/environment.md`, `env_doctor.sh`.
| Feature Flags | MODE, MICRO_MODE, NEW_DAS_DISCOVERY, EXIT_FEATURES_ON, RL_WEIGHTS_DISABLED captured and exported to metrics. | `docs/feature_flags.md`; `solhunter_zero/feature_flags.py`; `solhunter_zero/startup.py`.
| Discovery | DAS-first search, dedupe, cursor resume, circuit breaker. | `docs/blueprint.md`, `tests/test_token_discovery.py`.
| Market State | Tape normalisation, self-trade filter, candle/depth SLOs. | `docs/blueprint.md`, `docs/SLOs.md`, `tests/test_market_stream.py`.
| Golden Snapshot | Join metadata, OHLCV, depth; publish-on-change with cache. | `docs/blueprint.md`, `solhunter_zero/golden_pipeline` package.
| Agents | Deterministic suggestions, zero HTTP, shared simulator. | `docs/blueprint.md`, `tests/test_agents.py`, `tests/test_simulation.py`.
| Swarm | Windowed consensus, quorum enforcement, RL weight guard. | `docs/blueprint.md`, `tests/test_swarm_pipeline.py`, `docs/SLOs.md`.
| Execution | Shadow fills, live gating, PnL math, exit diagnostics. | `docs/blueprint.md`, `tests/test_execution.py`, `tests/test_exit_management.py`.
| Exit Engine | Must-exit rails, trailing profit, micro clamps. | `docs/blueprint.md`, `docs/SLOs.md`, `tests/test_exit_agent.py`.
| RL | Shadow weights publishing, kill switch, uplift dashboards. | `docs/blueprint.md`, `tests/test_multi_rl.py`, `docs/SLOs.md`.
| UI | Topic map, staleness chips, pre-flight sweeps. | `docs/ui_topic_map.md`, `docs/blueprint.md`.
| Observability | Dashboards, alerts, chaos drills. | `docs/blueprint.md`, `docs/SLOs.md`, `scripts/health_soak.py`.
| Packaging | Containers, compose manifests, runbook. | `Dockerfile`, `docker-compose.yml`, `docs/runbook.md`.
| Micro Mode | $20 Home-Run gates, strategy filters, kill conditions. | `docs/feature_flags.md`, `docs/SLOs.md`, `portfolio.json` presets.

## Automation helpers

Run `python -m scripts.acceptance_report` to emit a green/red summary of the
checks above. The script validates the presence of required docs, CLIs, and
config files so CI can alert when a deliverable is missing.
