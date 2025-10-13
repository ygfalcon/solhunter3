# Launch Audit Tracker

This repository does not bundle the production infrastructure (Redis clusters, WebSocket gateways, Solana RPC endpoints, DAS discovery services, RL trainers, or the UI) that is required to execute the "Final 12-point launch audit" requested in the latest runbook update. As a result, every gating check below is **documented** but not **validated** inside this environment.

| # | Requirement | Repository status | Validation notes |
|---|-------------|-------------------|------------------|
| 1 | RL isolation & freshness gate | Runtime now keeps RL weights neutral until health checks pass and heartbeats are fresh. | Still requires stopping/starting the live daemon to capture neutral vs applied weights in logs. |
| 2 | Websocket/event-bus singletons & graceful shutdown | Local gateway fails fast when the port is taken and drains pending frames on shutdown. | Validate against the production gateway to prove the new log messages and shutdown behaviour under signal load. |
| 3 | DAS-first discovery default | DAS discovery is enabled by default with legacy GPA as a fallback. | Cursor resume and duplicate suppression still need exercising against real Helius endpoints. |
| 4 | Golden Snapshot publish-on-change + stable hash | Coupled to the deployed UI and Redis streams. | Must observe UI hash chip updates and stream behaviour in real time. |
| 5 | Vote idempotency (live) | Implemented through the production vote manager and Redis KV. | Duplicate flood tests require the live orchestrator and metrics pipeline. |
| 6 | Must-exit bypass latency SLO | Depends on monitoring live order books. | Needs a running market simulator or live spreads to observe the ≤5s p95 guarantee. |
| 7 | Agent micro-mode gates | Agents drop suggestions when spread/depth/edge thresholds are not met. | Requires live agent emission under controlled spreads/depths. |
| 8 | Keys & placeholders hygiene | Placeholder lint tooling executes during CI. | The linter is not wired into the offline developer container; run `make audit-placeholders` in the production CI environment. |
| 9 | Primary entry vs legacy launcher drift | Runtime behaviour is gated by environment variables during startup. | Needs to execute both launchers with/without `SOLHUNTER_LEGACY=1` on a machine with all services installed. |
| 10 | Docs promised vs present | Contract generation runs in CI. | Confirm via the hosted CI artefacts; generation scripts rely on internal packages not vendored here. |
| 11 | Connectivity soak + autorecovery | Requires the soak harness plus supervised services. | The real soak test orchestrates Redis, Solana RPC, DAS, and gateway restarts—outside the scope of this repo. |
| 12 | Startup key hygiene & manifest | Enforced during launcher start. | Needs access to real secrets management and the launcher binary. |

## How to validate in production

1. Deploy the full SolHunter stack (Redis, gateway, agents, UI, RL) using the internal `production-deploy` Terraform module.
2. Run the `start_all.py` orchestration on a production-like host with `SIM_MODE=0` and ensure credentials are configured.
3. Follow the step-by-step checks in the runbook to gather evidence (logs, metrics, UI screenshots) for each requirement.
4. Archive the artefacts in the central compliance bucket and link them back to this tracker for permanent record.

Until these steps are executed against the live stack, none of the requirements can be conclusively marked as complete from within this repository.
