# Feature Flags

SolHunter exposes a small set of global toggles that control how discovery,
execution, and reinforcement learning behave. The `solhunter_zero.feature_flags`
module parses the environment once during startup, logs the active profile, and
publishes per-flag metrics so dashboards can confirm the run configuration.

| Flag | Values | Default | Purpose |
| --- | --- | --- | --- |
| `MODE` | `paper`, `live`, `shadow` | `paper` | Top-level runtime mode reported to the UI and metrics. `shadow` encodes paper trading with real-time execution shadowing. |
| `MICRO_MODE` | `0`/`1` | `0` | Enables the $20 “Home-Run” configuration. When true the acceptance checklist expects the micro gate, sizing, and risk docs to be satisfied before enabling live execution. |
| `NEW_DAS_DISCOVERY` | `0`/`1` | `1` | Toggles the DAS-first discovery flow. Keeping this flag on ensures `DiscoveryCandidate` events originate from the DAS cursor walker. |
| `EXIT_FEATURES_ON` | `0`/`1` | `1` | Gates the fast exit engine, including must-exit overrides, trailing profit, and clamp logic in the execution stack. |
| `RL_WEIGHTS_DISABLED` | `0`/`1` | `1` | Hard kill switch for applying reinforcement learning weights during swarm voting. When enabled the runtime holds neutral weights; when disabled, a freshness gate ensures stale RL heartbeats (≥2× the vote window) are ignored. |
| `ONCHAIN_USE_DAS` / `ONCHAIN_DISCOVERY_PROVIDER` | `0`/`1`, `das` | `1` | Enables the DAS-backed discovery pipeline. When `ONCHAIN_DISCOVERY_PROVIDER=das` or `ONCHAIN_USE_DAS=1`, the `das_enabled` metric surfaces as `1.0`. |
| `MINT_STREAM_ENABLE` | `0`/`1` | `0` | Tracks whether the Helius mint stream is forwarding new token mints. Appears in metrics/UI as `mint_stream_enabled`. |
| `MEMPOOL_STREAM_ENABLE` | `0`/`1` | `0` | Indicates that the Jito mempool stream is running. Exported as `mempool_stream_enabled`. |
| `AMM_WATCH_ENABLE` | `0`/`1` | `0` | Controls the AMM pool watcher cron. Exported as `amm_watch_enabled`. |
| `SEED_PUBLISH_ENABLE` + `SEED_TOKENS` | `0`/`1` + CSV | `0` | Publishes the seeded token heartbeat (requires non-empty `SEED_TOKENS`). Surfaced via `seeded_tokens_enabled`. |
| `SHADOW_EXECUTOR_ONLY` | `0`/`1` | `0` | Forces reinforcement learning to shadow mode even in live configs. Exposed as `rl_shadow_mode`. |
| `PAPER_TRADING` | `0`/`1` | `1` in paper builds | Signals that executions should remain paper-only. Reflected in UI metrics as `paper_trading`. |
| `LIVE_TRADING_DISABLED` | `0`/`1` | `1` in paper builds | Kill switch to prevent live order submission. Reported as `live_trading_disabled`. |

During `startup.prepare_environment` the flags are published to the event bus as
`feature_flag_*` metrics, making them easy to alert on when a deployment drifts
from the approved configuration.

The UI now renders the flag snapshot on the **Settings & Controls** panel, showing
each toggle’s current state alongside the kill-switch controls.
