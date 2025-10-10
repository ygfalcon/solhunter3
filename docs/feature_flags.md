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
| `RL_WEIGHTS_DISABLED` | `0`/`1` | `1` | Hard kill switch for applying reinforcement learning weights during swarm voting. Paper runs leave the flag on to prevent stale updates from influencing live execution. |

During `startup.prepare_environment` the flags are published to the event bus as
`feature_flag_*` metrics, making them easy to alert on when a deployment drifts
from the approved configuration.
