# Configuration Overview

SolHunter centralises configuration in a small set of Python helpers and
documented defaults so operators can audit the full runtime surface quickly.
This page links the moving pieces you need to inspect before launch.

## Loading order

1. **Environment files** – `solhunter_zero.env.load_env_file` backfills a missing
   `.env` from the static template and injects key/value pairs without
   overwriting existing process values.
2. **Config file** – `solhunter_zero.env_config.configure_environment` parses
   `config.toml`, applies defaults, and mirrors any values into the environment
   variables defined by `solhunter_zero.config.ENV_VARS`. Placeholders are
   stripped to avoid shipping example API keys.
3. **Runtime snapshot** – `solhunter_zero.startup.prepare_environment` loads the
   user config, applies overrides, and materialises a `Config` dataclass via
   `solhunter_zero.config_runtime.Config.from_env` so the trading loop can use
   strongly typed values.

## Defaults & schema

* `solhunter_zero/env_defaults.py` enumerates canonical defaults used when a
  setting is omitted. Updating the defaults file keeps the behaviour consistent
  across local shells, Docker, and CI.
* `solhunter_zero/config_schema.ConfigModel` describes the full config surface in
  Pydantic so we validate at load time, fail fast, and get helpful errors during
  CI runs.

## Feature flags integration

Feature toggles are captured once via `solhunter_zero.feature_flags`. The helper
records the current values in `startup.log` and publishes metrics such as
`feature_flag_micro_mode` through the event bus during environment prep. That
makes the active profile visible to observability dashboards without requiring
additional instrumentation.

## Key operator tooling

* `scripts/preflight.py` and `preflight.json` perform local readiness checks.
* `env_doctor.sh` and `scripts/env_doctor.py` verify RPC, Redis, and keypair
  credentials as part of bring-up.
* `scripts/bus_smoke.py` proves the event bus can create streams, publish
  messages, and respect TTLs.
