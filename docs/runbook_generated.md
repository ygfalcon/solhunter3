# Chaos Remediation Guide (Generated)

This file is generated from pytest chaos/health fixtures. Do not edit manually.

## RL daemon

* **When** RL health gate blocks startup when the daemon reports unhealthy (from `tests/test_startup_integration.py::test_start_all_blocks_when_rl_unhealthy`):
  * **Detection:** `scripts.start_all.launch_detached` raises `RL daemon gate failed: rl health down` and `start_all.main([])` returns exit code 1.
  * **Impact:** Trading startup aborts before the runtime and agents come online.
  * **Remediation:**
    1. Check the RL daemon logs for recent errors (for example `journalctl -u solhunter-rl --since -15m`).
    2. Restart the RL daemon service or container so the `/health` endpoint responds 200 (e.g. `systemctl restart solhunter-rl`).
    3. Re-run `python scripts/healthcheck.py` to confirm the RL gate passes before retrying startup.
  * **Verification:** `python -m scripts.startup --non-interactive` proceeds past the RL gate and the UI reports the daemon as healthy.
  * **Severity:** critical
  * **Tags:** startup, healthcheck, rl
  * **Source artifact:** `artifacts/chaos/tests__test_startup_integration.py__test_start_all_blocks_when_rl_unhealthy.json`
  * **Pytest markers:** chaos

* **When** The runtime cannot find `rl_daemon.health.json` (from `tests/test_trading_runtime_rl_status.py::test_probe_rl_daemon_health_missing`):
  * **Detection:** `_probe_rl_daemon_health()` returns `detected: False` with no URL recorded.
  * **Impact:** The RL watcher never enables external policy updates because no daemon is registered.
  * **Remediation:**
    1. Start the RL daemon via `python scripts/run_rl_daemon.py` so it writes `rl_daemon.health.json` into the runtime root.
    2. If the RL daemon runs remotely, mount or sync the generated health file onto the trading runtime host.
    3. Confirm the health file exists and rerun `python scripts/healthcheck.py` or restart the runtime process.
  * **Verification:** `ls rl_daemon.health.json` succeeds and `_probe_rl_daemon_health()` subsequently reports `detected: True`.
  * **Severity:** medium
  * **Tags:** runtime, healthcheck, rl
  * **Source artifact:** `artifacts/chaos/tests__test_trading_runtime_rl_status.py__test_probe_rl_daemon_health_missing.json`
  * **Pytest markers:** chaos

* **When** Trading runtime cannot reach the RL health endpoint (from `tests/test_trading_runtime_rl_status.py::test_probe_rl_daemon_health_failure`):
  * **Detection:** `_probe_rl_daemon_health` raises urllib errors for the URL recorded in `rl_daemon.health.json`, setting `running` to False.
  * **Impact:** The runtime treats the RL service as offline and skips RL-assisted policy updates.
  * **Remediation:**
    1. Verify the RL health endpoint `http://127.0.0.1:8080/health` is reachable from the runtime host (e.g. `curl http://127.0.0.1:8080/health`).
    2. Restart the RL daemon with `python scripts/run_rl_daemon.py` or restart the managed service to re-register the health server.
    3. After restart, re-run `python scripts/healthcheck.py` to confirm the RL check reports PASS.
  * **Verification:** `curl http://127.0.0.1:8080/health` returns HTTP 200 and `_probe_rl_daemon_health()` reports `running: True`.
  * **Severity:** high
  * **Tags:** runtime, healthcheck, rl
  * **Source artifact:** `artifacts/chaos/tests__test_trading_runtime_rl_status.py__test_probe_rl_daemon_health_failure.json`
  * **Pytest markers:** chaos
