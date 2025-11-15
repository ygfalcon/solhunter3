"""Integration tests for the lightweight startup gates."""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest


def test_connectivity_check_aborts_on_required_probe_failure(monkeypatch):
    import scripts.start_all as start_module
    from solhunter_zero.production.connectivity import ConnectivityResult

    async def fake_check_all(_self):
        return [
            ConnectivityResult(
                name="solana-rpc",
                target="https://rpc.example",
                ok=False,
                error="rpc timeout",
            ),
            ConnectivityResult(
                name="ui-http",
                target="https://ui.example/health",
                ok=True,
            ),
        ]

    monkeypatch.setattr(
        start_module.ConnectivityChecker,
        "check_all",
        fake_check_all,
        raising=False,
    )

    with pytest.raises(SystemExit) as exc:
        start_module._connectivity_check()

    assert "solana-rpc" in str(exc.value)


def test_start_all_respects_ui_selftest_exit_code(monkeypatch):
    import start_all

    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("scripts.start_all.ui_selftest", return_value=2, create=True):
        with pytest.raises(SystemExit) as exc:
            start_all.main()
        assert exc.value.code == 2


@pytest.mark.chaos
def test_start_all_blocks_when_rl_unhealthy(monkeypatch, chaos_remediator):
    """RL daemon is mandatory: unhealthy RL must block startup."""

    import start_all
    import scripts.start_all as start_module

    # Stub out heavy stages so the pipeline focuses on the failure we simulate.
    monkeypatch.setattr(start_module, "kill_lingering_processes", lambda: None)
    monkeypatch.setattr(
        start_module,
        "ensure_environment",
        lambda _cfg: {"config_path": "/tmp/config.toml", "config": {}},
    )
    monkeypatch.setattr(start_module, "_load_production_environment", lambda: {})
    monkeypatch.setattr(start_module, "_validate_keys", lambda: "ok")
    monkeypatch.setattr(start_module, "_write_manifest", lambda *_: Path("/tmp/manifest.json"))
    monkeypatch.setattr(start_module, "_connectivity_check", lambda: [])
    monkeypatch.setattr(start_module, "_connectivity_soak", lambda: {"disabled": True})
    def _fake_live_keypair(_cfg):
        os.environ["KEYPAIR_PATH"] = "/tmp/key.json"
        os.environ["SOLANA_KEYPAIR"] = "/tmp/key.json"
        return {"keypair_path": "/tmp/key.json", "keypair_pubkey": "pub"}

    monkeypatch.setattr(start_module, "ensure_live_keypair", _fake_live_keypair)

    failure = RuntimeError("RL daemon gate failed: rl health down")
    with patch("scripts.start_all.launch_detached", side_effect=failure):
        exit_code = start_all.main([])

    assert exit_code == 1
    chaos_remediator(
        component="RL daemon",
        failure="RL health gate blocks startup when the daemon reports unhealthy",
        detection=(
            "`scripts.start_all.launch_detached` raises `RL daemon gate failed: rl health down` and `start_all.main([])` returns exit code 1."
        ),
        impact="Trading startup aborts before the runtime and agents come online.",
        remediation=[
            "Check the RL daemon logs for recent errors (for example `journalctl -u solhunter-rl --since -15m`).",
            "Restart the RL daemon service or container so the `/health` endpoint responds 200 (e.g. `systemctl restart solhunter-rl`).",
            "Re-run `python scripts/healthcheck.py` to confirm the RL gate passes before retrying startup.",
        ],
        verification="`python -m scripts.startup --non-interactive` proceeds past the RL gate and the UI reports the daemon as healthy.",
        severity="critical",
        tags=["startup", "healthcheck", "rl"],
        metadata={"exception": str(failure)},
    )


def test_start_all_allows_when_rl_healthy(monkeypatch):
    """Healthy RL gate must pass."""

    import start_all

    # Bypass preflight success
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("scripts.start_all.ui_selftest", return_value=0, create=True):
        # Make RL gate healthy; also don't block on other waits
        with patch("scripts.start_all.wait_for", return_value=(True, "ok"), create=True):
            try:
                start_all.main()
            except SystemExit as e:
                # If later gates exit, ensure it's NOT the RL gate
                assert "RL daemon gate failed" not in str(e)


def test_launch_detached_writes_runtime_log(tmp_path, monkeypatch):
    import scripts.start_all as start_module

    log_dir = tmp_path / "logs"
    monkeypatch.setattr(start_module, "RUNTIME_LOG_DIR", log_dir)
    monkeypatch.setattr(start_module.time, "sleep", lambda _delay: None)

    def fake_popen(cmd, stdout, stderr, env):
        assert stderr is start_module.subprocess.STDOUT
        stdout.write(b"boom\n")
        stdout.flush()

        class Proc:
            def __init__(self):
                self.returncode = 1

            def poll(self):
                return self.returncode

        return Proc()

    monkeypatch.setattr(start_module.subprocess, "Popen", fake_popen)

    args = SimpleNamespace(
        ui_port=9999,
        ui_host="127.0.0.1",
        loop_delay=None,
        min_delay=None,
        max_delay=None,
    )

    with pytest.raises(RuntimeError) as excinfo:
        start_module.launch_detached(args, "/tmp/config.toml")

    log_path = log_dir / start_module.RUNTIME_LOG_NAME
    assert log_path.exists()
    assert "Check logs at" in str(excinfo.value)
    assert str(log_path) in str(excinfo.value)
    assert "boom" in log_path.read_text()


def test_launch_detached_waits_for_readiness_success(monkeypatch, tmp_path):
    import scripts.start_all as start_module

    log_dir = tmp_path / "logs"
    monkeypatch.setattr(start_module, "RUNTIME_LOG_DIR", log_dir)
    monkeypatch.setattr(start_module.time, "sleep", lambda _delay: None)

    class Proc:
        pid = 4242
        returncode = None

        def poll(self):
            return None

        def terminate(self):
            raise AssertionError("terminate should not be called")

        def wait(self, timeout=None):
            return 0

        def kill(self):
            raise AssertionError("kill should not be called")

    created: list[Proc] = []

    def fake_popen(cmd, stdout, stderr, env):
        assert stderr is start_module.subprocess.STDOUT
        proc = Proc()
        created.append(proc)
        return proc

    monkeypatch.setattr(start_module.subprocess, "Popen", fake_popen)

    calls: list[str] = []

    def fake_http_ok(url):
        calls.append(url)
        return True, "http 200"

    monkeypatch.setattr(start_module, "http_ok", fake_http_ok)
    monkeypatch.setenv("START_ALL_READY_URL", "http://ready.local/health")

    args = SimpleNamespace(
        ui_port=9999,
        ui_host="127.0.0.1",
        loop_delay=None,
        min_delay=None,
        max_delay=None,
    )

    assert start_module.launch_detached(args, "/tmp/config.toml") == 0
    assert calls == ["http://ready.local/health"]
    assert created and created[0].poll() is None


def test_launch_detached_times_out_waiting_for_readiness(monkeypatch, tmp_path):
    import scripts.start_all as start_module

    log_dir = tmp_path / "logs"
    monkeypatch.setattr(start_module, "RUNTIME_LOG_DIR", log_dir)
    monkeypatch.setattr(start_module.time, "sleep", lambda _delay: None)

    class Proc:
        def __init__(self):
            self.pid = 4242
            self.returncode = None
            self.terminated = False
            self.killed = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):
            raise subprocess.TimeoutExpired(cmd=["runtime"], timeout=timeout or 0)

        def kill(self):
            self.killed = True

    proc = Proc()

    def fake_popen(cmd, stdout, stderr, env):
        assert stderr is start_module.subprocess.STDOUT
        return proc

    monkeypatch.setattr(start_module.subprocess, "Popen", fake_popen)

    calls: list[str] = []

    def fake_http_ok(url):
        calls.append(url)
        return False, "unreachable"

    monkeypatch.setattr(start_module, "http_ok", fake_http_ok)
    monkeypatch.setenv("START_ALL_READY_URL", "http://ready.local/health")
    monkeypatch.setenv("START_ALL_READY_RETRIES", "3")
    monkeypatch.setenv("START_ALL_READY_INTERVAL", "0.01")

    args = SimpleNamespace(
        ui_port=9999,
        ui_host="127.0.0.1",
        loop_delay=None,
        min_delay=None,
        max_delay=None,
    )

    with pytest.raises(RuntimeError) as excinfo:
        start_module.launch_detached(args, "/tmp/config.toml")

    assert "Runtime readiness check timed out" in str(excinfo.value)
    assert calls == ["http://ready.local/health"] * 3
    assert proc.terminated
    assert proc.killed


def test_launch_detached_uses_notify_file_when_headless(monkeypatch, tmp_path):
    import scripts.start_all as start_module

    log_dir = tmp_path / "logs"
    monkeypatch.setattr(start_module, "RUNTIME_LOG_DIR", log_dir)

    notify_path = tmp_path / "artifacts" / "prelaunch" / "live_ready"

    class Proc:
        def __init__(self):
            self.pid = 777
            self.returncode = None

        def poll(self):
            return None

        def terminate(self):
            pass

        def wait(self, timeout=None):
            return None

        def kill(self):
            pass

    proc = Proc()

    def fake_popen(cmd, stdout, stderr, env):
        assert stderr is start_module.subprocess.STDOUT
        return proc

    monkeypatch.setattr(start_module.subprocess, "Popen", fake_popen)

    sleep_calls = {"count": 0}

    def fake_sleep(_delay):
        sleep_calls["count"] += 1
        if sleep_calls["count"] >= 2:
            notify_path.write_text("ready", encoding="utf-8")

    monkeypatch.setattr(start_module.time, "sleep", fake_sleep)
    monkeypatch.setenv("UI_ENABLED", "0")
    monkeypatch.setattr(start_module, "_resolve_orchestrator_notify_path", lambda: notify_path)

    args = SimpleNamespace(
        ui_port=9999,
        ui_host="127.0.0.1",
        loop_delay=None,
        min_delay=None,
        max_delay=None,
    )

    assert start_module.launch_detached(args, "/tmp/config.toml") == 0
    assert notify_path.read_text(encoding="utf-8") == "ready"

