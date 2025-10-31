"""Integration tests for the lightweight startup gates."""

from __future__ import annotations

from pathlib import Path
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

