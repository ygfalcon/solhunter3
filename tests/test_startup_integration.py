"""Integration tests for the lightweight startup gates."""

from __future__ import annotations

from unittest.mock import patch

import pytest


def test_start_all_respects_ui_selftest_exit_code(monkeypatch):
    import start_all

    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=2):
        with pytest.raises(SystemExit) as exc:
            start_all.main()
        assert exc.value.code == 2


def test_start_all_blocks_when_rl_unhealthy(monkeypatch):
    """RL daemon is mandatory: unhealthy RL must block startup."""

    import start_all

    # Bypass preflight success
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")  # don't require Redis in this unit test
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")  # don't poll UI in this unit test
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=0):
        # Prevent long waits: force wait_for to return unhealthy quickly
        with patch("start_all.wait_for", return_value=(False, "down")):
            with pytest.raises(SystemExit) as exc:
                start_all.main()
            # Any non-zero code is fine; message should mention RL gate
            assert "RL daemon gate failed" in str(exc.value)


def test_start_all_allows_when_rl_healthy(monkeypatch):
    """Healthy RL gate must pass."""

    import start_all

    # Bypass preflight success
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("USE_REDIS", "0")
    monkeypatch.setenv("CHECK_UI_HEALTH", "0")
    monkeypatch.setenv("RL_HEALTH_URL", "http://rl:7070/health")

    with patch("start_all.ui_selftest", return_value=0):
        # Make RL gate healthy; also don't block on other waits
        with patch("start_all.wait_for", return_value=(True, "ok")):
            try:
                start_all.main()
            except SystemExit as e:
                # If later gates exit, ensure it's NOT the RL gate
                assert "RL daemon gate failed" not in str(e)


def test_perform_checks_assembles_context(tmp_path):
    from types import SimpleNamespace
    from pathlib import Path

    from solhunter_zero import startup_checks

    args = SimpleNamespace(
        skip_deps=False,
        full_deps=False,
        skip_setup=False,
        offline=False,
        skip_rpc_check=False,
        skip_endpoint_check=False,
        one_click=True,
    )
    rest = ["--flag"]

    call_order: list[str] = []
    target_calls: list[str] = []

    def record(name):
        def _inner(*a, **kwargs):
            call_order.append(name)
            if name == "ensure_deps":
                return {"summary_rows": [("Dependencies", "ready")]}
            if name == "ensure_wallet_cli":
                return {"summary_rows": [("Wallet CLI", "ok")]}
            if name == "ensure_cargo":
                return {"summary_rows": [("Rust toolchain", "ok")]}
            if name == "run_quick_setup":
                return {
                    "config_path": Path("/tmp/config.toml"),
                    "keypair_path": Path("/tmp/id.json"),
                    "mnemonic_path": Path("/tmp/mnemonic.txt"),
                    "active_keypair": "default",
                    "summary_rows": [("Quick setup", "done")],
                }
            return None

        return _inner

    def ensure_target(name: str) -> None:
        target_calls.append(name)
        call_order.append(f"ensure_target:{name}")

    rpc_calls: list[bool] = []

    def ensure_rpc(*, warn_only: bool) -> None:
        rpc_calls.append(warn_only)
        call_order.append("ensure_rpc")

    endpoints_cfg: list[dict] = []

    def ensure_endpoints(cfg: dict) -> None:
        endpoints_cfg.append(cfg)
        call_order.append("ensure_endpoints")

    logs: list[str] = []

    def log_startup(msg: str) -> None:
        logs.append(msg)
        call_order.append("log_startup")

    def apply_env_overrides(cfg: dict) -> dict:
        call_order.append("apply_env_overrides")
        return {**cfg, "overridden": True}

    def load_config(path: Path) -> dict:
        call_order.append("load_config")
        return {"path": str(path)}

    ctx = startup_checks.perform_checks(
        args,
        rest,
        ensure_deps=record("ensure_deps"),
        ensure_target=ensure_target,
        ensure_wallet_cli=record("ensure_wallet_cli"),
        ensure_rpc=ensure_rpc,
        ensure_endpoints=ensure_endpoints,
        ensure_cargo=record("ensure_cargo"),
        run_quick_setup=record("run_quick_setup"),
        log_startup=log_startup,
        apply_env_overrides=apply_env_overrides,
        load_config=load_config,
    )

    assert ctx["rest"] == rest
    assert ctx["config_path"] == Path("/tmp/config.toml")
    assert ctx["keypair_path"] == Path("/tmp/id.json")
    assert ctx["mnemonic_path"] == Path("/tmp/mnemonic.txt")
    assert ctx["active_keypair"] == "default"
    assert ctx["config"] == {"path": "/tmp/config.toml", "overridden": True}

    names = [name for name in call_order if not name.startswith("log_startup")]
    expected_sequence = [
        "ensure_deps",
        "ensure_target:protos",
        "ensure_target:route_ffi",
        "ensure_target:depth_service",
        "ensure_wallet_cli",
        "ensure_cargo",
        "run_quick_setup",
        "load_config",
        "apply_env_overrides",
        "ensure_rpc",
        "ensure_endpoints",
    ]
    for item in expected_sequence:
        assert item in names
    assert names.index("ensure_deps") < names.index("ensure_target:protos")
    assert names.index("ensure_wallet_cli") < names.index("ensure_cargo")
    assert names.index("run_quick_setup") < names.index("ensure_rpc")
    assert names.index("ensure_rpc") < names.index("ensure_endpoints")

    assert rpc_calls == [False]
    assert endpoints_cfg == [{"path": "/tmp/config.toml", "overridden": True}]

    summary = ctx["summary_rows"]
    assert ("Dependencies", "ready") in summary
    assert ("Quick setup", "done") in summary
    assert any(row[0] == "Endpoints" for row in summary)

