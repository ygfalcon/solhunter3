import asyncio
import logging
import socket
from unittest.mock import AsyncMock, Mock

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_run_cleans_up_on_start_failure(monkeypatch, caplog):
    runtime = trading_runtime.TradingRuntime()
    loop = asyncio.get_running_loop()
    monkeypatch.setattr(loop, "add_signal_handler", lambda *_, **__: None)

    async def fake_prepare_configuration() -> None:
        runtime.cfg = {}

    async def fake_start_event_bus() -> None:
        runtime.bus_started = True

    ui_server = Mock()
    ui_server.stop = Mock()

    async def fake_start_ui() -> None:
        runtime.ui_server = ui_server

    async def failing_start_agents() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(runtime, "_prepare_configuration", fake_prepare_configuration)
    monkeypatch.setattr(runtime, "_start_event_bus", fake_start_event_bus)
    monkeypatch.setattr(runtime, "_start_ui", fake_start_ui)
    monkeypatch.setattr(runtime, "_start_agents", failing_start_agents)
    monkeypatch.setattr(runtime, "_start_rl_status_watcher", lambda: None)
    monkeypatch.setattr(runtime, "_start_loop", AsyncMock())

    stop_ws_mock = AsyncMock()
    monkeypatch.setattr(trading_runtime, "stop_ws_server", stop_ws_mock)

    with caplog.at_level(logging.ERROR):
        with pytest.raises(RuntimeError, match="boom"):
            await runtime._run()

    assert runtime.stop_event.is_set()
    ui_server.stop.assert_called_once_with()
    assert stop_ws_mock.await_count == 1

    entries = runtime.activity.snapshot()
    assert any(
        entry["stage"] == "runtime"
        and entry["ok"] is False
        and "failed: boom" in entry["detail"]
        for entry in entries
    )

    assert any(
        "Trading runtime failed during run" in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_start_ui_falls_back_to_ephemeral_port(monkeypatch):
    runtime = trading_runtime.TradingRuntime(ui_host="127.0.0.1")

    monkeypatch.setenv("UI_STARTUP_PROBE", "0")

    busy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        busy_sock.bind(("127.0.0.1", 0))
        busy_sock.listen(1)
        busy_port = busy_sock.getsockname()[1]
        runtime.ui_port = busy_port

        await runtime._start_ui()

        try:
            assert runtime.ui_server is not None
            assert runtime.ui_port != busy_port
            entries = runtime.activity.snapshot()
            assert any(
                entry["stage"] == "ui"
                and entry["ok"] is False
                and "failed to bind" in entry["detail"]
                for entry in entries
            )
            assert any(
                entry["stage"] == "ui"
                and entry["ok"] is True
                and entry["detail"]
                == (
                    "http://"
                    f"{runtime.ui_server._format_host_for_url(runtime.ui_server.resolved_host)}"
                    f":{runtime.ui_port}"
                )
                for entry in entries
            )
        finally:
            runtime.ui_server.stop()
    finally:
        busy_sock.close()


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_config_reports_live_port_after_fallback(monkeypatch):
    runtime = trading_runtime.TradingRuntime(ui_host="127.0.0.1")

    monkeypatch.setenv("UI_STARTUP_PROBE", "0")

    busy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        busy_sock.bind(("127.0.0.1", 0))
        busy_sock.listen(1)
        busy_port = busy_sock.getsockname()[1]

        runtime.ui_port = busy_port
        runtime.cfg = {"ui_port": busy_port, "ui_host": "stale-host"}

        await runtime._start_ui()

        try:
            assert runtime.ui_server is not None
            config_snapshot = runtime.ui_state.snapshot_config()

            assert config_snapshot["ui_port"] == runtime.ui_port
            assert config_snapshot["ui_host"] == runtime.ui_server.resolved_host

            sanitized = config_snapshot["sanitized_config"]
            assert sanitized["ui_port"] == runtime.ui_port
            assert sanitized["ui_host"] == runtime.ui_server.resolved_host
        finally:
            runtime.ui_server.stop()
    finally:
        busy_sock.close()


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_start_ui_uses_configured_port_range(monkeypatch):
    runtime = trading_runtime.TradingRuntime(ui_host="127.0.0.1")

    monkeypatch.setenv("UI_STARTUP_PROBE", "0")

    busy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    fallback_hint = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        busy_sock.bind(("127.0.0.1", 0))
        busy_sock.listen(1)
        primary_port = busy_sock.getsockname()[1]
        runtime.ui_port = primary_port

        fallback_hint.bind(("127.0.0.1", 0))
        fallback_hint_port = fallback_hint.getsockname()[1]
    finally:
        fallback_hint.close()

    monkeypatch.setenv("UI_PORT_RANGE", str(fallback_hint_port))

    try:
        await runtime._start_ui()
        try:
            assert runtime.ui_server is not None
            assert runtime.ui_port == fallback_hint_port
            entries = runtime.activity.snapshot()
            assert any(
                entry["stage"] == "ui"
                and entry["ok"] is False
                and "failed to bind" in entry["detail"]
                for entry in entries
            )
            assert any(
                entry["stage"] == "ui"
                and entry["ok"] is True
                and entry["detail"]
                == (
                    "http://"
                    f"{runtime.ui_server._format_host_for_url(runtime.ui_server.resolved_host)}"
                    f":{fallback_hint_port}"
                )
                for entry in entries
            )
        finally:
            runtime.ui_server.stop()
    finally:
        busy_sock.close()


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_start_ui_formats_unspecified_host(monkeypatch):
    runtime = trading_runtime.TradingRuntime(ui_host="0.0.0.0")

    monkeypatch.setenv("UI_STARTUP_PROBE", "0")

    await runtime._start_ui()
    try:
        assert runtime.ui_server is not None
        entries = runtime.activity.snapshot()
        expected_detail = f"http://127.0.0.1:{runtime.ui_port}"
        assert any(
            entry["stage"] == "ui"
            and entry["ok"] is True
            and entry["detail"] == expected_detail
            for entry in entries
        )
    finally:
        runtime.ui_server.stop()


@pytest.mark.anyio("asyncio")
async def test_prepare_configuration_uses_offline_flags(monkeypatch, tmp_path):
    runtime = trading_runtime.TradingRuntime(
        config_path=str(tmp_path / "config.toml")
    )

    monkeypatch.setenv("SOLHUNTER_OFFLINE", "1")
    monkeypatch.setenv("DRY_RUN", "1")
    monkeypatch.setenv("TESTNET", "1")
    monkeypatch.setenv("LIVE_DISCOVERY", "0")

    captured: dict[str, object] = {}

    async def fake_startup(
        config_path: str | None,
        *,
        offline: bool,
        dry_run: bool,
        testnet: bool,
        preloaded_config: dict | None = None,
    ) -> tuple[dict, object, None]:
        captured["config_path"] = config_path
        captured["offline"] = offline
        captured["dry_run"] = dry_run
        captured["testnet"] = testnet
        captured["preloaded_config"] = preloaded_config
        return {}, object(), None

    monkeypatch.setattr(trading_runtime, "perform_startup_async", fake_startup)
    load_calls = {"count": 0}

    def fake_load_config(path=None):
        load_calls["count"] += 1
        return {}

    monkeypatch.setattr(trading_runtime, "load_config", fake_load_config)
    monkeypatch.setattr(trading_runtime, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(trading_runtime, "set_env_from_config", lambda cfg: None)

    await runtime._prepare_configuration()

    assert captured["config_path"] == runtime.config_path
    assert captured["offline"] is True
    assert captured["dry_run"] is True
    assert captured["testnet"] is True
    assert captured["preloaded_config"] == {}
    assert load_calls["count"] == 1

    modes = runtime._derive_offline_modes()
    assert modes == (True, True, False, True)


@pytest.mark.anyio("asyncio")
async def test_prepare_configuration_uses_saved_config_selection(monkeypatch, tmp_path):
    runtime = trading_runtime.TradingRuntime()

    config_dir = tmp_path / "configs"
    config_dir.mkdir()
    config_file = config_dir / "saved.toml"
    config_file.write_text("[solana]\ncluster = \"mainnet\"\n", encoding="utf-8")

    monkeypatch.setattr(trading_runtime, "CONFIG_DIR", str(config_dir))
    monkeypatch.setattr(trading_runtime, "get_active_config_name", lambda: "saved.toml")

    captured: dict[str, object] = {}

    async def fake_startup(
        config_path: str | None,
        *,
        offline: bool,
        dry_run: bool,
        testnet: bool,
        preloaded_config: dict | None = None,
    ) -> tuple[dict, object, None]:
        captured["config_path"] = config_path
        captured["offline"] = offline
        captured["dry_run"] = dry_run
        captured["testnet"] = testnet
        captured["preloaded_config"] = preloaded_config
        return {}, object(), None

    def fake_load_config(path=None):
        captured.setdefault("loaded_configs", []).append(path)
        return {}

    monkeypatch.setattr(trading_runtime, "perform_startup_async", fake_startup)
    monkeypatch.setattr(trading_runtime, "load_config", fake_load_config)
    monkeypatch.setattr(trading_runtime, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(trading_runtime, "set_env_from_config", lambda cfg: None)

    await runtime._prepare_configuration()

    expected_path = str(config_file.resolve())
    assert runtime.config_path == expected_path
    assert captured["config_path"] == expected_path
    assert captured.get("loaded_configs") == [expected_path]
    assert captured["offline"] is False
    assert captured["dry_run"] is False
    assert captured["testnet"] is False
    assert captured["preloaded_config"] == {}
