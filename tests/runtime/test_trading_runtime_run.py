import asyncio
import logging
import socket
from unittest.mock import AsyncMock, Mock

import pytest

from solhunter_zero import config as config_module
from solhunter_zero.runtime import trading_runtime


def test_resolve_config_path_uses_selected_config(monkeypatch, tmp_path):
    monkeypatch.setattr(config_module, "CONFIG_DIR", str(tmp_path))
    active_file = tmp_path / "active"
    monkeypatch.setattr(config_module, "ACTIVE_CONFIG_FILE", str(active_file))
    monkeypatch.setattr(config_module, "_publish", lambda *_, **__: None)
    monkeypatch.setattr(config_module, "_ACTIVE_CONFIG", {})

    cfg_file = tmp_path / "demo.toml"
    cfg_file.write_text(
        "\n".join(
            [
                'solana_rpc_url = "https://example.com"',
                'dex_base_url = "https://example.com"',
                'agents = ["sim"]',
                "[agent_weights]",
                "sim = 1.0",
            ]
        ),
        encoding="utf-8",
    )

    config_module.select_config("demo.toml")

    runtime = trading_runtime.TradingRuntime()
    resolved = runtime._resolve_config_path()

    assert resolved == str(cfg_file.resolve())


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
async def test_trading_runtime_start_ui_reports_port_in_use(caplog):
    runtime = trading_runtime.TradingRuntime(ui_host="127.0.0.1")

    busy_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        busy_sock.bind(("127.0.0.1", 0))
        busy_sock.listen(1)
        runtime.ui_port = busy_sock.getsockname()[1]

        with caplog.at_level(logging.ERROR):
            with pytest.raises(trading_runtime.UIStartupError):
                await runtime._start_ui()

        entries = runtime.activity.snapshot()
        assert any(
            entry["stage"] == "ui"
            and entry["ok"] is False
            and "failed to start" in entry["detail"]
            for entry in entries
        )

        assert runtime.ui_server is None
        assert any(
            "failed to start UI server" in record.getMessage()
            for record in caplog.records
        )
    finally:
        busy_sock.close()


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
        config_path: str | None, *, offline: bool, dry_run: bool, testnet: bool
    ) -> tuple[dict, object, None]:
        captured["config_path"] = config_path
        captured["offline"] = offline
        captured["dry_run"] = dry_run
        captured["testnet"] = testnet
        return {}, object(), None

    monkeypatch.setattr(trading_runtime, "perform_startup_async", fake_startup)
    monkeypatch.setattr(trading_runtime, "load_config", lambda path=None: {})
    monkeypatch.setattr(trading_runtime, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(trading_runtime, "set_env_from_config", lambda cfg: None)

    await runtime._prepare_configuration()

    assert captured["config_path"] == runtime.config_path
    assert captured["offline"] is True
    assert captured["dry_run"] is True
    assert captured["testnet"] is True

    modes = runtime._derive_offline_modes()
    assert modes == (True, True, False, True)
