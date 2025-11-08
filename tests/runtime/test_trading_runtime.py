import asyncio
import logging
import os
import re
import sys
import threading
import time
import types
from typing import Dict, List

import anyio
import pytest


def _install_sqlalchemy_stub() -> None:
    def _noop(*_args, **_kwargs):
        return None

    class _SQLAlchemyModule(types.ModuleType):
        def __getattr__(self, name: str):  # type: ignore[override]
            if name == "event":
                return types.SimpleNamespace(listens_for=lambda *a, **k: (lambda func: func))
            return _noop

    stub = _SQLAlchemyModule("sqlalchemy")
    event_module = types.ModuleType("sqlalchemy.event")
    event_module.listens_for = lambda *a, **k: (lambda func: func)
    sys.modules["sqlalchemy.event"] = event_module
    sys.modules["sqlalchemy"] = stub

    orm_stub = types.ModuleType("sqlalchemy.orm")
    orm_stub.declarative_base = lambda *a, **k: type("Base", (), {})
    orm_stub.sessionmaker = lambda *a, **k: lambda **_kw: None
    sys.modules["sqlalchemy.orm"] = orm_stub


_install_sqlalchemy_stub()


def _install_base58_stub() -> None:
    if "base58" in sys.modules:
        return
    module = types.ModuleType("base58")
    module.b58decode = lambda *a, **k: b""
    module.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = module


_install_base58_stub()


def _install_jsonschema_stub() -> None:
    if "jsonschema" in sys.modules:
        return

    module = types.ModuleType("jsonschema")

    class _ValidationError(Exception):
        def __init__(self, message: str = "", *, instance=None, schema=None) -> None:
            super().__init__(message)
            self.message = message
            self.instance = instance
            self.schema = schema

    class _Validator:
        def __init__(self, schema):
            self.schema = schema

        def validate(self, _payload):
            return True

    module.Draft202012Validator = _Validator
    module.ValidationError = _ValidationError

    exceptions = types.ModuleType("jsonschema.exceptions")
    exceptions.ValidationError = _ValidationError

    module.exceptions = exceptions

    sys.modules["jsonschema"] = module
    sys.modules["jsonschema.exceptions"] = exceptions


_install_jsonschema_stub()


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"

from solhunter_zero import event_bus, resource_monitor
import solhunter_zero.runtime.trading_runtime as runtime_module
from solhunter_zero.runtime.trading_runtime import TradingRuntime
from solhunter_zero.ui import create_app
from solhunter_zero import ui as ui_module


@pytest.mark.anyio("asyncio")
async def test_exit_panel_provider_exposes_manager_summary(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    runtime._exit_manager.register_entry(
        "ABC",
        entry_price=100.0,
        size=5.0,
        breakeven_bps=25.0,
        ts=0.0,
        hot_watch=True,
    )
    runtime._exit_manager.record_missed_exit(
        "ABC", reason="spread_gate", diagnostics={"spread": 150.0}, ts=1.0
    )

    await runtime._start_ui()

    panel_snapshot = runtime.ui_state.exit_provider()
    assert panel_snapshot["hot_watch"], "expected hot_watch entries"
    assert panel_snapshot["diagnostics"], "expected diagnostics entries"

    app = create_app(runtime.ui_state)
    client = app.test_client()
    response = client.get("/swarm/exits")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["hot_watch"], "API should expose hot_watch data"
    assert payload["diagnostics"], "API should expose diagnostics data"


@pytest.mark.anyio("asyncio")
async def test_discovery_recent_endpoint_returns_tokens(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()

    with runtime._discovery_lock:
        for token in ("mint1", "mint2", "mint3"):
            runtime._recent_tokens.appendleft(token)

    await runtime._start_ui()

    app = create_app(runtime.ui_state)
    client = app.test_client()
    resp = client.get("/api/discovery/recent?limit=2")

    assert resp.status_code == 200
    data = resp.get_json()
    assert [entry["value"] for entry in data] == ["mint3", "mint2"]


@pytest.mark.anyio("asyncio")
async def test_market_panel_renders_pipeline_keys(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()

    now = time.time()
    with runtime._swarm_lock:
        runtime._market_ohlcv["PIPE"] = {
            "mint": "PIPE",
            "c": 1.23,
            "vol_usd": 456.0,
            "vol_base": 12.0,
            "buyers": 7,
            "sellers": 4,
            "asof_close": now,
            "_received": now,
        }
        runtime._market_depth["PIPE"] = {
            "mint": "PIPE",
            "spread_bps": 12.0,
            "asof": now,
            "_received": now,
        }

    market_panel = runtime._collect_market_panel()
    assert market_panel["markets"], "expected market snapshot"
    entry = market_panel["markets"][0]
    assert entry["close"] == pytest.approx(1.23)
    assert entry["volume"] == pytest.approx(456.0)
    assert entry["volume_base"] == pytest.approx(12.0)
    assert entry["buyers"] == 7
    assert entry["sellers"] == 4

    await runtime._start_ui()

    app = create_app(runtime.ui_state)
    client = app.test_client()
    response = client.get("/")
    assert response.status_code == 200
    html = response.get_data(as_text=True)
    row_match = re.search(r"<tr[^>]*data-mint=\"PIPE\"[^>]*>(.*?)</tr>", html, re.DOTALL)
    assert row_match, "expected market row to render"
    cells = [
        re.sub(r"<[^>]+>", "", cell).strip()
        for cell in re.findall(r"<td[^>]*>(.*?)</td>", row_match.group(1), re.DOTALL)
    ]
    assert len(cells) >= 3
    assert cells[1] != "—", "close cell should show a price"
    assert cells[2] != "—", "volume cell should show a value"


@pytest.mark.anyio("asyncio")
async def test_close_position_endpoint_uses_runtime_handler(monkeypatch):
    monkeypatch.setenv("UI_ENABLED", "0")
    runtime = TradingRuntime()
    runtime.pipeline = types.SimpleNamespace()
    runtime._loop = asyncio.get_running_loop()
    runtime._loop_thread = threading.current_thread()

    recorded: Dict[str, float] = {}
    executed = asyncio.Event()

    async def _fake_queue(mint: str, qty: float) -> None:
        recorded["mint"] = mint
        recorded["qty"] = qty
        executed.set()

    monkeypatch.setattr(runtime, "_queue_manual_exit", _fake_queue)
    await runtime._start_ui()

    app = create_app(runtime.ui_state)
    client = app.test_client()

    response = client.post("/api/execution/close?mint=SOL&qty=2.5")
    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "mint": "SOL", "qty": 2.5}

    await executed.wait()
    assert recorded == {"mint": "SOL", "qty": 2.5}


@pytest.mark.anyio("asyncio")
async def test_ui_health_reports_websocket_channels_ok(monkeypatch):
    pytest.importorskip("websockets")

    monkeypatch.setenv("UI_ENABLED", "1")

    runtime = TradingRuntime()

    class _StubUIServer:
        def __init__(self, state, host: str, port: int) -> None:
            self.state = state
            self.host = host
            self.port = port
            self._running = False

        def start(self) -> None:
            self._running = True

        def stop(self) -> None:
            self._running = False

    monkeypatch.setattr(runtime_module, "UIServer", _StubUIServer)

    try:
        await runtime._start_ui()
        assert runtime._ui_ws_threads, "expected websocket threads to be recorded"

        def _invoke_health():
            app = create_app(runtime.ui_state)
            client = app.test_client()
            return client.get("/ui/health")

        response = await anyio.to_thread.run_sync(_invoke_health)
        assert response.status_code == 200
        payload = response.get_json()
        assert payload["rl_ws"] == "ok"
        assert payload["events_ws"] == "ok"
        assert payload["logs_ws"] == "ok"
    finally:
        await runtime.stop()


def test_collect_health_metrics(monkeypatch):
    runtime = TradingRuntime()
    runtime.status.event_bus = True
    runtime._loop_delay = 12.0
    runtime._loop_min_delay = 2.0
    runtime._loop_max_delay = 30.0
    runtime._resource_snapshot = {"cpu": 50.0}
    runtime._resource_alerts = {"cpu": {"resource": "cpu", "active": True}}
    runtime.pipeline = types.SimpleNamespace(queue_snapshot=lambda: {"execution_queue": 4})

    monkeypatch.setattr(
        resource_monitor,
        "get_budget_status",
        lambda: {"cpu": {"active": True, "action": "exit", "ceiling": 80.0, "value": 85.0}},
    )
    monkeypatch.setattr(
        ui_module,
        "get_ws_client_counts",
        lambda: {"events": 1, "logs": 0, "rl": 0},
    )
    monkeypatch.setattr(runtime, "_internal_heartbeat_age", lambda: 5.0)

    metrics = runtime._collect_health_metrics()
    assert metrics["event_bus"]["connected"] is True
    assert metrics["resource"]["exit_active"] is True
    assert metrics["ui"]["ws_clients"]["events"] == 1


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_sets_stop_on_resource_exit(monkeypatch):
    runtime = TradingRuntime()
    runtime.memory = object()
    runtime.portfolio = object()
    runtime.agent_manager = object()

    async def fake_run_iteration(*_args, **_kwargs):
        return {}

    async def fake_apply(self, summary, stage="loop"):
        return None

    monkeypatch.setattr(runtime_module, "run_iteration", fake_run_iteration)
    monkeypatch.setattr(TradingRuntime, "_apply_iteration_summary", fake_apply)

    def fake_active(action: str):
        if action == "exit":
            return [{"resource": "cpu", "action": "exit"}]
        return []

    monkeypatch.setattr(runtime_module.resource_monitor, "active_budget", fake_active)

    task = asyncio.create_task(runtime._trading_loop())
    await asyncio.wait_for(task, timeout=0.5)
    assert runtime.stop_event.is_set()


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_updates_event_bus_url_for_auto_port(monkeypatch):
    monkeypatch.setenv("EVENT_BUS_WS_PORT", "0")
    monkeypatch.setenv("UI_ENABLED", "0")
    monkeypatch.setenv("EVENT_BUS_HEALTH_DISABLE", "1")

    runtime = TradingRuntime()

    ensure_calls: Dict[str, int] = {"count": 0}

    def _fake_ensure(*_a, **_k):
        ensure_calls["count"] += 1

    monkeypatch.setattr(runtime_module, "ensure_local_redis_if_needed", _fake_ensure)
    monkeypatch.setattr(resource_monitor, "start_monitor", lambda: None)

    assigned_port: Dict[str, int] = {"value": 0}
    connection_urls: List[str] = []

    class _DummySocket:
        def __init__(self, host: str, port: int) -> None:
            self._host = host
            self._port = port

        def getsockname(self) -> tuple[str, int]:
            return self._host, self._port

    class _DummyServer:
        def __init__(self, host: str, port: int) -> None:
            self.sockets = [_DummySocket(host, port)]

        def close(self) -> None:
            return None

        async def wait_closed(self) -> None:
            return None

    async def _fake_serve(handler, host: str, port: int, **_kwargs):
        actual_port = 43210 if not port else port
        assigned_port["value"] = actual_port
        return _DummyServer(host, actual_port)

    class _DummyConnection:
        def __init__(self, url: str) -> None:
            self.url = url

        async def close(self) -> None:
            return None

    async def _fake_connect(url: str, **_kwargs):
        connection_urls.append(url)
        return _DummyConnection(url)

    monkeypatch.setattr(
        event_bus,
        "websockets",
        types.SimpleNamespace(serve=_fake_serve, connect=_fake_connect),
    )

    async def _fake_verify(*_a, **_k) -> bool:
        return True

    monkeypatch.setattr(runtime_module, "verify_broker_connection", _fake_verify)

    prev_event_bus_url = os.environ.get("EVENT_BUS_URL")
    prev_broker_ws_urls = os.environ.get("BROKER_WS_URLS")
    original_url = event_bus.DEFAULT_WS_URL
    original_host, original_port = event_bus.get_ws_address()

    try:
        await asyncio.shield(runtime._start_event_bus())

        listen_host, listen_port = event_bus.get_ws_address()
        assert listen_port != 0, "expected auto-assigned port"

        expected_url = f"ws://{listen_host}:{listen_port}"
        assert os.environ["EVENT_BUS_URL"] == expected_url
        assert os.environ["BROKER_WS_URLS"] == expected_url

        entries = [e for e in runtime.activity.snapshot() if e["stage"] == "event_bus"]
        assert entries, "expected event bus activity entry"
        assert expected_url in entries[-1]["detail"]
        assert runtime.status.event_bus is True
        conn = await event_bus.websockets.connect(expected_url)
        await conn.close()
        assert connection_urls[-1] == expected_url
        assert assigned_port["value"] == listen_port
        assert ensure_calls["count"] == 1
    finally:
        try:
            await event_bus.disconnect_ws()
        except Exception:
            pass
        try:
            await event_bus.stop_ws_server()
        except asyncio.CancelledError:
            pass
        except Exception:
            pass

        event_bus.DEFAULT_WS_URL = original_url
        event_bus._WS_LISTEN_HOST = original_host
        event_bus._WS_LISTEN_PORT = original_port

        if prev_event_bus_url is None:
            os.environ.pop("EVENT_BUS_URL", None)
        else:
            os.environ["EVENT_BUS_URL"] = prev_event_bus_url

        if prev_broker_ws_urls is None:
            os.environ.pop("BROKER_WS_URLS", None)
        else:
            os.environ["BROKER_WS_URLS"] = prev_broker_ws_urls


@pytest.mark.anyio("asyncio")
async def test_runtime_websockets_use_public_host(monkeypatch):
    for key in (
        "UI_HOST",
        "UI_WS_HOST",
        "UI_WS_URL",
        "UI_EVENTS_WS",
        "UI_EVENTS_WS_URL",
        "UI_RL_WS",
        "UI_RL_WS_URL",
        "UI_LOGS_WS",
        "UI_LOG_WS_URL",
    ):
        monkeypatch.delenv(key, raising=False)

    monkeypatch.setenv("UI_PUBLIC_HOST", "public.runtime.test")

    captured: Dict[str, str | None] = {}

    def _fake_start_websockets() -> dict[str, threading.Thread]:
        captured["ui_host_env"] = os.getenv("UI_HOST")
        captured["ui_ws_host_env"] = os.getenv("UI_WS_HOST")
        base_host = captured["ui_host_env"] or "127.0.0.1"
        os.environ["UI_EVENTS_WS_URL"] = f"ws://{base_host}:9100{ui_module._channel_path('events')}"
        os.environ["UI_RL_WS_URL"] = f"ws://{base_host}:9101{ui_module._channel_path('rl')}"
        os.environ["UI_LOG_WS_URL"] = f"ws://{base_host}:9102{ui_module._channel_path('logs')}"
        return {}

    monkeypatch.setattr(ui_module, "start_websockets", _fake_start_websockets)

    class _DummyUIServer:
        def __init__(self, _state, *, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.started = False

        def start(self) -> None:
            self.started = True

        def stop(self) -> None:
            self.started = False

    monkeypatch.setattr(runtime_module, "UIServer", _DummyUIServer)

    runtime = TradingRuntime(ui_host="0.0.0.0", ui_port=6200)
    assert os.getenv("UI_HOST") == "0.0.0.0"
    assert os.getenv("UI_WS_HOST") == "0.0.0.0"

    await runtime._start_ui()

    ui_module.start_websockets()

    assert captured["ui_host_env"] == "0.0.0.0"
    assert captured["ui_ws_host_env"] == "0.0.0.0"

    manifest = ui_module.build_ui_manifest(None)
    assert manifest["events_ws"] == "ws://public.runtime.test:9100/ws/events"
    assert manifest["rl_ws"] == "ws://public.runtime.test:9101/ws/rl"
    assert manifest["logs_ws"] == "ws://public.runtime.test:9102/ws/logs"

    for key in ("UI_WS_URL", "UI_EVENTS_WS_URL", "UI_RL_WS_URL", "UI_LOG_WS_URL"):
        monkeypatch.delenv(key, raising=False)

    if runtime.ui_server is not None:
        runtime.ui_server.stop()


@pytest.mark.anyio("asyncio")
async def test_start_ui_logs_ready_and_persists_url(monkeypatch, tmp_path, caplog):
    monkeypatch.setenv("RUNTIME_ARTIFACT_ROOT", str(tmp_path))
    monkeypatch.setenv("RUN_ID", "ui-ready-test")
    monkeypatch.delenv("UI_HTTP_SCHEME", raising=False)
    monkeypatch.delenv("UI_SCHEME", raising=False)

    published: list[str] = []

    class _StubUIServer:
        def __init__(self, _state, *, host: str, port: int) -> None:
            self.host = host
            self.port = port
            self.started = False

        def start(self) -> None:
            self.started = True

        def stop(self) -> None:
            self.started = False

    monkeypatch.setattr(runtime_module, "UIServer", _StubUIServer)
    monkeypatch.setattr(runtime_module, "start_websockets", lambda: {})
    monkeypatch.setattr(runtime_module, "get_ws_urls", lambda: {
        "rl": "ws://rl.test/ws/rl",
        "events": "ws://events.test/ws/events",
        "logs": "ws://logs.test/ws/logs",
    })
    monkeypatch.setattr(
        runtime_module,
        "_publish_ui_url_to_redis",
        lambda url: published.append(url),
    )

    caplog.set_level(logging.INFO, logger=runtime_module.__name__)
    runtime = runtime_module.TradingRuntime(ui_host="0.0.0.0", ui_port=6101)

    await runtime._start_ui()

    expected_url = "http://127.0.0.1:6101"
    assert any(
        record.message.startswith("UI_READY ") and f"url={expected_url}" in record.message
        for record in caplog.records
    )

    artifact_file = tmp_path / "ui-ready-test" / "ui_url.txt"
    assert artifact_file.exists()
    assert artifact_file.read_text(encoding="utf-8") == expected_url
    assert published == [expected_url]


@pytest.mark.anyio("asyncio")
async def test_trading_runtime_starts_golden_pipeline_by_default(monkeypatch):
    runtime = TradingRuntime()
    runtime.config_path = "dummy-config.toml"
    runtime._use_new_pipeline = False

    for key in ("GOLDEN_PIPELINE", "MODE", "RUNTIME_MODE", "TRADING_MODE"):
        monkeypatch.delenv(key, raising=False)

    async def fake_startup(*_args, **_kwargs):
        cfg = {
            "mode": "live",
            "memory_path": "memory://",
            "portfolio_path": "portfolio.json",
        }
        return cfg, {}, None

    class DummyMemory:
        def __init__(self, *_args, **_kwargs) -> None:
            self.started = False

        def start_writer(self) -> None:
            self.started = True

    class DummyPortfolio:
        def __init__(self, *_args, **_kwargs) -> None:
            self.path = "portfolio.json"

    class DummyAgentManager:
        def __init__(self) -> None:
            self.agents = ["stub-agent"]
            self._rl_window_sec = 0.4

        @classmethod
        def from_config(cls, _cfg):
            return cls()

        @classmethod
        def from_default(cls):
            return cls()

        def set_rl_disabled(self, *_args, **_kwargs) -> None:
            return None

    golden_calls: Dict[str, object] = {}

    class DummyGoldenService:
        def __init__(self, agent_manager, portfolio) -> None:
            golden_calls["agent_manager"] = agent_manager
            golden_calls["portfolio"] = portfolio
            golden_calls["started"] = False

        async def start(self) -> None:
            golden_calls["started"] = True

        async def stop(self) -> None:
            golden_calls["stopped"] = True

    monkeypatch.setattr(runtime_module, "perform_startup_async", fake_startup)
    monkeypatch.setattr(runtime_module, "set_env_from_config", lambda _cfg: None)
    monkeypatch.setattr(runtime_module, "load_selected_config", lambda: {})
    monkeypatch.setattr(runtime_module, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_module, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_module, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.service.GoldenPipelineService",
        DummyGoldenService,
    )

    await runtime._prepare_configuration()
    assert runtime._use_golden_pipeline is True

    await runtime._start_agents()
    assert golden_calls.get("started") is True
    assert isinstance(runtime._golden_service, DummyGoldenService)
