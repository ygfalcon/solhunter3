import asyncio
import logging
import threading
import time
import types
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

from solhunter_zero.loop import ResourceBudgetExceeded
from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_orchestrator_reports_ui_ws_failure(monkeypatch):
    events: list[dict[str, object]] = []

    def capture_publish(topic, payload, *_args, **_kwargs):
        if topic == "runtime.stage_changed":
            events.append(payload)

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.event_bus.publish",
        capture_publish,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {})
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )

    def fail_ws_start() -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        fail_ws_start,
    )

    orch = RuntimeOrchestrator(run_http=False)
    with pytest.raises(RuntimeError) as excinfo:
        await orch.start_ui()
    assert "UI websocket startup failed" in str(excinfo.value)

    ws_events = [event for event in events if event.get("stage") == "ui:ws"]
    assert ws_events, "expected ui:ws stage emission"
    ui_stage = ws_events[-1]
    assert ui_stage.get("ok") is False
    assert "boom" in str(ui_stage.get("detail"))


@pytest.mark.anyio("asyncio")
async def test_orchestrator_start_aborts_on_required_ws_failure(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.event_bus.subscribe",
        lambda *_args, **_kwargs: lambda: None,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {})
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )

    def fail_ws_start() -> dict[str, object]:
        raise RuntimeError("boom")

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        fail_ws_start,
    )

    start_bus = AsyncMock()
    start_agents = AsyncMock()
    monkeypatch.setattr(RuntimeOrchestrator, "start_bus", start_bus)
    monkeypatch.setattr(RuntimeOrchestrator, "start_agents", start_agents)

    orch = RuntimeOrchestrator(run_http=False)

    with pytest.raises(RuntimeError) as excinfo:
        await orch.start()

    assert "UI websocket startup failed" in str(excinfo.value)
    assert start_bus.await_count == 0
    assert start_agents.await_count == 0


@pytest.mark.anyio("asyncio")
async def test_orchestrator_logs_ui_ws_ready(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ws_urls = {
        "events": "ws://localhost:1111/events",
        "logs": "ws://localhost:1111/logs",
        "rl": "ws://localhost:1111/rl",
    }
    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: ws_urls)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        lambda: {"events": object()},
    )

    orch = RuntimeOrchestrator(run_http=False)
    await orch.start_ui()

    ws_logs = [message for message in caplog.messages if message.startswith("UI_WS_READY ")]
    assert ws_logs, "expected UI_WS_READY log entry"
    latest = ws_logs[-1]
    assert "status=ok" in latest
    assert "events_ws=ws://localhost:1111/events" in latest
    assert "logs_ws=ws://localhost:1111/logs" in latest
    assert "rl_ws=ws://localhost:1111/rl" in latest


@pytest.mark.anyio("asyncio")
async def test_orchestrator_waits_for_delayed_http_server(monkeypatch):
    events: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        events.append((stage, ok, detail))

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {})
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        lambda: {"events": object()},
    )

    shutdown_signal = threading.Event()

    class DummyServer:
        daemon_threads = True

        def serve_forever(self) -> None:
            shutdown_signal.wait()

        def shutdown(self) -> None:
            shutdown_signal.set()

        def server_close(self) -> None:
            return None

    def delayed_make_server(_host: str, _port: int, _app: object) -> DummyServer:
        time.sleep(6.0)
        return DummyServer()

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.make_server",
        delayed_make_server,
    )

    orch = RuntimeOrchestrator(run_http=True)
    await orch.start_ui()

    http_events = [event for event in events if event[0] == "ui:http"]
    assert http_events, "expected ui:http stage emission"
    assert http_events[-1][1] is True

    shutdown_signal.set()
    threads = orch.handles.ui_threads or {}
    http_thread_info = threads.get("http") if isinstance(threads, dict) else None
    if isinstance(http_thread_info, dict):
        thread_obj = http_thread_info.get("thread")
        if isinstance(thread_obj, threading.Thread):
            thread_obj.join(timeout=1)


@pytest.mark.anyio("asyncio")
async def test_stop_all_shuts_down_http_thread(monkeypatch):
    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        return None

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ui_stub = types.SimpleNamespace(
        UIState=lambda: object(),
        get_ws_urls=lambda: {},
        stop_websockets=lambda: None,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws", lambda: {"events": object()}
    )
    monkeypatch.setattr(RuntimeOrchestrator, "_emit_ui_ready", lambda *_args, **_kwargs: None)

    async def fake_close_session() -> None:
        return None

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.close_session",
        fake_close_session,
        raising=False,
    )

    server_stop = threading.Event()
    serve_started = threading.Event()

    class DummyServer:
        daemon_threads = True

        def __init__(self) -> None:
            self.shutdown_calls = 0
            self.server_close_calls = 0
            self.timeout = None

        def handle_request(self) -> None:
            serve_started.set()
            if server_stop.wait(0.01):
                raise OSError("stopped")

        def shutdown(self) -> None:
            self.shutdown_calls += 1
            server_stop.set()

        def server_close(self) -> None:
            self.server_close_calls += 1

        def serve_forever(self) -> None:  # pragma: no cover - ensured unused
            raise AssertionError("serve_forever should not be called")

    dummy_server = DummyServer()

    def fake_make_server(_host: str, _port: int, _app: object) -> DummyServer:
        return dummy_server

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.make_server",
        fake_make_server,
    )

    orch = RuntimeOrchestrator(run_http=True)
    await orch.start_ui()

    assert serve_started.wait(timeout=1), "expected HTTP server loop to start"

    threads = orch.handles.ui_threads or {}
    http_thread_info = threads.get("http") if isinstance(threads, dict) else None
    assert isinstance(http_thread_info, dict)
    thread_obj = http_thread_info.get("thread")
    shutdown_event = http_thread_info.get("shutdown_event")

    assert isinstance(thread_obj, threading.Thread)
    assert thread_obj.is_alive()
    assert shutdown_event is not None
    assert hasattr(shutdown_event, "is_set")
    assert not shutdown_event.is_set()

    await orch.stop_all()

    thread_obj.join(timeout=1)
    assert not thread_obj.is_alive()
    assert shutdown_event.is_set()
    assert server_stop.is_set()
    assert dummy_server.shutdown_calls >= 1
    assert dummy_server.server_close_calls >= 1


@pytest.mark.anyio("asyncio")
async def test_orchestrator_start_aborts_on_http_failure(monkeypatch):
    events: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        events.append((stage, ok, detail))

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.event_bus.subscribe",
        lambda *_args, **_kwargs: lambda: None,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )
    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {})
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        lambda: {"events": object()},
    )

    def failing_make_server(_host: str, _port: int, _app: object) -> object:
        raise RuntimeError("http boom")

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.make_server",
        failing_make_server,
    )

    start_bus = AsyncMock()
    start_agents = AsyncMock()
    monkeypatch.setattr(RuntimeOrchestrator, "start_bus", start_bus)
    monkeypatch.setattr(RuntimeOrchestrator, "start_agents", start_agents)

    orch = RuntimeOrchestrator(run_http=True)

    with pytest.raises(RuntimeError) as excinfo:
        await orch.start()

    assert "UI HTTP server failed" in str(excinfo.value)
    assert start_bus.await_count == 0
    assert start_agents.await_count == 0

    http_events = [event for event in events if event[0] == "ui:http"]
    assert http_events, "expected ui:http stage emission"
    assert http_events[-1][1] is False
    assert "http boom" in http_events[-1][2]

    threads = orch.handles.ui_threads or {}
    http_thread_info = threads.get("http") if isinstance(threads, dict) else None
    if isinstance(http_thread_info, dict):
        thread_obj = http_thread_info.get("thread")
        if isinstance(thread_obj, threading.Thread):
            thread_obj.join(timeout=1)


@pytest.mark.anyio("asyncio")
async def test_orchestrator_emits_ready_when_http_disabled(monkeypatch):
    events: list[tuple[str, bool, str]] = []
    ready_calls: list[tuple[str, int]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        events.append((stage, ok, detail))

    def fake_emit_ui_ready(self, host: str, port: int) -> None:
        ready_calls.append((host, port))

    monkeypatch.setenv("UI_DISABLE_HTTP_SERVER", "1")
    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(RuntimeOrchestrator, "_emit_ui_ready", fake_emit_ui_ready)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )
    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {})
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        lambda: {"events": object()},
    )

    orch = RuntimeOrchestrator(run_http=True)
    await orch.start_ui()

    assert ready_calls == [("127.0.0.1", 5000)]
    http_events = [event for event in events if event[0] == "ui:http"]
    assert http_events, "expected ui:http stage emission"
    latest = http_events[-1]
    assert latest[1] is True
    assert "disabled" in latest[2]
    assert "port=5000" in latest[2]


def test_emit_ui_ready_skips_invalid_port(monkeypatch, caplog, tmp_path):
    captured_urls: list[str] = []
    artifact_called = False

    def fake_publish_ui_url(ui_url: str) -> None:
        captured_urls.append(ui_url)

    def fake_runtime_artifact_dir() -> Path:
        nonlocal artifact_called
        artifact_called = True
        return tmp_path

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._publish_ui_url_to_redis",
        fake_publish_ui_url,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._runtime_artifact_dir",
        fake_runtime_artifact_dir,
    )

    caplog.set_level(logging.INFO)

    orch = RuntimeOrchestrator(run_http=False)
    orch._emit_ui_ready("127.0.0.1", 0)

    readiness_logs = [message for message in caplog.messages if message.startswith("UI_READY ")]
    assert readiness_logs, "expected UI_READY log entry"
    assert all(":0" not in message for message in readiness_logs)
    assert all("url=unavailable" in message for message in readiness_logs)
    assert not artifact_called
    assert captured_urls == []
    assert not (tmp_path / "ui_url.txt").exists()


def test_emit_ui_ready_uses_detected_host(monkeypatch, caplog, tmp_path):
    caplog.set_level(logging.INFO)

    for key in ("UI_PUBLIC_HOST", "PUBLIC_URL_HOST", "UI_EXTERNAL_HOST"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("UI_HOST", "0.0.0.0")

    detected_host = "ui-runtime.example.test"
    monkeypatch.setattr(
        "solhunter_zero.ui.socket.getfqdn", lambda: detected_host, raising=False
    )
    monkeypatch.setattr(
        "solhunter_zero.ui.socket.gethostname", lambda: detected_host, raising=False
    )

    captured_urls: list[str] = []

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._publish_ui_url_to_redis",
        lambda url: captured_urls.append(url),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._runtime_artifact_dir",
        lambda: tmp_path,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module.get_ws_urls",
        lambda request_host=None: {},
        raising=False,
    )

    orch = RuntimeOrchestrator(run_http=False)
    orch._emit_ui_ready("0.0.0.0", 5000)

    readiness_logs = [message for message in caplog.messages if message.startswith("UI_READY ")]
    assert readiness_logs, "expected UI_READY log entry"
    assert any(f"url=http://{detected_host}:5000" in message for message in readiness_logs)
    assert captured_urls == [f"http://{detected_host}:5000"]
    assert (tmp_path / "ui_url.txt").read_text(encoding="utf-8") == f"http://{detected_host}:5000"


def test_orchestrator_stops_on_resource_budget(monkeypatch):
    events: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        events.append((stage, ok, detail))

    async def fake_stop_all(self) -> None:
        events.append(("stop_all", True, ""))
        self._closed = True

    async def runner() -> None:
        monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
        monkeypatch.setattr(RuntimeOrchestrator, "stop_all", fake_stop_all)

        orch = RuntimeOrchestrator(run_http=False)

        async def failing_task() -> None:
            raise ResourceBudgetExceeded("cpu", {"action": "exit", "ceiling": 80.0, "value": 95.0})

        task = asyncio.create_task(failing_task())
        orch._register_task(task)
        await asyncio.sleep(0)
        await asyncio.sleep(0.01)

        assert orch.stop_reason is not None
        assert "cpu" in orch.stop_reason
        assert orch._closed is True
        assert any(stage == "runtime:resource_exit" for stage, _, _ in events)
        assert any(stage == "stop_all" for stage, _, _ in events)

    asyncio.run(runner())


@pytest.mark.anyio("asyncio")
async def test_orchestrator_starts_golden_pipeline_by_default(monkeypatch):
    for key in ("GOLDEN_PIPELINE", "MODE", "RUNTIME_MODE", "TRADING_MODE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("EVENT_DRIVEN", "0")

    golden_events: list[str] = []

    class DummyGoldenService:
        def __init__(self, agent_manager, portfolio) -> None:
            golden_events.append("init")
            self.agent_manager = agent_manager
            self.portfolio = portfolio

        async def start(self) -> None:
            golden_events.append("start")

        async def stop(self) -> None:
            golden_events.append("stop")

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
            self.agents = ["agent"]
            self._rl_window_sec = 0.4

        @classmethod
        def from_config(cls, _cfg):
            return cls()

        @classmethod
        def from_default(cls):
            return cls()

        def set_rl_disabled(self, *_args, **_kwargs) -> None:
            return None

    published: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        published.append((stage, ok, detail))

    async def fake_startup(*_args, **_kwargs):
        cfg = {
            "mode": "live",
            "memory_path": "memory://",
            "portfolio_path": "portfolio.json",
        }
        depth_proc = types.SimpleNamespace(poll=lambda: None)
        return cfg, {}, depth_proc

    async def fake_trading_loop(*_args, **_kwargs):
        return None

    class DummyEventBus:
        def publish(self, *_args, **_kwargs) -> None:
            return None

        def subscribe(self, *_args, **_kwargs):
            return lambda: None

        async def start_ws_server(self, *_args, **_kwargs) -> None:
            return None

        async def stop_ws_server(self, *_args, **_kwargs) -> None:
            return None

        async def verify_broker_connection(self, *_args, **_kwargs) -> bool:
            return True

    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.service.GoldenPipelineService",
        DummyGoldenService,
    )
    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(RuntimeOrchestrator, "_ensure_ui_forwarder", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(RuntimeOrchestrator, "_register_task", lambda self, task: None)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.perform_startup_async", fake_startup)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Memory", DummyMemory)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Portfolio", DummyPortfolio)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.AgentManager", DummyAgentManager)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.get_feature_flags",
        lambda: types.SimpleNamespace(mode="live"),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.verify_live_account",
        lambda: {
            "balance_sol": 1.234567,
            "min_required_sol": 0.5,
            "blockhash": "abc123",
        },
    )
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator._trading_loop", fake_trading_loop)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.event_bus", DummyEventBus(), raising=False)

    orch = RuntimeOrchestrator(run_http=False)
    await orch.start_agents()

    assert "start" in golden_events
    assert orch._golden_service is not None
    assert any(stage == "golden:start" and ok and "providers=" in detail for stage, ok, detail in published)
    assert any(stage == "wallet:balance" and ok for stage, ok, _ in published)

    await orch.stop_all()


@pytest.mark.anyio("asyncio")
async def test_start_agents_aborts_when_wallet_verification_fails(monkeypatch):
    for key in ("GOLDEN_PIPELINE", "MODE", "RUNTIME_MODE", "TRADING_MODE"):
        monkeypatch.delenv(key, raising=False)
    monkeypatch.setenv("EVENT_DRIVEN", "0")

    published: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        published.append((stage, ok, detail))

    async def fake_startup(*_args, **_kwargs):
        cfg = {
            "mode": "live",
            "memory_path": "memory://",
            "portfolio_path": "portfolio.json",
        }
        depth_proc = types.SimpleNamespace(poll=lambda: None)
        return cfg, {}, depth_proc

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
            self.agents = ["agent"]
            self._rl_window_sec = 0.4

        @classmethod
        def from_config(cls, _cfg):
            return cls()

        @classmethod
        def from_default(cls):
            return cls()

        def set_rl_disabled(self, *_args, **_kwargs) -> None:
            return None

    async def fake_trading_loop(*_args, **_kwargs):
        return None

    class DummyEventBus:
        def publish(self, *_args, **_kwargs) -> None:
            return None

        def subscribe(self, *_args, **_kwargs):
            return lambda: None

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(RuntimeOrchestrator, "_ensure_ui_forwarder", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(RuntimeOrchestrator, "_register_task", lambda self, task: None)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.perform_startup_async", fake_startup)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Memory", DummyMemory)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Portfolio", DummyPortfolio)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.AgentManager", DummyAgentManager)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.get_feature_flags",
        lambda: types.SimpleNamespace(mode="live"),
    )

    def failing_verify() -> dict:
        raise SystemExit("insufficient funds")

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.verify_live_account", failing_verify
    )
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator._trading_loop", fake_trading_loop)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.event_bus", DummyEventBus(), raising=False)

    orch = RuntimeOrchestrator(run_http=False)

    with pytest.raises(RuntimeError) as excinfo:
        await orch.start_agents()

    assert "insufficient funds" in str(excinfo.value)
    assert any(
        stage == "wallet:balance" and not ok and "insufficient funds" in detail
        for stage, ok, detail in published
    )

    await orch.stop_all()


@pytest.mark.anyio("asyncio")
async def test_orchestrator_errors_when_default_agents_missing(monkeypatch, caplog):
    caplog.set_level("ERROR")

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        return None

    async def fake_startup(*_args, **_kwargs):
        cfg = {"mode": "live"}
        depth_proc = types.SimpleNamespace(poll=lambda: None)
        return cfg, {}, depth_proc

    class DummyMemory:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

        def start_writer(self) -> None:
            return None

    class DummyPortfolio:
        def __init__(self, *_args, **_kwargs) -> None:
            pass

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(RuntimeOrchestrator, "_ensure_ui_forwarder", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(RuntimeOrchestrator, "_register_task", lambda self, task: None)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.perform_startup_async", fake_startup)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Memory", DummyMemory)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Portfolio", DummyPortfolio)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.AgentManager.from_default",
        classmethod(lambda cls: None),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.resolve_golden_enabled", lambda cfg: False
    )

    orch = RuntimeOrchestrator(run_http=False)

    with pytest.raises(RuntimeError):
        await orch.start_agents()

    assert "no default agent manager" in caplog.text.lower()
