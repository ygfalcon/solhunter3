import asyncio
import os
import socket
import threading
import time
import types
from contextlib import closing

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
    await orch.start_ui()

    ws_events = [event for event in events if event.get("stage") == "ui:ws"]
    assert ws_events, "expected ui:ws stage emission"
    ui_stage = ws_events[-1]
    assert ui_stage.get("ok") is False
    assert "boom" in str(ui_stage.get("detail"))


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
        lambda: {},
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
async def test_stop_all_releases_http_socket(monkeypatch):
    stage_events: list[tuple[str, bool, str]] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        stage_events.append((stage, ok, detail))

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._create_ui_app",
        lambda _state: object(),
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.initialise_runtime_wiring",
        lambda _state: None,
    )

    ui_stub = types.SimpleNamespace(UIState=lambda: object(), get_ws_urls=lambda: {}, __name__="stub_ui")
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._ui_module",
        ui_stub,
        raising=False,
    )
    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator._start_ui_ws",
        lambda: {},
    )

    class DummyServer:
        daemon_threads = True

        def __init__(self, host: str, port: int, _app: object) -> None:
            self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._sock.bind((host, port))
            self._sock.listen()
            self.server_address = self._sock.getsockname()
            self._shutdown = threading.Event()
            self._closed = False

        def serve_forever(self) -> None:
            while not self._shutdown.is_set():
                self._shutdown.wait(timeout=0.05)

        def shutdown(self) -> None:
            self._shutdown.set()

        def server_close(self) -> None:
            if not self._closed:
                self._sock.close()
                self._closed = True

    def make_dummy_server(host: str, port: int, app: object) -> DummyServer:
        return DummyServer(host, port, app)

    monkeypatch.setattr(
        "solhunter_zero.runtime.orchestrator.make_server",
        make_dummy_server,
    )

    monkeypatch.setenv("UI_HOST", "127.0.0.1")
    monkeypatch.setenv("UI_PORT", "0")
    monkeypatch.delenv("UI_DISABLE_HTTP_SERVER", raising=False)

    orch = RuntimeOrchestrator(run_http=True)
    await orch.start_ui()

    http_threads = orch.handles.ui_threads or {}
    http_info = http_threads.get("http") if isinstance(http_threads, dict) else None
    assert isinstance(http_info, dict)
    http_thread = http_info.get("thread")
    assert isinstance(http_thread, threading.Thread)
    assert http_thread.is_alive()

    port_value = int(os.getenv("UI_PORT", "0"))
    assert port_value > 0

    await orch.stop_all()

    assert any(stage == "runtime:stopped" for stage, _, _ in stage_events)
    assert not http_thread.is_alive()

    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as test_sock:
        test_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        test_sock.bind(("127.0.0.1", port_value))


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
        lambda: {},
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
