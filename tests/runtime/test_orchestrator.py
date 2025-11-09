import asyncio
import importlib
import socket
import sys
import types
from contextlib import closing

import pytest


def _install_solana_stub() -> None:
    try:
        importlib.import_module("solana.rpc.core")
        return
    except Exception:
        solana_module = types.ModuleType("solana")
        rpc_module = types.ModuleType("solana.rpc")
        api_module = types.ModuleType("solana.rpc.api")
        async_module = types.ModuleType("solana.rpc.async_api")
        core_module = types.ModuleType("solana.rpc.core")

        class _RPCException(RuntimeError):
            pass

        api_module.Client = lambda *args, **kwargs: None
        async_module.AsyncClient = lambda *args, **kwargs: None
        core_module.RPCException = _RPCException

        solana_module.__path__ = []  # mark as package
        solana_module.rpc = rpc_module
        rpc_module.__path__ = []
        rpc_module.api = api_module
        rpc_module.async_api = async_module
        rpc_module.core = core_module

        sys.modules["solana"] = solana_module
        sys.modules["solana.rpc"] = rpc_module
        sys.modules["solana.rpc.api"] = api_module
        sys.modules["solana.rpc.async_api"] = async_module
        sys.modules["solana.rpc.core"] = core_module


_install_solana_stub()

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

from solhunter_zero.loop import ResourceBudgetExceeded
from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


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
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator._trading_loop", fake_trading_loop)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.event_bus", DummyEventBus(), raising=False)

    orch = RuntimeOrchestrator(run_http=False)
    await orch.start_agents()

    assert "start" in golden_events
    assert orch._golden_service is not None
    assert any(stage == "golden:start" and ok and "providers=" in detail for stage, ok, detail in published)

    await orch.stop_all()


@pytest.mark.anyio("asyncio")
async def test_orchestrator_ui_restart_rebinds_ports(monkeypatch):
    host = "127.0.0.1"
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind((host, 0))
        http_port = sock.getsockname()[1]
    ws_port = http_port + 100

    ports: list[str] = []
    active_sockets: list[socket.socket] = []

    async def fake_publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        if stage == "ui:http" and ok:
            parts = detail.split("port=")
            if len(parts) > 1:
                ports.append(parts[1].split()[0])

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)

    import solhunter_zero.runtime.orchestrator as orch_module

    def fake_start_ui_ws() -> dict[str, object]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, ws_port))
        except OSError as exc:
            sock.close()
            raise RuntimeError(f"ws port {ws_port} busy") from exc
        sock.listen()
        active_sockets.append(sock)
        return {"events": sock}

    def fake_stop_ws() -> None:
        while active_sockets:
            active_sockets.pop().close()

    monkeypatch.setattr(orch_module, "_start_ui_ws", fake_start_ui_ws)
    monkeypatch.setattr(orch_module._ui_module, "stop_websockets", fake_stop_ws, raising=False)

    monkeypatch.setenv("UI_HOST", host)
    monkeypatch.setenv("UI_PORT", str(http_port))
    monkeypatch.delenv("UI_DISABLE_HTTP_SERVER", raising=False)

    for _ in range(2):
        orch = RuntimeOrchestrator(run_http=True)
        await orch.start_ui()
        await orch.stop_all()
        assert not active_sockets
        assert orch.handles.ui_threads is None
        assert orch.handles.ui_server is None
        assert orch.handles.ui_http_thread is None

    assert len(ports) == 2
    assert ports[0] == ports[1] == str(http_port)
