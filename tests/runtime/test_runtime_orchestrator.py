import asyncio
import contextlib
import importlib
import sys
import types

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_start_agents_aborts_when_no_agents(monkeypatch, request):
    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(importlib.import_module("solhunter_zero.runtime.orchestrator"))
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)
    stages: list[tuple[str, bool, str]] = []

    async def capture_stage(stage: str, ok: bool, detail: str = "") -> None:
        stages.append((stage, ok, detail))

    monkeypatch.setattr(orchestrator, "_publish_stage", capture_stage)

    async def fake_startup(config_path: str | None, offline: bool, dry_run: bool):
        return {}, {}, None

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", fake_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_, **__):
            pass

    class DummyAgentManager:
        agents: list = []

        @classmethod
        def from_config(cls, *_: object, **__: object):
            return None

        @classmethod
        def from_default(cls):
            return None

    class DummyStrategyManager:
        def __init__(self, *_, **__):
            pass

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", DummyStrategyManager)

    result = await orchestrator.start_agents()

    assert result is False
    assert orchestrator.handles.tasks == []

    failure_messages = [detail for stage, ok, detail in stages if stage == "agents:loaded" and not ok]
    assert failure_messages, "Expected a failed agents:loaded stage"
    assert "no agents available" in failure_messages[0]


@pytest.mark.anyio("asyncio")
async def test_discovery_loop_respects_offline(monkeypatch, tmp_path, request):
    ui_module = importlib.import_module("solhunter_zero.ui")
    monkeypatch.setattr(ui_module, "create_app", lambda *_, **__: types.SimpleNamespace(), raising=False)
    monkeypatch.setattr(ui_module, "start_websockets", lambda: {}, raising=False)

    runtime_orchestrator = importlib.reload(
        importlib.import_module("solhunter_zero.runtime.orchestrator")
    )
    request.addfinalizer(lambda: importlib.reload(runtime_orchestrator))

    token_file = tmp_path / "tokens.txt"
    token_file.write_text("TokenA\nTokenB\n", encoding="utf-8")

    monkeypatch.setenv("SOLHUNTER_OFFLINE", "1")

    scanner_module = importlib.import_module("solhunter_zero.token_scanner")
    network_called = False

    async def fake_scan_tokens_async(*args, **kwargs):
        nonlocal network_called
        network_called = True
        return []

    monkeypatch.setattr(scanner_module, "scan_tokens_async", fake_scan_tokens_async)

    discovery_module = importlib.import_module("solhunter_zero.agents.discovery")
    original_discover = discovery_module.DiscoveryAgent.discover_tokens
    seen_args: dict[str, object] = {}

    async def wrapped_discover(self, *, offline=False, token_file=None, method=None):
        seen_args["offline"] = offline
        seen_args["token_file"] = token_file
        return await original_discover(
            self, offline=offline, token_file=token_file, method=method
        )

    monkeypatch.setattr(
        discovery_module.DiscoveryAgent,
        "discover_tokens",
        wrapped_discover,
        raising=False,
    )

    async def fake_startup(config_path: str | None, offline: bool, dry_run: bool):
        return (
            {
                "agents": {"dummy": {}},
                "token_file": str(token_file),
                "loop_delay": 1,
            },
            {},
            None,
        )

    monkeypatch.setattr(runtime_orchestrator, "perform_startup_async", fake_startup)

    class DummyMemory:
        def __init__(self, *_: object, **__: object) -> None:
            pass

        def start_writer(self) -> None:
            pass

    class DummyPortfolio:
        def __init__(self, *_, **__):
            pass

    class DummyAgentManager:
        evolve_interval = 30
        mutation_threshold = 0.0

        def __init__(self) -> None:
            self.agents = [types.SimpleNamespace(name="dummy")]

        @classmethod
        def from_config(cls, *_: object, **__: object):
            return cls()

        @classmethod
        def from_default(cls):
            return None

        async def evolve(self, *_, **__):
            pass

        async def update_weights(self):
            pass

        def save_weights(self) -> None:
            pass

    class DummyStrategyManager:
        def __init__(self, *_, **__):
            pass

    class DummyAgentRuntime:
        def __init__(self, *_, **__):
            pass

        async def start(self):
            pass

    class DummyTradeExecutor:
        def __init__(self, *_, **__):
            pass

        def start(self):
            pass

    async def fake_init_rl_training(*_, **__):
        return None

    monkeypatch.setattr(runtime_orchestrator, "Memory", DummyMemory)
    monkeypatch.setattr(runtime_orchestrator, "Portfolio", DummyPortfolio)
    monkeypatch.setattr(runtime_orchestrator, "TradingState", lambda: object())
    monkeypatch.setattr(runtime_orchestrator, "AgentManager", DummyAgentManager)
    monkeypatch.setattr(runtime_orchestrator, "StrategyManager", DummyStrategyManager)
    loop_module = importlib.import_module("solhunter_zero.loop")

    fake_runtime_module = types.ModuleType("solhunter_zero.agents.runtime")
    fake_runtime_module.AgentRuntime = DummyAgentRuntime
    fake_exec_module = types.ModuleType("solhunter_zero.exec_service")
    fake_exec_module.TradeExecutor = DummyTradeExecutor

    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.runtime", fake_runtime_module)
    monkeypatch.setitem(sys.modules, "solhunter_zero.exec_service", fake_exec_module)
    monkeypatch.setattr(loop_module, "_init_rl_training", fake_init_rl_training)

    orchestrator = runtime_orchestrator.RuntimeOrchestrator(run_http=False)

    async def _noop_stage(*_, **__):
        return None

    monkeypatch.setattr(orchestrator, "_publish_stage", _noop_stage)

    discovery_event = asyncio.Event()

    def fake_publish(topic: str, payload: object) -> None:
        if topic == "token_discovered":
            discovery_event.set()

    monkeypatch.setattr(runtime_orchestrator.event_bus, "publish", fake_publish, raising=False)

    result = await orchestrator.start_agents()
    assert result is True

    await asyncio.wait_for(discovery_event.wait(), timeout=1.0)

    for task in list(orchestrator.handles.tasks or []):
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task

    assert seen_args.get("offline") is True
    assert seen_args.get("token_file") == str(token_file)
    assert network_called is False
