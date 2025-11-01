import importlib
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
