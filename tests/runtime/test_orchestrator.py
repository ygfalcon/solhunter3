import asyncio
import types

import pytest

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

from contextlib import suppress
from typing import Any

from solhunter_zero.loop import ResourceBudgetExceeded
from solhunter_zero.runtime.orchestrator import RuntimeOrchestrator
from solhunter_zero.ui import UIState, create_app


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
async def test_event_runtime_close_endpoint_simulates_exit(monkeypatch, tmp_path):
    class _StubEventBus:
        def __init__(self) -> None:
            self._subscribers: dict[str, list] = {}
            self.published: list[tuple[str, Any]] = []
            self._BROKER_URLS: list[str] = []

        def publish(self, topic: str, payload: Any) -> None:
            self.published.append((topic, payload))

        def subscribe(self, topic: str, handler):
            handlers = self._subscribers.setdefault(topic, [])
            handlers.append(handler)

            def _unsubscribe() -> None:
                with suppress(ValueError):
                    handlers.remove(handler)

            return _unsubscribe

        async def start_ws_server(self, *_args, **_kwargs) -> None:
            return None

        async def stop_ws_server(self, *_args, **_kwargs) -> None:
            return None

        async def verify_broker_connection(self, *_args, **_kwargs) -> bool:
            return True

    monkeypatch.setenv("EVENT_DRIVEN", "1")
    monkeypatch.setenv("GOLDEN_PIPELINE", "0")
    monkeypatch.setenv("UI_DISABLE_HTTP_SERVER", "1")
    monkeypatch.setenv("PORTFOLIO_PATH", str(tmp_path / "portfolio.json"))

    stub_bus = _StubEventBus()
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.event_bus", stub_bus, raising=False)
    monkeypatch.setattr("solhunter_zero.ui.event_bus", stub_bus, raising=False)

    async def fake_startup(*_args, **_kwargs):
        cfg = {
            "agents": [{"name": "stub"}],
            "strategies": [],
            "loop_delay": 5,
            "min_delay": 1,
            "max_delay": 5,
        }
        depth_proc = types.SimpleNamespace(terminate=lambda: None)
        return cfg, {}, depth_proc

    class DummyPortfolio:
        def __init__(self, *_args, **_kwargs) -> None:
            self.balances: dict[str, dict[str, float]] = {}
            self.closed_positions: dict[str, dict[str, float]] = {}

        def _apply(self, token: str, delta: float, price: float) -> None:
            record = self.balances.get(token)
            if record is None:
                record = {"amount": 0.0, "entry_price": price, "last_mark": price}
                self.balances[token] = record
            record.setdefault("entry_price", price)
            record["last_mark"] = price
            new_amount = max(record.get("amount", 0.0) + delta, 0.0)
            record["amount"] = new_amount
            if new_amount <= 0:
                self.closed_positions[token] = dict(record)

        def update(self, token: str, amount: float, price: float, **_kwargs) -> None:
            self._apply(token, amount, price)

        async def update_async(self, token: str, amount: float, price: float, **_kwargs) -> None:
            self._apply(token, amount, price)

        def get_position(self, token: str):
            record = self.balances.get(token) or self.closed_positions.get(token)
            if record is None:
                return None
            return types.SimpleNamespace(
                token=token,
                amount=float(record.get("amount", 0.0)),
                entry_price=float(record.get("entry_price", 0.0)),
                last_mark=float(record.get("last_mark", 0.0)),
            )

    class DummyMemory:
        def __init__(self, *_args, **_kwargs) -> None:
            self.started = False
            self.logged: list[dict[str, Any]] = []

        def start_writer(self) -> None:
            self.started = True

        async def log_trade(self, **kwargs) -> None:
            self.logged.append(dict(kwargs))

    class DummyAgentManager:
        def __init__(self) -> None:
            self.agents = [types.SimpleNamespace(name="stub")]
            self.evolve_interval = 10
            self.mutation_threshold = 0.0

        @classmethod
        def from_config(cls, _cfg):
            return cls()

        @classmethod
        def from_default(cls):
            return cls()

        async def evolve(self, *_args, **_kwargs) -> None:
            return None

        async def update_weights(self) -> None:
            return None

        def save_weights(self) -> None:
            return None

        def set_rl_disabled(self, *_args, **_kwargs) -> None:
            return None

    class DummyAgentRuntime:
        def __init__(self, *_args, **_kwargs) -> None:
            self.started = False

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.started = False

    class DummyTradeExecutor:
        def __init__(self, memory, portfolio) -> None:
            self.memory = memory
            self.portfolio = portfolio
            self.started = False

        def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.started = False

    class DummyDiscoveryAgent:
        async def discover_tokens(self, *_args, **_kwargs):
            return []

    async def fake_rl_init(*_args, **_kwargs):
        return None

    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.perform_startup_async", fake_startup)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Memory", DummyMemory)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Portfolio", DummyPortfolio)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.AgentManager", DummyAgentManager)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.AgentRuntime", DummyAgentRuntime, raising=False)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.TradeExecutor", DummyTradeExecutor, raising=False)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.DiscoveryAgent", DummyDiscoveryAgent, raising=False)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator._init_rl_training", fake_rl_init, raising=False)

    orch = RuntimeOrchestrator(run_http=False)
    orch.handles.ui_state = UIState()

    await orch.start_agents()

    assert callable(orch.handles.ui_state.close_position_handler)

    trade_exec = orch.handles.trade_executor
    assert trade_exec is not None
    portfolio = trade_exec.portfolio
    portfolio.update("SOL", 5.0, 12.0)

    app = create_app(orch.handles.ui_state)
    client = app.test_client()

    response = client.post("/api/execution/close?mint=SOL&qty=3")
    assert response.status_code == 200
    assert response.get_json() == {"ok": True, "mint": "SOL", "qty": 3.0}

    remaining = portfolio.get_position("SOL")
    assert remaining is not None
    assert remaining.amount == pytest.approx(2.0)

    assert trade_exec.memory.logged == [
        {
            "token": "SOL",
            "direction": "sell",
            "amount": 3.0,
            "price": 12.0,
            "source": "manual_exit",
        }
    ]

    assert (
        "action_executed",
        {
            "action": {
                "agent": "manual_exit",
                "token": "SOL",
                "side": "sell",
                "amount": 3.0,
                "price": 12.0,
            },
            "result": {"status": "simulated"},
        },
    ) in stub_bus.published

    await orch.stop_all()
