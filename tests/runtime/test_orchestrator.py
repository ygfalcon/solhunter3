import asyncio
import types

import pytest

import tests.runtime.test_trading_runtime  # ensure optional deps stubbed

from solhunter_zero import ui
from solhunter_zero.loop import ResourceBudgetExceeded
from solhunter_zero.portfolio import Portfolio as RealPortfolio, Position
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
async def test_orchestrator_exposes_portfolio_snapshots_via_http(monkeypatch):
    monkeypatch.setenv("EVENT_DRIVEN", "0")

    seeded_portfolio = RealPortfolio(path=None)
    seeded_portfolio.balances = {
        "MINT_LONG": Position(
            token="MINT_LONG",
            amount=2.5,
            entry_price=1.1,
            last_mark=1.6,
            realized_pnl=3.0,
            unrealized_pnl=1.25,
            breakeven_price=1.05,
            breakeven_bps=45.0,
            lifecycle="open",
            attribution={"entries": ["seed"], "exits": []},
        ),
        "MINT_SHORT": Position(
            token="MINT_SHORT",
            amount=-1.2,
            entry_price=0.9,
            last_mark=0.75,
            realized_pnl=-1.5,
            unrealized_pnl=0.4,
            breakeven_price=0.82,
            breakeven_bps=30.0,
            lifecycle="open",
            attribution={"entries": ["hedge"], "exits": []},
        ),
    }

    async def fake_publish_stage(self, *_args, **_kwargs) -> None:
        return None

    async def fake_startup(*_args, **_kwargs):
        cfg = {
            "strategies": [],
            "agents": [],
            "loop_delay": 10,
            "min_delay": 5,
            "max_delay": 10,
            "cpu_low_threshold": 10.0,
            "cpu_high_threshold": 90.0,
            "depth_freq_low": 1.0,
            "depth_freq_high": 5.0,
            "depth_rate_limit": 0.1,
            "rl_auto_train": False,
            "rl_interval": 3600.0,
            "use_mev_bundles": False,
        }
        depth_proc = types.SimpleNamespace(poll=lambda: None)
        return cfg, {}, depth_proc

    async def fake_trading_loop(*_args, **_kwargs):
        return None

    class DummyMemory:
        def __init__(self, *_args, **_kwargs) -> None:
            self.started = False

        def start_writer(self) -> None:
            self.started = True

    class DummyAgentManager:
        def __init__(self) -> None:
            self.agents: list[str] = []

        @classmethod
        def from_config(cls, *_args, **_kwargs):
            return cls()

        @classmethod
        def from_default(cls, *_args, **_kwargs):
            return cls()

        def set_rl_disabled(self, *_args, **_kwargs) -> None:
            return None

    monkeypatch.setattr(RuntimeOrchestrator, "_publish_stage", fake_publish_stage)
    monkeypatch.setattr(RuntimeOrchestrator, "_ensure_ui_forwarder", lambda self: asyncio.sleep(0))
    monkeypatch.setattr(RuntimeOrchestrator, "_register_task", lambda self, task: None)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.perform_startup_async", fake_startup)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Memory", DummyMemory)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.Portfolio", lambda *args, **kwargs: seeded_portfolio)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.AgentManager", DummyAgentManager)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator._trading_loop", fake_trading_loop)
    monkeypatch.setattr("solhunter_zero.runtime.orchestrator.resolve_golden_enabled", lambda _cfg: False)

    orch = RuntimeOrchestrator(run_http=False)
    orch.handles.ui_state = ui.UIState()

    await orch.start_agents()

    positions = orch.handles.ui_state.snapshot_positions()
    assert {entry["mint"] for entry in positions} == {"MINT_LONG", "MINT_SHORT"}
    long_entry = next(entry for entry in positions if entry["mint"] == "MINT_LONG")
    assert long_entry["side"] == "long"
    assert pytest.approx(long_entry["notional_usd"], rel=1e-6) == 2.5 * 1.6

    pnl = orch.handles.ui_state.snapshot_pnl("6h")
    assert pnl["window"] == "6h"
    assert pnl["positions"] == 2
    expected_realized = 3.0 - 1.5
    expected_unrealized = 1.25 + 0.4
    assert pytest.approx(pnl["realized_usd"], rel=1e-6) == expected_realized
    assert pytest.approx(pnl["unrealized_usd"], rel=1e-6) == expected_unrealized
    assert pytest.approx(pnl["total_usd"], rel=1e-6) == expected_realized + expected_unrealized

    app = ui.create_app(state=orch.handles.ui_state)
    client = app.test_client()

    positions_resp = client.get("/api/portfolio/positions")
    assert positions_resp.status_code == 200
    payload = positions_resp.get_json()
    assert {entry["mint"] for entry in payload} == {"MINT_LONG", "MINT_SHORT"}

    pnl_resp = client.get("/api/portfolio/pnl?window=12h")
    assert pnl_resp.status_code == 200
    pnl_payload = pnl_resp.get_json()
    assert pnl_payload["window"] == "12h"
    assert pytest.approx(pnl_payload["realized_usd"], rel=1e-6) == expected_realized
    assert pytest.approx(pnl_payload["unrealized_usd"], rel=1e-6) == expected_unrealized

    await orch.stop_all()
