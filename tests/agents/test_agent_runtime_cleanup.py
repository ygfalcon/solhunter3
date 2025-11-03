import asyncio
import importlib.util
import sys
from pathlib import Path
from unittest.mock import Mock

import pytest

from solhunter_zero import event_bus
from solhunter_zero.exec_service.service import TradeExecutor


_SPEC = importlib.util.spec_from_file_location(
    "solhunter_zero.agents.runtime_impl",
    Path(__file__).resolve().parents[2] / "solhunter_zero" / "agents" / "runtime.py",
)
_MODULE = importlib.util.module_from_spec(_SPEC)
assert _SPEC is not None and _SPEC.loader is not None
sys.modules[_SPEC.name] = _MODULE
_SPEC.loader.exec_module(_MODULE)
AgentRuntime = _MODULE.AgentRuntime


class _DummyManager:
    async def evaluate(self, token: str, portfolio):  # pragma: no cover - simple stub
        return []


class _DummyPortfolio:
    def __init__(self) -> None:
        self.price_history: dict[str, list[float]] = {}
        self.balances: dict[str, Mock] = {}

    def record_prices(self, _: dict[str, float]) -> None:  # pragma: no cover - simple stub
        pass


@pytest.fixture(autouse=True)
def reset_event_bus():
    event_bus.reset()
    yield
    event_bus.reset()


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_agent_runtime_stop_unsubscribes_and_cancels(monkeypatch):
    monkeypatch.setenv("PRICE_BACKFILL", "0")
    runtime = AgentRuntime(_DummyManager(), _DummyPortfolio())

    await runtime.start()

    assert "token_discovered" in event_bus._subscribers
    assert "price_update" in event_bus._subscribers

    extra_task = asyncio.create_task(asyncio.sleep(1))
    runtime._tasks.append(extra_task)

    runtime.stop()
    await asyncio.sleep(0)

    assert extra_task.cancelled()
    assert "token_discovered" not in event_bus._subscribers
    assert "price_update" not in event_bus._subscribers


def test_trade_executor_stop_unsubscribes():
    memory = Mock()
    portfolio = Mock()

    executor = TradeExecutor(memory, portfolio)
    executor.start()

    assert "action_decision" in event_bus._subscribers

    executor.stop()

    assert "action_decision" not in event_bus._subscribers
