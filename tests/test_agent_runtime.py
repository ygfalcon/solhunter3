import asyncio
import sys
import types
from unittest.mock import AsyncMock, MagicMock

import pytest


# Provide lightweight stubs for heavy optional dependencies that AgentRuntime imports.
if "solhunter_zero.runtime" not in sys.modules:
    sys.modules["solhunter_zero.runtime"] = types.ModuleType("solhunter_zero.runtime")

if "solhunter_zero.event_bus" not in sys.modules:
    event_bus = types.ModuleType("solhunter_zero.event_bus")

    def _subscribe(_topic: str, _callback):
        return lambda: None

    def _publish(_topic: str, _payload, _broadcast: bool = False):
        return None

    event_bus.subscribe = _subscribe  # type: ignore[attr-defined]
    event_bus.publish = _publish  # type: ignore[attr-defined]
    sys.modules["solhunter_zero.event_bus"] = event_bus

if "solhunter_zero.agent_manager" not in sys.modules:
    agent_manager = types.ModuleType("solhunter_zero.agent_manager")

    class _AgentManager:  # pragma: no cover - simple stub
        pass

    agent_manager.AgentManager = _AgentManager  # type: ignore[attr-defined]
    sys.modules["solhunter_zero.agent_manager"] = agent_manager

if "solhunter_zero.portfolio" not in sys.modules:
    portfolio = types.ModuleType("solhunter_zero.portfolio")

    class _Portfolio:  # pragma: no cover - simple stub
        def __init__(self) -> None:
            self.price_history: dict[str, list[float]] = {}
            self.balances: dict[str, object] = {}

        def record_prices(self, *_args, **_kwargs):
            return None

    portfolio.Portfolio = _Portfolio  # type: ignore[attr-defined]
    sys.modules["solhunter_zero.portfolio"] = portfolio

if "solhunter_zero.prices" not in sys.modules:
    prices = types.ModuleType("solhunter_zero.prices")

    async def _fetch_token_prices_async(_tokens):  # pragma: no cover - simple stub
        return {}

    prices.fetch_token_prices_async = _fetch_token_prices_async  # type: ignore[attr-defined]
    sys.modules["solhunter_zero.prices"] = prices

from solhunter_zero.agents.runtime.agent_runtime import AgentRuntime


def test_on_tokens_accepts_dict_payload(monkeypatch):
    runtime = AgentRuntime(manager=MagicMock(), portfolio=MagicMock())
    runtime._running = True

    mock_evaluate = AsyncMock(return_value=None)
    monkeypatch.setattr(runtime, "_evaluate_and_publish", mock_evaluate)

    async def _invoke() -> None:
        runtime._on_tokens({"mint": "TestMint"})
        # Allow the scheduled task to run
        await asyncio.sleep(0)
        await asyncio.sleep(0)

    asyncio.run(_invoke())

    mock_evaluate.assert_awaited_once_with("TestMint")
    assert "TestMint" in runtime._tokens
    assert not runtime._tasks
