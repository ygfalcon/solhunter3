import asyncio

import pytest

from solhunter_zero.agents.runtime.agent_runtime import AgentRuntime
from solhunter_zero import event_bus


class DummyPortfolio:
    def __init__(self) -> None:
        self.price_history: dict[str, list[float]] = {}
        self.balances: dict[str, object] = {}

    def record_prices(self, prices: dict[str, float]) -> None:
        for token, price in prices.items():
            self.price_history.setdefault(token, []).append(price)


class DummyManager:
    def __init__(self, expected: int, delay: float = 0.05) -> None:
        self.expected = expected
        self.delay = delay
        self.current = 0
        self.max_current = 0
        self.completed = 0
        self.all_done = asyncio.Event()

    async def evaluate(self, token: str, portfolio: DummyPortfolio):  # type: ignore[override]
        self.current += 1
        self.max_current = max(self.max_current, self.current)
        try:
            await asyncio.sleep(self.delay)
            decision = {
                "token": token,
                "side": "buy",
                "amount": 1.0,
                "price": 1.0,
                "agent": "test",
                "conviction_delta": 0.0,
            }
            return [decision]
        finally:
            self.current -= 1
            self.completed += 1
            if self.completed >= self.expected:
                self.all_done.set()


async def _run_runtime_concurrency_test(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PRICE_BACKFILL", "0")
    monkeypatch.setenv("AGENT_EVALUATION_CONCURRENCY", "3")
    event_bus.reset()
    prev_serialization = getattr(event_bus.BUS, "serialization", None)
    prev_token_pb = event_bus._PB_MAP.get("token_discovered")  # type: ignore[attr-defined]
    event_bus.configure(serialization="json")
    event_bus._PB_MAP["token_discovered"] = None  # type: ignore[attr-defined]

    tokens = [f"token-{idx}" for idx in range(10)]
    manager = DummyManager(expected=len(tokens))
    portfolio = DummyPortfolio()
    runtime = AgentRuntime(manager, portfolio)

    await runtime.start()
    try:
        for token in tokens:
            event_bus.publish("token_discovered", {"token": token})

        await asyncio.wait_for(manager.all_done.wait(), timeout=5)
        assert manager.completed == len(tokens)
        assert manager.max_current == 3
    finally:
        await runtime.stop()
        if prev_serialization:
            event_bus.configure(serialization=prev_serialization)
        event_bus._PB_MAP["token_discovered"] = prev_token_pb  # type: ignore[attr-defined]
        event_bus.reset()


def test_agent_runtime_limits_concurrent_evaluations(monkeypatch: pytest.MonkeyPatch) -> None:
    asyncio.run(_run_runtime_concurrency_test(monkeypatch))
