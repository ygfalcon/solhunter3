import importlib.util
import sys
from pathlib import Path

import pytest

from solhunter_zero import event_bus
import solhunter_zero.agents as agents_pkg

_RUNTIME_IMPL_SPEC = importlib.util.spec_from_file_location(
    "solhunter_zero.agents.agent_runtime_impl",
    Path(agents_pkg.__file__).with_name("runtime.py"),
)
assert _RUNTIME_IMPL_SPEC is not None and _RUNTIME_IMPL_SPEC.loader is not None
_runtime_impl = importlib.util.module_from_spec(_RUNTIME_IMPL_SPEC)
sys.modules[_RUNTIME_IMPL_SPEC.name] = _runtime_impl
_RUNTIME_IMPL_SPEC.loader.exec_module(_runtime_impl)
AgentRuntime = _runtime_impl.AgentRuntime


class DummyManager:
    async def evaluate(self, token, portfolio):  # pragma: no cover - stub
        return []


class DummyPortfolio:
    def __init__(self) -> None:
        self.price_history: dict[str, list[float]] = {}
        self.balances: dict[str, float] = {}

    def record_prices(self, prices):  # pragma: no cover - stub
        for token, price in prices.items():
            self.price_history.setdefault(token, []).append(price)


class TrackingAgentRuntime(AgentRuntime):
    def __init__(self, manager, portfolio) -> None:
        super().__init__(manager, portfolio)
        self.tokens_events: list[object] = []

    def _on_tokens(self, payload):
        self.tokens_events.append(payload)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_stop_unsubscribes_event_handlers(monkeypatch):
    monkeypatch.setenv("PRICE_BACKFILL", "0")

    runtime = TrackingAgentRuntime(DummyManager(), DummyPortfolio())
    await runtime.start()

    event_bus.publish("token_discovered", {"tokens": ["alpha"]})
    assert len(runtime.tokens_events) == 1
    first_event = runtime.tokens_events[0]
    assert getattr(first_event, "tokens", None) == ["alpha"]

    runtime.stop()

    event_bus.publish("token_discovered", {"tokens": ["beta"]})
    assert len(runtime.tokens_events) == 1
