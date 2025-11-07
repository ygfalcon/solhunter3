import asyncio
import importlib.util
import sys
from pathlib import Path

import pytest

import solhunter_zero
from solhunter_zero.portfolio import Portfolio, Position


_RUNTIME_PATH = Path(solhunter_zero.__file__).resolve().parent / "agents" / "runtime.py"
_RUNTIME_SPEC = importlib.util.spec_from_file_location(
    "solhunter_zero.agents._event_runtime_impl", _RUNTIME_PATH
)
assert _RUNTIME_SPEC and _RUNTIME_SPEC.loader
_RUNTIME_MODULE = importlib.util.module_from_spec(_RUNTIME_SPEC)
sys.modules[_RUNTIME_SPEC.name] = _RUNTIME_MODULE
_RUNTIME_SPEC.loader.exec_module(_RUNTIME_MODULE)
AgentRuntime = _RUNTIME_MODULE.AgentRuntime


class RecordingManager:
    def __init__(self, ready: asyncio.Event, expected_tokens: set[str]):
        self._ready = ready
        self._expected = set(expected_tokens)
        self.calls: list[tuple[str, Portfolio]] = []

    async def evaluate(self, token: str, portfolio: Portfolio):
        self.calls.append((token, portfolio))
        seen = {tok for tok, _ in self.calls}
        if self._expected.issubset(seen):
            self._ready.set()
        return []


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_agent_runtime_start_evaluates_existing_portfolio(monkeypatch):
    monkeypatch.setenv("PRICE_BACKFILL", "0")

    portfolio = Portfolio(path=None)
    portfolio.balances = {
        "SOL": Position("SOL", 1.0, 25.0),
        "BONK": Position("BONK", 1000.0, 0.00001),
    }
    portfolio.price_history["SRM"] = [1.0]

    ready = asyncio.Event()
    manager = RecordingManager(ready, set(portfolio.balances.keys()))
    runtime = AgentRuntime(manager, portfolio)

    await runtime.start()
    await asyncio.wait_for(ready.wait(), timeout=0.1)

    called_tokens = [token for token, _ in manager.calls]
    for token in portfolio.balances:
        assert token in called_tokens

    runtime.stop()
