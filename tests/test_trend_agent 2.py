import asyncio
import sys
import types

import pytest
trans = pytest.importorskip("transformers")
if not hasattr(trans, "pipeline"):
    trans.pipeline = lambda *a, **k: lambda x: []

dummy_torch = types.ModuleType("torch")
dummy_nn = types.ModuleType("torch.nn")
class DummyModule:  # minimal stand-in for torch.nn.Module
    pass
dummy_nn.Module = DummyModule
dummy_torch.nn = dummy_nn
class DummyTensor:
    pass
dummy_torch.Tensor = DummyTensor
sys.modules.setdefault("torch", dummy_torch)
sys.modules.setdefault("torch.nn", dummy_nn)

from solhunter_zero.agents.trend import TrendAgent
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.agent_manager import AgentManager
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_trend_agent_buy(monkeypatch):
    async def fake_trend():
        return ["tok"]

    async def fake_metrics(token):
        return {"volume": 200.0}

    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_token_metrics_async", fake_metrics
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_sentiment_async", lambda *a, **k: 0.5
    )

    agent = TrendAgent(volume_threshold=100.0, sentiment_threshold=0.1, feeds=["f"])
    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))
    assert actions and actions[0]["side"] == "buy"


def test_agent_manager_trend_integration(monkeypatch):
    async def fake_trend():
        return ["tok"]

    async def fake_metrics(token):
        return {"volume": 200.0}

    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_trending_tokens_async", fake_trend
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_token_metrics_async", fake_metrics
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.trend.fetch_sentiment_async", lambda *a, **k: 0.5
    )

    captured = {}

    async def fake_place(token, side, amount, price, **_):
        captured["side"] = side
        return {"ok": True}

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.place_order_async", fake_place
    )

    exec_agent = ExecutionAgent(rate_limit=0)
    agent = TrendAgent(volume_threshold=100.0, sentiment_threshold=0.1, feeds=["f"])
    mgr = AgentManager([agent], executor=exec_agent)

    pf = DummyPortfolio()
    asyncio.run(mgr.execute("tok", pf))

    assert captured.get("side") == "buy"
