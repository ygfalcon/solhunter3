import asyncio
import types

import pytest

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.execution import ExecutionAgent


class _TestPortfolio:
    def __init__(self):
        self.price_history = {"TEST": [1.0, 1.1, 1.2]}
        self.balances = {}


class _StrongBuyer:
    name = "strong_buyer"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return [
            {
                "token": token,
                "side": "buy",
                "amount": 3.0,
                "price": 12.0,
            }
        ]


class _WeakBuyer:
    name = "weak_buyer"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return [
            {
                "token": token,
                "side": "buy",
                "amount": 1.0,
                "price": 20.0,
            }
        ]


class _Seller:
    name = "seller"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return [
            {
                "token": token,
                "side": "sell",
                "amount": 2.0,
                "price": 15.0,
            }
        ]


def test_bot_execution_merges_actions(monkeypatch):
    orders = []

    async def fake_place_order(token, side, amount, price, **kwargs):
        orders.append(
            {
                "token": token,
                "side": side,
                "amount": amount,
                "price": price,
            }
        )
        return {"ok": True, "filled": amount}

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.place_order_async", fake_place_order
    )
    monkeypatch.setattr(
        "solhunter_zero.event_bus.publish",
        lambda *a, **k: None,
    )

    exec_agent = ExecutionAgent(rate_limit=0)
    cfg = AgentManagerConfig()
    mgr = AgentManager(
        [_StrongBuyer(), _WeakBuyer(), _Seller()],
        executor=exec_agent,
        config=cfg,
    )

    async def _run():
        return await mgr.execute("TEST", _TestPortfolio())

    try:
        results = asyncio.run(_run())
    finally:
        mgr.close()

    assert len(orders) == 1
    order = orders[0]
    assert order["token"] == "TEST"
    assert order["side"] == "buy"
    assert pytest.approx(order["amount"]) == 2.0
    assert pytest.approx(order["price"]) == 14.0

    assert results == [{"ok": True, "filled": pytest.approx(2.0)}]
