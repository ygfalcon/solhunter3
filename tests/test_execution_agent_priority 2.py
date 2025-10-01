import asyncio
import types
import sys
import pytest

sys.modules.setdefault("torch", types.ModuleType("torch"))

from solhunter_zero.agents.execution import ExecutionAgent


async def fake_place(token, side, amount, price, **_):
    return {"amount": amount}


def test_execution_agent_priority_fee(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.execution.place_order_async",
        fake_place,
    )

    monkeypatch.setattr(
        "solhunter_zero.depth_client.snapshot",
        lambda tok: ({}, 5.0),
    )

    async def fake_fee(urls):
        return 0.1

    monkeypatch.setattr(
        "solhunter_zero.gas.get_priority_fee_estimate",
        fake_fee,
    )

    agent = ExecutionAgent(
        rate_limit=0,
        priority_fees=[0.0, 2.0],
        priority_rpc=["u"],
    )

    res = asyncio.run(
        agent.execute(
            {
                "token": "tok",
                "side": "buy",
                "amount": 1.0,
                "price": 0.0,
                "priority": 1,
            }
        )
    )
    assert res["amount"] == pytest.approx(0.8)
