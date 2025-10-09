from __future__ import annotations

import asyncio

from solhunter_zero.agents import BaseAgent
from solhunter_zero.agents.swarm import AgentSwarm
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


class StaticAgent(BaseAgent):
    def __init__(self, name: str, proposals: list[dict[str, float | str]]):
        self.name = name
        self._proposals = proposals

    async def propose_trade(self, token, portfolio, **_):
        return [dict(p) for p in self._proposals]


def test_swarm_marks_needs_price(monkeypatch):
    monkeypatch.setattr(
        "solhunter_zero.agents.swarm.order_book_ws.snapshot",
        lambda token: (0.0, 0.0, 0.0),
    )

    zero_agent = StaticAgent(
        "zero",
        [{"token": "tok", "side": "buy", "amount": 1.0, "price": 0.0}],
    )
    priced_agent = StaticAgent(
        "priced",
        [{"token": "tok", "side": "buy", "amount": 1.0, "price": 1.0}],
    )

    swarm = AgentSwarm([zero_agent, priced_agent])
    portfolio = DummyPortfolio()

    actions = asyncio.run(swarm.propose("tok", portfolio))
    assert actions and actions[0]["side"] == "buy"
    metadata = actions[0].get("metadata")
    assert metadata and metadata.get("needs_price") is True
    assert metadata.get("needs_price_agents") == ["zero"]
