import asyncio
import types

import pytest

from solhunter_zero.agents.conviction import ConvictionAgent
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self) -> None:
        super().__init__(path=None)
        self.balances = {}
        self.price_history = {}


@pytest.mark.parametrize(
    "expected_roi, side",
    [
        (0.2, "buy"),
        (-0.2, "sell"),
    ],
)
def test_conviction_agent_emits_positive_price(monkeypatch, expected_roi, side):
    agent = ConvictionAgent(threshold=0.05, count=1)
    portfolio = DummyPortfolio()
    if side == "sell":
        portfolio.balances["tok"] = Position("tok", 1.0, 1.0, 1.0)

    monkeypatch.setattr(
        "solhunter_zero.agents.conviction.run_simulations",
        lambda t, count=1, **_: [types.SimpleNamespace(expected_roi=expected_roi)],
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.conviction.predict_price_movement",
        lambda token, *, sentiment=None, model_path=None: 0.0,
    )
    monkeypatch.setattr(
        ConvictionAgent,
        "_predict_activity",
        lambda self, token: 0.0,
    )

    async def price_lookup(tokens):
        return {next(iter(tokens)): 1.5}

    monkeypatch.setattr(
        "solhunter_zero.agents.conviction.fetch_token_prices_async",
        price_lookup,
    )
    monkeypatch.setattr(
        "solhunter_zero.agents.conviction.get_cached_price",
        lambda token: None,
    )

    actions = asyncio.run(agent.propose_trade("tok", portfolio))

    assert actions, "agent should emit a trade"
    assert {a["side"] for a in actions} == {side}
    for action in actions:
        assert action["price"] > 0
