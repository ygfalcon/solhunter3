import asyncio
import numpy as np

from solhunter_zero.agents.strange_attractor import StrangeAttractorAgent
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_strange_attractor_trade(monkeypatch):
    agent = StrangeAttractorAgent(divergence=0.5)
    agent.add_example([1.0, 1.0, 1.0], "buy")

    monkeypatch.setattr(
        agent,
        "_compute_attractor",
        lambda d, e, v: np.array([1.1, 1.0, 1.0], dtype="float32"),
    )

    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("TOK", pf, depth=1.0))
    assert actions and actions[0]["side"] == "buy"
    assert actions[0]["manifold_overlap"] > 0.0

