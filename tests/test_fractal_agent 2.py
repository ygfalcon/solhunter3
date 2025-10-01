import asyncio
import numpy as np

from solhunter_zero.agents.fractal_agent import FractalAgent
from solhunter_zero.memory import Memory
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_fractal_agent_trade(monkeypatch):
    class DummyPywt:
        @staticmethod
        def cwt(data, scales, wavelet):
            return np.ones((len(scales), len(data))), None

    monkeypatch.setattr(
        "solhunter_zero.agents.fractal_agent.pywt",
        DummyPywt,
    )

    monkeypatch.setattr(FractalAgent, "_hurst", lambda self, s: 0.4)

    mem = Memory("sqlite:///:memory:")
    for price in [1.0, 1.0, 1.0, 1.0]:
        mem.log_trade(token="tok", direction="buy", amount=1.0, price=price)
    for price in [1.0, 1.0, 1.0, 1.0]:
        mem.log_trade(token="oth", direction="buy", amount=1.0, price=price)

    agent = FractalAgent(similarity_threshold=0.8, memory=mem)
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"
