import asyncio

from solhunter_zero.agents.alien_cipher_agent import AlienCipherAgent
from solhunter_zero.datasets.alien_cipher import DEFAULT_PATH as CIPHER_PATH
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_alien_cipher_agent_buy():
    agent = AlienCipherAgent(threshold=0.5, dataset_path=CIPHER_PATH)
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


def test_alien_cipher_agent_sell():
    agent = AlienCipherAgent(threshold=0.5, dataset_path=CIPHER_PATH)
    pf = DummyPortfolio()
    pf.balances["token"] = Position("token", 1, 1.0, 1.0)
    actions = asyncio.run(agent.propose_trade("token", pf))
    assert actions and actions[0]["side"] == "sell"


def test_alien_cipher_agent_no_action():
    agent = AlienCipherAgent(threshold=0.5, dataset_path=CIPHER_PATH)
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("unknown", pf))
    assert actions == []
