import asyncio
import pytest
pytest.importorskip("torch.nn.utils.rnn")

from solhunter_zero.agents.ppo_agent import PPOAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.portfolio import Portfolio, Position


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_ppo_agent_buy(tmp_path):
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1)
    mem.log_trade(token="tok", direction="sell", amount=1, price=2)
    mem_agent = MemoryAgent(mem)
    agent = PPOAgent(memory_agent=mem_agent, model_path=tmp_path / "ppo.pt")
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


def test_ppo_agent_sell(tmp_path):
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=2)
    mem.log_trade(token="tok", direction="sell", amount=1, price=1)
    mem_agent = MemoryAgent(mem)
    agent = PPOAgent(memory_agent=mem_agent, model_path=tmp_path / "ppo.pt")
    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 1, 1.0, 1.0)
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions
    assert agent._fitted


def test_ppo_serialization(tmp_path):
    path = tmp_path / "model.pt"
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1)
    mem.log_trade(token="tok", direction="sell", amount=1, price=2)
    agent = PPOAgent(memory_agent=MemoryAgent(mem), model_path=path)
    pf = DummyPortfolio()
    asyncio.run(agent.propose_trade("tok", pf))
    assert path.exists()

    agent2 = PPOAgent(memory_agent=MemoryAgent(mem), model_path=path)
    pf2 = DummyPortfolio()
    asyncio.run(agent2.propose_trade("tok", pf2))
    assert agent2._fitted
