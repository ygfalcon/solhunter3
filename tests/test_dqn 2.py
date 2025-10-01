import asyncio

from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.replay import ReplayBuffer
import pytest
pytest.importorskip("torch.nn.utils.rnn")
import torch
import random

torch.manual_seed(0)
random.seed(0)


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_dqn_agent_buy(tmp_path):
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1)
    mem.log_trade(token="tok", direction="sell", amount=1, price=2)
    mem_agent = MemoryAgent(mem)
    agent = DQNAgent(memory_agent=mem_agent, epsilon=0.0, model_path=tmp_path / "dqn.pt")
    pf = DummyPortfolio()
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions and actions[0]["side"] == "buy"


def test_dqn_agent_sell(tmp_path):
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=2)
    mem.log_trade(token="tok", direction="sell", amount=1, price=1)
    mem_agent = MemoryAgent(mem)
    agent = DQNAgent(memory_agent=mem_agent, epsilon=0.0, model_path=tmp_path / "dqn.pt")
    pf = DummyPortfolio()
    pf.balances["tok"] = Position("tok", 1, 1.0, 1.0)
    actions = asyncio.run(agent.propose_trade("tok", pf))
    assert actions
    assert agent._fitted


def test_dqn_serialization(tmp_path):
    path = tmp_path / "model.pt"
    mem = Memory("sqlite:///:memory:")
    mem.log_trade(token="tok", direction="buy", amount=1, price=1)
    mem.log_trade(token="tok", direction="sell", amount=1, price=2)
    agent = DQNAgent(memory_agent=MemoryAgent(mem), epsilon=0.0, model_path=path)
    pf = DummyPortfolio()
    first = asyncio.run(agent.propose_trade("tok", pf))
    assert path.exists()

    # New agent should load from file
    agent2 = DQNAgent(memory_agent=MemoryAgent(mem), epsilon=0.0, model_path=path)
    assert agent2._fitted
    pf2 = DummyPortfolio()
    second = asyncio.run(agent2.propose_trade("tok", pf2))


def test_replay_sampling_bias(tmp_path):
    buf = ReplayBuffer(url=f"sqlite:///{tmp_path/'replay.db'}")
    for _ in range(5):
        buf.add([1.0], "buy", 1.0, "confident")
    for _ in range(20):
        buf.add([1.0], "buy", -1.0, "anxious")
    buf.add([1.0], "buy", -1.0, "regret")

    counts = {"confident": 0, "anxious": 0}
    for _ in range(50):
        state, action, reward, emo, reg = buf.sample(1)[0]
        assert emo != "regret"
        counts[emo] += 1

    ratio = counts["confident"] / counts["anxious"]
    assert ratio > 0.25
