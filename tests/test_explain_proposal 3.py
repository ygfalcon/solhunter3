import asyncio

import pytest
pytest.importorskip("transformers")
pytest.importorskip("torch.nn.utils.rnn")

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


class ExplainAgent:
    name = "explain"

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None):
        return [
            {
                "token": token,
                "side": "buy",
                "amount": 1.0,
                "price": 1.0,
            }
        ]

    def explain_proposal(self, actions=None, token=None, portfolio=None):
        return "because reasons"


def test_explanation_persisted(tmp_path, monkeypatch):
    db = tmp_path / "mem.db"
    idx = tmp_path / "index.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    exec_agent = ExecutionAgent(rate_limit=0)

    async def fake_place(token, side, amount, price, **_):
        return {"ok": True, "price": price}

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.place_order_async", fake_place
    )

    agent = ExplainAgent()
    cfg = AgentManagerConfig(memory_agent=mem_agent)
    mgr = AgentManager([agent, mem_agent], executor=exec_agent, config=cfg)

    pf = DummyPortfolio()
    asyncio.run(mgr.execute("TOK", pf))

    trades = mem.list_trades()
    assert trades and trades[0].context == "because reasons"
