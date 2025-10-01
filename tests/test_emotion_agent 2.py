import asyncio
import types

import pytest
trans = pytest.importorskip("transformers")
if not hasattr(trans, "pipeline"):
    trans.pipeline = lambda *a, **k: lambda x: []

from solhunter_zero.agent_manager import AgentManager, AgentManagerConfig
from solhunter_zero.agents.execution import ExecutionAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.emotion_agent import EmotionAgent
from solhunter_zero.advanced_memory import AdvancedMemory
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_emotion_logged(tmp_path, monkeypatch):
    db = tmp_path / "mem.db"
    idx = tmp_path / "index.faiss"
    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx))
    mem_agent = MemoryAgent(mem)
    emo_agent = EmotionAgent()
    exec_agent = ExecutionAgent(rate_limit=0)

    async def fake_place(token, side, amount, price, **_):
        return {"ok": True, "price": price}

    monkeypatch.setattr(
        "solhunter_zero.agents.execution.place_order_async", fake_place
    )

    async def proposer(token, pf, *, depth=None, imbalance=None):
        return [
            {
                "token": token,
                "side": "buy",
                "amount": 1.0,
                "price": 1.0,
                "conviction_delta": 1.0,
                "regret": 0.0,
                "misfires": 0,
            }
        ]

    dummy = types.SimpleNamespace(propose_trade=proposer, name="dummy")
    cfg = AgentManagerConfig(memory_agent=mem_agent, emotion_agent=emo_agent)
    mgr = AgentManager(
        [dummy, emo_agent, mem_agent],
        executor=exec_agent,
        config=cfg,
    )

    pf = DummyPortfolio()
    asyncio.run(mgr.execute("TOK", pf))

    trades = mem.list_trades()
    assert trades and trades[0].emotion == "confident"
