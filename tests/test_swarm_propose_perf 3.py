import asyncio
import inspect
import time

from tests.stubs import stub_numpy
from solhunter_zero.agents.swarm import AgentSwarm
from solhunter_zero.portfolio import Portfolio
from solhunter_zero import order_book_ws


class DummyAgent:
    def __init__(self, name):
        self.name = name

    async def propose_trade(self, token, portfolio, *, depth=None, imbalance=None, rl_action=None, summary=None):
        return []

def _baseline(swarm, token, portfolio):
    depth, imbalance, _ = order_book_ws.snapshot(token)

    async def run(agent):
        kwargs = dict(depth=depth, imbalance=imbalance)
        if "rl_action" in inspect.signature(agent.propose_trade).parameters:
            kwargs["rl_action"] = None
        if "summary" in inspect.signature(agent.propose_trade).parameters:
            kwargs["summary"] = None
        return await agent.propose_trade(token, portfolio, **kwargs)

    return asyncio.gather(*(run(a) for a in swarm.agents))


def test_propose_cached_faster(monkeypatch):
    stub_numpy()
    monkeypatch.setattr(order_book_ws, "snapshot", lambda t: (0.0, 0.0, 0.0))
    agents = [DummyAgent(str(i)) for i in range(5)]
    swarm = AgentSwarm(agents)
    pf = Portfolio(path=None)

    async def measure(func):
        start = time.perf_counter()
        for _ in range(20):
            await func()
        return time.perf_counter() - start

    async def baseline():
        await _baseline(swarm, "tok", pf)

    async def cached():
        await swarm.propose("tok", pf)

    baseline_time = asyncio.run(measure(baseline))
    cached_time = asyncio.run(measure(cached))
    assert cached_time <= baseline_time

