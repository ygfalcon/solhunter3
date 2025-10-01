import asyncio
from solhunter_zero.agents.arbitrage import ArbitrageAgent
from solhunter_zero.portfolio import Portfolio


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


async def low(token):
    return 1.0


async def high(token):
    return 1.2


def test_arbitrage_fees_block_trade():
    agent = ArbitrageAgent(
        threshold=0.0,
        amount=1.0,
        feeds={"dex1": low, "dex2": high},
        fees={"dex1": 0.1, "dex2": 0.1},
        gas={"dex1": 0.1, "dex2": 0.1},
    )
    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))
    assert actions == []


def test_gas_multiplier(monkeypatch):
    monkeypatch.setenv("GAS_MULTIPLIER", "3.0")
    agent = ArbitrageAgent(
        threshold=0.0,
        amount=1.0,
        feeds={"dex1": low, "dex2": high},
        gas={"dex1": 0.05, "dex2": 0.05},
    )
    actions = asyncio.run(agent.propose_trade("tok", DummyPortfolio()))
    assert actions == []

