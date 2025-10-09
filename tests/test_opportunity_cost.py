import asyncio


from solhunter_zero.agents.opportunity_cost import OpportunityCostAgent
from solhunter_zero.portfolio import Portfolio, Position
from solhunter_zero.simulation import SimulationResult


class DummyPortfolio(Portfolio):
    def __init__(self):
        super().__init__(path=None)
        self.balances = {}


def test_opportunity_cost_sell_after_flags(monkeypatch):
    pf = DummyPortfolio()
    pf.balances['HOLD'] = Position('HOLD', 1.0, 1.0, 1.0)

    async def fake_run(token, count=1):
        roi = 0.1 if token == 'HOLD' else 0.5
        return [SimulationResult(1.0, roi)]

    monkeypatch.setattr(
        'solhunter_zero.agents.opportunity_cost.run_simulations_async', fake_run
    )

    async def fake_price(token, portfolio):
        return 1.0, {}

    monkeypatch.setattr(
        'solhunter_zero.agents.opportunity_cost.resolve_price', fake_price
    )

    agent = OpportunityCostAgent(
        candidates=['A', 'B', 'C', 'D', 'E'], memory_agent=None
    )

    actions1 = asyncio.run(agent.propose_trade('HOLD', pf))
    assert actions1 == []

    actions2 = asyncio.run(agent.propose_trade('HOLD', pf))
    assert actions2 and actions2[0]['side'] == 'sell'


def test_opportunity_cost_scoring_does_not_block(monkeypatch):
    pf = DummyPortfolio()
    pf.balances['HOLD'] = Position('HOLD', 1.0, 1.0, 1.0)

    async def slow_run(token, count=1):
        await asyncio.sleep(0.05)
        roi = 0.1 if token == 'HOLD' else 0.5
        return [SimulationResult(1.0, roi)]

    monkeypatch.setattr(
        'solhunter_zero.agents.opportunity_cost.run_simulations_async', slow_run
    )

    async def fake_price(token, portfolio):
        return 1.0, {}

    monkeypatch.setattr(
        'solhunter_zero.agents.opportunity_cost.resolve_price', fake_price
    )

    agent = OpportunityCostAgent(candidates=['ALT'])

    async def background_task():
        await asyncio.sleep(0.01)
        return 'done'

    async def runner():
        trade_task = asyncio.create_task(agent.propose_trade('HOLD', pf))
        other_task = asyncio.create_task(background_task())
        done, _ = await asyncio.wait(
            {trade_task, other_task}, return_when=asyncio.FIRST_COMPLETED
        )
        assert other_task in done
        await trade_task
        await other_task

    asyncio.run(runner())
