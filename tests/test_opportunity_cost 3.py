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

    def fake_run(token, count=1):
        roi = 0.1 if token == 'HOLD' else 0.5
        return [SimulationResult(1.0, roi)]

    monkeypatch.setattr(
        'solhunter_zero.agents.opportunity_cost.run_simulations', fake_run
    )

    agent = OpportunityCostAgent(
        candidates=['A', 'B', 'C', 'D', 'E'], memory_agent=None
    )

    actions1 = asyncio.run(agent.propose_trade('HOLD', pf))
    assert actions1 == []

    actions2 = asyncio.run(agent.propose_trade('HOLD', pf))
    assert actions2 and actions2[0]['side'] == 'sell'
