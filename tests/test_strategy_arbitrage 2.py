import asyncio
import os
from solhunter_zero.strategy_manager import StrategyManager
from solhunter_zero import arbitrage


def test_strategy_manager_returns_arbitrage_actions(monkeypatch):
    async def fake_detect(token, threshold, amount, dry_run=False, **kwargs):
        called['args'] = (token, threshold, amount, dry_run)
        return (0, 1)

    called = {}
    monkeypatch.setattr(arbitrage, 'detect_and_execute_arbitrage', fake_detect)
    monkeypatch.setenv('ARBITRAGE_THRESHOLD', '0.1')
    monkeypatch.setenv('ARBITRAGE_AMOUNT', '2.0')

    sm = StrategyManager(strategies=['solhunter_zero.arbitrage'])
    portfolio = object()
    actions = asyncio.run(sm.evaluate('tok', portfolio))

    assert called['args'] == ('tok', 0.1, 2.0, True)
    assert actions == [
        {'token': 'tok', 'side': 'buy', 'amount': 2.0, 'price': 0.0},
        {'token': 'tok', 'side': 'sell', 'amount': 2.0, 'price': 0.0},
    ]

