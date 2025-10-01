import pytest

from solhunter_zero.backtest_pipeline import rolling_backtest
from solhunter_zero.risk import RiskManager


def test_rolling_backtest_respects_min_portfolio_value():
    history = {"TKN": [100.0, 110.0, 121.0]}
    weights = {"TKN": 1.0}
    rm = RiskManager(min_portfolio_value=100)

    result = rolling_backtest(history, weights, rm, window=2, step=1)

    assert result.roi == pytest.approx(0.099, rel=1e-3)
    assert result.risk.min_portfolio_value == 100
