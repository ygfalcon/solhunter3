import numpy as np
import importlib.util
from pathlib import Path

# Load backtester module without importing the package __init__ which
# requires optional dependencies like `solders`.
_path = Path(__file__).resolve().parents[1] / "solhunter_zero" / "backtester.py"
spec = importlib.util.spec_from_file_location("backtester", _path)
backtester = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = backtester
assert spec.loader is not None
spec.loader.exec_module(backtester)


def random_walk_prices(n=100, start=100.0):
    """Generate a simple random walk of length ``n``."""
    np.random.seed(0)
    return (np.cumsum(np.random.normal(0, 1, size=n)) + start).tolist()


def test_backtester_on_random_data():
    prices = random_walk_prices(50)

    results = backtester.backtest_strategies(prices)
    assert results
    for res in results:
        assert isinstance(res, backtester.StrategyResult)
        assert isinstance(res.roi, float)
        assert isinstance(res.sharpe, float)
        assert isinstance(res.max_drawdown, float)
        assert isinstance(res.volatility, float)
        assert isinstance(res.cumulative_returns, list)
        assert not np.isnan(res.roi)
        assert not np.isnan(res.sharpe)
        assert not np.isnan(res.max_drawdown)
        assert not np.isnan(res.volatility)
        if res.cumulative_returns:
            assert all(isinstance(v, float) for v in res.cumulative_returns)

    weights = {name: 1.0 for name, _ in backtester.DEFAULT_STRATEGIES}
    weighted = backtester.backtest_weighted(prices, weights)
    assert isinstance(weighted, backtester.StrategyResult)
    assert isinstance(weighted.roi, float)
    assert isinstance(weighted.sharpe, float)
    assert isinstance(weighted.max_drawdown, float)
    assert isinstance(weighted.volatility, float)
    assert isinstance(weighted.cumulative_returns, list)
    assert not np.isnan(weighted.roi)
    assert not np.isnan(weighted.sharpe)
    assert not np.isnan(weighted.max_drawdown)
    assert not np.isnan(weighted.volatility)
    if weighted.cumulative_returns:
        assert all(isinstance(v, float) for v in weighted.cumulative_returns)
