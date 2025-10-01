import numpy as np
import importlib.util
from pathlib import Path

# Load backtester module directly
_path = Path(__file__).resolve().parents[1] / "solhunter_zero" / "backtester.py"
spec = importlib.util.spec_from_file_location("backtester", _path)
backtester = importlib.util.module_from_spec(spec)
import sys
sys.modules[spec.name] = backtester
assert spec.loader is not None
spec.loader.exec_module(backtester)


def test_metric_calculations_known_series():
    prices = [100.0, 110.0, 105.0, 120.0]
    results = backtester.backtest_strategies(prices, strategies=[("buy_hold", backtester.buy_and_hold)])
    assert len(results) == 1
    res = results[0]

    # compute expected values
    rets = [backtester._net_return(prices[i - 1], prices[i]) for i in range(1, len(prices))]
    arr = np.array(rets, dtype=float)
    cum = np.cumprod(1 + arr)
    cum_returns = cum - 1
    running_max = np.maximum.accumulate(cum)
    drawdowns = (running_max - cum) / running_max

    roi = float(cum_returns[-1])
    vol = float(arr.std())
    mean = float(arr.mean())
    sharpe = mean / vol if vol > 0 else 0.0
    max_dd = float(drawdowns.max())

    assert np.isclose(res.roi, roi)
    assert np.isclose(res.volatility, vol)
    assert np.isclose(res.sharpe, sharpe)
    assert np.isclose(res.max_drawdown, max_dd)
    assert np.allclose(res.cumulative_returns, cum_returns.tolist())
