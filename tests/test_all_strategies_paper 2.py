import importlib
import pkgutil
import asyncio
import warnings

import pytest

from solhunter_zero.strategy_manager import StrategyManager
from solhunter_zero import backtester
from solhunter_zero.datasets.live_ticks import load_live_ticks


def test_all_strategies_paper():
    """Run all strategies against recent SOL/USD candles.

    Fetches live data from a public Codex (Coingecko) endpoint via
    :func:`load_live_ticks`.  When the request fails the loader returns an
    empty list and the test is skipped.
    """
    ticks = load_live_ticks()
    if not ticks:
        warnings.warn("live market data unavailable, skipping")
        pytest.skip("live market data unavailable")
    prices = [float(entry["price"]) for entry in ticks]

    import solhunter_zero.agents as agents_pkg

    modules = [
        f"{agents_pkg.__name__}.{name}"
        for _, name, ispkg in pkgutil.iter_modules(agents_pkg.__path__)
        if not ispkg and not name.startswith("_")
    ]
    sm = StrategyManager(modules)
    missing = set(sm.list_missing())
    loaded = [m for m in modules if m not in missing]
    assert loaded, "no strategies loaded"

    strategies: list[tuple[str, backtester.StrategyFunc]] = []
    for name in loaded:
        mod = importlib.import_module(name)
        fn = getattr(mod, "evaluate", None)
        if not callable(fn):
            continue

        def wrap(func):
            def runner(prices, liquidity=None):
                if asyncio.iscoroutinefunction(func):
                    return asyncio.run(func(prices, liquidity))
                return func(prices, liquidity)

            return runner

        strat = wrap(fn)
        try:
            result = strat(prices)
        except Exception as exc:  # pragma: no cover - unexpected error
            pytest.fail(f"{name} evaluation raised {exc}")
        assert result, f"{name} returned no result"
        strategies.append((name, strat))

    if not strategies:
        pytest.skip("no strategies with evaluate functions")
    results = backtester.backtest_strategies(prices, strategies=strategies)
    assert len(results) == len(strategies)
    for res in results:
        assert isinstance(res.roi, float)

    rotation_interval = 50
    weights: dict[str, float] = {}
    for idx, (name, strat) in enumerate(strategies):
        start = (idx * rotation_interval) % max(len(prices) - rotation_interval, 1)
        window = prices[start : start + rotation_interval]
        res = backtester.backtest_weighted(window, {name: 1.0}, strategies=[(name, strat)])
        weights[name] = max(res.roi, 0.0) + 1.0

    combined = backtester.backtest_weighted(prices, weights, strategies=strategies)
    assert isinstance(combined.roi, float)
