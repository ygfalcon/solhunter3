#!/usr/bin/env python
"""Run bundled strategies on dummy price data.

This script demonstrates the
:class:`solhunter_zero.strategy_manager.StrategyManager` by loading a small
multi-token price dataset and executing every available strategy module.  All
strategy modules under :mod:`solhunter_zero.agents` are automatically
discovered and supplied to :class:`StrategyManager`.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Dict, List

from solhunter_zero.strategy_manager import (
    StrategyManager,
    discover_strategy_modules,
)
from solhunter_zero.investor_demo import load_prices


async def run_demo() -> None:
    """Load price data and execute all strategies."""

    data_path = (
        Path(__file__).resolve().parent.parent
        / "tests"
        / "data"
        / "prices_multitoken.json"
    )
    prices = load_prices(path=data_path)

    modules = StrategyManager.DEFAULT_STRATEGIES + discover_strategy_modules()
    manager = StrategyManager(modules)
    missing = manager.list_missing()
    if missing:
        raise RuntimeError(f"Missing strategies: {', '.join(missing)}")

    from solhunter_zero.portfolio import Portfolio

    portfolio = Portfolio(path=None)
    merged_actions: List[Dict[str, float]] = []

    for token in prices.keys():
        actions = await manager.evaluate(token, portfolio)
        merged_actions.extend(actions)

    # Build a simple summary entry for each loaded strategy
    summary: Dict[str, int] = {}
    first_token = next(iter(prices.keys()))
    for mod, name in manager._modules:
        func = getattr(mod, "evaluate", None)
        if func is None:
            summary[name] = 0
            continue
        try:
            if asyncio.iscoroutinefunction(func):
                res = await func(first_token, portfolio)
            else:
                res = await asyncio.to_thread(func, first_token, portfolio)
            summary[name] = len(res) if res else 0
        except Exception:
            summary[name] = 0

    print("Merged actions:")
    for action in merged_actions:
        print(f"  {action}")

    print("\nLoaded strategies:")
    for name, count in summary.items():
        print(f"  {name}: {count} actions")


def main() -> None:
    asyncio.run(run_demo())


if __name__ == "__main__":
    main()
