from __future__ import annotations

import json
import os
from argparse import ArgumentParser
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import tomllib
import asyncio
import csv

from .http import close_session
from .util import install_uvloop

install_uvloop()

from .trade_analyzer import analyze_trades

from .backtester import (
    backtest_strategies,
    backtest_configs,
    backtest_weighted,
    DEFAULT_STRATEGIES,
)


def bayesian_optimize_weights(
    prices: List[float],
    keys: List[str],
    strategies: List[tuple[str, callable]],
    iterations: int = 20,
) -> Dict[str, float]:
    """Search for the best agent weights using Bayesian optimisation."""
    from sklearn.gaussian_process import GaussianProcessRegressor
    from sklearn.gaussian_process.kernels import Matern
    import numpy as np

    rng = np.random.default_rng(0)
    X: List[List[float]] = []
    y: List[float] = []
    gp = GaussianProcessRegressor(kernel=Matern(nu=2.5), alpha=1e-6, normalize_y=True)

    def evaluate(point: List[float]) -> float:
        weights = {k: point[i] for i, k in enumerate(keys)}
        res = backtest_weighted(prices, weights, strategies=strategies)
        return res.roi

    for _ in range(iterations):
        if len(X) >= 3:
            gp.fit(np.array(X), np.array(y))
            cand = rng.uniform(0.0, 2.0, size=(100, len(keys)))
            preds = gp.predict(cand)
            x = cand[int(np.argmax(preds))]
        else:
            x = rng.uniform(0.0, 2.0, size=len(keys))
        score = evaluate(x.tolist())
        X.append(x.tolist())
        y.append(score)

    best = X[int(np.argmax(y))]
    return {k: float(best[i]) for i, k in enumerate(keys)}


def _load_history(
    path: str | None, start: str | None, end: str | None
) -> Tuple[List[float], Optional[List[float]]]:
    """Load price and optional liquidity history.

    ``path`` may point to JSON or CSV files. JSON files can contain either a
    list of numbers, a list of objects with ``date``/``price``/``liquidity``
    fields, or an object with ``prices`` and optional ``liquidity`` arrays.
    Large newline-delimited JSON files are processed incrementally to avoid
    loading the entire file into memory.
    """

    if not path:
        return [], None

    start_dt = datetime.fromisoformat(start) if start else None
    end_dt = datetime.fromisoformat(end) if end else None

    prices: List[float] = []
    liquidity: Optional[List[float]] = None

    def add_liq(val: Optional[str | float]) -> None:
        nonlocal liquidity
        if val is None or val == "":
            return
        if liquidity is None:
            liquidity = []
        liquidity.append(float(val))

    if path.endswith(".csv"):
        with open(path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            if (start_dt or end_dt) and "date" not in (reader.fieldnames or []):
                raise ValueError("Date range specified but history has no dates")
            for row in reader:
                if "date" in row and row["date"]:
                    d = datetime.fromisoformat(row["date"])
                    if start_dt and d < start_dt:
                        continue
                    if end_dt and d > end_dt:
                        continue
                prices.append(float(row["price"]))
                add_liq(row.get("liquidity"))
        return prices, liquidity

    size = os.path.getsize(path)
    if size > 5_000_000:  # 5 MB -> stream line-delimited JSON
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                item = json.loads(line)
                if "date" in item:
                    d = datetime.fromisoformat(item["date"])
                    if start_dt and d < start_dt:
                        continue
                    if end_dt and d > end_dt:
                        continue
                elif start_dt or end_dt:
                    raise ValueError("Date range specified but history has no dates")
                prices.append(float(item["price"]))
                add_liq(item.get("liquidity"))
        return prices, liquidity

    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not data:
        return [], None

    if isinstance(data, dict):
        prices_data = data.get("prices", [])
        liquidity_data = data.get("liquidity")
        dates = data.get("dates")
        if dates:
            for i, p in enumerate(prices_data):
                d = datetime.fromisoformat(dates[i])
                if start_dt and d < start_dt:
                    continue
                if end_dt and d > end_dt:
                    continue
                prices.append(float(p))
                if liquidity_data and i < len(liquidity_data):
                    add_liq(liquidity_data[i])
            return prices, liquidity
        if start_dt or end_dt:
            raise ValueError("Date range specified but history has no dates")
        prices = [float(x) for x in prices_data]
        if liquidity_data:
            liquidity = [float(x) for x in liquidity_data]
        return prices, liquidity

    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            for item in data:
                if "date" in item:
                    d = datetime.fromisoformat(item["date"])
                    if start_dt and d < start_dt:
                        continue
                    if end_dt and d > end_dt:
                        continue
                elif start_dt or end_dt:
                    raise ValueError("Date range specified but history has no dates")
                prices.append(float(item["price"]))
                add_liq(item.get("liquidity"))
            return prices, liquidity
        if start_dt or end_dt:
            raise ValueError("Date range specified but history has no dates")
        prices = [float(x) for x in data]
        return prices, None

    return [], None


def main(argv: list[str] | None = None) -> int:
    parser = ArgumentParser(description="Run strategy backtests")
    parser.add_argument("history", nargs="?", help="JSON file with price history list")
    parser.add_argument(
        "-c",
        "--config",
        dest="configs",
        action="append",
        default=[],
        help="Configuration file with agent weights",
    )
    parser.add_argument(
        "-s",
        "--strategy",
        dest="strategies",
        action="append",
        help="Strategy name to test",
    )
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument(
        "--analyze-trades",
        action="store_true",
        help="Analyze recorded trades and suggest weight updates",
    )
    parser.add_argument(
        "--memory",
        dest="memory",
        default="sqlite:///memory.db",
        help="Memory database URL",
    )
    parser.add_argument(
        "--weights-out",
        dest="weights_out",
        help="Write updated weights to FILE",
    )
    parser.add_argument(
        "--optimize",
        action="store_true",
        help="Run Bayesian optimisation for agent weights",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=20,
        help="Number of optimisation iterations",
    )
    args = parser.parse_args(argv)

    if args.analyze_trades:
        analyze_trades(
            args.memory,
            args.configs,
            weights_out=args.weights_out,
        )
        return 0

    if not args.history:
        parser.error("history is required unless --analyze-trades is used")

    prices, liquidity = _load_history(args.history, args.start, args.end)

    strategy_map = dict(DEFAULT_STRATEGIES)
    if args.strategies:
        strategies = []
        for name in args.strategies:
            if name not in strategy_map:
                parser.error(f"Unknown strategy: {name}")
            strategies.append((name, strategy_map[name]))
    else:
        strategies = DEFAULT_STRATEGIES

    if not args.configs:
        results = backtest_strategies(prices, liquidity=liquidity, strategies=strategies)
    else:
        cfgs = []
        for path in args.configs:
            with open(path, "rb") as f:
                cfg = tomllib.load(f)
            weights = cfg.get("agent_weights", {})
            name = os.path.basename(path)
            cfgs.append((name, {str(k): float(v) for k, v in weights.items()}))
        if args.optimize:
            base = cfgs[0][1] if cfgs else {}
            keys = list(base.keys())
            best = bayesian_optimize_weights(prices, keys, strategies, args.iterations)
            print(json.dumps(best))
            return 0
        results = backtest_configs(prices, cfgs, strategies=strategies, liquidity=liquidity)

    for res in results:
        print(f"{res.name}\tROI={res.roi:.4f}\tSharpe={res.sharpe:.2f}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        asyncio.run(close_session())
