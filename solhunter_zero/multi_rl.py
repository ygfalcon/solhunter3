from __future__ import annotations

import json
from collections import defaultdict
import inspect
import os
import random
from typing import Any, Dict, Iterable, List

from .http import dumps

from .agents.memory import MemoryAgent


class PopulationRL:
    """Explore agent weights and risk parameters using a simple evolutionary loop."""

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        population_size: int = 4,
        weights_path: str = "weights.json",
    ) -> None:
        self.memory_agent = memory_agent
        self.population_size = int(population_size)
        self.weights_path = weights_path
        self.population: List[dict[str, Any]] = []
        self._load()

    # ------------------------------------------------------------------
    def _load(self) -> None:
        if self.weights_path and os.path.exists(self.weights_path):
            try:
                with open(self.weights_path, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                if isinstance(data, list):
                    self.population = data
                elif isinstance(data, dict):
                    self.population = [data]
            except Exception:
                self.population = []
        if not self.population:
            self.population = [
                {
                    "weights": {},
                    "risk": {"risk_multiplier": 1.0},
                    "score": 0.0,
                }
            ]

    def _save(self) -> None:
        if not self.weights_path:
            return
        with open(self.weights_path, "w", encoding="utf-8") as fh:
            json.dump(self.population, fh)

    # ------------------------------------------------------------------
    async def _list_trades(self) -> List[Any]:
        if not self.memory_agent:
            return []
        memory = getattr(self.memory_agent, "memory", None)
        if memory is None:
            return []
        loader = getattr(memory, "list_trades", None)
        if loader is None:
            return []
        trades = loader()
        if inspect.isawaitable(trades):
            trades = await trades
        return list(trades or [])

    async def _roi_map(self) -> Dict[str, float]:
        trades = await self._list_trades()
        if not trades:
            return {}
        spent: Dict[str, float] = defaultdict(float)
        revenue: Dict[str, float] = defaultdict(float)
        for trade in trades:
            reason = getattr(trade, "reason", None)
            if reason is None and isinstance(trade, dict):
                reason = trade.get("reason")
            if not reason:
                continue
            direction = getattr(trade, "direction", None)
            if direction is None and isinstance(trade, dict):
                direction = trade.get("direction")
            if not direction:
                continue
            dir_key = str(direction).lower()
            if dir_key not in {"buy", "sell"}:
                continue
            amount = getattr(trade, "amount", None)
            if amount is None and isinstance(trade, dict):
                amount = trade.get("amount", 0.0)
            price = getattr(trade, "price", None)
            if price is None and isinstance(trade, dict):
                price = trade.get("price", 0.0)
            try:
                value = float(amount or 0.0) * float(price or 0.0)
            except Exception:
                continue
            if dir_key == "buy":
                spent[str(reason)] += value
            else:
                revenue[str(reason)] += value
        roi: Dict[str, float] = {}
        for name, spent_val in spent.items():
            if spent_val > 0:
                roi[name] = (revenue.get(name, 0.0) - spent_val) / spent_val
        return roi

    def _score_cfg(self, cfg: dict[str, Any], roi_map: Dict[str, float]) -> float:
        weights = cfg.get("weights", {})
        risk = cfg.get("risk", {})
        score = 0.0
        if isinstance(weights, dict):
            for name, weight in weights.items():
                try:
                    w = float(weight)
                except Exception:
                    continue
                score += w * roi_map.get(str(name), 0.0)
        try:
            risk_mult = float(risk.get("risk_multiplier", 1.0)) if isinstance(risk, dict) else 1.0
        except Exception:
            risk_mult = 1.0
        return score * risk_mult

    # ------------------------------------------------------------------
    async def evolve(self, agent_names: Iterable[str] | None = None) -> dict[str, Any]:
        """Generate a new population and persist the best configuration."""
        roi_map = await self._roi_map()
        names = list(agent_names or [])
        if names and not any(cfg.get("weights") for cfg in self.population):
            self.population = [
                {"weights": {n: 1.0 for n in names}, "risk": {"risk_multiplier": 1.0}, "score": 0.0},
                {"weights": {n: 0.5 for n in names}, "risk": {"risk_multiplier": 1.0}, "score": 0.0},
            ]
        for cfg in self.population:
            cfg["score"] = self._score_cfg(cfg, roi_map)
        self.population.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        keep = self.population[: max(1, len(self.population) // 2)]
        while len(keep) < self.population_size:
            parent = random.choice(keep)
            child = {
                "weights": {
                    k: max(0.0, float(v) * random.uniform(0.8, 1.2))
                    for k, v in parent.get("weights", {}).items()
                },
                "risk": {
                    k: max(0.0, float(v) * random.uniform(0.8, 1.2))
                    for k, v in parent.get("risk", {}).items()
                },
                "score": 0.0,
            }
            keep.append(child)
        self.population = keep
        self._save()
        return self.population[0]

    # ------------------------------------------------------------------
    def best_config(self) -> dict[str, Any]:
        self.population.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        return self.population[0]


if __name__ == "__main__":  # pragma: no cover - simple CLI
    import argparse
    import asyncio
    from .memory import Memory

    ap = argparse.ArgumentParser(description="Evolve RL population weights")
    ap.add_argument("--memory", default="sqlite:///memory.db")
    ap.add_argument("--weights", dest="weights_path", default="weights.json")
    ap.add_argument("--population-size", type=int, default=4)
    ap.add_argument("--num-workers", type=int, default=None)
    args = ap.parse_args()

    if args.num_workers is not None:
        os.environ["RL_NUM_WORKERS"] = str(args.num_workers)

    mgr = MemoryAgent(Memory(args.memory))
    rl = PopulationRL(mgr, population_size=args.population_size, weights_path=args.weights_path)
    best = asyncio.run(rl.evolve())
    print(dumps(best).decode())
