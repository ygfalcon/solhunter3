from __future__ import annotations

from typing import Iterable, Dict, Any

from . import BaseAgent
from .memory import MemoryAgent
from ..multi_rl import PopulationRL


class RLWeightAgent(BaseAgent):
    """Suggest agent weights using a simple reinforcement learning loop."""

    name = "rl_weight"

    def __init__(
        self,
        memory_agent: MemoryAgent | None = None,
        *,
        population_size: int = 4,
        weights_path: str = "rl_weights.json",
    ) -> None:
        self.memory_agent = memory_agent or MemoryAgent()
        self.rl = PopulationRL(
            self.memory_agent,
            population_size=population_size,
            weights_path=weights_path,
        )
        self.weights: Dict[str, float] = {}

    # ------------------------------------------------------------------
    async def train(self, agent_names: Iterable[str]) -> Dict[str, float]:
        """Evolve the population and return the best weight vector."""
        names = list(agent_names)
        if not any(cfg.get("weights") for cfg in self.rl.population):
            self.rl.population = [
                {"weights": {n: 1.0 for n in names}, "risk": {"risk_multiplier": 1.0}},
                {"weights": {n: 0.5 for n in names}, "risk": {"risk_multiplier": 1.0}},
            ]
        best = await self.rl.evolve(agent_names=names)
        w = best.get("weights", {}) if isinstance(best, dict) else {}
        self.weights = {n: float(w.get(n, 1.0)) for n in names}
        return self.weights

    async def propose_trade(
        self,
        token: str,
        portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> list[dict[str, Any]]:
        return []
