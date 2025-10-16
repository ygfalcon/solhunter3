from __future__ import annotations

from typing import Iterable, Dict, Any

from . import BaseAgent
from ..rl_training import MultiAgentRL


class HierarchicalRLAgent(BaseAgent):
    """Use ``MultiAgentRL`` to generate dynamic agent weights."""

    name = "hierarchical_rl"

    def __init__(self, rl: MultiAgentRL) -> None:
        self.rl = rl
        self.weights: Dict[str, float] = {}

    def train(self, agent_names: Iterable[str]) -> Dict[str, float]:
        """Train the RL controller and return updated weights."""
        self.weights = self.rl.train_controller(agent_names)
        return self.weights

    async def propose_trade(
        self,
        token: str,
        portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> list[Dict[str, Any]]:
        return []
