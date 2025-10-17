from __future__ import annotations

from typing import Iterable, Dict, Any
import asyncio
import logging
import random
from contextlib import AbstractContextManager, ExitStack
from pathlib import Path

from . import BaseAgent
from .memory import MemoryAgent
from ..memory import Memory
from ..multi_rl import PopulationRL
from ..event_bus import subscription
from ..schemas import WeightsUpdated


logger = logging.getLogger(__name__)


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
        self.memory_agent = memory_agent or MemoryAgent(Memory("sqlite:///memory.db"))
        self.rl = PopulationRL(
            self.memory_agent,
            population_size=population_size,
            weights_path=weights_path,
        )
        self.weights: Dict[str, float] = {}
        self._weights_sub: AbstractContextManager | None = None
        self._refresh_lock: asyncio.Lock | None = None
        self._last_agent_names: list[str] = []
        self._subscribe_to_weight_updates()

    # ------------------------------------------------------------------
    async def train(self, agent_names: Iterable[str]) -> Dict[str, float]:
        """Evolve the population and return the best weight vector."""
        names = list(agent_names)
        self._last_agent_names = list(names)
        if not names:
            self.weights = {}
            return {}
        if not any(cfg.get("weights") for cfg in self.rl.population):
            self.rl.population = [
                {"weights": {n: 1.0 for n in names}, "risk": {"risk_multiplier": 1.0}},
                {"weights": {n: 0.5 for n in names}, "risk": {"risk_multiplier": 1.0}},
            ]
        best = await self.rl.evolve(agent_names=names)
        w = best.get("weights", {}) if isinstance(best, dict) else {}
        self.weights = self._normalize_weights(w, names)
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

    # ------------------------------------------------------------------
    def close(self) -> None:
        if self._weights_sub is not None:
            self._weights_sub.__exit__(None, None, None)
            self._weights_sub = None

    # ------------------------------------------------------------------
    def _subscribe_to_weight_updates(self) -> None:
        if self._weights_sub is not None:
            return

        async def _handler(_payload: WeightsUpdated) -> None:
            await self._refresh_checkpoint()

        stack = ExitStack()
        for topic in ("weights_updated", "rl_weights"):
            stack.enter_context(subscription(topic, _handler))
        self._weights_sub = stack

    async def _refresh_checkpoint(
        self, *, attempts: int = 3, base_delay: float = 0.5
    ) -> None:
        if not self.rl.weights_path:
            return

        if self._refresh_lock is None:
            self._refresh_lock = asyncio.Lock()

        async with self._refresh_lock:
            path = Path(self.rl.weights_path)
            delay = base_delay
            last_success = False
            for attempt in range(1, attempts + 1):
                success = self.rl._load()
                if success:
                    last_success = True
                    logger.debug(
                        "Refreshed RLWeightAgent checkpoint from %s (attempt %d)",
                        path,
                        attempt,
                    )
                    try:
                        best = self.rl.best_config()
                        if isinstance(best, dict) and self._last_agent_names:
                            best_weights = best.get("weights", {}) or {}
                            self.weights = self._normalize_weights(
                                best_weights, self._last_agent_names
                            )
                    except Exception:
                        logger.debug("Post-refresh weight recompute failed", exc_info=True)
                    break
                if attempt < attempts:
                    await asyncio.sleep(delay + random.uniform(0, base_delay))
                    delay *= 2
            if not last_success:
                if path.exists():
                    logger.error(
                        "Failed to refresh RL weights from %s after %d attempts",
                        path,
                        attempts,
                    )
                else:
                    logger.debug(
                        "Skipped RL weights refresh because %s does not exist",
                        path,
                    )

    # ------------------------------------------------------------------
    @staticmethod
    def _normalize_weights(weights: Dict[str, Any], names: Iterable[str]) -> Dict[str, float]:
        ordered_names = list(dict.fromkeys(names))
        if not ordered_names:
            return {}
        clean: Dict[str, float] = {}
        for name in ordered_names:
            try:
                value = float(weights.get(name, 1.0))
            except Exception:
                value = 0.0
            clean[name] = max(0.0, value)
        total = sum(clean.values())
        if total > 0:
            return {key: (val / total) for key, val in clean.items()}
        uniform = 1.0 / len(ordered_names)
        return {name: uniform for name in ordered_names}
