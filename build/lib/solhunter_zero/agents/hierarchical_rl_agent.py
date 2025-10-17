from __future__ import annotations

import time
from typing import Iterable, Dict, Any

from . import BaseAgent
from ..rl_training import MultiAgentRL

# Optional: publish RL weights onto the event bus so AgentManager can ingest them
try:  # pragma: no cover - event bus optional
    from ..event_bus import publish
except Exception:  # pragma: no cover - event bus optional
    def publish(*_a: Any, **_k: Any) -> None:
        pass

# Optional: structured schema for RL weights
try:  # pragma: no cover - schema optional
    from ..schemas import RLWeights
except Exception:  # pragma: no cover - schema optional
    RLWeights = None  # type: ignore[assignment]


class HierarchicalRLAgent(BaseAgent):
    """Weight-only agent leveraging ``MultiAgentRL`` to produce weights."""

    name = "hierarchical_rl"

    def __init__(
        self,
        rl: MultiAgentRL,
        *,
        min_train_interval_s: float = 30.0,
        topic: str = "rl_weights",
        schema: str = "solhunter.rlweights.v1",
        version: int = 1,
    ) -> None:
        self.rl = rl
        self.weights: Dict[str, float] = {}
        self._last_train_ts: float = 0.0
        self._min_interval = float(min_train_interval_s)
        self._topic = topic
        self._schema = schema
        self._version = int(version)

    def train(self, agent_names: Iterable[str]) -> Dict[str, float]:
        """Train the RL controller and return updated weights."""
        now = time.time()
        if now - self._last_train_ts < self._min_interval and self.weights:
            return dict(self.weights)

        weights = self.rl.train_controller(agent_names)
        clean: Dict[str, float] = {}
        for key, value in (weights or {}).items():
            try:
                clean[str(key)] = float(value)
            except Exception:
                continue

        self.weights = clean
        self._last_train_ts = now
        self._broadcast_weights()
        return dict(self.weights)

    async def propose_trade(
        self,
        token: str,
        portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> list[Dict[str, Any]]:
        return []

    def _broadcast_weights(self) -> None:
        """Publish latest weights onto the event bus."""
        if not self.weights:
            return

        payload: Any
        if RLWeights is not None:
            payload = RLWeights(weights=dict(self.weights))  # type: ignore[call-arg]
            setattr(payload, "schema", self._schema)
            setattr(payload, "version", self._version)
            setattr(payload, "asof", time.time())
            setattr(payload, "source", self.name)
        else:
            payload = {
                "schema": self._schema,
                "version": self._version,
                "asof": time.time(),
                "source": self.name,
                "weights": dict(self.weights),
            }

        try:
            publish(self._topic, payload)
        except Exception:
            pass

    def requirements(self) -> list[dict[str, Any]]:
        return []

    def apply_threshold_profile(self, _profile: dict[str, dict[str, float]]) -> None:
        return
