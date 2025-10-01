"""Sequential reinforcement-learning daemon.

The original daemon attempted to orchestrate multiple asynchronous pipelines,
background threads and optional Ray workers.  The rewritten daemon keeps the
surface area familiar but executes each training cycle in a single coroutine:

1. fetch trade + snapshot history
2. train the requested algorithm via :func:`solhunter_zero.rl_training.fit`
3. persist checkpoints and poke any registered agents to reload weights

The implementation remains event-bus aware so risk broadcasts, heartbeats and
weight updates continue to behave as before, but the control flow is entirely
sequential which eliminates the "coroutine was never awaited" warnings.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Dict, Iterable, List, Optional

from .event_bus import publish, subscribe
from .memory import Memory
from .offline_data import OfflineData
from .rl_training import TrainingConfig, TrainingSummary, fit

logger = logging.getLogger(__name__)


@dataclass
class RLDaemon:
    """Single-threaded controller that keeps RL checkpoints fresh."""

    memory_path: str = "sqlite:///memory.db"
    data_path: str = "offline_data.db"
    model_path: Path = Path("ppo_model.pt")
    algo: str = "ppo"
    interval: float = 3600.0
    queue: asyncio.Queue | None = None
    health_state: Any | None = None
    training_config: TrainingConfig = field(default_factory=TrainingConfig)

    def __post_init__(self) -> None:
        self.memory = Memory(self.memory_path)
        self.offline = OfflineData(f"sqlite:///{self.data_path}")
        self.model_path = Path(self.model_path)
        self.agents: List[Any] = []
        self.current_risk: float = 1.0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._risk_unsub = subscribe("risk_updated", self._on_risk_update)

    # ------------------------------------------------------------------
    # Agent management
    # ------------------------------------------------------------------

    def register_agent(self, agent: Any) -> None:
        if agent not in self.agents:
            self.agents.append(agent)

    def unregister_agent(self, agent: Any) -> None:
        if agent in self.agents:
            self.agents.remove(agent)

    # ------------------------------------------------------------------
    # Event bus integration
    # ------------------------------------------------------------------

    def _on_risk_update(self, payload: Any) -> None:
        multiplier = None
        if isinstance(payload, dict):
            multiplier = payload.get("multiplier")
        elif hasattr(payload, "multiplier"):
            multiplier = getattr(payload, "multiplier")
        val = None
        if multiplier is not None:
            try:
                val = float(multiplier)
            except Exception:  # pragma: no cover - defensive cast
                val = None
        if val is not None and val > 0:
            self.current_risk = val

    # ------------------------------------------------------------------
    # Sample collection helpers
    # ------------------------------------------------------------------

    async def _drain_queue(self) -> List[Any]:
        if self.queue is None:
            return []
        drained: List[Any] = []
        while True:
            try:
                item = self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            drained.append(item)
        if not drained:
            return []
        trades: List[Any] = []
        loop = asyncio.get_running_loop()
        now = loop.time()
        for item in drained:
            if isinstance(item, dict):
                trades.append(
                    SimpleNamespace(
                        token=item.get("token"),
                        direction=item.get("side") or item.get("direction", "buy"),
                        amount=item.get("amount", 0.0),
                        price=item.get("price", 0.0),
                        timestamp=item.get("timestamp") or now,
                    )
                )
            else:
                trades.append(item)
        return trades

    async def _collect_samples(self) -> tuple[List[Any], List[Any]]:
        queue_trades = await self._drain_queue()
        if queue_trades:
            trades = queue_trades
        else:
            trades = list(await self.memory.list_trades())
        snapshots = list(await self.offline.list_snapshots())
        return trades, snapshots

    # ------------------------------------------------------------------
    # Training lifecycle
    # ------------------------------------------------------------------

    async def train(self) -> TrainingSummary | None:
        trades, snaps = await self._collect_samples()
        if not trades:
            return None

        loop = asyncio.get_running_loop()
        summary = await loop.run_in_executor(
            None,
            lambda: fit(
                trades,
                snaps,
                model_path=self.model_path,
                algo=self.algo,
                config=self.training_config,
            ),
        )

        if self.health_state is not None:
            try:
                self.health_state.touch()
                self.health_state.record_training(self.algo, len(trades), len(snaps))
            except Exception:  # pragma: no cover - best effort
                logger.debug("Health state update failed", exc_info=True)

        await self._notify_agents()
        publish(
            "rl_weights",
            {
                "weights": {f"{self.algo}_mean_reward": float(summary.mean_reward)},
                "risk": {"multiplier": float(self.current_risk)},
            },
        )
        return summary

    async def _notify_agents(self) -> None:
        for agent in list(self.agents):
            try:
                if hasattr(agent, "reload_weights"):
                    maybe = agent.reload_weights()
                    if asyncio.iscoroutine(maybe):
                        await maybe
                elif hasattr(agent, "_load_weights"):
                    agent._load_weights()
            except Exception:  # pragma: no cover - agent specific issues
                logger.exception("Agent reload failed")

    async def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                await self.train()
            except Exception:  # pragma: no cover - logging only
                logger.exception("RL training cycle failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.interval)
            except asyncio.TimeoutError:
                continue

    def start(self) -> asyncio.Task:
        if self._task is None:
            self._task = asyncio.create_task(self.run_forever())
        return self._task

    def stop(self) -> None:
        self._stop.set()

    async def close(self) -> None:
        self.stop()
        if self._task is not None:
            with contextlib.suppress(asyncio.CancelledError):  # type: ignore[name-defined]
                await self._task
            self._task = None
        if self._risk_unsub is not None:
            try:
                self._risk_unsub()
            except Exception:  # pragma: no cover - defensive
                pass
            self._risk_unsub = None
        await self.offline.close()
        await self.memory.engine.dispose()  # type: ignore[attr-defined]


__all__ = ["RLDaemon"]
