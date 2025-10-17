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
import json
import logging
import os
import time
from dataclasses import dataclass, field
import hashlib
import random
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, List

from .event_bus import publish, subscribe
from .memory import Memory
from .offline_data import OfflineData
from .paths import ROOT
from .rl_training import TrainingConfig, TrainingSummary, fit

logger = logging.getLogger(__name__)


_RL_SCHEMA = "solhunter.rlweights.v1"
_RL_VERSION = 1


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

    _hb_task: asyncio.Task | None = field(init=False, default=None)
    _vote_window_ms: float | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        self.memory = Memory(self.memory_path)
        self.offline = OfflineData(f"sqlite:///{self.data_path}")
        self.model_path = Path(self.model_path)
        self.agents: List[Any] = []
        self.current_risk: float = 1.0
        self._stop = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._hb_task = None
        self._risk_unsub = subscribe("risk_updated", self._on_risk_update)

        raw_window = os.getenv("VOTE_WINDOW_MS", "400") or 400
        try:
            self._vote_window_ms = max(1.0, float(raw_window))
        except Exception:
            logger.debug("Invalid VOTE_WINDOW_MS=%r; defaulting to 400", raw_window)
            self._vote_window_ms = 400.0

        raw_interval = os.getenv("RL_INTERVAL")
        if raw_interval:
            try:
                self.interval = float(raw_interval)
            except Exception:
                logger.debug("Invalid RL_INTERVAL=%r; keeping default %s", raw_interval, self.interval)

        self._vote_window_ms = float(self._vote_window_ms or 400.0)
        self._window_span = max(self._vote_window_ms / 1000.0, 0.1)

        self._health_path = ROOT / "rl_daemon.health.json"
        self._health_url: str | None = None
        self._status = "starting"
        self._last_heartbeat: float | None = None
        self._last_training: float | None = None
        self._last_error: str | None = None
        self._last_updated: float | None = None

        self._last_weights: Dict[str, float] | None = None
        self._bootstrap_window_hash = hashlib.sha256(b"bootstrap").hexdigest()
        self._last_window_hash: str | None = self._bootstrap_window_hash

        self._write_health_json()

    def _write_health_json(self) -> None:
        updated = self._last_updated if self._last_updated is not None else time.time()
        payload = {
            "url": self._health_url,
            "status": self._status,
            "last_heartbeat": self._last_heartbeat,
            "last_training": self._last_training,
            "last_error": self._last_error,
            "updated": updated,
        }
        try:
            self._health_path.parent.mkdir(parents=True, exist_ok=True)
            self._health_path.write_text(json.dumps(payload), encoding="utf-8")
        except Exception:  # pragma: no cover - filesystem best effort
            logger.debug("Failed to write RL health file", exc_info=True)

    def _record_health(
        self,
        *,
        status: str | None = None,
        last_error: str | None = None,
        training: bool = False,
    ) -> None:
        if status is not None:
            self._status = status
        if last_error is not None:
            self._last_error = last_error
        elif status == "running":
            self._last_error = None

        now = time.time()
        self._last_heartbeat = now
        if training:
            self._last_training = now
        self._last_updated = now
        self._write_health_json()

    async def _publish_status_frame(
        self,
        *,
        status: str,
        weights: Dict[str, float] | None = None,
        window_hash: str | None = None,
        last_error: str | None = None,
    ) -> None:
        loop = asyncio.get_running_loop()
        asof = float(time.time())
        window_id = int(loop.time() / self._window_span)
        frame_window_hash = window_hash or self._last_window_hash or self._bootstrap_window_hash
        if window_hash:
            self._last_window_hash = window_hash
        elif self._last_window_hash is None:
            self._last_window_hash = frame_window_hash

        weights_payload = weights if weights is not None else (self._last_weights or {})
        payload = {
            "schema": _RL_SCHEMA,
            "version": _RL_VERSION,
            "risk": {"multiplier": float(self.current_risk)},
            "asof": asof,
            "window_id": window_id,
            "window_hash": frame_window_hash,
            "source": self.algo,
            "vote_window_ms": float(self._vote_window_ms),
            "status": status,
            "weights": dict(weights_payload),
        }
        if last_error:
            payload["last_error"] = last_error

        await self._publish_with_backoff("rl:weights.applied", payload)

        heartbeat_payload = {
            "status": status,
            "algo": self.algo,
            "asof": asof,
            "risk": {"multiplier": float(self.current_risk)},
        }
        if last_error:
            heartbeat_payload["last_error"] = last_error

        await self._publish_with_backoff("rl:heartbeat", heartbeat_payload)

    async def _handle_training_error(self, exc: BaseException) -> None:
        message = str(exc).strip() or exc.__class__.__name__
        short_message = message.splitlines()[0][:256]
        if self.health_state is not None and hasattr(self.health_state, "record_error"):
            try:
                self.health_state.record_error(short_message)
            except Exception:  # pragma: no cover - health errors shouldn't crash
                logger.debug("Health state error update failed", exc_info=True)
        self._record_health(status="error", last_error=short_message)
        await self._publish_status_frame(status="error", last_error=short_message)

    async def _heartbeat_loop(self) -> None:
        window_seconds = max(self._vote_window_ms / 1000.0, 0.2)
        delay = min(self.interval, window_seconds)
        if delay <= 0:
            delay = window_seconds
        if delay <= 0:
            delay = 0.2
        while not self._stop.is_set():
            status = self._resolve_status()
            if self.health_state is not None:
                try:
                    self.health_state.touch()
                except Exception:  # pragma: no cover - best effort
                    logger.debug("Health state touch failed", exc_info=True)
            self._record_health(status=status)
            try:
                await self._publish_status_frame(status=status)
            except Exception:  # pragma: no cover - logging only
                logger.exception("RL heartbeat publish failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                continue

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

    def _resolve_status(self) -> str:
        status = (self._status or "").strip().lower()
        if status in {"", "starting"}:
            return "running"
        return self._status

    # ------------------------------------------------------------------
    # Training lifecycle
    # ------------------------------------------------------------------

    async def train(self) -> TrainingSummary | None:
        trades, snaps = await self._collect_samples()
        if not trades:
            status = self._resolve_status()
            if self.health_state is not None:
                try:
                    self.health_state.touch()
                except Exception:  # pragma: no cover - best effort
                    logger.debug("Health state touch failed", exc_info=True)
            self._record_health(status=status)
            await self._publish_status_frame(status=status)
            return None

        loop = asyncio.get_running_loop()
        try:
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
        except Exception as exc:  # pragma: no cover - logged and signalled
            logger.exception("RL training failed")
            await self._handle_training_error(exc)
            return None

        if self.health_state is not None:
            try:
                self.health_state.touch()
                self.health_state.record_training(self.algo, len(trades), len(snaps))
            except Exception:  # pragma: no cover - best effort
                logger.debug("Health state update failed", exc_info=True)

        await self._notify_agents()
        weights = {f"{self.algo}_mean_reward": float(summary.mean_reward)}
        self._last_weights = dict(weights)
        weight_items = tuple(sorted((str(k), float(v)) for k, v in weights.items()))
        window_hash = hashlib.sha256(repr(weight_items).encode("utf-8")).hexdigest()

        await self._publish_with_backoff(
            "rl_weights",
            {"weights": weights, "risk": {"multiplier": float(self.current_risk)}},
        )

        self._record_health(status="running", training=True)
        await self._publish_status_frame(
            status="running",
            weights=weights,
            window_hash=window_hash,
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
            self._stop.clear()
            self._task = asyncio.create_task(self.run_forever())
        if self._hb_task is None:
            self._hb_task = asyncio.create_task(self._heartbeat_loop())
        return self._task

    def stop(self) -> None:
        self._stop.set()

    async def close(self) -> None:
        self.stop()
        if self._hb_task is not None:
            self._hb_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):  # type: ignore[name-defined]
                await self._hb_task
            self._hb_task = None
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

    async def _publish_with_backoff(
        self,
        topic: str,
        payload: Dict[str, Any],
        *,
        attempts: int = 4,
        base_delay: float = 0.25,
    ) -> bool:
        """Publish ``payload`` with exponential backoff on transport failure."""

        delay = max(0.05, float(base_delay))
        for attempt in range(1, attempts + 1):
            try:
                publish(topic, payload)
                return True
            except Exception:  # pragma: no cover - network/backplane failure
                jitter = random.uniform(0.0, delay * 0.3)
                logger.warning(
                    "RL daemon failed to publish %s on attempt %d; backing off %.2fs",
                    topic,
                    attempt,
                    delay + jitter,
                    exc_info=True,
                )
                await asyncio.sleep(min(delay + jitter, 5.0))
                delay = min(delay * 2.0, 5.0)
        logger.error(
            "RL daemon dropped %s payload after %d attempts", topic, attempts
        )
        return False


__all__ = ["RLDaemon"]
