from __future__ import annotations

from typing import Any, Awaitable, Dict, List, Protocol, runtime_checkable
import asyncio
import inspect
import logging

from . import BaseAgent
from ..memory import Memory
from ..advanced_memory import AdvancedMemory
from ..offline_data import OfflineData
from ..portfolio import Portfolio
from ..simple_memory import SimpleMemory
from ..event_bus import publish
from ..schemas import TradeLogged


logger = logging.getLogger(__name__)


@runtime_checkable
class MemoryBackend(Protocol):
    """Protocol describing the subset of memory backends used by the agent."""

    def log_trade(self, *args: Any, **kwargs: Any) -> Awaitable[Any] | Any:  # pragma: no cover - runtime typing
        ...


class MemoryAgent(BaseAgent):
    """Record executed trades and classify outcomes."""

    name = "memory"

    def __init__(
        self,
        memory: Memory | AdvancedMemory | SimpleMemory | MemoryBackend | None = None,
        offline_data: OfflineData | None = None,
        *,
        queue: asyncio.Queue | None = None,
    ):
        backend_name = None
        if memory is not None:
            self.memory = memory
            backend_name = type(memory).__name__
        else:
            try:
                self.memory = Memory("sqlite:///:memory:")
                backend_name = "Memory"
            except ModuleNotFoundError as exc:
                if "aiosqlite" in str(exc):
                    self.memory = SimpleMemory()
                    backend_name = "SimpleMemory"
                else:
                    raise
        self.offline_data = offline_data
        self.queue = queue
        if backend_name:
            logger.info("memory agent using %s backend", backend_name)

    async def log(self, action: Dict[str, Any], *, skip_db: bool = False) -> None:
        """Record ``action`` in memory unless ``skip_db`` is True."""
        amount = float(action.get("amount", 0.0) or 0.0)
        price = float(action.get("price", 0.0) or 0.0)

        if not skip_db:
            extra: Dict[str, Any] = {}
            if isinstance(self.memory, AdvancedMemory):
                context = action.get("context", action.get("thought", ""))
                trade_uuid = action.get("uuid") or action.get("trade_id")
                extra = {
                    "context": context,
                    "emotion": action.get("emotion", ""),
                    "simulation_id": action.get("simulation_id"),
                }
                if trade_uuid:
                    extra["uuid"] = str(trade_uuid)
            log_trade = getattr(self.memory, "log_trade", None)
            if callable(log_trade):
                call_kwargs = {
                    "token": action.get("token"),
                    "direction": action.get("side"),
                    "amount": amount,
                    "price": price,
                    "reason": action.get("agent"),
                    **extra,
                }
                try:
                    params = inspect.signature(log_trade).parameters
                except (TypeError, ValueError):
                    params = {}
                should_publish = True
                if "_broadcast" in params:
                    call_kwargs["_broadcast"] = True
                    should_publish = False
                try:
                    result = log_trade(**call_kwargs)
                    if inspect.isawaitable(result):
                        await result
                    if should_publish:
                        publish(
                            "trade_logged",
                            TradeLogged(
                                token=str(action.get("token", "")),
                                direction=str(action.get("side", "")),
                                amount=amount,
                                price=price,
                                reason=action.get("agent"),
                                context=extra.get("context"),
                                emotion=extra.get("emotion"),
                                simulation_id=extra.get("simulation_id"),
                                uuid=extra.get("uuid"),
                            ),
                        )
                except Exception:
                    logger.debug("memory backend log_trade failed", exc_info=True)
            if self.offline_data:
                try:
                    await self.offline_data.log_trade(
                        token=str(action.get("token", "")),
                        side=str(action.get("side", "")),
                        price=price,
                        amount=amount,
                    )
                except Exception:
                    logger.debug("offline_data.log_trade failed", exc_info=True)

        if self.queue is not None:
            from types import SimpleNamespace

            trade = SimpleNamespace(
                token=action.get("token"),
                direction=action.get("side"),
                amount=amount,
                price=price,
            )
            try:
                self.queue.put_nowait(trade)
            except Exception:
                await self.queue.put(trade)

    def __repr__(self) -> str:
        backend = type(self.memory).__name__
        offline = type(self.offline_data).__name__ if self.offline_data else "None"
        if isinstance(self.queue, asyncio.Queue):
            queue_repr = f"Queue(maxsize={self.queue.maxsize})"
        else:
            queue_repr = "None"
        return f"MemoryAgent(memory={backend}, offline_data={offline}, queue={queue_repr})"

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        # Memory agent does not produce trades
        return []
