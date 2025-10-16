from __future__ import annotations

from typing import List, Dict, Any
import asyncio
import inspect

from . import BaseAgent
from ..memory import Memory
from ..advanced_memory import AdvancedMemory
from ..offline_data import OfflineData
from ..portfolio import Portfolio
from ..simple_memory import SimpleMemory


class MemoryAgent(BaseAgent):
    """Record executed trades and classify outcomes."""

    name = "memory"

    def __init__(
        self,
        memory: Memory | AdvancedMemory | None = None,
        offline_data: OfflineData | None = None,
        *,
        queue: asyncio.Queue | None = None,
    ):
        if memory is not None:
            self.memory = memory
        else:
            try:
                self.memory = Memory("sqlite:///:memory:")
            except ModuleNotFoundError as exc:
                if "aiosqlite" in str(exc):
                    self.memory = SimpleMemory()
                else:
                    raise
        self.offline_data = offline_data
        self.queue = queue

    async def log(self, action: Dict[str, Any], *, skip_db: bool = False) -> None:
        """Record ``action`` in memory unless ``skip_db`` is True."""
        if not skip_db:
            extra = {}
            if isinstance(self.memory, AdvancedMemory):
                extra = {
                    "context": action.get("context", action.get("thought", "")),
                    "emotion": action.get("emotion", ""),
                    "simulation_id": action.get("simulation_id"),
                }
            log_trade = getattr(self.memory, "log_trade", None)
            if inspect.iscoroutinefunction(log_trade):
                await log_trade(
                    token=action.get("token"),
                    direction=action.get("side"),
                    amount=action.get("amount", 0.0),
                    price=action.get("price", 0.0),
                    reason=action.get("agent"),
                    **extra,
                )
            elif callable(log_trade):
                log_trade(
                    token=action.get("token"),
                    direction=action.get("side"),
                    amount=action.get("amount", 0.0),
                    price=action.get("price", 0.0),
                    reason=action.get("agent"),
                    **extra,
                )
            if self.offline_data:
                await self.offline_data.log_trade(
                    token=action.get("token", ""),
                    side=action.get("side", ""),
                    price=float(action.get("price", 0.0)),
                    amount=float(action.get("amount", 0.0)),
                )
        if self.queue is not None:
            from types import SimpleNamespace

            trade = SimpleNamespace(
                token=action.get("token"),
                direction=action.get("side"),
                amount=float(action.get("amount", 0.0)),
                price=float(action.get("price", 0.0)),
            )
            await self.queue.put(trade)

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
