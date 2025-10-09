"""Depth aggregation for Golden Snapshots."""

from __future__ import annotations

from typing import Awaitable, Callable

from .types import DepthSnapshot


class DepthStage:
    """Pass-through stage storing the freshest depth snapshot."""

    def __init__(self, emit: Callable[[DepthSnapshot], Awaitable[None]]) -> None:
        self._emit = emit

    async def submit(self, snapshot: DepthSnapshot) -> None:
        await self._emit(snapshot)
