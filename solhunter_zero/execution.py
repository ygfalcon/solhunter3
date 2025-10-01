"""Event-driven transaction execution loop."""

from __future__ import annotations

import asyncio
import logging
import os
import time

from .event_bus import subscription

from .depth_client import (
    stream_depth,
    submit_raw_tx,
    auto_exec as service_auto_exec,
    DEPTH_SERVICE_SOCKET,
)
from .util import parse_bool_env

USE_DEPTH_STREAM = parse_bool_env("USE_DEPTH_STREAM", True)


class EventExecutor:
    """Trigger transaction submission on depth/mempool updates."""

    def __init__(
        self,
        token: str,
        *,
        rate_limit: float = 0.05,
        threshold: float = 0.0,
        socket_path: str = DEPTH_SERVICE_SOCKET,
        priority_rpc: list[str] | None = None,
        auto_exec: bool = False,
    ) -> None:
        self.token = token
        self.rate_limit = rate_limit
        self.threshold = threshold
        self.socket_path = socket_path
        self.priority_rpc = list(priority_rpc) if priority_rpc else None
        self.auto_exec = auto_exec
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._last = 0.0

    async def enqueue(self, tx_b64: str) -> None:
        """Queue a pre-signed transaction for immediate submission."""

        if self.auto_exec:
            await service_auto_exec(
                self.token,
                self.threshold,
                [tx_b64],
                socket_path=self.socket_path,
            )
        else:
            await self._queue.put(tx_b64)

    async def _handle_update(self, data: dict) -> None:
        entry = data.get(self.token)
        if entry is None:
            return
        rate = float(entry.get("tx_rate", 0.0))
        now = time.monotonic()
        if rate < self.threshold or now - self._last < self.rate_limit:
            return
        try:
            tx = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            return
        self._last = now
        try:
            await submit_raw_tx(
                tx,
                socket_path=self.socket_path,
                priority_rpc=self.priority_rpc,
            )
        except Exception:
            logging.exception("submit_raw_tx failed")
            await self._queue.put(tx)
        finally:
            self._queue.task_done()

    async def run(self) -> None:
        """Start the event loop."""

        if USE_DEPTH_STREAM:
            with subscription("depth_update", self._handle_update):
                await asyncio.Future()
            return

        async for update in stream_depth(
            self.token, rate_limit=self.rate_limit
        ):
            await self._handle_update({self.token: update})


async def run_event_loop(
    token: str,
    tx_source: "asyncio.Queue[str] | asyncio.Queue[bytes]",
    *,
    rate_limit: float = 0.05,
    threshold: float = 0.0,
    socket_path: str = DEPTH_SERVICE_SOCKET,
    priority_rpc: list[str] | None = None,
    auto_exec: bool = False,
) -> None:
    """Convenience wrapper around :class:`EventExecutor`."""

    execer = EventExecutor(
        token,
        rate_limit=rate_limit,
        threshold=threshold,
        socket_path=socket_path,
        priority_rpc=priority_rpc,
        auto_exec=auto_exec,
    )

    async def _feed() -> None:
        while True:
            tx = await tx_source.get()
            await execer.enqueue(tx if isinstance(tx, str) else tx.decode())
            tx_source.task_done()

    if auto_exec:
        await _feed()
    else:
        await asyncio.gather(execer.run(), _feed())

