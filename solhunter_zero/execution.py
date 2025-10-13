"""Event-driven transaction execution loop."""

from __future__ import annotations

import asyncio
import logging
import os
import time

from .event_bus import subscription
from .logging_utils import serialize_for_log

from .depth_client import (
    stream_depth,
    submit_raw_tx,
    auto_exec as service_auto_exec,
    DEPTH_SERVICE_SOCKET,
)
from .util import parse_bool_env, sanitize_priority_urls

USE_DEPTH_STREAM = parse_bool_env("USE_DEPTH_STREAM", True)

logger = logging.getLogger(__name__)


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
        cleaned_priority = sanitize_priority_urls(priority_rpc)
        self.priority_rpc = cleaned_priority or None
        self.auto_exec = auto_exec
        self._queue: asyncio.Queue[str] = asyncio.Queue()
        self._last = 0.0
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "EventExecutor[%s]: initialized auto_exec=%s rate_limit=%.3f threshold=%.3f priority_rpc=%s",
                self.token,
                self.auto_exec,
                self.rate_limit,
                self.threshold,
                serialize_for_log(self.priority_rpc),
            )

    async def enqueue(self, tx_b64: str) -> None:
        """Queue a pre-signed transaction for immediate submission."""

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "EventExecutor[%s]: enqueue request auto_exec=%s queue_size=%s",
                self.token,
                self.auto_exec,
                self._queue.qsize(),
            )
        if self.auto_exec:
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: forwarding to service auto_exec threshold=%.3f tx=%s",
                    self.token,
                    self.threshold,
                    serialize_for_log(tx_b64),
                )
            await service_auto_exec(
                self.token,
                self.threshold,
                [tx_b64],
                socket_path=self.socket_path,
            )
        else:
            await self._queue.put(tx_b64)
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: transaction queued new_size=%s",
                    self.token,
                    self._queue.qsize(),
                )

    async def _handle_update(self, data: dict) -> None:
        entry = data.get(self.token)
        if entry is None:
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "EventExecutor[%s]: received update without token entry keys=%s",
                    self.token,
                    list(data.keys()),
                )
            return
        rate = float(entry.get("tx_rate", 0.0))
        now = time.monotonic()
        elapsed = now - self._last
        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "EventExecutor[%s]: depth update rate=%.6f elapsed=%.6f queue=%s threshold=%.3f",
                self.token,
                rate,
                elapsed,
                self._queue.qsize(),
                self.threshold,
            )
        if rate < self.threshold:
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: update skipped (rate %.6f < threshold %.3f)",
                    self.token,
                    rate,
                    self.threshold,
                )
            return
        if elapsed < self.rate_limit:
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: update skipped (elapsed %.6f < rate_limit %.6f)",
                    self.token,
                    elapsed,
                    self.rate_limit,
                )
            return
        try:
            tx = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: update received with empty queue",
                    self.token,
                )
            return
        self._last = now
        try:
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: submitting transaction queue_remaining=%s",
                    self.token,
                    self._queue.qsize(),
                )
            await submit_raw_tx(
                tx,
                socket_path=self.socket_path,
                priority_rpc=self.priority_rpc,
            )
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: submit_raw_tx succeeded",
                    self.token,
                )
        except Exception:
            logger.exception("EventExecutor[%s]: submit_raw_tx failed", self.token)
            await self._queue.put(tx)
        finally:
            self._queue.task_done()
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "EventExecutor[%s]: queue task marked done size=%s",
                    self.token,
                    self._queue.qsize(),
                )

    async def run(self) -> None:
        """Start the event loop."""

        if logger.isEnabledFor(logging.INFO):
            logger.info(
                "EventExecutor[%s]: starting run loop depth_stream=%s",
                self.token,
                USE_DEPTH_STREAM,
            )
        if USE_DEPTH_STREAM:
            with subscription("depth_update", self._handle_update):
                if logger.isEnabledFor(logging.INFO):
                    logger.info(
                        "EventExecutor[%s]: listening for depth_update events",
                        self.token,
                    )
                await asyncio.Future()
            return

        async for update in stream_depth(
            self.token, rate_limit=self.rate_limit
        ):
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(
                    "EventExecutor[%s]: streaming update received",
                    self.token,
                )
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
    if logger.isEnabledFor(logging.INFO):
        logger.info(
            "run_event_loop[%s]: initialized auto_exec=%s rate_limit=%.3f threshold=%.3f",
            token,
            auto_exec,
            rate_limit,
            threshold,
        )

    async def _feed() -> None:
        while True:
            tx = await tx_source.get()
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "run_event_loop[%s]: received transaction from source -> %s",
                    token,
                    serialize_for_log(
                        tx if isinstance(tx, str) else f"<bytes {len(tx)}>"
                    ),
                )
            await execer.enqueue(tx if isinstance(tx, str) else tx.decode())
            tx_source.task_done()
            if logger.isEnabledFor(logging.INFO):
                logger.info(
                    "run_event_loop[%s]: source task marked done",
                    token,
                )

    if auto_exec:
        if logger.isEnabledFor(logging.INFO):
            logger.info("run_event_loop[%s]: running feed in auto_exec mode", token)
        await _feed()
    else:
        if logger.isEnabledFor(logging.INFO):
            logger.info("run_event_loop[%s]: starting executor and feed tasks", token)
        await asyncio.gather(execer.run(), _feed())
