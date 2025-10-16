import asyncio
import json
import logging
from contextlib import suppress
from typing import Mapping, Iterable, Dict, Any

import os
import websockets

_WS_PING_INTERVAL = float(os.getenv("WS_PING_INTERVAL", "20") or 20)
_WS_PING_TIMEOUT = float(os.getenv("WS_PING_TIMEOUT", "20") or 20)
from .event_bus import publish

logger = logging.getLogger(__name__)

class PriceStreamManager:
    """Manage websocket price streams and publish updates."""

    def __init__(self, streams: Mapping[str, str], tokens: Iterable[str]):
        self.streams = dict(streams)
        self.tokens = list(tokens)
        self._tasks: Dict[str, asyncio.Task] = {}
        self._running = False

    async def start(self) -> None:
        self._running = True
        for venue, url in self.streams.items():
            if venue in self._tasks:
                continue
            self._tasks[venue] = asyncio.create_task(self._run_stream(venue, url))

    async def stop(self) -> None:
        self._running = False
        tasks = list(self._tasks.values())
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._tasks.clear()

    async def _run_stream(self, venue: str, url: str) -> None:
        backoff = 1.0
        while self._running:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=_WS_PING_INTERVAL,
                    ping_timeout=_WS_PING_TIMEOUT,
                ) as ws:
                    for tok in self.tokens:
                        try:
                            await ws.send(json.dumps({"token": tok}))
                        except Exception:
                            pass
                    backoff = 1.0
                    async for msg in ws:
                        try:
                            data: Dict[str, Any] = json.loads(msg)
                        except Exception:
                            continue
                        token = data.get("token")
                        price = data.get("price")
                        if isinstance(token, str) and isinstance(price, (int, float)):
                            publish(
                                "price_update",
                                {"venue": venue, "token": token, "price": float(price)},
                            )
            except asyncio.CancelledError:
                break
            except Exception as exc:  # pragma: no cover - network errors
                logger.error("price stream error for %s: %s", venue, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

