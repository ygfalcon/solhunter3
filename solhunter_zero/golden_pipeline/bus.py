"""Lightweight async message bus abstraction for the Golden pipeline."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional


class MessageBus:
    """Protocol-like base class for message buses."""

    async def publish(self, stream: str, payload: Mapping[str, object]) -> None:
        raise NotImplementedError

    async def healthcheck(self) -> bool:
        return True


@dataclass
class _Subscription:
    stream: str
    handler: Callable[[Mapping[str, object]], Awaitable[None]]


class InMemoryBus(MessageBus):
    """A simple async message bus used in tests and offline runs."""

    def __init__(self) -> None:
        self._subscribers: MutableMapping[str, list[_Subscription]] = {}
        self._lock = asyncio.Lock()
        self.events: Dict[str, list[Mapping[str, object]]] = {}

    async def publish(self, stream: str, payload: Mapping[str, object]) -> None:
        async with self._lock:
            self.events.setdefault(stream, []).append(dict(payload))
            subs = list(self._subscribers.get(stream, ()))
        for sub in subs:
            await sub.handler(payload)

    async def subscribe(
        self, stream: str, handler: Callable[[Mapping[str, object]], Awaitable[None]]
    ) -> Callable[[], None]:
        subscription = _Subscription(stream=stream, handler=handler)
        async with self._lock:
            self._subscribers.setdefault(stream, []).append(subscription)

        def _unsubscribe() -> None:
            async def _inner() -> None:
                async with self._lock:
                    entries = self._subscribers.get(stream)
                    if not entries:
                        return
                    try:
                        entries.remove(subscription)
                    except ValueError:
                        return

            asyncio.create_task(_inner())

        return _unsubscribe


class EventBusAdapter(MessageBus):
    """Adapter that exposes :mod:`solhunter_zero.event_bus` as a ``MessageBus``."""

    def __init__(self, event_bus: Any) -> None:
        self._event_bus = event_bus

    async def publish(self, stream: str, payload: Mapping[str, object]) -> None:
        materialised = dict(payload)
        self._event_bus.publish(stream, materialised)

    async def healthcheck(self) -> bool:
        return True


__all__ = ["MessageBus", "InMemoryBus", "EventBusAdapter"]
