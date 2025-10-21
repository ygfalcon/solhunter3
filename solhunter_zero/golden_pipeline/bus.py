"""Lightweight async message bus abstraction for the Golden pipeline."""

from __future__ import annotations

import asyncio
import logging
from collections import Counter
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, Mapping, MutableMapping, Optional

from .validation import SchemaValidationError, validate_stream_payload


log = logging.getLogger(__name__)


class MessageBus:
    """Protocol-like base class for message buses."""

    async def publish(
        self,
        stream: str,
        payload: Mapping[str, object],
        *,
        dedupe_key: str | None = None,
    ) -> None:
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
        self.validation_failures: Counter[str] = Counter()
        self.subscriber_failures: Counter[str] = Counter()

    async def publish(
        self,
        stream: str,
        payload: Mapping[str, object],
        *,
        dedupe_key: str | None = None,
    ) -> None:
        try:
            materialised = validate_stream_payload(stream, payload)
        except SchemaValidationError as exc:
            self.validation_failures[stream] += 1
            log.warning("dropping invalid payload for %s: %s", stream, exc)
            raise
        async with self._lock:
            self.events.setdefault(stream, []).append(dict(materialised))
            subs = list(self._subscribers.get(stream, ()))
        for sub in subs:
            await sub.handler(materialised)

    async def subscribe(
        self, stream: str, handler: Callable[[Mapping[str, object]], Awaitable[None]]
    ) -> Callable[[], None]:
        async def _wrapped(payload: Mapping[str, object]) -> None:
            try:
                materialised = validate_stream_payload(stream, payload)
            except SchemaValidationError as exc:
                self.subscriber_failures[stream] += 1
                log.error("subscriber rejected payload for %s: %s", stream, exc)
                return
            await handler(materialised)

        subscription = _Subscription(stream=stream, handler=_wrapped)
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
        self.validation_failures: Counter[str] = Counter()

    async def publish(
        self,
        stream: str,
        payload: Mapping[str, object],
        *,
        dedupe_key: str | None = None,
    ) -> None:
        try:
            materialised = validate_stream_payload(stream, payload)
        except SchemaValidationError as exc:
            self.validation_failures[stream] += 1
            log.warning("dropping invalid payload for %s: %s", stream, exc)
            raise
        self._event_bus.publish(stream, dict(materialised), dedupe_key=dedupe_key)

    async def healthcheck(self) -> bool:
        return True


__all__ = ["MessageBus", "InMemoryBus", "EventBusAdapter"]
