"""Utilities for bridging between async code and synchronous call-sites."""

from __future__ import annotations

import asyncio
import threading
from typing import Awaitable, Callable, TypeVar

T = TypeVar("T")


def run_async(coro_factory: Callable[[], Awaitable[T]]) -> T:
    """Run ``coro_factory`` to completion and return its result.

    When called from a synchronous context without an active event loop we use
    :func:`asyncio.run`. If a loop is already running (for example inside an
    async task) we spin up a dedicated loop in a background thread so the
    synchronous caller still gets a blocking result without "coroutine was
    never awaited" warnings.
    """

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro_factory())

    result: list[T] = []
    error: list[BaseException] = []

    def _runner() -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            result.append(loop.run_until_complete(coro_factory()))
        except BaseException as exc:  # pragma: no cover - bubbled up
            error.append(exc)
        finally:
            loop.close()

    thread = threading.Thread(target=_runner, daemon=True)
    thread.start()
    thread.join()

    if error:
        raise error[0]
    # ``result`` always populated unless coroutine raised; mypy guard
    return result[0]


__all__ = ["run_async"]

