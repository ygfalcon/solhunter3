# Utility functions for runtime helpers.

from __future__ import annotations

from typing import Any, Coroutine, Iterable, List, TypeVar

import asyncio
import importlib
import logging
import os

logger = logging.getLogger(__name__)


_TRUE_VALUES = {"1", "true", "yes"}
_FALSE_VALUES = {"0", "false", "no"}
_PLACEHOLDER_MARKERS = ("YOUR_KEY", "YOUR_HELIUS_KEY", "CHANGE_ME", "EXAMPLE", "REDACTED")


def parse_bool_env(name: str, default: bool = False) -> bool:
    """Return the boolean value for environment variable ``name``.

    The lookup falls back to ``default`` when the variable is unset or when
    the value does not match any known true/false strings.  Comparisons are
    performed on ``strip().lower()`` forms to tolerate whitespace and case
    variations.
    """

    val = os.getenv(name)
    if val is None:
        return default
    norm = val.strip().lower()
    if norm in _TRUE_VALUES:
        return True
    if norm in _FALSE_VALUES:
        return False
    return default


def install_uvloop() -> None:
    """Install ``uvloop`` if available.

    Importing and calling this function sets ``asyncio``'s event loop
    policy to ``uvloop``. If the optional dependency is not installed
    the function silently does nothing.
    """
    try:
        uvloop = importlib.import_module("uvloop")
    except Exception:  # pragma: no cover - optional dependency missing
        logger.debug("uvloop not installed")
        return

    if getattr(asyncio, "_uvloop_installed", False):
        return

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    asyncio._uvloop_installed = True  # type: ignore[attr-defined]
    logger.debug("uvloop installed as event loop policy")


T = TypeVar("T")


def run_coro(coro: Coroutine[Any, Any, T] | T) -> T | asyncio.Task:
    """Run ``coro`` using the active loop if present.

    When called with no running event loop this function falls back to
    :func:`asyncio.run` and returns the coroutine result.  If a loop is
    already running, the coroutine is scheduled with
    :func:`asyncio.AbstractEventLoop.create_task` and the resulting task
    is returned for awaiting by the caller.
    """

    if not asyncio.iscoroutine(coro):
        return coro
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return loop.create_task(coro)


def sanitize_priority_urls(urls: Iterable[str] | None) -> List[str]:
    """Return ``urls`` without placeholders, empties, or duplicates."""

    seen: set[str] = set()
    cleaned: List[str] = []
    if not urls:
        return cleaned
    for raw in urls:
        if not isinstance(raw, str):
            continue
        text = raw.strip()
        if not text:
            continue
        if any(marker in text for marker in _PLACEHOLDER_MARKERS):
            continue
        if text in seen:
            continue
        seen.add(text)
        cleaned.append(text)
    return cleaned
