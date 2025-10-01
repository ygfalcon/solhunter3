from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Callable

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

if aiohttp is not None:  # pragma: no cover - optional dependency
    from .http import get_session
else:  # pragma: no cover - optional dependency
    get_session = None

from .event_bus import subscribe

logger = logging.getLogger(__name__)


def _extract(msg: Any) -> dict[str, float] | None:
    loss = getattr(msg, "loss", None)
    reward = getattr(msg, "reward", None)
    if isinstance(msg, dict):
        if loss is None:
            loss = msg.get("loss")
        if reward is None:
            reward = msg.get("reward")
    if loss is None or reward is None:
        return None
    try:
        return {"loss": float(loss), "reward": float(reward)}
    except Exception:
        return None


def start_metrics_exporter(url: str | None = None) -> Callable[[], None]:
    """Subscribe to ``rl_metrics`` events and POST them to ``url``."""

    url = url or os.getenv("METRICS_URL")
    if not url:
        return lambda: None

    async def _post(payload: dict[str, float]) -> None:
        if aiohttp is None or get_session is None:
            return
        try:
            session = await get_session()
            async with session.post(url, json=payload, timeout=5) as resp:
                resp.raise_for_status()
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("failed to export metrics: %s", exc)

    def _handler(msg: Any) -> None:
        data = _extract(msg)
        if data is None:
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(_post(data))
        else:
            loop.create_task(_post(data))

    return subscribe("rl_metrics", _handler)
