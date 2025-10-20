"""Periodic publisher for seeded tokens to keep discovery alive."""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import List, Tuple

import redis.asyncio as aioredis


def _parse_tokens(name: str) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return tuple()
    tokens: List[str] = []
    for part in raw.split(","):
        candidate = part.strip()
        if candidate:
            tokens.append(candidate)
    return tuple(tokens)


def _parse_float_env(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = default
    if minimum is not None:
        return max(minimum, value)
    return value


async def run_seed_token_publisher() -> None:
    tokens = _parse_tokens("SEED_TOKENS")
    if not tokens:
        raise RuntimeError("SEED_TOKENS must contain at least one mint")

    redis_url = os.getenv("SEED_PUBLISH_REDIS_URL") or os.getenv("AMM_WATCH_REDIS_URL") or os.getenv(
        "EVENT_BUS_URL", "redis://localhost:6379/0"
    )
    channel = os.getenv("SEED_PUBLISH_BROKER_CHANNEL") or os.getenv("AMM_WATCH_BROKER_CHANNEL") or os.getenv(
        "BROKER_CHANNEL", "solhunter-events-v2"
    )
    interval = _parse_float_env("SEED_PUBLISH_INTERVAL", 180.0, minimum=30.0)

    redis_client = aioredis.from_url(redis_url, decode_responses=True)

    while True:
        now = time.time()
        payloads = []
        for token in tokens:
            event = {
                "topic": "token_discovered",
                "ts": now,
                "source": "seeded",
                "mint": token,
                "tx": "",
                "tags": ["seeded"],
                "interface": "FungibleToken",
                "discovery": {"method": "seeded"},
            }
            payloads.append(json.dumps(event, separators=(",", ":")))
        for payload in payloads:
            await redis_client.publish(channel, payload)
        await asyncio.sleep(interval)

