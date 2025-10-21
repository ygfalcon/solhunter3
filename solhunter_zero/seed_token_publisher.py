"""Periodic publisher for seeded tokens to keep discovery alive."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import redis.asyncio as aioredis

from . import prices
from .token_aliases import canonical_mint


logger = logging.getLogger(__name__)


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


def configured_seed_tokens() -> Tuple[str, ...]:
    """Return the currently configured seed tokens from the environment."""

    return _parse_tokens("SEED_TOKENS")


def build_seeded_token_metadata(
    tokens: Sequence[str],
    mapping: Mapping[str, prices.PythIdentifier] | None = None,
) -> Dict[str, Dict[str, Any]]:
    """Resolve Pyth metadata for each ``token``.

    When ``mapping`` is ``None`` the configured Pyth overrides (including
    defaults) are loaded via :func:`prices._parse_pyth_mapping`.
    """

    resolved_mapping: Mapping[str, prices.PythIdentifier]
    if mapping is None:
        try:
            resolved_mapping, _ = prices._parse_pyth_mapping()
        except Exception:
            resolved_mapping = {}
    else:
        resolved_mapping = mapping

    metadata: Dict[str, Dict[str, Any]] = {}
    shared: Dict[str, Dict[str, Any]] = {}
    for token in tokens:
        canonical = canonical_mint(token)
        lookup = canonical or token
        existing = shared.get(lookup)
        if existing is not None:
            metadata[token] = dict(existing)
            continue
        identifier = resolved_mapping.get(lookup)
        if identifier is None and lookup != token:
            identifier = resolved_mapping.get(token)
        if identifier is not None:
            info: Dict[str, Any] = {
                "status": "available",
                "kind": identifier.kind,
                "feed_id": identifier.feed_id,
                "canonical_mint": lookup,
            }
            if identifier.account:
                info["account"] = identifier.account
        else:
            info = {
                "status": "missing",
                "canonical_mint": lookup,
            }
        shared[lookup] = info
        metadata[token] = dict(info)
    return metadata


async def _publish_local(topic: str, payload: Any, *, logger: logging.Logger) -> None:
    """Best-effort local fallback publish when Redis is unavailable."""
    try:
        from . import event_bus

        event_bus.publish(topic, payload, _broadcast=True)
    except Exception:
        logger.exception("Seed tokens: local event_bus publish failed for topic %s", topic)


async def run_seed_token_publisher() -> None:
    tokens = configured_seed_tokens()
    if not tokens:
        raise RuntimeError("SEED_TOKENS must contain at least one mint")

    redis_url = os.getenv("SEED_PUBLISH_REDIS_URL") or os.getenv("AMM_WATCH_REDIS_URL") or os.getenv(
        "EVENT_BUS_URL", "redis://localhost:6379/0"
    )
    channel = os.getenv("SEED_PUBLISH_BROKER_CHANNEL") or os.getenv("AMM_WATCH_BROKER_CHANNEL") or os.getenv(
        "BROKER_CHANNEL", "solhunter-events-v2"
    )
    interval = _parse_float_env("SEED_PUBLISH_INTERVAL", 180.0, minimum=30.0)

    redis_client: aioredis.Redis | None = None
    redis_error_logged = False
    try:
        redis_client = aioredis.from_url(redis_url, decode_responses=True)
        await redis_client.ping()
        logger.info("Seed tokens: connected to Redis broker at %s (channel=%s)", redis_url, channel)
    except Exception:
        redis_client = None
        if not redis_error_logged:
            logger.exception("Seed tokens: unable to connect to Redis at %s; using in-process fallback", redis_url)
            redis_error_logged = True

    try:
        pyth_mapping, _ = prices._parse_pyth_mapping()
    except Exception as exc:
        logger.debug("Seed tokens: failed to parse Pyth mapping: %s", exc)
        pyth_mapping = {}
    metadata = build_seeded_token_metadata(tokens, pyth_mapping)
    logged: set[str] = set()
    for token in tokens:
        info = metadata.get(token) or {}
        canonical = str(info.get("canonical_mint") or token)
        if canonical in logged:
            continue
        logged.add(canonical)
        if info.get("status") == "available":
            account = info.get("account")
            feed_id = info.get("feed_id")
            if account:
                logger.info("Seed tokens: %s uses Pyth account %s", canonical, account)
            else:
                logger.info("Seed tokens: %s uses Pyth feed %s", canonical, feed_id)
        else:
            logger.warning("Seed tokens: %s missing Pyth identifier", canonical)

    while True:
        now = time.time()
        redis_messages: List[str] = []
        local_events: List[tuple[str, Any]] = []
        depth_entries: Dict[str, Dict[str, Any]] = {}
        for token in tokens:
            discovery_payload = {
                "ts": now,
                "source": "seeded",
                "mint": token,
                "tx": "",
                "tags": ["seeded"],
                "interface": "FungibleToken",
                "discovery": {"method": "seeded"},
            }
            info = metadata.get(token)
            if info is not None:
                discovery_payload["discovery"]["pyth"] = dict(info)
            local_events.append(("token_discovered", discovery_payload))
            redis_messages.append(
                json.dumps({"topic": "token_discovered", "payload": discovery_payload}, separators=(",", ":"))
            )

        try:
            quotes = await prices.fetch_price_quotes_async(tokens)
        except Exception:
            quotes = {}

        for token in tokens:
            quote = quotes.get(token)
            if quote and getattr(quote, "price_usd", 0.0) > 0:
                price_payload = {
                    "venue": quote.source or "seed_bootstrap",
                    "token": token,
                    "price": float(quote.price_usd),
                    "ts": now,
                }
                local_events.append(("price_update", price_payload))
                redis_messages.append(
                    json.dumps({"topic": "price_update", "payload": price_payload}, separators=(",", ":"))
                )
                depth_entries[token] = {
                    "mint": token,
                    "bids": float(quote.price_usd) * 1000.0,
                    "asks": float(quote.price_usd) * 1000.0,
                    "depth": float(quote.price_usd) * 2000.0,
                    "tx_rate": 0.0,
                    "ts": now,
                }

        if depth_entries:
            depth_payload = {mint: data for mint, data in depth_entries.items()}
            local_events.append(("depth_update", depth_payload))
            redis_messages.append(
                json.dumps({"topic": "depth_update", "payload": depth_payload}, separators=(",", ":"))
            )

        total_published = 0
        for message in redis_messages:
            if redis_client is not None:
                try:
                    await redis_client.publish(channel, message)
                    total_published += 1
                except Exception:
                    if not redis_error_logged:
                        logger.exception("Seed tokens: redis publish failed; falling back to local bus")
                        redis_error_logged = True
                    redis_client = None

        for topic, payload in local_events:
            await _publish_local(topic, payload, logger=logger)

        if total_published:
            logger.debug("Seed tokens: published %d payload(s) via Redis channel %s", total_published, channel)

        await asyncio.sleep(interval)
