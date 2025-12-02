"""Lightweight discovery feed health probe.

The check intentionally stays simple so it can run immediately after
``scripts.bus_smoke`` without requiring the full runtime:

* ``discovery:cursor`` TTL is used to approximate freshness. The TTL is set
  when discovery advances, so a shrinking TTL implies the cursor is aging out.
* The discovery candidate stream (``x:discovery.candidates``) is sampled to
  estimate a recent message rate using entry timestamps encoded in stream IDs.

The script exits with ``0`` when both freshness and rate thresholds are met;
otherwise it prints a short diagnostic and exits ``1``.
"""
from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import dataclass
from typing import Iterable, List, Tuple

try:  # optional dependency
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore


STREAM = "x:discovery.candidates"
CURSOR_KEY = "discovery:cursor"
DEFAULT_CURSOR_TTL = 24 * 3600
DEFAULT_MAX_CURSOR_AGE = 15 * 60
DEFAULT_MIN_MSGS_PER_MIN = 2.0


@dataclass
class DiscoveryHealth:
    cursor_ttl: float | None
    cursor_age: float | None
    latest_ts_ms: int | None
    message_rate_per_min: float | None
    samples: int


async def _connect(redis_url: str):
    if aioredis is None:
        return None
    return aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)


def _extract_ts_ms(message_id: str) -> int | None:
    try:
        return int(message_id.split("-", 1)[0])
    except Exception:
        return None


def _estimate_message_rate(entries: Iterable[Tuple[str, dict]], *, window_ms: int = 300_000) -> tuple[float | None, int, int | None]:
    latest: int | None = None
    earliest: int | None = None
    count = 0
    for msg_id, _ in entries:
        ts = _extract_ts_ms(msg_id)
        if ts is None:
            continue
        if latest is None:
            latest = ts
        if earliest is None:
            earliest = ts
        count += 1
        if latest - ts > window_ms:
            break
        earliest = ts

    if count < 2 or latest is None or earliest is None:
        return 0.0 if count else None, count, latest

    span_sec = max(1.0, (latest - earliest) / 1000.0)
    rate_per_min = (count - 1) / span_sec * 60.0
    return rate_per_min, count, latest


async def _gather_health(redis_url: str, *, cursor_ttl: int = DEFAULT_CURSOR_TTL, sample: int = 50) -> DiscoveryHealth:
    client = await _connect(redis_url)
    if client is None:
        return DiscoveryHealth(None, None, None, None, 0)
    try:
        ttl = await asyncio.wait_for(client.ttl(CURSOR_KEY), timeout=3.0)
        entries: List[Tuple[str, dict]] = await asyncio.wait_for(
            client.xrevrange(STREAM, count=sample), timeout=3.0
        )
    finally:
        await client.close()

    ttl_val = float(ttl) if ttl is not None and ttl >= 0 else None
    age = cursor_ttl - ttl_val if ttl_val is not None else None
    rate, count, latest = _estimate_message_rate(entries)
    return DiscoveryHealth(ttl_val, age, latest, rate, count)


def _evaluate_health(health: DiscoveryHealth, *, max_age: float, min_rate: float) -> tuple[bool, str]:
    if health.cursor_age is None:
        return False, "discovery cursor TTL missing"
    if health.cursor_age > max_age:
        return False, f"discovery cursor stale ({health.cursor_age:.1f}s > {max_age:.1f}s)"

    if health.message_rate_per_min is None:
        return False, "no discovery messages observed"
    if health.message_rate_per_min < min_rate:
        return False, (
            f"discovery rate low ({health.message_rate_per_min:.2f}/min < {min_rate:.2f}/min; "
            f"samples={health.samples})"
        )

    return True, (
        f"discovery healthy (age={health.cursor_age:.1f}s, "
        f"rate={health.message_rate_per_min:.2f}/min, samples={health.samples})"
    )


async def _run(args: argparse.Namespace) -> int:
    redis_url = args.redis_url or os.getenv("REDIS_URL")
    if not redis_url:
        print("discovery: skipped (REDIS_URL not set)")
        return 0

    health = await _gather_health(redis_url)
    ok, msg = _evaluate_health(
        health,
        max_age=float(args.max_cursor_age),
        min_rate=float(args.min_msgs_per_min),
    )
    status = "OK" if ok else "FAIL"
    print(f"discovery: {status} - {msg}")
    return 0 if ok else 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Discovery health probe")
    parser.add_argument("--redis-url", dest="redis_url", help="Redis URL override")
    parser.add_argument(
        "--max-cursor-age",
        type=float,
        default=DEFAULT_MAX_CURSOR_AGE,
        help="Maximum allowed cursor age in seconds",
    )
    parser.add_argument(
        "--min-msgs-per-min",
        type=float,
        default=DEFAULT_MIN_MSGS_PER_MIN,
        help="Minimum acceptable discovery message rate (per minute)",
    )
    args = parser.parse_args(argv)
    try:
        return asyncio.run(_run(args))
    except KeyboardInterrupt:  # pragma: no cover - script convenience
        return 1


if __name__ == "__main__":  # pragma: no cover - script entry point
    raise SystemExit(main())
