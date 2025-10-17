#!/usr/bin/env python3
"""Smoke test utility for the Redis event bus."""
from __future__ import annotations

import argparse
import asyncio
import os
import time
import uuid
from dataclasses import dataclass
from typing import Iterable, List, Tuple

try:  # optional dependency
    import redis.asyncio as aioredis
except Exception:  # pragma: no cover
    aioredis = None  # type: ignore

STREAMS: Tuple[str, ...] = (
    "x:discovery.candidates",
    "x:token.snap",
    "x:market.ohlcv.5m",
    "x:market.depth",
    "x:mint.golden",
    "x:trade.suggested",
    "x:vote.decisions",
    "x:virt.fills",
    "x:live.fills",
)

KV_KEYS: Tuple[Tuple[str, int], ...] = (
    ("discovery:seen:SMOKE", 24 * 3600),
    ("discovery:cursor", 24 * 3600),
    ("snap:golden:SMOKE", 90),
    ("vote:dupe:SMOKE", 5 * 60),
)


@dataclass
class SmokeResult:
    name: str
    ok: bool
    detail: str


def _fail(name: str, msg: str) -> SmokeResult:
    return SmokeResult(name=name, ok=False, detail=msg)


def _ok(name: str, msg: str) -> SmokeResult:
    return SmokeResult(name=name, ok=True, detail=msg)


async def _connect(redis_url: str):
    if aioredis is None:
        # redis is an optional dependency; return None so callers can soft-skip.
        return None
    return aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)


async def _check_stream(client, stream: str) -> SmokeResult:
    if client is None:
        return _ok(stream, "skipped (redis not installed)")
    marker = f"smoke-{uuid.uuid4()}"
    fields = {"_smoke": marker, "ts": str(time.time())}
    try:
        message_id = await asyncio.wait_for(
            client.xadd(
                stream,
                fields,
                maxlen=1000,
                approximate=True,
                nomkstream=True,
            ),
            timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(stream, f"xadd failed: {exc}")
    try:
        rows = await asyncio.wait_for(
            client.xrange(stream, min=message_id, max=message_id),
            timeout=3,
        )
    except Exception as exc:  # noqa: BLE001
        return _fail(stream, f"xrange failed: {exc}")
    finally:
        try:
            await asyncio.wait_for(client.xdel(stream, message_id), timeout=3)
        except Exception:
            pass
    if not rows:
        return _fail(stream, "no rows returned")
    _id, payload = rows[0]
    if payload.get("_smoke") != marker:
        return _fail(stream, "payload mismatch")
    return _ok(stream, "read/write ok")


async def _check_key(client, key: str, ttl_seconds: int) -> SmokeResult:
    if client is None:
        return _ok(key, "skipped (redis not installed)")
    marker = f"smoke-{uuid.uuid4()}"
    try:
        await asyncio.wait_for(client.set(key, marker, ex=ttl_seconds), timeout=3)
    except Exception as exc:  # noqa: BLE001
        return _fail(key, f"set failed: {exc}")
    try:
        value = await asyncio.wait_for(client.get(key), timeout=3)
        ttl = await asyncio.wait_for(client.ttl(key), timeout=3)
    except Exception as exc:  # noqa: BLE001
        return _fail(key, f"read failed: {exc}")
    finally:
        try:
            await asyncio.wait_for(client.delete(key), timeout=3)
        except Exception:
            pass
    if value != marker:
        return _fail(key, "value mismatch")
    if ttl is None or ttl < 0:
        return _fail(key, "ttl missing")
    if ttl > ttl_seconds or ttl < max(1, ttl_seconds - 10):
        return _fail(key, f"ttl unexpected ({ttl}s)")
    return _ok(key, f"kv ok (ttl={ttl}s)")


async def run_ping(redis_url: str) -> List[SmokeResult]:
    client = await _connect(redis_url)
    results: List[SmokeResult] = []
    try:
        for stream in STREAMS:
            results.append(await _check_stream(client, stream))
    finally:
        if client is not None:
            await client.close()
    return results


async def run_keys(redis_url: str) -> List[SmokeResult]:
    client = await _connect(redis_url)
    results: List[SmokeResult] = []
    try:
        for key, ttl in KV_KEYS:
            results.append(await _check_key(client, key, ttl))
    finally:
        if client is not None:
            await client.close()
    return results


def render(results: Iterable[SmokeResult]) -> int:
    failures = 0
    for result in results:
        status = "OK" if result.ok else "FAIL"
        print(f"{result.name}: {status} - {result.detail}")
        if not result.ok:
            failures += 1
    return 0 if failures == 0 else 1


async def _dispatch(args: argparse.Namespace) -> int:
    redis_url = args.redis_url or os.getenv("REDIS_URL")
    if not redis_url or str(redis_url).strip().lower() == "skip":
        print("redis: OK - skipped (REDIS_URL not set or 'skip')")
        return 0
    if args.command == "ping":
        results = await run_ping(redis_url)
    elif args.command == "keys":
        results = await run_keys(redis_url)
    else:  # pragma: no cover
        raise SystemExit("unknown command")
    return render(results)


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Redis bus smoke tool")
    parser.add_argument("command", choices=("ping", "keys"), help="sub-command to run")
    parser.add_argument("--redis-url", dest="redis_url", help="override redis url")
    args = parser.parse_args(argv)
    return asyncio.run(_dispatch(args))


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
