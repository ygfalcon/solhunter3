#!/usr/bin/env python3
"""Inspect the broker channel for non-protobuf frames."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from collections import Counter
from typing import Any

from solhunter_zero.event_bus import (
    _ProtoDecodeError,
    _maybe_decompress,
    _PB_MAP,
    _decode_payload,
    pb,
)

try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover - optional dependency guard
    aioredis = None


log = logging.getLogger("event_bus_inspector")


async def _inspect_channel(
    url: str,
    channel: str,
    limit: int | None,
    idle_timeout: float,
    classify_good: bool,
) -> None:
    if aioredis is None:
        raise RuntimeError("redis.asyncio is required to inspect redis channels")

    conn = aioredis.from_url(url)
    pubsub = conn.pubsub()
    await pubsub.subscribe(channel)
    counts: Counter[str] = Counter()
    topics: Counter[str] = Counter()
    start = time.monotonic()
    processed = 0
    try:
        while limit is None or processed < limit:
            msg = await pubsub.get_message(ignore_subscribe_messages=True, timeout=1.0)
            if msg is None:
                if idle_timeout and (time.monotonic() - start) > idle_timeout:
                    break
                continue
            start = time.monotonic()
            data = msg.get("data")
            if isinstance(data, memoryview):
                data = bytes(data)
            if not isinstance(data, (bytes, bytearray)):
                counts["non-bytes"] += 1
                log.warning("non-bytes frame encountered: %r", type(data))
                continue
            raw = bytes(data)
            payload = _maybe_decompress(raw)
            event = pb.Event()
            try:
                event.ParseFromString(payload)
            except _ProtoDecodeError:
                counts["incompatible"] += 1
                try:
                    text = payload.decode("utf-8", errors="ignore")
                    obj = json.loads(text)
                    topic = obj.get("topic")
                    counts["json"] += 1
                    if topic:
                        topics[f"json:{topic}"] += 1
                        log.warning("JSON frame detected for topic %s", topic)
                    else:
                        log.warning("JSON frame without topic: %s", text[:120])
                except Exception:
                    log.warning("undecodable frame (%d bytes)", len(payload))
                continue
            processed += 1
            try:
                decoded = _decode_payload(event)
            except Exception as exc:  # pragma: no cover - decode errors are rare
                counts["decode_error"] += 1
                topics[f"decode_error:{event.topic}"] += 1
                log.warning("failed to decode topic %s: %s", event.topic, exc)
                continue
            counts["protobuf"] += 1
            topics[event.topic] += 1
            if classify_good:
                log.info("protobuf topic=%s keys=%s", event.topic, sorted(_extract_keys(decoded)))
    finally:
        await pubsub.close()
        await conn.close()

    print("\n=== Inspector Summary ===")
    print(f"Redis URL: {url}")
    print(f"Channel: {channel}")
    for kind, count in sorted(counts.items()):
        print(f"{kind:>12}: {count}")
    if topics:
        print("\nTop topics:")
        for topic, count in topics.most_common():
            print(f"  {topic}: {count}")

    missing = sorted(topic for topic, cls in _PB_MAP.items() if cls is None)
    if missing:
        print("\nWarning: topics without protobuf schema:")
        for topic in missing:
            print(f"  {topic}")


def _extract_keys(decoded: Any) -> list[str]:
    if isinstance(decoded, dict):
        return list(decoded.keys())
    if isinstance(decoded, list):
        return ["len", str(len(decoded))]
    if decoded is None:
        return ["<none>"]
    return [type(decoded).__name__]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--url",
        default=os.getenv("BROKER_URL") or "redis://127.0.0.1:6379",
        help="Redis URL to inspect (default: %(default)s)",
    )
    parser.add_argument(
        "--channel",
        default=os.getenv("BROKER_CHANNEL") or "solhunter-events-v2",
        help="Channel/subject to subscribe to",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of protobuf frames to inspect (default: %(default)s)",
    )
    parser.add_argument(
        "--idle-timeout",
        type=float,
        default=15.0,
        help="Exit after N seconds without traffic (default: %(default)s)",
    )
    parser.add_argument(
        "--print-good",
        action="store_true",
        help="Also log successfully decoded protobuf frames",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    logging.basicConfig(level=logging.INFO if args.print_good else logging.WARNING)
    try:
        asyncio.run(
            _inspect_channel(
                url=args.url,
                channel=args.channel,
                limit=args.limit if args.limit and args.limit > 0 else None,
                idle_timeout=args.idle_timeout,
                classify_good=args.print_good,
            )
        )
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        return 1
    except Exception as exc:
        log.error("inspection failed: %s", exc)
        return 2
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())
