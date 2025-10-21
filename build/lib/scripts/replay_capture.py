#!/usr/bin/env python3
"""Replay a captured event bus stream into a running Golden pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from solhunter_zero.event_bus import BUS as RUNTIME_BUS


@dataclass(slots=True)
class ReplayConfig:
    """Runtime configuration controlling capture replay behaviour."""

    path: Path
    speed: float = 1.0
    topics: Sequence[str] | None = None
    start_seq: int | None = None
    end_seq: int | None = None
    max_events: int | None = None
    bus: Any = RUNTIME_BUS


@dataclass(slots=True)
class ReplayResult:
    """Summary produced after :func:`replay_capture` completes."""

    path: Path
    replayed: int
    duration: float
    first_seq: int | None
    last_seq: int | None
    metadata: Mapping[str, Any]


def load_capture(path: Path) -> tuple[Mapping[str, Any], list[Mapping[str, Any]]]:
    """Return ``(metadata, events)`` for the capture stored at *path*."""

    metadata: Mapping[str, Any] = {}
    events: list[Mapping[str, Any]] = []
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            text = line.strip()
            if not text:
                continue
            data = json.loads(text)
            if "meta" in data and not metadata:
                metadata = data["meta"] or {}
                continue
            events.append(data)
    return metadata, events


async def replay_capture(config: ReplayConfig) -> ReplayResult:
    """Replay the captured stream using the supplied configuration."""

    metadata, events = load_capture(config.path)
    if not events:
        return ReplayResult(
            path=config.path,
            replayed=0,
            duration=0.0,
            first_seq=None,
            last_seq=None,
            metadata=metadata,
        )

    topics_filter = set(config.topics) if config.topics else None
    speed = float(config.speed)
    if speed < 0:
        speed = 0.0

    start_time = time.monotonic()
    count = 0
    first_seq: int | None = None
    last_seq: int | None = None
    previous_elapsed: float | None = None

    for entry in events:
        topic = entry.get("topic")
        if topics_filter and topic not in topics_filter:
            continue
        seq = int(entry.get("seq", 0)) or None
        if config.start_seq and seq and seq < config.start_seq:
            continue
        if config.end_seq and seq and seq > config.end_seq:
            break
        elapsed = float(entry.get("elapsed", 0.0) or 0.0)
        delay = 0.0
        if previous_elapsed is not None:
            delta = max(0.0, elapsed - previous_elapsed)
            if speed > 0:
                delay = delta / speed
        previous_elapsed = elapsed
        if delay > 0:
            await asyncio.sleep(delay)

        payload = entry.get("payload")
        config.bus.publish(topic, payload)
        count += 1
        if first_seq is None:
            first_seq = seq
        last_seq = seq
        if config.max_events and count >= config.max_events:
            break

    duration = time.monotonic() - start_time
    return ReplayResult(
        path=config.path,
        replayed=count,
        duration=duration,
        first_seq=first_seq,
        last_seq=last_seq,
        metadata=metadata,
    )


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("capture", type=Path, help="Path to the JSONL capture file")
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        help="Playback multiplier (0 disables delays)",
    )
    parser.add_argument(
        "--topic",
        "-t",
        action="append",
        default=None,
        help="Restrict playback to specific topics",
    )
    parser.add_argument(
        "--start-seq",
        type=int,
        default=None,
        help="Skip events with a sequence lower than this value",
    )
    parser.add_argument(
        "--end-seq",
        type=int,
        default=None,
        help="Stop once this sequence has been replayed",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Replay at most this many events",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    config = ReplayConfig(
        path=args.capture,
        speed=args.speed,
        topics=args.topic,
        start_seq=args.start_seq,
        end_seq=args.end_seq,
        max_events=args.max_events,
    )
    try:
        result = asyncio.run(replay_capture(config))
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        return 1
    except FileNotFoundError:
        print(f"capture not found: {config.path}", file=sys.stderr)
        return 2
    except Exception as exc:  # pragma: no cover - CLI feedback
        print(f"replay failed: {exc}", file=sys.stderr)
        return 3
    print(
        f"Replayed {result.replayed} events from {result.path} "
        f"in {result.duration:.2f}s"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())

