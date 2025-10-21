#!/usr/bin/env python3
"""Capture runtime event bus traffic for later replay."""

from __future__ import annotations

import argparse
import asyncio
import base64
import contextlib
import json
import signal
import socket
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from solhunter_zero import event_bus as event_bus_module
from solhunter_zero.event_bus import BUS as RUNTIME_BUS
from solhunter_zero.schemas import TOPICS as SCHEMA_TOPICS, to_dict


CAPTURE_VERSION = 1


def _default_topics() -> list[str]:
    topics: set[str] = {t for t in SCHEMA_TOPICS if t}
    pb_map = getattr(event_bus_module, "_PB_MAP", None)
    if isinstance(pb_map, Mapping):
        topics.update(str(name) for name in pb_map.keys() if name)
    return sorted(topics)


def _json_sanitize(payload: Any) -> Any:
    materialised = to_dict(payload)
    if isinstance(materialised, Mapping):
        return {str(k): _json_sanitize(v) for k, v in materialised.items()}
    if isinstance(materialised, (list, tuple, set)):
        return [_json_sanitize(v) for v in materialised]
    if isinstance(materialised, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(materialised)).decode("ascii")
    return materialised


def _json_default(obj: Any) -> Any:
    if isinstance(obj, (bytes, bytearray, memoryview)):
        return base64.b64encode(bytes(obj)).decode("ascii")
    if hasattr(obj, "__dataclass_fields__"):
        return _json_sanitize(obj)
    raise TypeError(f"Object of type {type(obj)!r} is not JSON serialisable")


@dataclass(slots=True)
class CaptureConfig:
    """Runtime configuration for :func:`capture_events`."""

    output: Path
    topics: Sequence[str]
    duration: float | None = None
    max_events: int | None = None
    force: bool = False
    metadata: Mapping[str, Any] = field(default_factory=dict)
    bus: Any = RUNTIME_BUS


@dataclass(slots=True)
class CaptureSummary:
    """Materialised capture metadata returned by :func:`capture_events`."""

    path: Path
    topics: tuple[str, ...]
    events: int
    started_at: float
    finished_at: float
    duration: float


async def capture_events(
    config: CaptureConfig,
    *,
    stop_event: asyncio.Event | None = None,
    register_signals: bool = True,
) -> CaptureSummary:
    """Capture events from the configured bus until stopped.

    Parameters
    ----------
    config:
        Capture options controlling the output path and subscribed topics.
    stop_event:
        Optional event that, when set, stops the capture loop once the backlog
        has been flushed.
    register_signals:
        When ``True`` the function installs ``SIGINT``/``SIGTERM`` handlers that
        trigger a graceful shutdown. Tests may disable this behaviour.
    """

    if not config.topics:
        raise ValueError("capture requires at least one topic")

    output = config.output
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists() and not config.force:
        raise FileExistsError(f"capture output already exists: {output}")

    loop = asyncio.get_running_loop()
    termination = stop_event or asyncio.Event()
    queue: asyncio.Queue[tuple[str, float, float, Any]] = asyncio.Queue()

    started_wall = time.time()
    started_monotonic = time.monotonic()
    sequence = 0
    drained = asyncio.Event()

    def _materialise(payload: Any) -> Any:
        return _json_sanitize(payload)

    def _register_handler(topic: str) -> Callable[[], None]:
        async def _handler(payload: Any) -> None:
            ts = time.time()
            elapsed = time.monotonic() - started_monotonic
            queue.put_nowait((topic, ts, elapsed, _materialise(payload)))

        return config.bus.subscribe(topic, _handler)

    unsubscribers = [_register_handler(topic) for topic in config.topics]

    async def _writer() -> None:
        nonlocal sequence
        meta = {
            "version": CAPTURE_VERSION,
            "started_at": started_wall,
            "topics": list(config.topics),
            "hostname": socket.gethostname(),
        }
        if config.duration:
            meta["duration_limit"] = float(config.duration)
        if config.max_events:
            meta["event_limit"] = int(config.max_events)
        if config.metadata:
            meta["labels"] = _json_sanitize(config.metadata)

        with output.open("w", encoding="utf-8") as fh:
            fh.write(json.dumps({"meta": meta}, default=_json_default) + "\n")
            fh.flush()

            while True:
                if termination.is_set() and queue.empty():
                    break
                try:
                    topic, ts, elapsed, payload = await asyncio.wait_for(
                        queue.get(), timeout=0.2
                    )
                except asyncio.TimeoutError:
                    continue
                sequence += 1
                record = {
                    "seq": sequence,
                    "topic": topic,
                    "ts": ts,
                    "elapsed": elapsed,
                    "payload": payload,
                }
                fh.write(json.dumps(record, default=_json_default) + "\n")
                fh.flush()
                queue.task_done()
                if config.max_events and sequence >= config.max_events:
                    termination.set()
            fh.flush()

        drained.set()

    writer_task = asyncio.create_task(_writer(), name="event_capture_writer")

    timer_task: asyncio.Task[None] | None = None
    if config.duration and config.duration > 0:
        async def _auto_stop() -> None:
            try:
                await asyncio.sleep(config.duration)
            finally:
                termination.set()

        timer_task = asyncio.create_task(_auto_stop(), name="event_capture_timer")

    def _trigger_stop() -> None:
        termination.set()

    installed_handlers: list[tuple[int, Callable[[], None]]] = []
    if register_signals:
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, _trigger_stop)
                installed_handlers.append((sig, _trigger_stop))
            except NotImplementedError:  # pragma: no cover - Windows event loop
                signal.signal(sig, lambda *_: termination.set())

    try:
        await termination.wait()
        await drained.wait()
    finally:
        for sig, _ in installed_handlers:
            with contextlib.suppress(Exception):
                loop.remove_signal_handler(sig)
        for unsub in unsubscribers:
            with contextlib.suppress(Exception):
                unsub()
        if timer_task:
            timer_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await timer_task
        with contextlib.suppress(asyncio.CancelledError):
            await writer_task

    finished_wall = time.time()
    duration = max(0.0, finished_wall - started_wall)
    return CaptureSummary(
        path=output,
        topics=tuple(config.topics),
        events=sequence,
        started_at=started_wall,
        finished_at=finished_wall,
        duration=duration,
    )


def _parse_labels(pairs: Iterable[str]) -> dict[str, str]:
    labels: dict[str, str] = {}
    for pair in pairs:
        if "=" not in pair:
            raise argparse.ArgumentTypeError(
                f"invalid label {pair!r}; expected key=value"
            )
        key, value = pair.split("=", 1)
        key = key.strip()
        if not key:
            raise argparse.ArgumentTypeError("label key cannot be empty")
        labels[key] = value.strip()
    return labels


def parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path.cwd() / f"event-capture-{int(time.time())}.jsonl",
        help="Destination JSONL file for the capture (default: %(default)s)",
    )
    parser.add_argument(
        "--topic",
        "-t",
        action="append",
        default=None,
        help="Topic to record (repeat for multiple). Defaults to all known topics.",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=None,
        help="Maximum capture duration in seconds",
    )
    parser.add_argument(
        "--max-events",
        type=int,
        default=None,
        help="Stop after capturing this many events",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=(),
        metavar="KEY=VALUE",
        help="Attach contextual metadata to the capture",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite the output file if it exists",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    topics = args.topic or _default_topics()
    config = CaptureConfig(
        output=args.output,
        topics=topics,
        duration=args.duration,
        max_events=args.max_events,
        force=args.force,
        metadata=_parse_labels(args.label),
    )
    try:
        summary = asyncio.run(capture_events(config))
    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        return 1
    except Exception as exc:  # pragma: no cover - CLI feedback
        print(f"capture failed: {exc}", file=sys.stderr)
        return 2
    print(
        f"Captured {summary.events} events across {len(summary.topics)} topic(s) "
        f"into {summary.path}"
    )
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    raise SystemExit(main())

