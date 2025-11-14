"""Module entry point to launch the new trading runtime."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from collections.abc import Mapping

from ..runtime_defaults import DEFAULT_UI_PORT
from .. import ui
from .trading_runtime import TradingRuntime


log = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch SolHunter runtime")
    parser.add_argument("--config", help="Path to configuration file", default=None)
    parser.add_argument("--ui-host", help="UI bind host", default="127.0.0.1")
    parser.add_argument(
        "--ui-port",
        help="UI bind port",
        default=os.getenv("UI_PORT", str(DEFAULT_UI_PORT)),
    )
    parser.add_argument(
        "--loop-delay",
        type=float,
        default=None,
        help="Override trading loop delay in seconds",
    )
    parser.add_argument(
        "--min-delay",
        type=float,
        default=None,
        help="Minimum sleep between iterations",
    )
    parser.add_argument(
        "--max-delay",
        type=float,
        default=None,
        help="Maximum sleep between iterations",
    )
    return parser.parse_args(argv)


def _announce_websocket_channels(threads: Mapping[str, object] | None = None) -> None:
    """Print a summary of websocket channels that are currently available."""

    summary_channels: list[str] = []
    try:
        urls = ui.get_ws_urls()
    except Exception as exc:  # pragma: no cover - defensive logging
        log.warning("Failed to resolve UI websocket URLs: %s", exc)
        urls = {}

    preferred_order = ("events", "rl", "logs")
    for name in preferred_order:
        url = urls.get(name)
        if isinstance(url, str) and url:
            summary_channels.append(f"{name}={url}")
        elif threads and name in threads:
            summary_channels.append(f"{name}=bound")

    for name, url in sorted(urls.items()):
        if name in preferred_order:
            continue
        if isinstance(url, str) and url:
            summary_channels.append(f"{name}={url}")

    if summary_channels:
        print("UI websocket channels: " + ", ".join(summary_channels), flush=True)
    else:
        print("UI websocket channels: none", flush=True)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    threads = ui.start_websockets()
    _announce_websocket_channels(threads)
    try:
        runtime = TradingRuntime(
            config_path=args.config,
            ui_host=args.ui_host,
            ui_port=int(args.ui_port),
            loop_delay=args.loop_delay,
            min_delay=args.min_delay,
            max_delay=args.max_delay,
        )
        runtime.run_forever()
    finally:
        ui.stop_websockets()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main(sys.argv[1:]))
