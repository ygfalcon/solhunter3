"""Module entry point to launch the new trading runtime."""

from __future__ import annotations

import argparse
import logging
import os
import sys

from .trading_runtime import TradingRuntime


log = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Launch SolHunter runtime")
    parser.add_argument("--config", help="Path to configuration file", default=None)
    parser.add_argument("--ui-host", help="UI bind host", default="127.0.0.1")
    parser.add_argument(
        "--ui-port",
        help="UI bind port",
        type=int,
        default=int(os.getenv("UI_PORT", "5001")),
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


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runtime = TradingRuntime(
        config_path=args.config,
        ui_host=args.ui_host,
        ui_port=args.ui_port,
        loop_delay=args.loop_delay,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    runtime.run_forever()
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry
    raise SystemExit(main(sys.argv[1:]))
