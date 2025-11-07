#!/usr/bin/env python3
"""Primary entry point for the SolHunter trading runtime."""

from __future__ import annotations

import argparse
import logging
import sys

from .runtime.trading_runtime import TradingRuntime


log = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the SolHunter trading runtime")
    parser.add_argument("--config", required=True, help="Path to configuration file")
    parser.add_argument("--ui-host", default="127.0.0.1", help="UI bind host")
    parser.add_argument("--ui-port", default="5000", help="UI bind port")
    parser.add_argument("--loop-delay", type=float, default=None, help="Override loop delay (seconds)")
    parser.add_argument("--min-delay", type=float, default=None, help="Minimum sleep between iterations")
    parser.add_argument("--max-delay", type=float, default=None, help="Maximum sleep between iterations")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    return build_parser().parse_args(argv)


def run_from_args(args: argparse.Namespace) -> None:
    runtime = TradingRuntime(
        config_path=args.config,
        ui_host=args.ui_host,
        ui_port=int(args.ui_port),
        loop_delay=args.loop_delay,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
    )
    runtime.run_forever()


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    run_from_args(args)


if __name__ == "__main__":
    main(sys.argv[1:])
