"""Entry point for running the live trading runtime."""

from __future__ import annotations

import os
import sys
from typing import Sequence

from .. import primary_entry_point


def _prepare_environment() -> None:
    """Ensure the runtime environment variables are set for live trading."""

    os.environ["NEW_RUNTIME"] = "1"
    os.environ["EVENT_DRIVEN"] = "1"
    # Force the TradingRuntime to boot with the modern pipeline so tests and
    # operators always exercise the new event-driven flow when using this CLI.
    os.environ["NEW_PIPELINE"] = "1"


def main(argv: Sequence[str] | None = None) -> int:
    """Launch the trading runtime using the modern event-driven pipeline."""

    _prepare_environment()
    return primary_entry_point.main(list(argv) if argv is not None else None)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
