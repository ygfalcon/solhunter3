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


def main(argv: Sequence[str] | None = None) -> int:
    """Launch the trading runtime using the modern event-driven pipeline."""

    _prepare_environment()
    parser = primary_entry_point.build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    primary_entry_point.run_from_args(args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
