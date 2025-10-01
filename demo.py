"""Run the investor demo using the shared simple bot runner."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from solhunter_zero.simple_bot import run as run_simple_bot
import solhunter_zero.investor_demo as investor_demo


def run(argv: list[str] | None = None) -> None:
    """Execute the investor demo.

    ``demo.py`` now mirrors :mod:`paper` by delegating to the shared
    :func:`solhunter_zero.simple_bot.run` helper.  The script accepts an
    optional preset name or URL to supply price data.  When no dataset source is
    provided the demo falls back to the short built-in dataset.  Reports are
    written to ``summary.json``, ``trade_history.json`` and ``highlights.json`` in
    the supplied ``--reports`` directory so tests can validate a common output
    schema across both entry points.
    """

    parser = argparse.ArgumentParser(description="Run the investor demo")
    parser.add_argument(
        "--reports",
        type=Path,
        default=Path("reports"),
        help="Directory where demo reports will be written",
    )
    parser.add_argument(
        "--preset",
        choices=sorted(investor_demo.PRESET_DATA_FILES.keys()),
        default=None,
        help="Bundled price dataset to use",
    )
    parser.add_argument(
        "--url",
        type=str,
        default=None,
        help="HTTP URL returning JSON price data; overrides --preset",
    )
    args = parser.parse_args(argv)

    if args.url and args.preset:
        raise ValueError("Provide only one of --url or --preset")

    dataset = args.url or args.preset
    run_simple_bot(dataset, args.reports)


if __name__ == "__main__":  # pragma: no cover
    run(sys.argv[1:])

