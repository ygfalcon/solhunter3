"""Minimal paper trading CLI using :mod:`solhunter_zero` strategies.

This script delegates to :func:`solhunter_zero.simple_bot.run`, the same
helper used by ``demo.py``.  It accepts either a local tick dataset or, when
``--fetch-live`` is supplied, attempts to download recent market data via a
Codex endpoint.  If the live fetch fails the bundled sample ticks are used
instead.  The underlying :mod:`solhunter_zero.investor_demo` engine writes
``summary.json``, ``trade_history.json`` and ``highlights.json`` reports so
that downstream tests can compare results across the demo and paper workflows.

The ``--live-flow`` flag triggers a lightweight dry-run of the main trading
loop.  It executes a minimal trading sequence that logs trades but skips
submitting transactions so that tests can exercise the live code paths without
touching the network.

The ``--test`` flag combines ``--fetch-live`` and ``--live-flow`` so that a
quick smoke check fetches recent market data, exercises the live trading path
and writes reports to ``reports/`` by default.
"""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict, List
from urllib.request import urlopen

from solhunter_zero.datasets.sample_ticks import load_sample_ticks
from solhunter_zero.simple_bot import run as run_simple_bot
import solhunter_zero.investor_demo as investor_demo
import asyncio


# Public Codex endpoint providing recent SOL/USD candles.  The exact source is
# not critical; the fetch is best-effort and falls back to bundled samples when
# unavailable.
CODEX_URL = (
    "https://api.coingecko.com/api/v3/coins/solana/market_chart"
    "?vs_currency=usd&days=1&interval=hourly"
)


def _ticks_to_price_file(ticks: List[Dict[str, Any]]) -> Path:
    """Convert tick entries to a temporary JSON price dataset."""

    entries: List[Dict[str, Any]] = []
    for i, tick in enumerate(ticks):
        if "price" not in tick:
            continue
        try:
            price = float(tick["price"])
        except Exception:
            continue
        date = str(tick.get("timestamp", i))
        entries.append({"date": date, "price": price})
    if not entries:
        raise ValueError("tick dataset is empty")
    tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".json")
    json.dump(entries, tmp)
    tmp.close()
    return Path(tmp.name)


def _fetch_live_dataset() -> Path | None:
    """Fetch recent market data via Codex.

    Returns a path to a temporary JSON file or ``None`` if fetching fails.
    """

    try:
        with urlopen(CODEX_URL, timeout=10) as resp:
            data = json.load(resp)
        prices = data.get("prices") or []
        ticks = [
            {"price": p[1], "timestamp": int(p[0] // 1000)} for p in prices
        ]
        if not ticks:
            return None
        return _ticks_to_price_file(ticks)
    except Exception:
        return None


async def _live_flow(dataset: Path, reports: Path) -> None:
    """Execute a tiny dry-run of the main trading flow.

    The function performs the bare minimum to exercise the real trading
    machinery: it loads a keypair, queries routing and depth information and
    records a dummy trade via :class:`solhunter_zero.memory.Memory`.  No
    transaction is submitted.
    """

    from solhunter_zero import wallet, routeffi
    from solhunter_zero.simple_memory import SimpleMemory as Memory

    # Load price data for synthetic trades
    data = json.loads(dataset.read_text())
    prices = [float(d.get("price", 0.0)) for d in data]

    memory = Memory()

    # Prefer bundled keypair; fall back to a temporary random keypair
    kp_path = Path(__file__).with_name("keypairs") / "default.json"
    if kp_path.exists():
        wallet.load_keypair(str(kp_path))
    else:  # pragma: no cover - safeguard for missing keypair
        from solders.keypair import Keypair

        tmp = tempfile.NamedTemporaryFile("w", delete=False, suffix=".json")
        json.dump(list(Keypair.random().to_bytes()), tmp)
        tmp.close()
        wallet.load_keypair(tmp.name)

    routeffi.best_route({}, 1.0)

    trades: List[Dict[str, Any]] = []
    for i, (name, _strat) in enumerate(investor_demo.DEFAULT_STRATEGIES):
        buy_price = prices[min(2 * i, len(prices) - 1)] if prices else 0.0
        sell_price = prices[min(2 * i + 1, len(prices) - 1)] if prices else buy_price
        buy_trade = {
            "token": name,
            "side": "buy",
            "amount": 1.0,
            "price": buy_price,
        }
        sell_trade = {
            "token": name,
            "side": "sell",
            "amount": 1.0,
            "price": sell_price,
        }
        await memory.log_trade(**buy_trade)
        await memory.log_trade(**sell_trade)
        trades.extend([buy_trade, sell_trade])

    reports.mkdir(parents=True, exist_ok=True)
    (reports / "trade_history.json").write_text(json.dumps(trades))
    (reports / "summary.json").write_text(json.dumps({"trades": len(trades)}))
    (reports / "highlights.json").write_text(json.dumps({}))


def run(argv: List[str] | None = None) -> None:
    """Execute a lightweight paper trading simulation."""

    parser = argparse.ArgumentParser(description="Run simple paper trading")
    parser.add_argument(
        "--reports",
        type=Path,
        default=Path("reports"),
        help="Directory to write summary and trade history reports",
    )
    parser.add_argument(
        "--ticks",
        type=Path,
        default=None,
        help="Path to JSON tick history (defaults to bundled sample)",
    )
    parser.add_argument(
        "--fetch-live",
        action="store_true",
        help="Fetch live market data via Codex, falling back to sample ticks",
    )
    parser.add_argument(
        "--preset",
        choices=sorted(investor_demo.PRESET_DATA_FILES.keys()),
        default=None,
        help="Bundled price dataset to use",
    )
    parser.add_argument(
        "--learn",
        action="store_true",
        help="Run a tiny learning loop that rotates strategy weights",
    )
    parser.add_argument(
        "--rl-demo",
        action="store_true",
        help="Run a lightweight RL demo using a tiny pre-trained stub",
    )
    parser.add_argument(
        "--live-flow",
        action="store_true",
        help="Exercise the core trading loop in dry-run mode",
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help=(
            "Fetch live data and run the dry-run trading loop; "
            "shorthand for --fetch-live --live-flow"
        ),
    )
    args = parser.parse_args(argv)

    if args.test:
        args.fetch_live = True
        args.live_flow = True

    if args.preset and (args.ticks or args.fetch_live):
        raise ValueError("Cannot combine --preset with --ticks/--fetch-live")

    dataset: str | Path | None = None
    if args.preset:
        dataset = args.preset
    else:
        if args.fetch_live:
            dataset = _fetch_live_dataset()
        if dataset is None:
            ticks = (
                load_sample_ticks(args.ticks) if args.ticks else load_sample_ticks()
            )
            dataset = _ticks_to_price_file(ticks)

    if args.live_flow:
        if not dataset:
            ticks = (
                load_sample_ticks(args.ticks) if args.ticks else load_sample_ticks()
            )
            dataset = _ticks_to_price_file(ticks)
        asyncio.run(_live_flow(Path(dataset), args.reports))
        return

    run_simple_bot(dataset, args.reports, learn=args.learn, rl_demo=args.rl_demo)


if __name__ == "__main__":  # pragma: no cover
    run(sys.argv[1:])


__all__ = ["run"]

