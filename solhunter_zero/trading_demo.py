from __future__ import annotations

"""Simplified trading demo utilities.

This module exposes :func:`run_demo` which performs a tiny synthetic trading
loop.  It is intentionally lightweight so that unit tests can exercise the
trading pipeline without touching the network or requiring the full trading
stack.  Callers supply a sequence of prices and an output directory where the
ROI summary will be written.
"""

from pathlib import Path
import json
import asyncio
from types import SimpleNamespace
from typing import Iterable, Sequence, Protocol, runtime_checkable

from .simple_memory import SimpleMemory


class SyncMemory(SimpleMemory):
    """Synchronous wrapper around :class:`SimpleMemory`.

    The real :class:`~solhunter_zero.memory.Memory` exposes an asynchronous
    interface.  For small demos and tests it is convenient to interact with the
    memory synchronously.  ``SyncMemory`` exposes blocking ``log_trade`` and
    ``list_trades`` methods which internally delegate to the asynchronous
    implementation using :func:`asyncio.run`.
    """

    def log_trade(self, **kwargs):  # type: ignore[override]
        return asyncio.run(super().log_trade(**kwargs))

    def list_trades(self, *args, **kwargs):  # type: ignore[override]
        trades = asyncio.run(super().list_trades(*args, **kwargs))
        return [SimpleNamespace(**t) for t in trades]


@runtime_checkable
class MemoryLike(Protocol):
    """Protocol describing the minimal memory interface used by ``run_demo``."""

    def log_trade(self, **kwargs): ...  # pragma: no cover - structural typing
    def list_trades(self, *args, **kwargs): ...  # pragma: no cover - structural typing


def run_demo(
    prices: Sequence[float] | Iterable[float],
    reports_dir: str | Path,
    *,
    token: str = "DEMO",
    memory: MemoryLike | None = None,
) -> dict[str, float]:
    """Execute a tiny trading sequence and write ROI reports.

    Parameters
    ----------
    prices:
        Sequence of prices used to simulate a buy followed by a sell.  Only the
        first and last prices are used which keeps the function deterministic
        while remaining trivial.
    reports_dir:
        Directory where the ROI summary will be written.  The file is named
        ``paper_roi.json`` to match the previous behaviour of ``paper.py``.
    token:
        Token symbol recorded for the synthetic trades.

    Returns
    -------
    dict
        Mapping of agent name to ROI.  The demo logs trades with the reason
        ``"demo"`` so the returned dictionary will typically contain a single
        ``{"demo": roi}`` entry.
    """

    prices = list(prices)
    if len(prices) < 2:
        raise ValueError("prices must contain at least two entries")

    reports_path = Path(reports_dir)
    reports_path.mkdir(parents=True, exist_ok=True)

    mem = memory or SyncMemory()
    first, last = float(prices[0]), float(prices[-1])
    mem.log_trade(token=token, direction="buy", amount=1.0, price=first, reason="demo")
    mem.log_trade(token=token, direction="sell", amount=1.0, price=last, reason="demo")
    trades = mem.list_trades(limit=1000)
    summary: dict[str, dict[str, float]] = {}
    for t in trades:
        name = getattr(t, "reason", "") or ""
        info = summary.setdefault(name, {"buy": 0.0, "sell": 0.0})
        if getattr(t, "direction", "") == "buy":
            info["buy"] += float(getattr(t, "amount", 0.0)) * float(getattr(t, "price", 0.0))
        elif getattr(t, "direction", "") == "sell":
            info["sell"] += float(getattr(t, "amount", 0.0)) * float(getattr(t, "price", 0.0))
    roi = {}
    for name, info in summary.items():
        spent = info.get("buy", 0.0)
        revenue = info.get("sell", 0.0)
        if spent > 0:
            roi[name] = (revenue - spent) / spent
    out_path = reports_path / "paper_roi.json"
    out_path.write_text(json.dumps(roi, indent=2))

    print("ROI by agent:", json.dumps(roi))
    print(f"Wrote paper trading report to {out_path}")

    return roi


__all__ = ["run_demo", "SyncMemory"]
