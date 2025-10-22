"""Market data aggregation for Golden Snapshots."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, Optional

from .types import OHLCVBar, TapeEvent
from .utils import now_ts

log = logging.getLogger(__name__)

_WINDOW_SEC = 300


@dataclass
class _BarState:
    mint: str
    window_start: float
    window_end: float
    open: float
    high: float
    low: float
    close: float
    vol_usd: float = 0.0
    vol_base: float = 0.0
    trades: int = 0
    buyers: set[str] = field(default_factory=set)
    flow_usd: float = 0.0

    def apply(self, event: TapeEvent) -> None:
        price = event.price_usd
        if self.trades == 0:
            self.open = price
            self.high = price
            self.low = price
        else:
            self.high = max(self.high, price)
            self.low = min(self.low, price)
        self.close = price
        self.vol_usd += abs(event.amount_quote)
        self.vol_base += abs(event.amount_base)
        self.trades += 1
        if event.buyer:
            self.buyers.add(event.buyer)
        self.flow_usd += float(event.amount_base) * price

    def to_bar(self) -> OHLCVBar:
        buyers = len(self.buyers)
        return OHLCVBar(
            mint=self.mint,
            open=self.open,
            high=self.high,
            low=self.low,
            close=self.close,
            vol_usd=self.vol_usd,
            vol_base=self.vol_base,
            trades=self.trades,
            buyers=buyers,
            flow_usd=self.flow_usd,
            zret=0.0,
            zvol=0.0,
            asof_close=self.window_end,
        )


class MarketDataStage:
    """Roll 5-minute OHLCV bars from tape events."""

    def __init__(self, emit: Callable[[OHLCVBar], Awaitable[None]]) -> None:
        self._emit = emit
        self._bars: Dict[str, _BarState] = {}
        self._lock = asyncio.Lock()

    async def submit(self, event: TapeEvent) -> None:
        if event.is_self:
            return
        window_start = (event.ts // _WINDOW_SEC) * _WINDOW_SEC
        window_end = window_start + _WINDOW_SEC
        bars_to_emit: list[OHLCVBar] = []
        async with self._lock:
            current = self._bars.get(event.mint_base)
            if current and event.ts >= current.window_end:
                # Close existing bar before starting new window.
                bars_to_emit.append(current.to_bar())
                current = None
            if not current:
                current = _BarState(
                    mint=event.mint_base,
                    window_start=window_start,
                    window_end=window_end,
                    open=event.price_usd,
                    high=event.price_usd,
                    low=event.price_usd,
                    close=event.price_usd,
                )
                self._bars[event.mint_base] = current
            current.apply(event)
        for bar in bars_to_emit:
            await self._emit(bar)

    async def flush(self, now: Optional[float] = None) -> None:
        now = now or now_ts()
        bars_to_emit: list[OHLCVBar] = []
        async with self._lock:
            closed = [
                mint for mint, state in self._bars.items() if now >= state.window_end + 1
            ]
            for mint in closed:
                state = self._bars.pop(mint)
                if state.trades == 0:
                    continue
                bars_to_emit.append(state.to_bar())
        for bar in bars_to_emit:
            await self._emit(bar)
