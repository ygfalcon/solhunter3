"""Execution stages for the Golden Snapshot pipeline."""

from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional

from .types import Decision, GoldenSnapshot, LiveFill, VirtualFill, VirtualPnL
from .utils import clamp, now_ts


@dataclass
class _Position:
    qty: float = 0.0
    avg_price: float = 0.0

    def apply_trade(
        self,
        *,
        direction: int,
        qty: float,
        price: float,
        fees: float = 0.0,
    ) -> float:
        """Update position for a trade and return realised PnL."""

        if qty <= 0:
            return 0.0
        signed_trade = qty if direction > 0 else -qty
        realized = 0.0
        if self.qty == 0 or self.qty * signed_trade > 0:
            total_qty = abs(self.qty) + qty
            numerator = self.avg_price * abs(self.qty) + price * qty
            numerator += fees if direction > 0 else -fees
            self.avg_price = numerator / total_qty if total_qty else 0.0
            self.qty += signed_trade
            return realized

        closing_qty = min(abs(self.qty), qty)
        realized += (price - self.avg_price) * closing_qty * (
            1 if self.qty > 0 else -1
        )
        residual = qty - closing_qty
        self.qty += signed_trade
        if self.qty == 0:
            self.avg_price = 0.0
        elif residual > 0:
            self.avg_price = price
        return realized

    def mark_to_market(self, mid: float, spread_bps: float) -> float:
        if self.qty == 0:
            return 0.0
        half_spread = mid * (spread_bps / 20000.0)
        mark = mid - half_spread if self.qty > 0 else mid + half_spread
        return (mark - self.avg_price) * self.qty


class ExecutionContext:
    """Stores the latest Golden Snapshots for execution lookup."""

    def __init__(
        self,
        *,
        max_entries: int = 256,
        ttl_seconds: float | None = 600.0,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("ExecutionContext max_entries must be positive")
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._snapshots: OrderedDict[str, tuple[float, GoldenSnapshot]] = OrderedDict()

    def record(self, snapshot: GoldenSnapshot) -> None:
        now = now_ts()
        self._snapshots[snapshot.hash] = (now, snapshot)
        self._snapshots.move_to_end(snapshot.hash)
        self._prune(now)

    def get(self, snapshot_hash: str) -> Optional[GoldenSnapshot]:
        self._prune(now_ts())
        entry = self._snapshots.get(snapshot_hash)
        if not entry:
            return None
        _, snapshot = entry
        return snapshot

    def _prune(self, now: float) -> None:
        if self._ttl_seconds is not None:
            cutoff = now - self._ttl_seconds
            while self._snapshots:
                oldest_hash, (ts, _) = next(iter(self._snapshots.items()))
                if ts >= cutoff:
                    break
                self._snapshots.popitem(last=False)
        while len(self._snapshots) > self._max_entries:
            self._snapshots.popitem(last=False)


class ShadowExecutor:
    """Simulate fills from Decisions using depth curves."""

    def __init__(
        self,
        emit_fill: Callable[[VirtualFill], Awaitable[None]],
        emit_pnl: Callable[[VirtualPnL], Awaitable[None]] | None = None,
        *,
        latency_bps: float = 2.0,
        fee_bps: float = 4.0,
    ) -> None:
        self._emit_fill = emit_fill
        self._emit_pnl = emit_pnl
        self._latency_bps = latency_bps
        self._fee_bps = fee_bps
        self._positions: Dict[str, _Position] = {}

    async def submit(self, decision: Decision, snapshot: GoldenSnapshot) -> None:
        depth1 = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0)) or 1.0
        mid = float(snapshot.px.get("mid_usd", 0.0)) or 1.0
        size_fraction = clamp(decision.notional_usd / max(depth1, 1e-9), 0.0, 1.0)
        impact_bps = size_fraction * 50.0  # crude impact model
        base_qty = decision.notional_usd / mid
        direction = 1 if decision.side.lower() == "buy" else -1
        total_slippage_bps = (impact_bps + self._latency_bps + self._fee_bps) * direction
        price = mid * (1 + total_slippage_bps / 10_000)
        fees = decision.notional_usd * (self._fee_bps / 10_000)
        position = self._positions.setdefault(decision.mint, _Position())
        inventory_realized = position.apply_trade(
            direction=direction,
            qty=base_qty,
            price=price,
            fees=fees,
        )
        trade_edge = (mid - price) * base_qty * direction - fees
        realized = inventory_realized + trade_edge
        fill = VirtualFill(
            order_id=decision.client_order_id,
            mint=decision.mint,
            side=decision.side,
            qty_base=base_qty,
            price_usd=price,
            fees_usd=fees,
            slippage_bps=abs(total_slippage_bps),
            snapshot_hash=decision.snapshot_hash,
            route="VIRTUAL",
            ts=now_ts(),
        )
        await self._emit_fill(fill)
        if self._emit_pnl:
            mark = position.mark_to_market(mid, float(snapshot.px.get("spread_bps", 0.0)))
            pnl = realized + mark
            pnl_event = VirtualPnL(
                order_id=decision.client_order_id,
                mint=decision.mint,
                snapshot_hash=decision.snapshot_hash,
                realized_usd=realized,
                unrealized_usd=mark,
                ts=fill.ts,
            )
            await self._emit_pnl(pnl_event)


class LiveExecutor:
    """Placeholder for real trade execution; emits LiveFill events."""

    def __init__(self, emit: Callable[[LiveFill], Awaitable[None]] | None = None) -> None:
        self._emit = emit

    async def submit(self, decision: Decision, snapshot: GoldenSnapshot) -> None:
        if not self._emit:
            return
        depth1 = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0)) or 1.0
        mid = float(snapshot.px.get("mid_usd", 0.0)) or 1.0
        qty = decision.notional_usd / mid
        fill = LiveFill(
            sig=decision.client_order_id,
            mint=decision.mint,
            side=decision.side,
            qty_base=qty,
            price_usd=mid,
            fees_usd=decision.notional_usd * 0.0005,
            slippage_bps=0.0,
            route="LIVE",
            snapshot_hash=decision.snapshot_hash,
        )
        await self._emit(fill)
