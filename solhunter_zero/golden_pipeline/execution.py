"""Execution stages for the Golden Snapshot pipeline."""

from __future__ import annotations

from typing import Awaitable, Callable, Dict, Optional

from .types import Decision, GoldenSnapshot, LiveFill, VirtualFill, VirtualPnL
from .utils import clamp, now_ts


class ExecutionContext:
    """Stores the latest Golden Snapshots for execution lookup."""

    def __init__(self) -> None:
        self._snapshots: Dict[str, GoldenSnapshot] = {}

    def record(self, snapshot: GoldenSnapshot) -> None:
        self._snapshots[snapshot.hash] = snapshot

    def get(self, snapshot_hash: str) -> Optional[GoldenSnapshot]:
        return self._snapshots.get(snapshot_hash)


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
            pnl_price_component = (price - mid) * base_qty * (-direction)
            pnl = pnl_price_component - fees
            pnl_event = VirtualPnL(
                order_id=decision.client_order_id,
                mint=decision.mint,
                snapshot_hash=decision.snapshot_hash,
                realized_usd=pnl,
                unrealized_usd=0.0,
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
