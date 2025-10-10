"""Exit management primitives for post-entry monitoring and liquidation."""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from .golden_pipeline.types import GoldenSnapshot

__all__ = [
    "MAX_SPREAD_BPS_EXIT",
    "MIN_DEPTH1PCT_USD_EXIT",
    "DEPTH_FRACTION_CAP",
    "PER_MINT_COOLDOWN_SEC",
    "compute_breakeven_bps",
    "PostEntryMonitor",
    "ExitSuggestion",
    "StrategySignals",
    "ExitManager",
    "ExitAccountant",
]

MAX_SPREAD_BPS_EXIT = 120.0
MIN_DEPTH1PCT_USD_EXIT = 4_000.0
DEPTH_FRACTION_CAP = 0.30
PER_MINT_COOLDOWN_SEC = 180.0


def compute_breakeven_bps(
    *, fees_bps: float, expected_latency_bps: float, impact_bps: float
) -> float:
    """Return the total breakeven level incorporating fees and costs."""

    components = [fees_bps, expected_latency_bps, impact_bps]
    return float(sum(max(0.0, comp) for comp in components))


@dataclass(slots=True)
class MicroSignal:
    ts: float
    price: float
    delta_bps: float
    liquidity_1pct: float
    liquidity_5pct: float
    spread_bps: float


class PostEntryMonitor:
    """Poll Golden snapshots after a fill to surface micro structure changes."""

    def __init__(
        self,
        fetch_snapshot: Callable[[str], Awaitable[Optional[GoldenSnapshot]]],
        *,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        schedule: Sequence[float] | None = None,
        hot_duration: float = 60.0,
    ) -> None:
        self._fetch = fetch_snapshot
        self._sleep = sleep or asyncio.sleep
        if schedule is None:
            schedule = tuple([1.0] * 30 + [5.0] * 54)
        self._schedule = tuple(max(0.1, float(step)) for step in schedule)
        self._hot_duration = float(hot_duration)
        self._tasks: Dict[str, asyncio.Task[None]] = {}
        self._micro: Dict[str, List[MicroSignal]] = {}
        self._hot_until: Dict[str, float] = {}

    async def watch_fill(
        self,
        mint: str,
        *,
        entry_price: float,
        on_update: Callable[[MicroSignal], None] | None = None,
        max_samples: int | None = None,
    ) -> None:
        """Start monitoring *mint* and emit ``MicroSignal`` snapshots."""

        mint_key = str(mint)
        self._hot_until[mint_key] = time.time() + self._hot_duration
        await self._cancel(mint_key)
        task = asyncio.create_task(
            self._run_monitor(
                mint_key,
                entry_price=float(entry_price),
                on_update=on_update,
                max_samples=max_samples,
            )
        )
        self._tasks[mint_key] = task
        await task

    async def _cancel(self, mint: str) -> None:
        existing = self._tasks.pop(mint, None)
        if existing is None:
            return
        existing.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await existing

    async def _run_monitor(
        self,
        mint: str,
        *,
        entry_price: float,
        on_update: Callable[[MicroSignal], None] | None,
        max_samples: int | None,
    ) -> None:
        high = entry_price
        count = 0
        for step in self._schedule:
            if max_samples is not None and count >= max_samples:
                break
            await self._sleep(step)
            snapshot = await self._fetch(mint)
            if snapshot is None:
                continue
            price = float(snapshot.px.get("mid_usd", entry_price) or entry_price)
            high = max(high, price)
            delta_bps = 0.0
            if entry_price > 0:
                delta_bps = (price - entry_price) / entry_price * 10_000
            liq = snapshot.liq.get("depth_pct", {}) if snapshot.liq else {}
            signal = MicroSignal(
                ts=time.time(),
                price=price,
                delta_bps=delta_bps,
                liquidity_1pct=float(liq.get("1", 0.0) or 0.0),
                liquidity_5pct=float(liq.get("5", 0.0) or 0.0),
                spread_bps=float(snapshot.px.get("spread_bps", 0.0) or 0.0),
            )
            self._micro.setdefault(mint, []).append(signal)
            if on_update:
                on_update(signal)
            count += 1
        self._hot_until.pop(mint, None)

    def hot_tokens(self) -> List[str]:
        now = time.time()
        return [mint for mint, expiry in self._hot_until.items() if expiry > now]

    def micro_chart(self, mint: str, *, horizon: float = 60.0) -> List[Tuple[float, float]]:
        cutoff = time.time() - max(0.0, horizon)
        series = self._micro.get(mint, [])
        return [(signal.ts, signal.price) for signal in series if signal.ts >= cutoff]


@dataclass(slots=True)
class StrategySignals:
    momentum_flip: bool = False
    opportunity_cost: bool = False
    cross_dex_rebalance: bool = False
    portfolio_drawdown: bool = False
    global_brake: bool = False


@dataclass(slots=True)
class ExitSuggestion:
    mint: str
    qty: float
    notional: float
    price: float
    reason: str
    must: bool
    max_slippage_bps: float
    tags: Tuple[str, ...] = ()
    trail_bps: float | None = None


class ExitManager:
    """Evaluate exit rails and apply execution gates."""

    def __init__(self) -> None:
        self._last_high: Dict[str, float] = {}
        self._armed: Dict[str, bool] = {}
        self._last_high_ts: Dict[str, float] = {}
        self._cooldown: Dict[str, float] = {}
        self._missed: List[Dict[str, object]] = []

    def evaluate(
        self,
        *,
        token: str,
        qty: float,
        entry_price: float,
        breakeven_bps: float,
        price: float,
        spread_bps: float,
        depth1pct_usd: float,
        signals: StrategySignals,
        now: float | None = None,
        max_slippage_bps: float = 80.0,
    ) -> Optional[ExitSuggestion]:
        now = now or time.time()
        qty = max(0.0, qty)
        if qty <= 0.0:
            return None
        mint = token
        self._last_high[mint] = max(self._last_high.get(mint, entry_price), price)
        reason: Optional[str] = None
        must = False
        drop_bps = 0.0
        if entry_price > 0:
            drop_bps = (price - entry_price) / entry_price * 10_000
        spread_blowout = (
            spread_bps >= MAX_SPREAD_BPS_EXIT
            and depth1pct_usd <= MIN_DEPTH1PCT_USD_EXIT
        )
        if spread_blowout:
            reason = "spread_blowout"
            must = True
        elif drop_bps <= -1.2 * breakeven_bps:
            reason = "fast_adverse_move"
            must = True
        elif signals.global_brake or signals.portfolio_drawdown:
            reason = "risk_brake"
            must = True
        if not reason and signals.momentum_flip:
            reason = "momentum_flip"
        if not reason and signals.opportunity_cost:
            reason = "opportunity_cost"
        if not reason and signals.cross_dex_rebalance:
            reason = "rebalance"
        trail_reason = self._check_trailing(
            token=mint,
            entry_price=entry_price,
            price=price,
            breakeven_bps=breakeven_bps,
            now=now,
        )
        if trail_reason and not reason:
            reason = trail_reason
        elif trail_reason and reason and not must:
            reason = reason
        if reason is None:
            return None
        notional = qty * price
        if not must and not self._passes_gates(
            mint,
            now,
            notional=notional,
            spread_bps=spread_bps,
            depth1pct_usd=depth1pct_usd,
        ):
            self._missed.append(
                {
                    "mint": mint,
                    "reason": reason,
                    "ts": now,
                    "spread_bps": spread_bps,
                    "depth1pct_usd": depth1pct_usd,
                    "must": must,
                }
            )
            return None
        if must:
            self._cooldown.pop(mint, None)
        else:
            self._cooldown[mint] = now
        trail_bps = None
        if trail_reason in {"trailing_stop", "time_stop"}:
            if entry_price > 0:
                trail_bps = (price - entry_price) / entry_price * 10_000
        return ExitSuggestion(
            mint=mint,
            qty=qty,
            notional=notional,
            price=price,
            reason=reason,
            must=must,
            max_slippage_bps=max_slippage_bps,
            tags=self._tags_for_reason(reason, must),
            trail_bps=trail_bps,
        )

    def _check_trailing(
        self,
        *,
        token: str,
        entry_price: float,
        price: float,
        breakeven_bps: float,
        now: float,
    ) -> Optional[str]:
        high = self._last_high.setdefault(token, entry_price)
        if price > high:
            self._last_high[token] = price
            self._last_high_ts[token] = now
        armed = self._armed.get(token, False)
        delta_bps = 0.0
        if entry_price > 0:
            delta_bps = (high - entry_price) / entry_price * 10_000
        if not armed and delta_bps >= 2.5 * breakeven_bps:
            self._armed[token] = True
            self._last_high_ts[token] = now
            return None
        if not self._armed.get(token, False):
            return None
        retrace_bps = 0.0
        if high > 0:
            retrace_bps = (high - price) / high * 10_000
        if retrace_bps >= 0.8 * breakeven_bps:
            return "trailing_stop"
        high_ts = self._last_high_ts.get(token, now)
        if now - high_ts >= 300.0:
            return "time_stop"
        return None

    def _passes_gates(
        self,
        mint: str,
        now: float,
        *,
        notional: float,
        spread_bps: float,
        depth1pct_usd: float,
    ) -> bool:
        if spread_bps >= MAX_SPREAD_BPS_EXIT:
            return False
        if depth1pct_usd < MIN_DEPTH1PCT_USD_EXIT:
            return False
        if depth1pct_usd <= 0:
            return False
        if notional > depth1pct_usd * DEPTH_FRACTION_CAP:
            return False
        last = self._cooldown.get(mint)
        if last and now - last < PER_MINT_COOLDOWN_SEC:
            return False
        return True

    def register_exit(self, mint: str, *, ts: float | None = None) -> None:
        ts = ts or time.time()
        self._cooldown[mint] = ts
        self._armed[mint] = False
        self._last_high_ts[mint] = ts

    def missed(self) -> List[Dict[str, object]]:
        return list(self._missed)

    @staticmethod
    def _tags_for_reason(reason: str, must: bool) -> Tuple[str, ...]:
        tags: List[str] = [reason]
        if must:
            tags.append("must_exit")
        return tuple(tags)


@dataclass(slots=True)
class SliceLog:
    mint: str
    qty: float
    price: float
    reason: str
    must: bool
    realized_usd: float
    remaining_qty: float
    ts: float
    agents: Tuple[str, ...] = ()


class ExitAccountant:
    """Track PnL attribution for exit slices."""

    def __init__(self) -> None:
        self._logs: List[SliceLog] = []

    def record_slice(
        self,
        position,
        *,
        qty: float,
        price: float,
        reason: str,
        must: bool,
        agents: Iterable[str] | None = None,
        ts: float | None = None,
    ) -> SliceLog:
        from .portfolio import Position

        if not isinstance(position, Position):
            raise TypeError("position must be a Portfolio Position")
        qty = min(max(qty, 0.0), position.amount)
        ts = ts or time.time()
        realized = qty * (price - position.entry_price)
        position.amount -= qty
        position.realized_pnl_usd += realized
        position.unrealized_pnl_usd = (position.amount * (price - position.entry_price))
        for agent in agents or []:
            key = str(agent)
            position.attribution[key] = position.attribution.get(key, 0.0) + realized
        log = SliceLog(
            mint=position.token,
            qty=qty,
            price=price,
            reason=reason,
            must=must,
            realized_usd=realized,
            remaining_qty=position.amount,
            ts=ts,
            agents=tuple(str(a) for a in agents or ()),
        )
        self._logs.append(log)
        return log

    def logs(self) -> Sequence[SliceLog]:
        return tuple(self._logs)
