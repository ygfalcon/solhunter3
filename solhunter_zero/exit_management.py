"""Utilities for managing exit behaviour, monitoring, and diagnostics."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence

from .golden_pipeline.types import GoldenSnapshot, TradeSuggestion

# Exit gating constants -------------------------------------------------------
MAX_SPREAD_BPS_EXIT = 120.0
MIN_DEPTH1PCT_USD_EXIT = 4_000.0
DEPTH_FRACTION_CAP = 0.30
PER_MINT_COOLDOWN_SEC = 180.0
PORTFOLIO_DRAWDOWN_TRIGGER = 0.03


@dataclass(slots=True)
class PostEntryEvent:
    """Recorded observation captured after a position entry."""

    ts: float
    price: float
    label: str
    payload: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExitSlice:
    """Represents a partial exit of an open position."""

    ts: float
    amount: float
    price: float
    reason: str
    pnl: float
    gating_passed: bool


class PostEntryMonitor:
    """Schedule observations after entries at sub-minute cadence."""

    first_phase_sec: float = 30.0
    first_phase_cadence: float = 1.0
    total_window_sec: float = 120.0
    late_cadence: float = 5.0

    def __init__(self, entry_ts: float) -> None:
        self.entry_ts = float(entry_ts)
        self._pointer = self.first_phase_cadence
        self._micro_signals: List[Dict[str, Any]] = []
        self._last_observation_ts = self.entry_ts

    def next_due(self) -> float:
        """Return the next due observation offset in seconds."""

        due = self._pointer
        self._pointer = self._advance_after(due)
        return round(due, 6)

    def _advance_after(self, offset: float) -> float:
        if offset < self.first_phase_sec:
            nxt = offset + self.first_phase_cadence
            if nxt > self.first_phase_sec:
                nxt = self.first_phase_sec + self.late_cadence
        else:
            nxt = min(offset + self.late_cadence, self.total_window_sec)
        if nxt <= offset:
            nxt = self.total_window_sec
        return round(nxt, 6)

    def advance_to(self, elapsed: float) -> None:
        """Fast-forward the schedule to *elapsed* seconds."""

        elapsed = max(0.0, float(elapsed))
        while self._pointer < min(elapsed, self.total_window_sec):
            self._pointer = self._advance_after(self._pointer)

    def observe(self, *, snapshot: GoldenSnapshot, entry_price: float) -> Dict[str, Any]:
        """Record micro-signals extracted from ``snapshot``."""

        price = float(snapshot.px.get("mid_usd", entry_price))
        spread = float(snapshot.px.get("spread_bps", 0.0))
        depth = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0))
        delta_bps = 0.0
        if entry_price:
            delta_bps = (price - entry_price) / entry_price * 10_000.0
        micro = {
            "ts": snapshot.asof,
            "mid_usd": price,
            "spread_bps": spread,
            "depth_1pct_usd": depth,
            "delta_mid_bps": delta_bps,
        }
        self._micro_signals.append(micro)
        self._last_observation_ts = snapshot.asof
        return micro

    @property
    def micro_signals(self) -> Sequence[Dict[str, Any]]:
        return tuple(self._micro_signals)


@dataclass
class _EntryState:
    token: str
    entry_price: float
    size: float
    initial_size: float
    breakeven_bps: float
    entered_at: float
    monitor: PostEntryMonitor
    high_price: float
    high_ts: float
    hot_watch: bool
    trailing_armed: bool = False
    queue_entry: Optional[Dict[str, Any]] = None
    slices: List[ExitSlice] = field(default_factory=list)
    diagnostics: Dict[str, Any] = field(default_factory=dict)
    cooldown_until: float = 0.0
    filled_qty: float = 0.0

    def update_high(self, price: float, ts: float) -> None:
        if price > self.high_price:
            self.high_price = price
            self.high_ts = ts

    def delta_bps(self, price: float) -> float:
        if self.entry_price == 0:
            return 0.0
        return (price - self.entry_price) / self.entry_price * 10_000.0

    def drawdown_bps(self, price: float) -> float:
        if self.high_price <= 0:
            return 0.0
        return (self.high_price - price) / self.high_price * 10_000.0


class ExitManager:
    """Stateful helper responsible for evaluating exit behaviour."""

    def __init__(self) -> None:
        self._entries: Dict[str, _EntryState] = {}
        self._closed: List[Dict[str, Any]] = []
        self._missed: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------
    def register_entry(
        self,
        token: str,
        *,
        entry_price: float,
        size: float,
        breakeven_bps: float,
        ts: Optional[float] = None,
        hot_watch: bool = False,
    ) -> None:
        """Register a newly opened position and reset diagnostics."""

        timestamp = time.time() if ts is None else float(ts)
        state = _EntryState(
            token=token,
            entry_price=float(entry_price),
            size=float(size),
            initial_size=abs(float(size)),
            breakeven_bps=max(0.0, float(breakeven_bps)),
            entered_at=timestamp,
            monitor=PostEntryMonitor(entry_ts=timestamp),
            high_price=float(entry_price),
            high_ts=timestamp,
            hot_watch=hot_watch,
        )
        state.diagnostics = {
            "registered_at": timestamp,
            "breakeven_bps": state.breakeven_bps,
            "size": state.size,
        }
        self._entries[token] = state

    # ------------------------------------------------------------------
    def evaluate_snapshot(
        self,
        snapshot: GoldenSnapshot,
        *,
        portfolio_drawdown_pct: float = 0.0,
        venue_anomaly: bool = False,
        global_brake: bool = False,
        strategy_reasons: Optional[Sequence[str]] = None,
    ) -> Optional[TradeSuggestion]:
        state = self._entries.get(snapshot.mint)
        if state is None:
            return None

        now = snapshot.asof or time.time()
        price = float(snapshot.px.get("mid_usd", state.entry_price))
        spread = float(snapshot.px.get("spread_bps", 0.0))
        depth = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0))
        if abs(state.size) > state.initial_size:
            state.initial_size = abs(state.size)
        state.monitor.observe(snapshot=snapshot, entry_price=state.entry_price)
        state.update_high(price, now)

        delta_bps = state.delta_bps(price)
        drawdown_bps = state.drawdown_bps(price)
        time_in_position = max(0.0, now - state.entered_at)
        diagnostics = {
            "delta_bps": delta_bps,
            "drawdown_bps": drawdown_bps,
            "spread_bps": spread,
            "depth_1pct_usd": depth,
            "rail": "hold",
            "timestamp": now,
            "hash": snapshot.hash,
            "breakeven_bps": state.breakeven_bps,
            "time_in_position_sec": time_in_position,
        }
        high_delta_bps = state.delta_bps(state.high_price)
        trail_line_bps = (
            high_delta_bps - 0.8 * state.breakeven_bps
            if state.trailing_armed
            else -0.8 * state.breakeven_bps
        )
        diagnostics["trail_line_bps"] = trail_line_bps
        diagnostics["entry_vwap_bps"] = 0.0

        must_exit = False
        rail_reason = None
        fast_adverse = False
        liquidity_gap = spread > MAX_SPREAD_BPS_EXIT and depth < MIN_DEPTH1PCT_USD_EXIT
        profit_trigger_bps = 2.5 * state.breakeven_bps
        diagnostics["profit_trigger_bps"] = profit_trigger_bps
        if delta_bps <= -1.2 * state.breakeven_bps:
            fast_adverse = True
            rail_reason = "price_drop"
        if liquidity_gap:
            fast_adverse = True
            rail_reason = rail_reason or "liquidity_gap"
        if venue_anomaly:
            fast_adverse = True
            rail_reason = rail_reason or "venue_anomaly"
        if global_brake or portfolio_drawdown_pct >= PORTFOLIO_DRAWDOWN_TRIGGER:
            fast_adverse = True
            rail_reason = rail_reason or "portfolio_brake"

        if fast_adverse:
            must_exit = True
            diagnostics["rail"] = "fast_adverse"
            diagnostics["fast_adverse_reason"] = rail_reason
        else:
            if delta_bps >= profit_trigger_bps:
                state.trailing_armed = True
            hold_limit = 120.0
            if time_in_position >= hold_limit:
                must_exit = True
                diagnostics["rail"] = "time_stop"
            elif not state.trailing_armed and time_in_position >= 90.0:
                must_exit = True
                diagnostics["rail"] = "time_stop"
            elif state.trailing_armed and drawdown_bps >= 0.8 * state.breakeven_bps:
                diagnostics["rail"] = "smart_trailing"
            elif now - state.high_ts >= hold_limit:
                must_exit = True
                diagnostics["rail"] = "time_stop"
            elif strategy_reasons:
                diagnostics["rail"] = "strategy"
                diagnostics["strategy_reasons"] = list(strategy_reasons)

        reason = diagnostics["rail"]
        if reason == "hold":
            state.queue_entry = None
            state.diagnostics = diagnostics
            return None

        request_notional = abs(state.size) * price
        gating_ok, gating_diag = self._check_gating(
            state,
            now,
            spread=spread,
            depth=depth,
            must_exit=must_exit,
            notional=request_notional,
        )
        diagnostics["gating"] = gating_diag
        diagnostics["must_exit"] = must_exit
        if not gating_ok:
            state.diagnostics = diagnostics
            return None

        suggestion = self._build_suggestion(
            state,
            price=price,
            notional=request_notional,
            must_exit=must_exit,
            diagnostics=diagnostics,
            gating=gating_diag,
        )
        state.queue_entry = {
            "token": state.token,
            "must": must_exit,
            "reason": diagnostics["rail"],
            "price": price,
            "notional_usd": request_notional,
            "ts": now,
            "gating": gating_diag,
        }
        state.diagnostics = diagnostics
        return suggestion

    # ------------------------------------------------------------------
    def _check_gating(
        self,
        state: _EntryState,
        ts: float,
        *,
        spread: float,
        depth: float,
        must_exit: bool,
        notional: float,
    ) -> tuple[bool, Dict[str, Any]]:
        diagnostics: Dict[str, Any] = {
            "requested_notional_usd": float(notional),
            "spread_bps": float(spread),
            "depth_1pct_usd": float(depth),
        }
        gating_fail = False
        if spread > MAX_SPREAD_BPS_EXIT:
            diagnostics["spread_gate"] = spread
            gating_fail = True
        if depth < MIN_DEPTH1PCT_USD_EXIT:
            diagnostics["depth_gate"] = depth
            gating_fail = True
        depth_capacity = depth * DEPTH_FRACTION_CAP
        if depth_capacity > 0:
            diagnostics["depth_fraction_cap_usd"] = depth_capacity
            if notional > depth_capacity:
                diagnostics["depth_fraction_gate"] = notional
                gating_fail = True
        if state.cooldown_until and ts < state.cooldown_until:
            diagnostics["cooldown_until"] = state.cooldown_until
            if not must_exit:
                gating_fail = True
            else:
                diagnostics["cooldown_override"] = True
        if must_exit:
            if gating_fail:
                diagnostics["must_exit_override"] = True
            return True, diagnostics
        return not gating_fail, diagnostics

    def _build_suggestion(
        self,
        state: _EntryState,
        *,
        price: float,
        notional: float,
        must_exit: bool,
        diagnostics: Dict[str, Any],
        gating: Dict[str, Any],
    ) -> TradeSuggestion:
        slippage_bps = max(state.breakeven_bps * 1.5, 20.0)
        suggestion = TradeSuggestion(
            agent="exit_manager",
            mint=state.token,
            side="sell",
            notional_usd=max(notional, 0.0),
            max_slippage_bps=slippage_bps,
            risk={},
            confidence=0.5 if must_exit else 0.2,
            inputs_hash=diagnostics.get("hash", ""),
            ttl_sec=1.0,
            generated_at=time.time(),
            gating=dict(gating),
            slices=[],
            must_exit=must_exit,
            hot_watch=state.hot_watch or must_exit,
            exit_diagnostics=dict(diagnostics),
        )
        state.cooldown_until = time.time() + PER_MINT_COOLDOWN_SEC
        return suggestion

    # ------------------------------------------------------------------
    def record_slice(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        ts: Optional[float] = None,
        reason: str = "exit",
        pnl: float = 0.0,
        gating_passed: bool = True,
    ) -> ExitSlice:
        state = self._entries.get(token)
        if state is None:
            raise KeyError(token)
        timestamp = time.time() if ts is None else float(ts)
        executed = max(-float(amount), 0.0)
        slice_info = ExitSlice(
            ts=timestamp,
            amount=float(amount),
            price=float(price),
            reason=reason,
            pnl=float(pnl),
            gating_passed=bool(gating_passed),
        )
        state.slices.append(slice_info)
        state.size = max(state.size + float(amount), 0.0)
        if state.initial_size <= 0 and state.size > 0:
            state.initial_size = state.size
        state.filled_qty = min(state.initial_size, state.filled_qty + executed)
        if state.queue_entry is not None:
            state.queue_entry["last_slice_ts"] = timestamp
        if state.size <= 1e-9:
            self._archive_state(state, closed_ts=timestamp)
        return slice_info

    def record_missed_exit(
        self,
        token: str,
        *,
        reason: str,
        diagnostics: Optional[Dict[str, Any]] = None,
        ts: Optional[float] = None,
    ) -> None:
        timestamp = time.time() if ts is None else float(ts)
        entry = {
            "token": token,
            "reason": reason,
            "diagnostics": dict(diagnostics or {}),
            "ts": timestamp,
        }
        self._missed.append(entry)
        if len(self._missed) > 50:
            self._missed = self._missed[-50:]

    # ------------------------------------------------------------------
    def summary(self) -> Dict[str, Any]:
        now = time.time()
        hot_watch: List[Dict[str, Any]] = []
        diagnostics: List[Dict[str, Any]] = []
        queue: List[Dict[str, Any]] = []
        for state in self._entries.values():
            recent_samples = [
                sample
                for sample in state.monitor.micro_signals
                if sample.get("ts") is None
                or (now - float(sample.get("ts")) <= 60.0)
            ]
            detail = {
                "token": state.token,
                "entry_price": state.entry_price,
                "size": state.size,
                "age_sec": now - state.entered_at,
                "diagnostics": dict(state.diagnostics),
                "monitor": list(recent_samples[-25:]) if recent_samples else [],
            }
            diagnostics.append(detail)
            if state.hot_watch or (state.queue_entry and state.queue_entry.get("must")):
                hot_watch.append(
                    {
                        "token": state.token,
                        "age_sec": now - state.entered_at,
                        "reason": (state.queue_entry or {}).get("reason", "monitor"),
                        "remaining": max(state.size, 0.0),
                        "progress": self._queue_progress(state),
                        "breakeven_bps": state.breakeven_bps,
                        "trail_status": "armed" if state.trailing_armed else "idle",
                        "window_remaining": max(
                            0.0,
                            getattr(state.monitor, "total_window_sec", 300.0)
                            - (now - state.entered_at),
                        ),
                    }
                )
            if state.queue_entry:
                snapshot = dict(state.queue_entry)
                snapshot.update(self._queue_snapshot(state, now))
                snapshot["post_fill_qty_preview"] = max(snapshot.get("remaining_qty", 0.0), 0.0)
                queue.append(snapshot)
        return {
            "hot_watch": hot_watch,
            "diagnostics": diagnostics,
            "queue": queue,
            "closed": list(self._closed),
            "missed_exits": list(self._missed),
        }

    # ------------------------------------------------------------------
    def _archive_state(self, state: _EntryState, *, closed_ts: float) -> None:
        snapshot = {
            "token": state.token,
            "entry_price": state.entry_price,
            "high_price": state.high_price,
            "initial_size": state.initial_size,
            "filled_qty": state.filled_qty,
            "slices": [
                {
                    "ts": sl.ts,
                    "amount": sl.amount,
                    "price": sl.price,
                    "reason": sl.reason,
                    "pnl": sl.pnl,
                    "gating_passed": sl.gating_passed,
                }
                for sl in state.slices
            ],
            "closed_ts": closed_ts,
            "diagnostics": dict(state.diagnostics),
        }
        self._closed.append(snapshot)
        if len(self._closed) > 50:
            self._closed = self._closed[-50:]
        self._entries.pop(state.token, None)

    def _queue_progress(self, state: _EntryState) -> float:
        total = state.initial_size if state.initial_size > 0 else abs(state.size)
        if total <= 0:
            return 0.0
        remaining = max(state.size, 0.0)
        filled = max(total - remaining, 0.0)
        return min(max(filled / total, 0.0), 1.0)

    def _queue_snapshot(self, state: _EntryState, now: float) -> Dict[str, Any]:
        total = state.initial_size if state.initial_size > 0 else abs(state.size)
        remaining = max(state.size, 0.0)
        filled = max(total - remaining, 0.0)
        progress = self._queue_progress(state)
        slice_reasons = sorted({sl.reason for sl in state.slices if sl.reason})
        return {
            "initial_qty": total,
            "filled_qty": filled,
            "remaining_qty": remaining,
            "progress": progress,
            "progress_pct": progress * 100.0,
            "slices_executed": len(state.slices),
            "slice_reasons": slice_reasons,
            "age_sec": now - state.entered_at,
        }


__all__ = [
    "ExitManager",
    "ExitSlice",
    "PostEntryEvent",
    "PostEntryMonitor",
    "MAX_SPREAD_BPS_EXIT",
    "MIN_DEPTH1PCT_USD_EXIT",
    "DEPTH_FRACTION_CAP",
    "PER_MINT_COOLDOWN_SEC",
]
