"""Integration layer wiring the Golden pipeline into the runtime event flow."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional, Sequence

from ..agent_manager import AgentManager
from ..event_bus import publish, subscribe
from ..portfolio import Portfolio
from ..token_aliases import canonical_mint
from ..token_scanner import TRENDING_METADATA

from .agents import BaseAgent
from .pipeline import GoldenPipeline
from .types import (
    Decision,
    DepthSnapshot,
    DiscoveryCandidate,
    GoldenSnapshot,
    TapeEvent,
    TokenSnapshot,
    TradeSuggestion,
    VirtualFill,
    VirtualPnL,
)

log = logging.getLogger(__name__)


def _coerce_float(value: Any) -> Optional[float]:
    """Best-effort conversion of ``value`` to ``float``."""

    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _default_symbol(mint: str) -> str:
    return mint[:6].upper()


@dataclass(slots=True)
class _AgentAction:
    side: str
    notional: float
    price: float
    max_slippage_bps: float
    confidence: float
    ttl_sec: float
    risk: Dict[str, float]


class AgentManagerAgent(BaseAgent):
    """Adapter that turns ``AgentManager`` actions into Golden suggestions."""

    def __init__(
        self,
        manager: AgentManager,
        portfolio: Portfolio,
        *,
        name: str = "swarm",
        default_notional: float = 1_000.0,
        default_slippage_bps: float = 60.0,
        default_ttl: float = 1.5,
        allowed_patterns: Sequence[str] | None = None,
        min_burst_zscore: float = 2.0,
        max_micro_spread_bps: float = 40.0,
        min_depth_usd: float = 15_000.0,
        buffer_bps: float = 20.0,
    ) -> None:
        super().__init__(name)
        self.manager = manager
        self.portfolio = portfolio
        self.default_notional = float(default_notional)
        self.default_slippage_bps = float(default_slippage_bps)
        self.default_ttl = float(default_ttl)
        patterns = allowed_patterns or (
            "first_pullback",
            "first_pullback_breakout",
            "breakout_pullback",
        )
        self._allowed_patterns = {str(p).lower() for p in patterns if str(p).strip()}
        self._last_buyers: Dict[str, int] = {}
        self._min_burst_zscore = float(min_burst_zscore)
        self._max_micro_spread_bps = float(max_micro_spread_bps)
        self._min_depth_usd = float(min_depth_usd)
        self._edge_buffer_bps = float(buffer_bps)

    async def generate(self, snapshot: GoldenSnapshot) -> List[TradeSuggestion]:
        try:
            ctx = await self.manager.evaluate_with_swarm(snapshot.mint, self.portfolio)
        except Exception:
            log.exception("AgentManager evaluation failed for %s", snapshot.mint)
            return []

        suggestions: List[TradeSuggestion] = []
        actions = getattr(ctx, "actions", [])
        for raw in actions or []:
            if not isinstance(raw, dict):
                continue
            action = self._normalise_action(snapshot, raw)
            if not action:
                continue
            gating_report = self._apply_entry_gates(snapshot, raw, action)
            if not gating_report.get("ruthless_filter", {}).get("passed"):
                continue
            if not gating_report.get("edge_pass"):
                continue
            suggestion = self.build_suggestion(
                snapshot=snapshot,
                side=action.side,
                notional_usd=action.notional,
                max_slippage_bps=action.max_slippage_bps,
                risk=self._augment_risk(action, gating_report),
                confidence=action.confidence,
                ttl_sec=action.ttl_sec,
                gating=gating_report,
            )
            suggestions.append(suggestion)
        return suggestions

    def _normalise_action(
        self, snapshot: GoldenSnapshot, action: Dict[str, Any]
    ) -> Optional[_AgentAction]:
        side = str(action.get("side", "")).lower()
        if side not in {"buy", "sell"}:
            return None

        price_candidates = [
            action.get("price"),
            action.get("entry_price"),
            snapshot.px.get("mid_usd"),
            snapshot.ohlcv5m.get("c"),
        ]
        price = next(
            (float(val) for val in price_candidates if _coerce_float(val)),
            None,
        )
        if price is None or price <= 0:
            return None

        amount = _coerce_float(action.get("amount"))
        if amount is None or amount <= 0:
            amount = _coerce_float(action.get("size"))
        if amount is None or amount <= 0:
            notional_hint = _coerce_float(action.get("notional_usd"))
            if notional_hint is not None and notional_hint > 0:
                amount = notional_hint / price

        notional = _coerce_float(action.get("notional_usd"))
        if notional is None or notional <= 0:
            if amount is not None and amount > 0:
                notional = amount * price
        if notional is None or notional <= 0:
            depth1 = _coerce_float(snapshot.liq.get("depth_pct", {}).get("1"))
            if depth1 is not None and depth1 > 0:
                notional = depth1 * 0.1
            else:
                notional = self.default_notional

        max_slippage = _coerce_float(action.get("max_slippage_bps"))
        if max_slippage is None or max_slippage <= 0:
            max_slippage = self.default_slippage_bps

        confidence = _coerce_float(action.get("confidence"))
        if confidence is None:
            expected = _coerce_float(action.get("expected_roi")) or 0.0
            success = _coerce_float(action.get("success_prob")) or 0.0
            confidence = max(0.0, expected) * max(0.0, success)
        if confidence <= 0:
            confidence = 0.05

        ttl = _coerce_float(action.get("ttl_sec"))
        if ttl is None or ttl <= 0:
            ttl = _coerce_float(action.get("ttl"))
        if ttl is None or ttl <= 0:
            ttl = self.default_ttl

        risk: Dict[str, float] = {}
        for key, alias in (
            ("stop_bps", "stop_loss"),
            ("take_bps", "take_profit"),
            ("time_stop_sec", "time_stop"),
        ):
            value = _coerce_float(action.get(key))
            if value is None or value <= 0:
                value = _coerce_float(action.get(alias))
            if value is not None and value > 0:
                risk[key] = float(value)

        return _AgentAction(
            side=side,
            notional=float(notional),
            price=float(price),
            max_slippage_bps=float(max_slippage),
            confidence=float(confidence),
            ttl_sec=float(ttl),
            risk=risk,
        )

    def _augment_risk(
        self, action: _AgentAction, gating: Dict[str, Any]
    ) -> Dict[str, float]:
        risk = dict(action.risk)
        breakeven = gating.get("breakeven_bps")
        if isinstance(breakeven, (int, float)) and breakeven > 0:
            risk.setdefault("breakeven_bps", float(breakeven))
        return risk

    def _apply_entry_gates(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Dict[str, Any],
        action: _AgentAction,
    ) -> Dict[str, Any]:
        ruthless_report = self._ruthless_filter(snapshot, raw_action)
        friction_report = self._friction_floor(snapshot, raw_action, action)
        gating: Dict[str, Any] = {
            "ruthless_filter": ruthless_report,
            "friction_floor": friction_report,
            "breakeven_bps": friction_report.get("breakeven_bps"),
            "expected_edge_bps": friction_report.get("expected_edge_bps"),
            "edge_buffer_bps": friction_report.get("edge_buffer_bps"),
            "edge_pass": friction_report.get("passed"),
        }
        return gating

    def _ruthless_filter(
        self, snapshot: GoldenSnapshot, action: Dict[str, Any]
    ) -> Dict[str, Any]:
        ohlcv = snapshot.ohlcv5m or {}
        px = snapshot.px or {}
        liq = snapshot.liq or {}
        zret = _coerce_float(ohlcv.get("zret")) or 0.0
        zvol = _coerce_float(ohlcv.get("zvol")) or 0.0
        buyers = int(_coerce_float(ohlcv.get("buyers")) or 0)
        prev_buyers = self._last_buyers.get(snapshot.mint)
        buyers_uptick = bool(prev_buyers is not None and buyers > prev_buyers)
        spread_bps = _coerce_float(px.get("spread_bps")) or 0.0
        depth = _coerce_float(liq.get("depth_pct", {}).get("1")) or 0.0
        pattern = str(action.get("pattern") or "").strip().lower()
        pattern_allowed = bool(pattern and pattern in self._allowed_patterns)
        passed = (
            zret > self._min_burst_zscore
            and zvol > self._min_burst_zscore
            and buyers_uptick
            and spread_bps <= self._max_micro_spread_bps
            and depth >= self._min_depth_usd
            and pattern_allowed
        )
        report = {
            "zret": float(zret),
            "zvol": float(zvol),
            "buyers": buyers,
            "previous_buyers": prev_buyers,
            "buyers_uptick": buyers_uptick,
            "spread_bps": float(spread_bps),
            "depth_1pct_usd": float(depth),
            "pattern": pattern or None,
            "pattern_allowed": pattern_allowed,
            "passed": passed,
        }
        self._last_buyers[snapshot.mint] = buyers
        return report

    def _friction_floor(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Dict[str, Any],
        action: _AgentAction,
    ) -> Dict[str, Any]:
        liq = snapshot.liq or {}
        depth = _coerce_float(liq.get("depth_pct", {}).get("1")) or 0.0
        notional = max(action.notional, 0.0)
        depth = max(depth, 1e-9)
        size_fraction = min(notional / depth, 1.0)
        impact_bps = min(size_fraction * 50.0, 200.0)
        fees_bps = (
            _coerce_float(raw_action.get("fees_bps"))
            or _coerce_float(raw_action.get("fee_bps"))
            or 4.0
        )
        latency_bps = _coerce_float(raw_action.get("latency_bps")) or 2.0
        breakeven = float(fees_bps + latency_bps + impact_bps)
        expected_edge = _coerce_float(raw_action.get("expected_edge_bps"))
        if expected_edge is None:
            expected_edge = _coerce_float(raw_action.get("edge_bps"))
        if expected_edge is None:
            expected_roi = _coerce_float(raw_action.get("expected_roi"))
            if expected_roi is not None:
                expected_edge = expected_roi * 10_000.0
        buffer_requirement = self._edge_buffer_bps
        edge_pass = False
        edge_buffer = None
        if expected_edge is not None:
            edge_buffer = float(expected_edge - breakeven)
            edge_pass = expected_edge >= breakeven + buffer_requirement
        report: Dict[str, Any] = {
            "fees_bps": float(fees_bps),
            "latency_bps": float(latency_bps),
            "impact_bps": float(impact_bps),
            "breakeven_bps": float(breakeven),
            "expected_edge_bps": expected_edge,
            "required_buffer_bps": float(buffer_requirement),
            "edge_buffer_bps": edge_buffer,
            "passed": edge_pass,
        }
        return report


class GoldenPipelineService:
    """Orchestrate Golden pipeline ingestion from existing runtime events."""

    def __init__(
        self,
        *,
        agent_manager: AgentManager,
        portfolio: Portfolio,
        enrichment_fetcher: Optional[Callable[[Iterable[str]], Awaitable[Dict[str, TokenSnapshot]]]] = None,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        if enrichment_fetcher is None:
            enrichment_fetcher = self._default_enrichment_fetcher
        self.pipeline = GoldenPipeline(
            enrichment_fetcher=enrichment_fetcher,
            agents=[AgentManagerAgent(agent_manager, portfolio)],
            on_decision=self._handle_decision,
            on_golden=self._handle_snapshot,
            on_suggestion=self._handle_suggestion,
            on_virtual_fill=self._handle_virtual_fill,
            on_virtual_pnl=self._handle_virtual_pnl,
        )

        self._subscriptions: List[Callable[[], None]] = []
        self._tasks: List[asyncio.Task] = []
        self._pending: set[asyncio.Task] = set()
        self._running = False
        self._last_price: Dict[str, float] = {}

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._subscriptions.append(subscribe("token_discovered", self._on_discovery))
        self._subscriptions.append(subscribe("price_update", self._on_price))
        self._subscriptions.append(subscribe("depth_update", self._on_depth))
        self._tasks.append(asyncio.create_task(self._market_flush_loop(), name="golden_market_flush"))
        log.info("GoldenPipelineService started (subscriptions=token_discovered, price_update, depth_update)")

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for unsub in self._subscriptions:
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()

        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()

        for task in list(self._pending):
            task.cancel()
        for task in list(self._pending):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._pending.clear()
        log.info("GoldenPipelineService stopped")

    async def _market_flush_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(5.0)
                await self.pipeline.flush_market()
        except asyncio.CancelledError:
            raise

    def _spawn(self, coro: Awaitable[Any]) -> None:
        if not self._running:
            return
        task = asyncio.create_task(coro)
        self._pending.add(task)
        task.add_done_callback(self._pending.discard)

    def _on_discovery(self, payload: Any) -> None:
        if not self._running:
            return
        tokens: Iterable[Any]
        if isinstance(payload, dict) and "tokens" in payload:
            tokens = payload.get("tokens") or []
        elif isinstance(payload, (list, tuple, set)):
            tokens = payload
        else:
            return
        now = time.time()
        for raw in tokens:
            mint = canonical_mint(str(raw))
            candidate = DiscoveryCandidate(mint=mint, asof=now)
            self._spawn(self.pipeline.submit_discovery(candidate))

    def _on_price(self, payload: Any) -> None:
        if not self._running or not isinstance(payload, dict):
            return
        token = payload.get("token")
        price = _coerce_float(payload.get("price"))
        if not token or price is None or price <= 0:
            return
        mint = canonical_mint(str(token))
        self._last_price[mint] = price
        try:
            self.portfolio.record_prices({mint: float(price)})
        except Exception:
            pass
        event = TapeEvent(
            mint_base=mint,
            mint_quote="USD",
            amount_base=0.0,
            amount_quote=0.0,
            route=str(payload.get("venue") or ""),
            program_id=str(payload.get("venue") or ""),
            pool=str(payload.get("pool") or ""),
            signer="",
            signature="",
            slot=0,
            ts=time.time(),
            fees_base=0.0,
            price_usd=float(price),
            fees_usd=0.0,
            is_self=False,
            buyer=None,
        )
        self._spawn(self.pipeline.submit_market_event(event))

    def _on_depth(self, payload: Any) -> None:
        if not self._running or not isinstance(payload, dict):
            return
        now = time.time()
        for token, entry in payload.items():
            try:
                mint = canonical_mint(str(token))
            except Exception:
                continue
            bids = _coerce_float(entry.get("bids")) or 0.0
            asks = _coerce_float(entry.get("asks")) or 0.0
            depth_val = _coerce_float(entry.get("depth")) or max(bids + asks, 0.0)
            depth_pct = {
                "1": float(depth_val),
                "2": float(depth_val * 1.5),
                "5": float(depth_val * 2.0),
            }
            mid = self._last_price.get(mint) or _coerce_float(entry.get("mid")) or 0.0
            spread_bps = _coerce_float(entry.get("spread_bps"))
            if spread_bps is None:
                spread_bps = 40.0 if bids and asks else 100.0
            snapshot = DepthSnapshot(
                mint=mint,
                venue=str(entry.get("venue") or "depth_service"),
                mid_usd=float(mid) if mid else float(self._last_price.get(mint, 0.0) or 0.0),
                spread_bps=float(spread_bps),
                depth_pct=depth_pct,
                asof=now,
            )
            self._spawn(self.pipeline.submit_depth(snapshot))

    async def _handle_decision(self, decision: Decision) -> None:
        snapshot = self.pipeline.context.get(decision.snapshot_hash)
        mid = float(snapshot.px.get("mid_usd", 0.0)) if snapshot else 0.0
        if mid <= 0:
            mid = float(self._last_price.get(decision.mint, 0.0) or 0.0)
        if mid <= 0:
            log.debug("Dropping decision for %s due to missing mid price", decision.mint)
            return
        size = decision.notional_usd / mid if mid > 0 else 0.0
        payload = {
            "token": decision.mint,
            "side": decision.side,
            "size": float(max(size, 0.0)),
            "price": float(mid),
            "rationale": {
                "agents": list(decision.agents),
                "confidence": float(decision.score),
                "snapshot_hash": decision.snapshot_hash,
            },
        }
        publish("action_decision", payload)

    async def _handle_virtual_fill(self, fill: VirtualFill) -> None:
        publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"virtual_fill:{fill.mint}:{fill.side}:{fill.price_usd:.4f}",
            },
        )

    async def _handle_virtual_pnl(self, pnl: VirtualPnL) -> None:
        publish(
            "virtual_pnl",
            {
                "order_id": pnl.order_id,
                "mint": pnl.mint,
                "snapshot_hash": pnl.snapshot_hash,
                "realized_usd": pnl.realized_usd,
                "unrealized_usd": pnl.unrealized_usd,
                "ts": pnl.ts,
            },
        )

    async def _handle_snapshot(self, snapshot: GoldenSnapshot) -> None:
        publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"snapshot:{snapshot.mint}:{snapshot.hash[:6]}",
            },
        )

    async def _handle_suggestion(self, suggestion: TradeSuggestion) -> None:
        publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"suggestion:{suggestion.agent}:{suggestion.mint}",
            },
        )

    async def _default_enrichment_fetcher(
        self, mints: Iterable[str]
    ) -> Dict[str, TokenSnapshot]:
        snapshots: Dict[str, TokenSnapshot] = {}
        now = time.time()
        for mint in mints:
            canonical = canonical_mint(str(mint))
            meta = TRENDING_METADATA.get(canonical) or {}
            symbol = meta.get("symbol") or meta.get("ticker") or _default_symbol(canonical)
            name = meta.get("name") or symbol
            decimals = _coerce_float(meta.get("decimals"))
            if decimals is None:
                nested = meta.get("metadata")
                if isinstance(nested, dict):
                    decimals = _coerce_float(nested.get("decimals"))
            try:
                decimals_int = int(decimals) if decimals is not None else 6
            except Exception:
                decimals_int = 6
            decimals_int = max(0, min(decimals_int, 12))
            token_program = str(meta.get("token_program") or "Tokenkeg")
            venues = None
            raw_venues = meta.get("venues")
            if isinstance(raw_venues, (list, tuple)):
                venues = [str(v) for v in raw_venues if isinstance(v, str)]
            flags = meta.get("flags") if isinstance(meta.get("flags"), dict) else {}
            snapshots[canonical] = TokenSnapshot(
                mint=canonical,
                symbol=str(symbol),
                name=str(name),
                decimals=decimals_int,
                token_program=token_program,
                venues=venues,
                flags=flags if isinstance(flags, dict) else {},
                asof=now,
            )
        return snapshots


__all__ = [
    "GoldenPipelineService",
    "AgentManagerAgent",
]
