"""High-level orchestration of the Golden Snapshot pipeline."""

from __future__ import annotations

import asyncio
import os
from dataclasses import asdict
from typing import Awaitable, Callable, Dict, Iterable, Mapping, Optional

from .agents import AgentStage, BaseAgent
from .bus import InMemoryBus, MessageBus
from .coalescer import SnapshotCoalescer
from .contracts import STREAMS
from .depth import DepthStage
from .discovery import DiscoveryStage
from .enrichment import EnrichmentStage, EnrichmentFetcher
from .execution import ExecutionContext, LiveExecutor, ShadowExecutor
from .kv import InMemoryKeyValueStore, KeyValueStore
from .market import MarketDataStage
from .metrics import GoldenMetrics
from .types import (
    Decision,
    DepthSnapshot,
    DiscoveryCandidate,
    GoldenSnapshot,
    LiveFill,
    OHLCVBar,
    TapeEvent,
    TokenSnapshot,
    TradeSuggestion,
    VirtualFill,
    VirtualPnL,
)
from .voting import VotingStage
from .utils import now_ts


class _PaperPositionState:
    """Track synthetic paper position state for shadow execution."""

    def __init__(self) -> None:
        self.qty_base: float = 0.0
        self.avg_cost: float = 0.0
        self.realized_usd: float = 0.0

    def apply_fill(self, side: str, qty: float, price: float, fees: float) -> None:
        if qty <= 0.0:
            return
        direction = 1 if side.lower() == "buy" else -1
        signed_qty = qty * direction
        if self.qty_base == 0.0 or self.qty_base * signed_qty > 0:
            total_qty = abs(self.qty_base) + qty
            numerator = self.avg_cost * abs(self.qty_base) + price * qty
            numerator += fees if direction > 0 else -fees
            self.avg_cost = numerator / total_qty if total_qty else 0.0
            self.qty_base += signed_qty
            return

        closing_qty = min(abs(self.qty_base), qty)
        residual = qty - closing_qty
        self.qty_base += signed_qty
        if self.qty_base == 0.0:
            self.avg_cost = 0.0
        elif residual > 0:
            self.avg_cost = price

    def mark_to_market(self, mid: float, spread_bps: float) -> float:
        if self.qty_base == 0.0:
            return 0.0
        half_spread = mid * (spread_bps / 20000.0)
        if self.qty_base > 0:
            mark = mid - half_spread
        else:
            mark = mid + half_spread
        return (mark - self.avg_cost) * self.qty_base


class GoldenPipeline:
    """Wire all stages from discovery to execution."""

    def __init__(
        self,
        *,
        enrichment_fetcher: EnrichmentFetcher,
        agents: Iterable[BaseAgent] | None = None,
        on_golden: Callable[[GoldenSnapshot], Awaitable[None]] | None = None,
        on_suggestion: Callable[[TradeSuggestion], Awaitable[None]] | None = None,
        on_decision: Callable[[Decision], Awaitable[None]] | None = None,
        on_virtual_fill: Callable[[VirtualFill], Awaitable[None]] | None = None,
        on_virtual_pnl: Callable[[VirtualPnL], Awaitable[None]] | None = None,
        live_fill_handler: Callable[[LiveFill], Awaitable[None]] | None = None,
        bus: MessageBus | None = None,
        kv: KeyValueStore | None = None,
        agent_blacklist: Iterable[str] | None = None,
        agent_cooldown_sec: float = 0.0,
        max_agent_spread_bps: float = 80.0,
        min_agent_depth1_usd: float = 8_000.0,
        vote_window_ms: int = 400,
        vote_quorum: int = 2,
        vote_min_score: float = 0.04,
        allow_inmemory_bus_for_tests: bool = False,
    ) -> None:
        mode = (os.getenv("SOLHUNTER_MODE") or "test").strip().lower() or "test"
        if bus is None:
            if not allow_inmemory_bus_for_tests:
                if mode != "test":
                    raise RuntimeError(
                        "GoldenPipeline requires a shared message bus in non-test modes"
                    )
                raise RuntimeError(
                    "GoldenPipeline requires a message bus; set allow_inmemory_bus_for_tests=True for unit tests"
                )
            bus = InMemoryBus()
        if mode in {"live", "paper"} and isinstance(bus, InMemoryBus):
            raise SystemExit(78)
        self._bus = bus
        self._kv = kv or InMemoryKeyValueStore()
        self._sequence_cache: Dict[str, int] = {}
        self._sequence_locks: Dict[str, asyncio.Lock] = {}
        self._context = ExecutionContext()
        self._latest_snapshots: Dict[str, GoldenSnapshot] = {}
        self._paper_positions: Dict[str, _PaperPositionState] = {}
        self._on_golden = on_golden
        self._on_suggestion = on_suggestion
        self._on_decision = on_decision
        self._on_virtual_fill = on_virtual_fill
        self._on_virtual_pnl = on_virtual_pnl
        self.metrics = GoldenMetrics()

        def _capture_discovery_metrics(snapshot: Mapping[str, int]) -> None:
            self.metrics.record_discovery(snapshot)

        async def _emit_golden(snapshot: GoldenSnapshot) -> None:
            self._context.record(snapshot)
            self._latest_snapshots[snapshot.mint] = snapshot
            self.metrics.record(snapshot)
            await self._publish(STREAMS.golden_snapshot, asdict(snapshot))
            await self._publish_metrics(snapshot)
            if self._on_golden:
                await self._on_golden(snapshot)
            await self._agent_stage.submit(snapshot)

        self._coalescer = SnapshotCoalescer(_emit_golden, kv=self._kv)

        async def _emit_suggestion(suggestion: TradeSuggestion) -> None:
            mint_key = str(suggestion.mint)
            sequence = await self._next_sequence(mint_key)
            suggestion.sequence = sequence
            payload = asdict(suggestion)
            dedupe_suffix = f"{mint_key}:{sequence}"
            await self._publish(
                STREAMS.trade_suggested,
                payload,
                dedupe_key=f"{STREAMS.trade_suggested}:{dedupe_suffix}",
            )
            await self._publish(
                "agent.suggestion",
                dict(payload),
                dedupe_key=f"agent.suggestion:{dedupe_suffix}",
            )
            if self._on_suggestion:
                await self._on_suggestion(suggestion)
            await self._voting_stage.submit(suggestion)

        self._agent_stage = AgentStage(
            _emit_suggestion,
            agents=list(agents or []),
            blacklist=agent_blacklist,
            cooldown_sec=agent_cooldown_sec,
            max_spread_bps=max_agent_spread_bps,
            min_depth1_pct_usd=min_agent_depth1_usd,
        )

        async def _emit_decision(decision: Decision) -> None:
            mint_key = str(decision.mint)
            sequence = await self._next_sequence(mint_key)
            decision.sequence = sequence
            payload = asdict(decision)
            dedupe_suffix = f"{mint_key}:{sequence}"
            await self._publish(
                STREAMS.vote_decisions,
                payload,
                dedupe_key=f"{STREAMS.vote_decisions}:{dedupe_suffix}",
            )
            await self._publish(
                "agent.vote",
                dict(payload),
                dedupe_key=f"agent.vote:{dedupe_suffix}",
            )
            if self._on_decision:
                await self._on_decision(decision)
            snapshot = self._context.get(decision.snapshot_hash)
            if not snapshot:
                return
            await self._shadow_executor.submit(decision, snapshot)
            await self._live_executor.submit(decision, snapshot)

        self._voting_stage = VotingStage(
            _emit_decision,
            window_ms=vote_window_ms,
            quorum=vote_quorum,
            min_score=vote_min_score,
            kv=self._kv,
        )
        from os import getenv

        rl_disabled = getenv("RL_WEIGHTS_DISABLED")
        if rl_disabled is not None:
            flag = rl_disabled.strip().lower()
            if flag in {"1", "true", "yes", "on", "enabled"}:
                self._voting_stage.set_rl_disabled(True)

        async def _emit_virtual(fill: VirtualFill) -> None:
            await self._publish(STREAMS.virtual_fills, asdict(fill))
            await self._publish("execution.shadow.fill", asdict(fill))
            state = self._paper_positions.setdefault(fill.mint, _PaperPositionState())
            state.apply_fill(
                fill.side,
                float(fill.qty_base),
                float(fill.price_usd),
                float(fill.fees_usd),
            )
            if self._on_virtual_fill:
                await self._on_virtual_fill(fill)

        async def _emit_virtual_pnl(pnl: VirtualPnL) -> None:
            state = self._paper_positions.setdefault(pnl.mint, _PaperPositionState())
            state.realized_usd += float(pnl.realized_usd)
            snapshot = self._latest_snapshots.get(pnl.mint)
            if snapshot is None:
                snapshot = self._context.get(pnl.snapshot_hash)
            unrealized = float(pnl.unrealized_usd)
            payload = {
                "mint": pnl.mint,
                "order_id": pnl.order_id,
                "snapshot_hash": pnl.snapshot_hash,
                "qty_base": state.qty_base,
                "avg_cost": state.avg_cost,
                "realized_usd": state.realized_usd,
                "unrealized_usd": unrealized,
                "total_pnl_usd": state.realized_usd + unrealized,
                "ts": pnl.ts,
            }
            if snapshot is not None:
                mid = float(snapshot.px.get("mid_usd", 0.0) or 0.0)
                spread = float(snapshot.px.get("spread_bps", 0.0) or 0.0)
                payload["mark_to_market_usd"] = state.mark_to_market(mid, spread)
                payload["mid_usd"] = mid
                payload["spread_bps"] = spread
            await self._publish("paper.position.update", payload)
            if self._on_virtual_pnl:
                await self._on_virtual_pnl(pnl)

        self._shadow_executor = ShadowExecutor(
            _emit_virtual,
            _emit_virtual_pnl,
        )

        async def _emit_live(fill: LiveFill) -> None:
            await self._publish(STREAMS.live_fills, asdict(fill))
            if live_fill_handler:
                await live_fill_handler(fill)

        self._live_executor = LiveExecutor(_emit_live)

        async def _on_metadata(snapshot: TokenSnapshot) -> None:
            await self._publish(STREAMS.token_snapshot, asdict(snapshot))
            await self._coalescer.update_metadata(snapshot)

        async def _on_bar(bar: OHLCVBar) -> None:
            await self._publish(
                STREAMS.market_ohlcv,
                {
                    "mint": bar.mint,
                    "o": bar.open,
                    "h": bar.high,
                    "l": bar.low,
                    "c": bar.close,
                    "vol_usd": bar.vol_usd,
                    "trades": bar.trades,
                    "buyers": bar.buyers,
                    "zret": bar.zret,
                    "zvol": bar.zvol,
                    "asof_close": bar.asof_close,
                },
            )
            await self._coalescer.update_bar(bar)

        async def _on_depth(depth: DepthSnapshot) -> None:
            await self._publish(
                STREAMS.market_depth,
                {
                    "mint": depth.mint,
                    "venue": depth.venue,
                    "mid_usd": depth.mid_usd,
                    "spread_bps": depth.spread_bps,
                    "depth_pct": depth.depth_pct,
                    "asof": depth.asof,
                    "schema_version": depth.schema_version,
                },
            )
            await self._coalescer.update_depth(depth)

        self._market_stage = MarketDataStage(_on_bar)
        self._depth_stage = DepthStage(_on_depth)
        self._enrichment_stage = EnrichmentStage(
            _on_metadata,
            fetcher=enrichment_fetcher,
        )

        async def _on_discovery(candidate: DiscoveryCandidate) -> None:
            await self._publish(STREAMS.discovery_candidates, asdict(candidate))
            await self._enrichment_stage.submit([candidate])

        self._discovery_stage = DiscoveryStage(
            _on_discovery,
            kv=self._kv,
            on_metrics=_capture_discovery_metrics,
        )

    async def submit_discovery(self, candidate: DiscoveryCandidate) -> bool:
        """Submit a discovery candidate."""

        return await self._discovery_stage.submit(candidate)

    async def submit_market_event(self, event: TapeEvent) -> None:
        await self._market_stage.submit(event)

    async def submit_depth(self, snapshot: DepthSnapshot) -> None:
        await self._depth_stage.submit(snapshot)

    async def flush_market(self) -> None:
        await self._market_stage.flush()

    def register_agent(self, agent: BaseAgent) -> None:
        self._agent_stage.register_agent(agent)

    async def inject_token_snapshot(self, snapshot: TokenSnapshot) -> None:
        await self._coalescer.update_metadata(snapshot)

    async def inject_bar(self, bar: OHLCVBar) -> None:
        await self._coalescer.update_bar(bar)

    async def inject_golden(self, snapshot: GoldenSnapshot) -> None:
        # Used for testing or external pipelines.
        self._context.record(snapshot)
        if self._on_golden:
            await self._on_golden(snapshot)
        await self._agent_stage.submit(snapshot)

    @property
    def context(self) -> ExecutionContext:
        return self._context

    def set_rl_weights(self, weights: Mapping[str, float]) -> None:
        """Update reinforcement learning weights for voting."""

        self._voting_stage.set_rl_weights(weights)

    def set_rl_enabled(self, enabled: bool) -> None:
        """Enable or disable RL influence on the voting stage."""

        self._voting_stage.set_rl_disabled(not enabled)
        if not enabled:
            # Ensure any stale weights do not leak once the gate closes
            self._voting_stage.set_rl_weights({})

    def metrics_snapshot(self) -> Dict[str, Dict[str, Optional[float]]]:
        """Return rolling Golden Snapshot telemetry summaries."""

        return self.metrics.snapshot()

    def _sequence_kv_key(self, mint: str) -> str:
        return f"golden:sequence:{mint}"

    def _sequence_lock_for(self, mint_key: str) -> asyncio.Lock:
        lock = self._sequence_locks.get(mint_key)
        if lock is None:
            new_lock = asyncio.Lock()
            existing = self._sequence_locks.setdefault(mint_key, new_lock)
            lock = existing if existing is not new_lock else new_lock
        return lock

    async def _load_sequence(self, mint_key: str) -> int:
        if self._kv is None:
            return 0
        try:
            stored = await self._kv.get(self._sequence_kv_key(mint_key))
        except Exception:
            return 0
        if stored is None:
            return 0
        try:
            return int(stored)
        except Exception:
            return 0

    async def _persist_sequence(self, mint_key: str, sequence: int) -> None:
        if self._kv is None:
            return
        try:
            await self._kv.set(self._sequence_kv_key(mint_key), str(sequence))
        except Exception:
            pass

    async def _next_sequence(self, mint: str) -> int:
        mint_key = str(mint)
        lock = self._sequence_lock_for(mint_key)
        async with lock:
            current = self._sequence_cache.get(mint_key)
            if current is None:
                current = await self._load_sequence(mint_key)
            next_seq = current + 1
            self._sequence_cache[mint_key] = next_seq
            await self._persist_sequence(mint_key, next_seq)
            return next_seq

    async def _publish(
        self, stream: str, payload: Mapping[str, object], *, dedupe_key: str | None = None
    ) -> None:
        if not self._bus:
            return
        await self._bus.publish(stream, payload, dedupe_key=dedupe_key)

    async def _publish_metrics(self, snapshot: GoldenSnapshot) -> None:
        metrics = snapshot.metrics or {}
        now = now_ts()
        bus_latency = None
        emitted = metrics.get("emitted_at")
        try:
            if emitted is not None:
                bus_latency = max(0.0, (now - float(emitted)) * 1000.0)
        except Exception:
            bus_latency = None
        mapping = {
            "metrics.bus_latency": bus_latency,
            "metrics.ohlcv_lag": metrics.get("candle_age_ms"),
            "metrics.depth_lag": metrics.get("depth_staleness_ms"),
            "metrics.golden_lag": metrics.get("latency_ms"),
        }
        base_payload = {
            "mint": snapshot.mint,
            "asof": snapshot.asof,
            "hash": snapshot.hash,
        }
        for topic, value in mapping.items():
            if value is None:
                continue
            payload = dict(base_payload)
            payload["value"] = float(value)
            await self._publish(topic, payload)
