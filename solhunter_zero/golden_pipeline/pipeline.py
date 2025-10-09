"""High-level orchestration of the Golden Snapshot pipeline."""

from __future__ import annotations

from dataclasses import asdict
from typing import Awaitable, Callable, Iterable, Mapping

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
    ) -> None:
        self._bus = bus or InMemoryBus()
        self._kv = kv or InMemoryKeyValueStore()
        self._context = ExecutionContext()
        self._on_golden = on_golden
        self._on_suggestion = on_suggestion
        self._on_decision = on_decision
        self._on_virtual_fill = on_virtual_fill
        self._on_virtual_pnl = on_virtual_pnl

        async def _emit_golden(snapshot: GoldenSnapshot) -> None:
            self._context.record(snapshot)
            await self._publish(STREAMS.golden_snapshot, asdict(snapshot))
            if self._on_golden:
                await self._on_golden(snapshot)
            await self._agent_stage.submit(snapshot)

        self._coalescer = SnapshotCoalescer(_emit_golden, kv=self._kv)

        async def _emit_suggestion(suggestion: TradeSuggestion) -> None:
            await self._publish(STREAMS.trade_suggested, asdict(suggestion))
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
            await self._publish(STREAMS.vote_decisions, asdict(decision))
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

        async def _emit_virtual(fill: VirtualFill) -> None:
            await self._publish(STREAMS.virtual_fills, asdict(fill))
            if self._on_virtual_fill:
                await self._on_virtual_fill(fill)

        async def _emit_virtual_pnl(pnl: VirtualPnL) -> None:
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

        self._discovery_stage = DiscoveryStage(_on_discovery, kv=self._kv)

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

    async def _publish(self, stream: str, payload: Mapping[str, object]) -> None:
        if not self._bus:
            return
        await self._bus.publish(stream, payload)
