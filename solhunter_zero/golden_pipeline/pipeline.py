"""High-level orchestration of the Golden Snapshot pipeline."""

from __future__ import annotations

from typing import Awaitable, Callable, Iterable, Mapping

from .agents import AgentStage, BaseAgent
from .coalescer import SnapshotCoalescer
from .depth import DepthStage
from .discovery import DiscoveryStage
from .enrichment import EnrichmentStage, EnrichmentFetcher
from .execution import ExecutionContext, LiveExecutor, ShadowExecutor
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
    ) -> None:
        self._context = ExecutionContext()
        self._on_golden = on_golden
        self._on_suggestion = on_suggestion
        self._on_decision = on_decision
        self._on_virtual_fill = on_virtual_fill
        self._on_virtual_pnl = on_virtual_pnl

        async def _emit_golden(snapshot: GoldenSnapshot) -> None:
            self._context.record(snapshot)
            if self._on_golden:
                await self._on_golden(snapshot)
            await self._agent_stage.submit(snapshot)

        self._coalescer = SnapshotCoalescer(_emit_golden)

        async def _emit_suggestion(suggestion: TradeSuggestion) -> None:
            if self._on_suggestion:
                await self._on_suggestion(suggestion)
            await self._voting_stage.submit(suggestion)

        self._agent_stage = AgentStage(
            _emit_suggestion,
            agents=list(agents or []),
        )

        async def _emit_decision(decision: Decision) -> None:
            if self._on_decision:
                await self._on_decision(decision)
            snapshot = self._context.get(decision.snapshot_hash)
            if not snapshot:
                return
            await self._shadow_executor.submit(decision, snapshot)
            await self._live_executor.submit(decision, snapshot)

        self._voting_stage = VotingStage(_emit_decision)

        async def _emit_virtual(fill: VirtualFill) -> None:
            if self._on_virtual_fill:
                await self._on_virtual_fill(fill)

        async def _emit_virtual_pnl(pnl: VirtualPnL) -> None:
            if self._on_virtual_pnl:
                await self._on_virtual_pnl(pnl)

        self._shadow_executor = ShadowExecutor(
            _emit_virtual,
            _emit_virtual_pnl,
        )
        self._live_executor = LiveExecutor(live_fill_handler)

        async def _on_metadata(snapshot: TokenSnapshot) -> None:
            await self._coalescer.update_metadata(snapshot)

        async def _on_bar(bar: OHLCVBar) -> None:
            await self._coalescer.update_bar(bar)

        async def _on_depth(depth: DepthSnapshot) -> None:
            await self._coalescer.update_depth(depth)

        self._market_stage = MarketDataStage(_on_bar)
        self._depth_stage = DepthStage(_on_depth)
        self._enrichment_stage = EnrichmentStage(
            _on_metadata,
            fetcher=enrichment_fetcher,
        )

        async def _on_discovery(candidate: DiscoveryCandidate) -> None:
            await self._enrichment_stage.submit([candidate])

        self._discovery_stage = DiscoveryStage(_on_discovery)

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
