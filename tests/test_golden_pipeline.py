import asyncio
import time
from collections import deque

import pytest

from solhunter_zero.golden_pipeline.agents import BaseAgent
from solhunter_zero.golden_pipeline.market import MarketDataStage
from solhunter_zero.golden_pipeline.depth import DepthStage
from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline.voting import VotingStage
from solhunter_zero.golden_pipeline.execution import ShadowExecutor
from solhunter_zero.golden_pipeline.types import (
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


def test_market_stage_computes_flow_and_excludes_self():
    async def runner() -> None:
        bars: deque[OHLCVBar] = deque()

        async def collect(bar: OHLCVBar) -> None:
            bars.append(bar)

        stage = MarketDataStage(collect)
        base_ts = 1_000.0
        event_buy = TapeEvent(
            mint_base="MINT",
            mint_quote="USD",
            amount_base=5.0,
            amount_quote=50.0,
            route="jup",
            program_id="amm",
            pool="pool1",
            signer="buyer1",
            signature="sig1",
            slot=0,
            ts=base_ts,
            fees_base=0.0,
            price_usd=10.0,
            fees_usd=0.05,
            is_self=False,
            buyer="buyer1",
        )
        event_sell = TapeEvent(
            mint_base="MINT",
            mint_quote="USD",
            amount_base=-2.0,
            amount_quote=-20.0,
            route="jup",
            program_id="amm",
            pool="pool1",
            signer="seller",
            signature="sig2",
            slot=1,
            ts=base_ts + 60,
            fees_base=0.0,
            price_usd=10.0,
            fees_usd=0.05,
            is_self=False,
            buyer=None,
        )
        await stage.submit(event_buy)
        await stage.submit(event_sell)
        await stage.flush(now=base_ts + 360)

        assert len(bars) == 1
        bar = bars[0]
        assert bar.trades == 2
        assert bar.buyers == 1
        assert pytest.approx(bar.flow_usd, rel=1e-6) == 30.0

    asyncio.run(runner())


def test_depth_stage_aggregates_by_mint():
    async def runner() -> None:
        snapshots: deque[DepthSnapshot] = deque()

        async def collect(snapshot: DepthSnapshot) -> None:
            snapshots.append(snapshot)

        stage = DepthStage(collect)
        await stage.submit(
            DepthSnapshot(
                mint="MINT",
                venue="dex-a",
                mid_usd=10.0,
                spread_bps=40.0,
                depth_pct={"1": 1_000.0, "2": 1_500.0, "5": 2_000.0},
                asof=1.0,
            )
        )
        await stage.submit(
            DepthSnapshot(
                mint="MINT",
                venue="dex-b",
                mid_usd=10.5,
                spread_bps=30.0,
                depth_pct={"1": 500.0, "2": 1_000.0, "5": 1_500.0},
                asof=2.0,
            )
        )

        assert len(snapshots) == 2
        agg = snapshots[-1]
        assert agg.venue == "aggregated"
        assert pytest.approx(agg.depth_pct["1"], rel=1e-6) == 1_500.0
        assert pytest.approx(agg.depth_pct["2"], rel=1e-6) == 2_500.0
        assert pytest.approx(agg.depth_pct["5"], rel=1e-6) == 3_500.0
        assert agg.spread_bps == 30.0
        assert agg.mid_usd < 10.5 and agg.mid_usd > 10.0

    asyncio.run(runner())


def test_voting_stage_applies_rl_weights():
    async def runner() -> None:
        decisions: deque[Decision] = deque()

        async def collect(decision: Decision) -> None:
            decisions.append(decision)

        stage = VotingStage(collect, window_ms=10, rl_weights={"alpha": 2.0, "beta": 1.0})
        now = time.time()

        suggestion_template = dict(
            mint="MINT",
            side="buy",
            notional_usd=1_000.0,
            max_slippage_bps=50.0,
            risk={},
            ttl_sec=1.0,
        )

        from solhunter_zero.golden_pipeline.types import TradeSuggestion

        suggestion_a = TradeSuggestion(
            agent="alpha",
            confidence=0.6,
            generated_at=now,
            inputs_hash="hash",
            **suggestion_template,
        )
        suggestion_b = TradeSuggestion(
            agent="beta",
            confidence=0.6,
            generated_at=now,
            inputs_hash="hash",
            notional_usd=500.0,
            **{k: v for k, v in suggestion_template.items() if k != "notional_usd"},
        )

        await stage.submit(suggestion_a)
        await stage.submit(suggestion_b)
        await asyncio.sleep(stage.window_sec + 0.1)

        assert len(decisions) == 1
        decision = decisions[0]
        assert decision.agents == ["alpha", "beta"]
        assert pytest.approx(decision.notional_usd, rel=1e-6) == pytest.approx(833.3333, rel=1e-3)
        assert pytest.approx(decision.score, rel=1e-6) == pytest.approx(0.6, rel=1e-6)

    asyncio.run(runner())


def test_voting_stage_rl_staleness_gate():
    async def runner() -> None:
        decisions: deque[Decision] = deque()

        async def collect(decision: Decision) -> None:
            decisions.append(decision)

        stage = VotingStage(collect, window_ms=80, rl_weights={"alpha": 2.0, "beta": 1.0})
        stage.set_rl_weights(
            {
                "weights": {"alpha": 4.0, "beta": 0.25},
                "asof": time.time() - stage.window_sec * 3.5,
                "window_hash": "stale",
            }
        )

        template = dict(
            mint="STALE",
            side="buy",
            notional_usd=1_000.0,
            max_slippage_bps=40.0,
            risk={},
            ttl_sec=1.0,
        )

        suggestion_a = TradeSuggestion(
            agent="alpha",
            confidence=0.55,
            generated_at=time.time(),
            inputs_hash="snap",
            **template,
        )
        suggestion_b = TradeSuggestion(
            agent="beta",
            confidence=0.55,
            generated_at=time.time(),
            inputs_hash="snap",
            notional_usd=500.0,
            **{k: v for k, v in template.items() if k != "notional_usd"},
        )

        await stage.submit(suggestion_a)
        await stage.submit(suggestion_b)
        await asyncio.sleep(stage.window_sec + 0.1)

        assert decisions, "decision should still be emitted"
        decision = decisions[0]
        assert decision.agents == ["alpha", "beta"]
        assert pytest.approx(decision.notional_usd, rel=1e-3) == 750.0

    asyncio.run(runner())


def test_voting_stage_rl_disabled_override():
    async def runner() -> None:
        decisions: deque[Decision] = deque()

        async def collect(decision: Decision) -> None:
            decisions.append(decision)

        stage = VotingStage(collect, window_ms=100, rl_weights={"alpha": 5.0, "beta": 0.1})
        stage.set_rl_disabled(True)
        stage.set_rl_weights(
            {
                "weights": {"alpha": 10.0, "beta": 0.01},
                "asof": time.time(),
            }
        )

        base_template = dict(
            mint="DISABLED",
            side="buy",
            notional_usd=900.0,
            max_slippage_bps=30.0,
            risk={},
            ttl_sec=1.0,
        )

        suggestion_a = TradeSuggestion(
            agent="alpha",
            confidence=0.6,
            generated_at=time.time(),
            inputs_hash="snap2",
            **base_template,
        )
        suggestion_b = TradeSuggestion(
            agent="beta",
            confidence=0.6,
            generated_at=time.time(),
            inputs_hash="snap2",
            notional_usd=300.0,
            **{k: v for k, v in base_template.items() if k != "notional_usd"},
        )

        await stage.submit(suggestion_a)
        await stage.submit(suggestion_b)
        await asyncio.sleep(stage.window_sec + 0.1)

        assert decisions
        decision = decisions[0]
        assert decision.agents == ["alpha", "beta"]
        assert pytest.approx(decision.notional_usd, rel=1e-3) == 600.0

    asyncio.run(runner())


def test_shadow_executor_emits_virtual_pnl():
    async def runner() -> None:
        fills: deque[VirtualFill] = deque()
        pnls = []

        async def collect_fill(fill: VirtualFill) -> None:
            fills.append(fill)

        async def collect_pnl(pnl) -> None:
            pnls.append(pnl)

        executor = ShadowExecutor(collect_fill, collect_pnl, latency_bps=0.0, fee_bps=0.0)
        snapshot = GoldenSnapshot(
            mint="MINT",
            asof=0.0,
            meta={},
            px={"mid_usd": 10.0, "spread_bps": 20.0},
            liq={"depth_pct": {"1": 5_000.0}},
            ohlcv5m={},
            hash="hash",
        )
        decision = Decision(
            mint="MINT",
            side="buy",
            notional_usd=1_000.0,
            score=0.6,
            snapshot_hash="hash",
            client_order_id="order",
            agents=["alpha", "beta"],
            ts=0.0,
        )

        await executor.submit(decision, snapshot)

        assert len(fills) == 1
        assert len(pnls) == 1
        pnl = pnls[0]
        assert pnl.order_id == "order"
        assert pnl.mint == "MINT"
        assert pnl.realized_usd < 0
        assert pytest.approx(pnl.realized_usd, rel=1e-6) == pytest.approx(-1.0, rel=1e-6)

    asyncio.run(runner())


def test_pipeline_end_to_end_flow():
    class StaticAgent(BaseAgent):
        def __init__(self, name: str, side: str = "buy") -> None:
            super().__init__(name)
            self._side = side

        async def generate(self, snapshot: GoldenSnapshot):
            return [
                self.build_suggestion(
                    snapshot=snapshot,
                    side=self._side,
                    notional_usd=5_000.0,
                    max_slippage_bps=40.0,
                    risk={},
                    confidence=0.6,
                    ttl_sec=1.0,
                )
            ]

    async def runner() -> None:
        goldens: deque[GoldenSnapshot] = deque()
        suggestions: deque[TradeSuggestion] = deque()
        decisions: deque[Decision] = deque()
        virtual_fills: deque[VirtualFill] = deque()
        virtual_pnls: deque[VirtualPnL] = deque()
        live_fills: deque[LiveFill] = deque()

        async def on_golden(snapshot: GoldenSnapshot) -> None:
            goldens.append(snapshot)

        async def on_suggestion(suggestion: TradeSuggestion) -> None:
            suggestions.append(suggestion)

        async def on_decision(decision: Decision) -> None:
            decisions.append(decision)

        async def on_virtual_fill(fill: VirtualFill) -> None:
            virtual_fills.append(fill)

        async def on_virtual_pnl(pnl: VirtualPnL) -> None:
            virtual_pnls.append(pnl)

        async def on_live(fill: LiveFill) -> None:
            live_fills.append(fill)

        async def fetch_metadata(mints):
            return {
                mint: TokenSnapshot(
                    mint=mint,
                    symbol="TEST",
                    name="Test Token",
                    decimals=6,
                    token_program="Tokenkeg",
                    asof=time.time(),
                )
                for mint in mints
            }

        pipeline = GoldenPipeline(
            enrichment_fetcher=fetch_metadata,
            agents=[StaticAgent("alpha"), StaticAgent("beta")],
            on_golden=on_golden,
            on_suggestion=on_suggestion,
            on_decision=on_decision,
            on_virtual_fill=on_virtual_fill,
            on_virtual_pnl=on_virtual_pnl,
            live_fill_handler=on_live,
        )

        mint = "Mint1111111111111111111111111111111111"
        await pipeline.submit_discovery(DiscoveryCandidate(mint=mint, asof=time.time()))

        depth = DepthSnapshot(
            mint=mint,
            venue="aggregated",
            mid_usd=1.5,
            spread_bps=20.0,
            depth_pct={"1": 10_000.0, "2": 15_000.0, "5": 25_000.0},
            asof=time.time(),
        )
        await pipeline.submit_depth(depth)

        now = time.time() - 600.0
        event = TapeEvent(
            mint_base=mint,
            mint_quote="USD",
            amount_base=1_000.0,
            amount_quote=1_500.0,
            route="test",
            program_id="prog",
            pool="pool",
            signer="signer",
            signature="sig",
            slot=0,
            ts=now,
            fees_base=0.0,
            price_usd=1.5,
            fees_usd=0.0,
            is_self=False,
            buyer="trader",
        )
        await pipeline.submit_market_event(event)
        await pipeline.flush_market()

        await asyncio.sleep(0.2)

        assert len(goldens) == 1
        golden = goldens[0]
        assert golden.mint == mint
        assert golden.hash

        assert len(suggestions) == 2
        for suggestion in suggestions:
            assert suggestion.inputs_hash == golden.hash

        await asyncio.sleep(0.5)
        assert len(decisions) == 1
        decision = decisions[0]
        assert decision.snapshot_hash == golden.hash
        assert decision.agents == ["alpha", "beta"]
        assert decision.notional_usd > 0

        await asyncio.sleep(0.2)
        assert len(virtual_fills) == 1
        assert len(virtual_pnls) == 1
        assert len(live_fills) == 1
        virtual_fill = virtual_fills[0]
        assert virtual_fill.snapshot_hash == golden.hash
        assert pytest.approx(virtual_fill.qty_base, rel=1e-6) == pytest.approx(
            decision.notional_usd / depth.mid_usd, rel=1e-6
        )

    asyncio.run(runner())
