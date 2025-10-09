import asyncio
import time
from collections import deque

import pytest

from solhunter_zero.golden_pipeline.market import MarketDataStage
from solhunter_zero.golden_pipeline.depth import DepthStage
from solhunter_zero.golden_pipeline.voting import VotingStage
from solhunter_zero.golden_pipeline.execution import ShadowExecutor
from solhunter_zero.golden_pipeline.types import (
    Decision,
    DepthSnapshot,
    GoldenSnapshot,
    OHLCVBar,
    TapeEvent,
    VirtualFill,
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
        await asyncio.sleep(0.05)

        assert len(decisions) == 1
        decision = decisions[0]
        assert decision.agents == ["alpha", "beta"]
        assert pytest.approx(decision.notional_usd, rel=1e-6) == pytest.approx(833.3333, rel=1e-3)
        assert pytest.approx(decision.score, rel=1e-6) == pytest.approx(0.6, rel=1e-6)

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
