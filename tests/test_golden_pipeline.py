import asyncio
import sys
import time
import types
from collections import deque
from types import SimpleNamespace
from typing import Any

import pytest

from solhunter_zero.golden_pipeline.agents import BaseAgent
from solhunter_zero.golden_pipeline.bus import InMemoryBus
from solhunter_zero.golden_pipeline.contracts import STREAMS, golden_hash_key
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
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
    GOLDEN_SNAPSHOT_SCHEMA_VERSION,
    TRADE_SUGGESTION_SCHEMA_VERSION,
    VIRTUAL_FILL_SCHEMA_VERSION,
    VOTE_DECISION_SCHEMA_VERSION,
    LIVE_FILL_SCHEMA_VERSION,
)

_datasets_pkg = types.ModuleType("solhunter_zero.datasets")
_sample_ticks = types.ModuleType("solhunter_zero.datasets.sample_ticks")
_sample_ticks.load_sample_ticks = lambda *args, **kwargs: []
_sample_ticks.DEFAULT_PATH = ""
_datasets_pkg.sample_ticks = _sample_ticks
sys.modules.setdefault("solhunter_zero.datasets", _datasets_pkg)
sys.modules.setdefault("solhunter_zero.datasets.sample_ticks", _sample_ticks)

_token_scanner_stub = types.ModuleType("solhunter_zero.token_scanner")
_token_scanner_stub.TRENDING_METADATA = {}
sys.modules.setdefault("solhunter_zero.token_scanner", _token_scanner_stub)

_portfolio_stub = types.ModuleType("solhunter_zero.portfolio")
if "solhunter_zero.portfolio" not in sys.modules:
    class _PortfolioStub:
        pass

    _portfolio_stub.Portfolio = _PortfolioStub
    sys.modules["solhunter_zero.portfolio"] = _portfolio_stub

_agent_manager_stub = types.ModuleType("solhunter_zero.agent_manager")
if "solhunter_zero.agent_manager" not in sys.modules:
    class _AgentManagerStub:
        async def evaluate_with_swarm(self, mint: str, portfolio: object) -> SimpleNamespace:
            return SimpleNamespace(actions=[])

    _agent_manager_stub.AgentManager = _AgentManagerStub
    sys.modules["solhunter_zero.agent_manager"] = _agent_manager_stub

from solhunter_zero.golden_pipeline.service import AgentManagerAgent


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
    class VotingAgent(BaseAgent):
        def __init__(self, name: str, *, notional: float, confidence: float) -> None:
            super().__init__(name)
            self._notional = notional
            self._confidence = confidence

        async def generate(self, snapshot: GoldenSnapshot):
            gating = {
                "edge_pass": True,
                "edge_buffer_bps": 30.0,
                "breakeven_bps": 10.0,
                "expected_edge_bps": 40.0,
            }
            risk = {"expected_edge_bps": 40.0, "breakeven_bps": 10.0}
            return [
                self.build_suggestion(
                    snapshot=snapshot,
                    side="buy",
                    notional_usd=self._notional,
                    max_slippage_bps=50.0,
                    risk=risk,
                    confidence=self._confidence,
                    ttl_sec=2.0,
                    gating=gating,
                )
            ]

    async def runner() -> None:
        bus = InMemoryBus()
        kv = InMemoryKeyValueStore()
        base_ts = time.time() - 600.0
        mint = "MintGdenTest111111111111111111111111111"

        metadata_requests: list[list[str]] = []
        goldens: deque[GoldenSnapshot] = deque()
        suggestions: deque[TradeSuggestion] = deque()
        decisions: deque[Decision] = deque()
        virtual_fills: deque[VirtualFill] = deque()
        virtual_pnls: deque[VirtualPnL] = deque()
        live_fills: deque[LiveFill] = deque()

        async def fetch_metadata(mints):
            requested = [str(m) for m in mints]
            metadata_requests.append(requested)
            asof = time.time()
            return {
                mint_id: TokenSnapshot(
                    mint=mint_id,
                    symbol="GOLD",
                    name="Golden Token",
                    decimals=6,
                    token_program="TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                    venues=("raydium", "orca"),
                    flags={"source": "test"},
                    asof=asof,
                )
                for mint_id in requested
            }

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

        pipeline = GoldenPipeline(
            enrichment_fetcher=fetch_metadata,
            agents=[
                VotingAgent("alpha_agent", notional=5_000.0, confidence=0.22),
                VotingAgent("beta_agent", notional=7_500.0, confidence=0.28),
            ],
            on_golden=on_golden,
            on_suggestion=on_suggestion,
            on_decision=on_decision,
            on_virtual_fill=on_virtual_fill,
            on_virtual_pnl=on_virtual_pnl,
            live_fill_handler=on_live,
            bus=bus,
            kv=kv,
            max_agent_spread_bps=200.0,
            min_agent_depth1_usd=4_000.0,
            vote_quorum=2,
            vote_window_ms=400,
            vote_min_score=0.05,
        )

        candidate = DiscoveryCandidate(mint=mint, asof=base_ts)
        accepted = await pipeline.submit_discovery(candidate)
        assert accepted is True
        assert metadata_requests and metadata_requests[0] == [mint]

        depth_asof = time.time()
        depth_primary = DepthSnapshot(
            mint=mint,
            venue="alpha-venue",
            mid_usd=1.0,
            spread_bps=22.0,
            depth_pct={"1": 12_000.0, "2": 18_000.0},
            asof=depth_asof,
        )
        depth_secondary = DepthSnapshot(
            mint=mint,
            venue="beta-venue",
            mid_usd=1.01,
            spread_bps=18.0,
            depth_pct={"1": 8_000.0, "2": 15_000.0},
            asof=depth_asof + 0.1,
        )
        await pipeline.submit_depth(depth_primary)
        await pipeline.submit_depth(depth_secondary)

        trade_a = TapeEvent(
            mint_base=mint,
            mint_quote="USDC",
            amount_base=500.0,
            amount_quote=500.0,
            route="jupiter",
            program_id="amm",
            pool="pool-alpha",
            signer="trader-a",
            signature="sig-a",
            slot=1,
            ts=base_ts,
            fees_base=0.0,
            price_usd=1.0,
            fees_usd=0.05,
            is_self=False,
            buyer="wallet-a",
        )
        trade_b = TapeEvent(
            mint_base=mint,
            mint_quote="USDC",
            amount_base=300.0,
            amount_quote=303.0,
            route="jupiter",
            program_id="amm",
            pool="pool-beta",
            signer="trader-b",
            signature="sig-b",
            slot=2,
            ts=base_ts + 60.0,
            fees_base=0.0,
            price_usd=1.01,
            fees_usd=0.04,
            is_self=False,
            buyer="wallet-b",
        )
        await pipeline.submit_market_event(trade_a)
        await pipeline.submit_market_event(trade_b)
        await pipeline.flush_market()

        async def eventually(predicate, *, timeout: float = 2.0) -> None:
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                if predicate():
                    return
                await asyncio.sleep(0.01)
            raise AssertionError("condition not satisfied within timeout")

        await eventually(lambda: len(decisions) == 1)

        assert len(goldens) >= 1
        assert len(suggestions) == 2
        assert len(virtual_fills) == 1
        assert len(virtual_pnls) == 1
        assert len(live_fills) == 1

        assert STREAMS.discovery_candidates in bus.events
        discovery_event = bus.events[STREAMS.discovery_candidates][0]
        assert discovery_event["mint"] == mint

        assert STREAMS.token_snapshot in bus.events
        token_event = bus.events[STREAMS.token_snapshot][0]
        assert token_event["mint"] == mint
        assert token_event["symbol"] == "GOLD"

        assert STREAMS.market_ohlcv in bus.events
        market_events = bus.events[STREAMS.market_ohlcv]
        assert market_events, "expected market OHLCV events"
        market_event = market_events[0]
        expected_close = ((base_ts // 300) * 300) + 300
        assert market_event["mint"] == mint
        assert market_event["trades"] == 2
        assert market_event["buyers"] == 2
        assert market_event["vol_usd"] == pytest.approx(803.0, rel=1e-6)
        assert market_event["asof_close"] == pytest.approx(expected_close, rel=1e-6)

        assert STREAMS.market_depth in bus.events
        depth_events = bus.events[STREAMS.market_depth]
        assert depth_events
        depth_event = depth_events[-1]
        assert depth_event["mint"] == mint
        assert depth_event["venue"] == "aggregated"
        assert depth_event["depth_pct"]["1"] == pytest.approx(20_000.0, rel=1e-6)
        assert depth_event["mid_usd"] == pytest.approx(1.004, rel=1e-3)

        assert STREAMS.golden_snapshot in bus.events
        golden_event = bus.events[STREAMS.golden_snapshot][-1]
        latest_snapshot = goldens[-1]
        assert golden_event["schema_version"] == GOLDEN_SNAPSHOT_SCHEMA_VERSION
        assert golden_event["mint"] == mint
        assert golden_event["hash"] == latest_snapshot.hash
        ohlcv_payload = golden_event["ohlcv5m"]
        assert ohlcv_payload["vol_usd"] == pytest.approx(803.0, rel=1e-6)
        assert ohlcv_payload["buyers"] == 2
        assert ohlcv_payload["flow_usd"] == pytest.approx(803.0, rel=1e-6)
        assert golden_event["liq"]["depth_pct"]["1"] == pytest.approx(20_000.0, rel=1e-6)

        stored_hash = await kv.get(golden_hash_key(mint))
        assert stored_hash == golden_event["hash"]

        assert STREAMS.trade_suggested in bus.events
        suggestion_events = bus.events[STREAMS.trade_suggested]
        assert len(suggestion_events) == 2
        assert all(
            event["schema_version"] == TRADE_SUGGESTION_SCHEMA_VERSION
            for event in suggestion_events
        )
        assert sorted(event["agent"] for event in suggestion_events) == [
            "alpha_agent",
            "beta_agent",
        ]

        assert STREAMS.vote_decisions in bus.events
        decision_events = bus.events[STREAMS.vote_decisions]
        assert len(decision_events) == 1
        decision_event = decision_events[0]
        assert decision_event["schema_version"] == VOTE_DECISION_SCHEMA_VERSION
        assert decision_event["mint"] == mint
        assert decision_event["snapshot_hash"] == golden_event["hash"]
        assert decision_event["agents"] == ["alpha_agent", "beta_agent"]
        assert decision_event["notional_usd"] == pytest.approx(6_250.0, rel=1e-6)
        assert decision_event["score"] == pytest.approx(0.25, rel=1e-6)

        assert STREAMS.virtual_fills in bus.events
        virtual_event = bus.events[STREAMS.virtual_fills][0]
        assert virtual_event["schema_version"] == VIRTUAL_FILL_SCHEMA_VERSION
        assert virtual_event["mint"] == mint
        assert virtual_event["snapshot_hash"] == golden_event["hash"]
        assert virtual_event["qty_base"] == pytest.approx(
            virtual_fills[0].qty_base, rel=1e-6
        )

        assert STREAMS.live_fills in bus.events
        live_event = bus.events[STREAMS.live_fills][0]
        assert live_event["schema_version"] == LIVE_FILL_SCHEMA_VERSION
        assert live_event["mint"] == mint
        assert live_event["snapshot_hash"] == golden_event["hash"]

        metrics = pipeline.metrics_snapshot()
        assert metrics["latency_ms"]["count"] >= 1.0
        assert metrics["candle_age_ms"]["max"] >= latest_snapshot.metrics["candle_age_ms"]

        context_snapshot = pipeline.context.get(golden_event["hash"])
        assert context_snapshot is not None

        pnl = virtual_pnls[0]
        assert pnl.mint == mint
        assert pnl.snapshot_hash == golden_event["hash"]

    asyncio.run(runner())


def test_golden_snapshot_metrics_and_determinism():
    class DeterministicAgent(BaseAgent):
        def __init__(self, name: str) -> None:
            super().__init__(name)
            self._invocations = 0

        async def generate(self, snapshot: GoldenSnapshot):
            self._invocations += 1
            return [
                self.build_suggestion(
                    snapshot=snapshot,
                    side="buy",
                    notional_usd=2_500.0,
                    max_slippage_bps=30.0,
                    risk={"stop_bps": 40.0},
                    confidence=0.8,
                    ttl_sec=2.0,
                )
            ]

    async def runner() -> None:
        goldens: deque[GoldenSnapshot] = deque()
        suggestions: deque[TradeSuggestion] = deque()

        async def on_golden(snapshot: GoldenSnapshot) -> None:
            goldens.append(snapshot)

        async def on_suggestion(suggestion: TradeSuggestion) -> None:
            suggestions.append(suggestion)

        async def fetch_metadata(mints):
            return {
                mint: TokenSnapshot(
                    mint=mint,
                    symbol="DET",
                    name="Deterministic",
                    decimals=6,
                    token_program="Tokenkeg",
                    asof=time.time() - 0.2,
                )
                for mint in mints
            }

        pipeline = GoldenPipeline(
            enrichment_fetcher=fetch_metadata,
            agents=[DeterministicAgent("alpha")],
            on_golden=on_golden,
            on_suggestion=on_suggestion,
            allow_inmemory_bus_for_tests=True,
        )

        mint = "MintDeterministic11111111111111111111111111"
        now = time.time()
        bar = OHLCVBar(
            mint=mint,
            open=1.0,
            high=1.2,
            low=0.9,
            close=1.1,
            vol_usd=500.0,
            trades=10,
            buyers=5,
            flow_usd=100.0,
            zret=0.1,
            zvol=0.2,
            asof_close=now - 0.3,
        )
        depth = DepthSnapshot(
            mint=mint,
            venue="aggregated",
            mid_usd=1.1,
            spread_bps=25.0,
            depth_pct={"1": 20_000.0},
            asof=now - 0.1,
        )

        await pipeline.inject_token_snapshot(
            TokenSnapshot(
                mint=mint,
                symbol="DET",
                name="Deterministic",
                decimals=6,
                token_program="Tokenkeg",
                asof=now - 0.2,
            )
        )
        await pipeline.inject_bar(bar)
        await pipeline.submit_depth(depth)

        assert goldens, "expected a Golden Snapshot emission"
        snapshot = goldens[-1]
        assert snapshot.liq["asof"] == pytest.approx(depth.asof)
        assert snapshot.metrics["depth_staleness_ms"] >= 0.0
        assert snapshot.metrics["latency_ms"] >= 0.0
        assert snapshot.metrics["candle_age_ms"] >= 0.0

        summary = pipeline.metrics_snapshot()
        assert summary["latency_ms"]["count"] >= 1.0
        assert summary["depth_staleness_ms"]["max"] >= snapshot.metrics["depth_staleness_ms"]

        assert suggestions, "agent should have emitted a suggestion"
        first = suggestions[0]
        assert first.generated_at == pytest.approx(snapshot.asof)

        agent_a = DeterministicAgent("alpha")
        agent_b = DeterministicAgent("alpha")
        result_a = await agent_a.generate(snapshot)
        result_b = await agent_b.generate(snapshot)
        assert result_a == result_b

    asyncio.run(runner())


def _build_snapshot(
    *,
    mint: str = "MINT",
    mid: float = 1.5,
    spread_bps: float = 20.0,
    depth_usd: float = 20_000.0,
    buyers: int = 20,
    zret: float = 3.0,
    zvol: float = 2.5,
) -> GoldenSnapshot:
    now = time.time()
    return GoldenSnapshot(
        mint=mint,
        asof=now,
        meta={},
        px={"mid_usd": mid, "spread_bps": spread_bps},
        liq={"depth_pct": {"1": depth_usd}, "asof": now},
        ohlcv5m={
            "o": mid,
            "h": mid,
            "l": mid,
            "c": mid,
            "vol_usd": 0.0,
            "trades": 10,
            "buyers": buyers,
            "flow_usd": 0.0,
            "zret": zret,
            "zvol": zvol,
            "asof_close": now,
        },
        hash="snapshot-hash",
        metrics={},
    )


class _DummyPortfolio:
    pass


class _StaticManager:
    def __init__(self, actions: list[dict[str, Any]]):
        self._actions = actions

    async def evaluate_with_swarm(self, mint: str, portfolio: _DummyPortfolio) -> Any:
        return SimpleNamespace(actions=list(self._actions))


def test_agent_manager_agent_filters_entries() -> None:
    raw_action = {
        "side": "buy",
        "notional_usd": 5_000.0,
        "pattern": "first_pullback",
        "expected_roi": 0.08,
    }
    manager = _StaticManager([raw_action])
    agent = AgentManagerAgent(manager, _DummyPortfolio())
    agent._last_buyers["MINT"] = 5

    snapshot = _build_snapshot(buyers=18)

    async def run() -> list[TradeSuggestion]:
        return await agent.generate(snapshot)

    suggestions = asyncio.run(run())
    assert suggestions, "expected gating to pass"
    suggestion = suggestions[0]
    gating = suggestion.gating
    assert gating["ruthless_filter"]["passed"] is True
    assert gating["edge_pass"] is True
    assert gating["breakeven_bps"] > 0.0
    assert gating["friction_floor"]["edge_buffer_bps"] >= 20.0


def test_agent_manager_agent_drops_when_edge_below_floor() -> None:
    raw_action = {
        "side": "buy",
        "notional_usd": 12_000.0,
        "pattern": "first_pullback",
        "expected_roi": 0.001,
        "fees_bps": 5.0,
    }
    manager = _StaticManager([raw_action])
    agent = AgentManagerAgent(manager, _DummyPortfolio())
    agent._last_buyers["MINT"] = 10

    snapshot = _build_snapshot(depth_usd=18_000.0, buyers=25)
    action = agent._normalise_action(snapshot, raw_action)
    assert action is not None
    gating = agent._apply_entry_gates(snapshot, raw_action, action)
    assert gating["ruthless_filter"]["passed"] is True
    assert gating["edge_pass"] is False

    async def run() -> list[TradeSuggestion]:
        return await agent.generate(snapshot)

    suggestions = asyncio.run(run())
    assert suggestions == []

