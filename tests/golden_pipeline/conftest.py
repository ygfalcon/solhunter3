import asyncio
import collections
import copy
import dataclasses
import time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Sequence

import pytest

from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline.types import (
    Decision,
    DepthSnapshot,
    DiscoveryCandidate,
    GoldenSnapshot,
    PriceQuoteUpdate,
    PriceSnapshot,
    TapeEvent,
    TokenSnapshot,
    TradeSuggestion,
    VirtualFill,
    VirtualPnL,
)
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
from solhunter_zero.golden_pipeline.agents import BaseAgent
from solhunter_zero.golden_pipeline.contracts import STREAMS


BASE58_MINTS: Mapping[str, str] = {
    "alpha": "MintAphex1111111111111111111111111111111",
    "beta": "MintBeton11111111111111111111111111111111",
}


@dataclass(slots=True)
class PriceFixture:
    provider: str
    bid: float
    ask: float
    mid: float
    liquidity: float
    ts_offset: float


@dataclass(slots=True)
class DepthFixture:
    venue: str
    mid: float
    spread_bps: float
    depth_pct: Mapping[str, float]
    ts_offset: float


@dataclass(slots=True)
class TapeFixture:
    signer: str
    signature: str
    slot: int
    price: float
    amount_base: float
    amount_quote: float
    fees_base: float
    fees_quote: float
    route: str
    program_id: str
    buyer: str | None
    ts_offset: float


SCENARIO_PAYLOADS: Mapping[str, Any] = {
    "metadata": {
        BASE58_MINTS["alpha"]: {
            "symbol": "ALPHA",
            "name": "Synthetic Alpha",
            "decimals": 6,
            "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "venues": ["raydium", "orca"],
            "flags": {
                "sources": ["rpc", "das", "mempool", "amm", "pumpfun"],
                "program": "pumpfun1111111111111111111111111111111111",
                "deployer": "AlphaLabs",
                "created_slot": 123456789,
            },
        },
        BASE58_MINTS["beta"]: {
            "symbol": "BETA",
            "name": "Synthetic Beta",
            "decimals": 6,
            "program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
            "venues": ["raydium"],
            "flags": {
                "sources": ["das"],
            },
        },
    },
    "price_updates": [
        PriceFixture("pyth", bid=0.998, ask=1.006, mid=1.002, liquidity=120_000.0, ts_offset=0.0),
        PriceFixture("birdeye", bid=1.000, ask=1.010, mid=1.005, liquidity=90_000.0, ts_offset=0.05),
        PriceFixture("jupiter", bid=1.001, ask=1.008, mid=1.004, liquidity=110_000.0, ts_offset=0.08),
    ],
    "depth_snapshots": [
        DepthFixture(
            venue="raydium",
            mid=1.004,
            spread_bps=32.0,
            depth_pct={"1": 12_000.0, "2": 18_500.0, "5": 30_000.0},
            ts_offset=0.12,
        ),
        DepthFixture(
            venue="orca",
            mid=1.000,
            spread_bps=28.0,
            depth_pct={"1": 8_000.0, "2": 12_500.0, "5": 19_000.0},
            ts_offset=0.16,
        ),
    ],
    "tape_events": [
        TapeFixture(
            signer="alpha-taker",
            signature="sig-alpha-1",
            slot=1001,
            price=1.003,
            amount_base=500.0,
            amount_quote=501.5,
            fees_base=0.0,
            fees_quote=0.25,
            route="jupiter",
            program_id="amm",
            buyer="alpha-taker",
            ts_offset=0.02,
        ),
        TapeFixture(
            signer="alpha-maker",
            signature="sig-alpha-2",
            slot=1002,
            price=1.007,
            amount_base=-320.0,
            amount_quote=-322.24,
            fees_base=0.0,
            fees_quote=0.18,
            route="jupiter",
            program_id="amm",
            buyer=None,
            ts_offset=60.0,
        ),
    ],
    "discovery_plan": {
        "das": [BASE58_MINTS["alpha"], BASE58_MINTS["beta"]],
        "fallback": [BASE58_MINTS["alpha"], BASE58_MINTS["beta"]],
        "mempool": [BASE58_MINTS["alpha"]],
        "amm": [BASE58_MINTS["alpha"]],
        "pumpfun": [BASE58_MINTS["alpha"]],
    },
    "orderbook_path": {
        "venue": "raydium",
        "expected_slippage_bps": 18.0,
        "max_slippage_bps": 45.0,
    },
}


class FakeBroker:
    """Minimal in-process broker with publish tracking."""

    def __init__(self) -> None:
        self.events: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        self.published: list[tuple[str, dict[str, Any]]] = []
        self._closed: list[str] = []

    async def publish(self, stream: str, payload: Mapping[str, Any]) -> None:
        materialised = dict(payload)
        self.published.append((stream, materialised))
        self.events[stream].append(materialised)

    async def healthcheck(self) -> bool:
        return True

    def streams(self) -> Sequence[str]:
        return sorted(self.events.keys())

    def simulate_close(self, stream: str) -> None:
        self._closed.append(stream)

    @property
    def closed_streams(self) -> Sequence[str]:
        return tuple(self._closed)


class FrozenClock:
    """Deterministic clock injected into pipeline modules."""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self._ts = start

    def time(self) -> float:
        return self._ts

    def tick(self, seconds: float) -> None:
        self._ts += seconds


def approx(value: float, rel: float = 1e-6, abs_tol: float = 1e-8) -> pytest.approx:
    return pytest.approx(value, rel=rel, abs=abs_tol)


def assert_tolerant_dict(actual: Mapping[str, Any], expected: Mapping[str, Any]) -> None:
    for key, expected_value in expected.items():
        assert key in actual, f"missing key {key!r}"
        actual_value = actual[key]
        if isinstance(expected_value, Mapping):
            assert isinstance(actual_value, Mapping), f"{key} not mapping"
            assert_tolerant_dict(actual_value, expected_value)
        elif isinstance(expected_value, float):
            assert actual_value == approx(expected_value)
        elif isinstance(expected_value, Iterable) and not isinstance(expected_value, (str, bytes)):
            assert set(actual_value) == set(expected_value)
        else:
            assert actual_value == expected_value


class MomentumAgent(BaseAgent):
    """Deterministic agent that proposes momentum entries."""

    def __init__(self) -> None:
        super().__init__("momentum_v1")

    async def generate(self, snapshot: GoldenSnapshot) -> Sequence[TradeSuggestion]:
        depth1 = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0) or 0.0)
        if depth1 <= 0:
            return []
        notional = depth1 * 0.25
        gating = {
            "edge_pass": True,
            "edge_buffer_bps": 28.0,
            "breakeven_bps": 18.0,
            "expected_edge_bps": 52.0,
        }
        risk = {"expected_edge_bps": 52.0, "breakeven_bps": 18.0}
        suggestion = self.build_suggestion(
            snapshot=snapshot,
            side="buy",
            notional_usd=notional,
            max_slippage_bps=45.0,
            risk=risk,
            confidence=0.22,
            ttl_sec=1.5,
            gating=gating,
            slices=[{"venue": "raydium", "share": 0.7}, {"venue": "orca", "share": 0.3}],
        )
        return [suggestion]


class MeanReversionAgent(BaseAgent):
    """Agent proposing a tempered follow-up entry."""

    def __init__(self) -> None:
        super().__init__("meanrev_v1")

    async def generate(self, snapshot: GoldenSnapshot) -> Sequence[TradeSuggestion]:
        depth1 = float(snapshot.liq.get("depth_pct", {}).get("1", 0.0) or 0.0)
        if depth1 <= 0:
            return []
        notional = depth1 * 0.18
        gating = {
            "edge_pass": True,
            "edge_buffer_bps": 26.0,
            "breakeven_bps": 16.0,
            "expected_edge_bps": 46.0,
        }
        risk = {"expected_edge_bps": 46.0, "breakeven_bps": 16.0}
        suggestion = self.build_suggestion(
            snapshot=snapshot,
            side="buy",
            notional_usd=notional,
            max_slippage_bps=40.0,
            risk=risk,
            confidence=0.18,
            ttl_sec=1.3,
            gating=gating,
            slices=[{"venue": "orca", "share": 0.55}, {"venue": "raydium", "share": 0.45}],
        )
        return [suggestion]


class GoldenPipelineHarness:
    """Orchestrates a synthetic end-to-end Golden pipeline run."""

    def __init__(
        self,
        clock: FrozenClock,
        bus: FakeBroker,
        *,
        scenario: Mapping[str, Any] | None = None,
    ) -> None:
        self.clock = clock
        self.bus = bus
        self._scenario = copy.deepcopy(scenario or SCENARIO_PAYLOADS)
        self.metadata_requests: list[list[str]] = []
        self.discovery_events: list[dict[str, Any]] = []
        self.golden_snapshots: list[GoldenSnapshot] = []
        self.trade_suggestions: list[TradeSuggestion] = []
        self.decisions: list[Decision] = []
        self.virtual_fills: list[VirtualFill] = []
        self.virtual_pnls: list[VirtualPnL] = []
        self.price_snapshots: list[PriceSnapshot] = []
        self._summary_cache: dict[str, Any] | None = None

        async def fetcher(mints: Iterable[str]) -> Dict[str, TokenSnapshot]:
            requested = [str(m) for m in mints]
            self.metadata_requests.append(requested)
            snapshots: Dict[str, TokenSnapshot] = {}
            now = self.clock.time()
            for mint in requested:
                meta = self._scenario["metadata"][mint]
                snapshots[mint] = TokenSnapshot(
                    mint=mint,
                    symbol=meta["symbol"],
                    name=meta["name"],
                    decimals=meta["decimals"],
                    token_program=meta["program"],
                    venues=tuple(meta.get("venues", [])),
                    flags=dict(meta.get("flags", {})),
                    asof=now,
                )
            return snapshots

        async def on_golden(snapshot: GoldenSnapshot) -> None:
            self.golden_snapshots.append(snapshot)

        async def on_suggestion(suggestion: TradeSuggestion) -> None:
            self.trade_suggestions.append(suggestion)

        async def on_decision(decision: Decision) -> None:
            self.decisions.append(decision)

        async def on_virtual_fill(fill: VirtualFill) -> None:
            self.virtual_fills.append(fill)

        async def on_virtual_pnl(pnl: VirtualPnL) -> None:
            self.virtual_pnls.append(pnl)

        async def on_price(snapshot: PriceSnapshot) -> None:
            self.price_snapshots.append(snapshot)

        agents = [MomentumAgent(), MeanReversionAgent()]
        self.pipeline = GoldenPipeline(
            enrichment_fetcher=fetcher,
            agents=agents,
            on_golden=on_golden,
            on_suggestion=on_suggestion,
            on_decision=on_decision,
            on_price=on_price,
            on_virtual_fill=on_virtual_fill,
            on_virtual_pnl=on_virtual_pnl,
            bus=bus,
            kv=InMemoryKeyValueStore(),
            max_agent_spread_bps=120.0,
            min_agent_depth1_usd=5_000.0,
            vote_window_ms=320,
            vote_quorum=2,
            vote_min_score=0.05,
        )

    async def run(self) -> None:
        await self._run_discovery_phase()
        await self._run_market_phase()
        await self._run_price_phase()
        await self._run_depth_phase()
        await self._await_voting(expected_decisions=1)
        await self._mutate_metadata_and_prices()
        await self._await_voting(expected_decisions=2)
        await self._exercise_resilience_checks()

    async def _run_discovery_phase(self) -> None:
        plan = self._scenario["discovery_plan"]
        stage = self.pipeline._discovery_stage  # type: ignore[attr-defined]

        for mint in plan["das"]:
            candidate = DiscoveryCandidate(mint=mint, asof=self.clock.time())
            accepted = await self.pipeline.submit_discovery(candidate)
            self.discovery_events.append({"source": "das", "mint": mint, "accepted": accepted})
            self.clock.tick(0.01)

        for _ in range(3):
            stage.mark_failure()
            self.discovery_events.append({"source": "das_timeout", "mint": None, "accepted": None})
            self.clock.tick(0.005)

        for source in ("fallback", "mempool", "amm", "pumpfun"):
            for mint in plan[source]:
                candidate = DiscoveryCandidate(mint=mint, asof=self.clock.time())
                accepted = await self.pipeline.submit_discovery(candidate)
                self.discovery_events.append({"source": source, "mint": mint, "accepted": accepted})
                self.clock.tick(0.01)

    async def _run_market_phase(self) -> None:
        mint = BASE58_MINTS["alpha"]
        for fixture in self._scenario["tape_events"]:
            self.clock.tick(fixture.ts_offset)
            event = TapeEvent(
                mint_base=mint,
                mint_quote="USDC",
                amount_base=fixture.amount_base,
                amount_quote=fixture.amount_quote,
                route=fixture.route,
                program_id=fixture.program_id,
                pool="alpha-pool",
                signer=fixture.signer,
                signature=fixture.signature,
                slot=fixture.slot,
                ts=self.clock.time(),
                fees_base=fixture.fees_base,
                price_usd=fixture.price,
                fees_usd=fixture.fees_quote,
                is_self=False,
                buyer=fixture.buyer,
            )
            await self.pipeline.submit_market_event(event)
        self.clock.tick(320.0)
        await self.pipeline.flush_market()

    async def _run_price_phase(self) -> None:
        mint = BASE58_MINTS["alpha"]
        fixtures = self._scenario.get("price_updates", [])
        for fixture in fixtures:
            self.clock.tick(fixture.ts_offset)
            quote = PriceQuoteUpdate(
                mint=mint,
                source=fixture.provider,
                bid_usd=fixture.bid,
                ask_usd=fixture.ask,
                mid_usd=fixture.mid,
                liquidity=fixture.liquidity,
                asof=self.clock.time(),
                extras={"scenario": "synthetic"},
            )
            await self.pipeline.submit_price(quote)
        idle_gap = float(self._scenario.get("price_idle_gap", 0.0) or 0.0)
        if idle_gap:
            self.clock.tick(idle_gap)

    async def _run_depth_phase(self) -> None:
        mint = BASE58_MINTS["alpha"]
        for fixture in self._scenario["depth_snapshots"]:
            self.clock.tick(fixture.ts_offset)
            snapshot = DepthSnapshot(
                mint=mint,
                venue=fixture.venue,
                mid_usd=fixture.mid,
                spread_bps=fixture.spread_bps,
                depth_pct=dict(fixture.depth_pct),
                asof=self.clock.time() - 0.012,
            )
            await self.pipeline.submit_depth(snapshot)

    async def _mutate_metadata_and_prices(self) -> None:
        mint = BASE58_MINTS["alpha"]
        latest = self.golden_snapshots[-1]
        meta = self._scenario["metadata"][mint]
        self.clock.tick(0.25)
        mutated = TokenSnapshot(
            mint=mint,
            symbol=meta["symbol"],
            name=meta["name"],
            decimals=meta["decimals"],
            token_program=meta["program"],
            venues=tuple(meta.get("venues", [])),
            flags={"note": "noop-mutation", **meta.get("flags", {})},
            asof=float(latest.meta.get("asof", self.clock.time())),
        )
        await self.pipeline.inject_token_snapshot(mutated)
        assert self.golden_snapshots[-1].hash == latest.hash

        depth_fixture = dataclasses.replace(
            self._scenario["depth_snapshots"][0],
            mid=self._scenario["depth_snapshots"][0].mid + 0.008,
            ts_offset=0.05,
        )
        self.clock.tick(depth_fixture.ts_offset)
        updated_depth = DepthSnapshot(
            mint=mint,
            venue=depth_fixture.venue,
            mid_usd=depth_fixture.mid,
            spread_bps=depth_fixture.spread_bps,
            depth_pct=dict(depth_fixture.depth_pct),
            asof=self.clock.time() - 0.01,
        )
        await self.pipeline.submit_depth(updated_depth)

    async def _await_voting(self, *, expected_decisions: int) -> None:
        window = self.pipeline._voting_stage.window_sec  # type: ignore[attr-defined]
        self.clock.tick(window + 0.05)
        await asyncio.sleep(window + 0.05)
        await self._wait_until(lambda: len(self.decisions) >= expected_decisions)

    async def _exercise_resilience_checks(self) -> None:
        mint = BASE58_MINTS["alpha"]
        candidate = DiscoveryCandidate(mint=mint, asof=self.clock.time())
        deduped = await self.pipeline.submit_discovery(candidate)
        self.discovery_events.append({"source": "replay", "mint": mint, "accepted": deduped})

        depth_fixture = self._scenario["depth_snapshots"][1]
        self.clock.tick(0.2)
        latest_snapshot = self.golden_snapshots[-1]
        replay_depth = DepthSnapshot(
            mint=mint,
            venue=depth_fixture.venue,
            mid_usd=depth_fixture.mid,
            spread_bps=depth_fixture.spread_bps,
            depth_pct=dict(depth_fixture.depth_pct),
            asof=float(latest_snapshot.liq.get("asof", self.clock.time())),
        )
        await self.pipeline.submit_depth(replay_depth)

    async def _wait_until(self, predicate, *, timeout: float = 2.0) -> None:
        start = time.monotonic()
        while time.monotonic() - start < timeout:
            if predicate():
                return
            await asyncio.sleep(0.01)
        raise AssertionError("condition not met within timeout")

    def summary(self) -> Mapping[str, Any]:
        if self._summary_cache is not None:
            return self._summary_cache
        discovery_counts = collections.Counter()
        for entry in self.discovery_events:
            if entry.get("accepted"):
                discovery_counts[entry["source"]] += 1
        snapshot_counts = collections.Counter(s.hash for s in self.golden_snapshots)
        suggestion_counts = collections.Counter(s.agent for s in self.trade_suggestions)
        decision_counts = collections.Counter(d.side for d in self.decisions)
        fill_counts = collections.Counter(f.route for f in self.virtual_fills)
        pnl_total = self.virtual_pnls[-1] if self.virtual_pnls else None
        self._summary_cache = {
            "discoveries_by_source": dict(discovery_counts),
            "snapshots_by_hash": dict(snapshot_counts),
            "suggestions_by_agent": dict(suggestion_counts),
            "decisions_by_side": dict(decision_counts),
            "shadow_fills_by_venue": dict(fill_counts),
            "paper_pnl": {
                "count": len(self.virtual_pnls),
                "latest_realized": pnl_total.realized_usd if pnl_total else 0.0,
                "latest_unrealized": pnl_total.unrealized_usd if pnl_total else 0.0,
            },
        }
        return self._summary_cache


@pytest.fixture(scope="module")
def golden_harness() -> Iterable[GoldenPipelineHarness]:
    clock = FrozenClock()
    monkeypatch = pytest.MonkeyPatch()
    for target in (
        "solhunter_zero.golden_pipeline.utils.now_ts",
        "solhunter_zero.golden_pipeline.market.now_ts",
        "solhunter_zero.golden_pipeline.coalescer.now_ts",
        "solhunter_zero.golden_pipeline.execution.now_ts",
        "solhunter_zero.golden_pipeline.agents.now_ts",
        "solhunter_zero.golden_pipeline.pricing.now_ts",
        "solhunter_zero.golden_pipeline.voting.now_ts",
    ):
        monkeypatch.setattr(target, clock.time, raising=False)
    bus = FakeBroker()
    harness = GoldenPipelineHarness(clock=clock, bus=bus)
    try:
        asyncio.run(harness.run())
        yield harness
    finally:
        monkeypatch.undo()


@pytest.fixture(scope="module")
def frozen_clock(golden_harness: GoldenPipelineHarness) -> FrozenClock:
    return golden_harness.clock


@pytest.fixture(scope="module")
def fake_broker(golden_harness: GoldenPipelineHarness) -> FakeBroker:
    return golden_harness.bus
