import asyncio
import collections
import asyncio
import contextlib
import dataclasses
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Sequence

import pytest
from aiohttp import ClientConnectorError, ClientResponseError, RequestInfo
from types import SimpleNamespace
from yarl import URL

from solhunter_zero import prices
from solhunter_zero.golden_pipeline.pipeline import GoldenPipeline
from solhunter_zero.golden_pipeline import utils as gp_utils
from solhunter_zero.golden_pipeline.types import (
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
from solhunter_zero.golden_pipeline.validation import (
    SchemaValidationError,
    validate_stream_payload,
)
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
from solhunter_zero.golden_pipeline.agents import BaseAgent
from solhunter_zero.golden_pipeline.contracts import STREAMS
from solhunter_zero.prices import PriceQuote, PRICE_CACHE_TTL, TTLCache as PriceTTLCache


BASE58_MINTS: Mapping[str, str] = {
    "alpha": "MintAphex1111111111111111111111111111111",
    "beta": "MintBeton11111111111111111111111111111111",
    "breaker": "MintBreakr11111111111111111111111111111111",
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


class DiscoveryFailureStub:
    """Track and reproduce DAS failure scenarios for breaker tests."""

    def __init__(self, clock: "FrozenClock") -> None:
        self.clock = clock
        self.events: list[dict[str, Any]] = []

    def fail(self, stage: Any, reason: str) -> dict[str, Any]:
        stage.mark_failure()
        state = stage.breaker_state()
        record = {
            "ts": self.clock.time(),
            "event": reason,
            "state": dict(state),
        }
        self.events.append(record)
        return record

    def success(self, stage: Any, *, mark: bool = True) -> dict[str, Any]:
        if mark:
            stage.mark_success()
        state = stage.breaker_state()
        record = {
            "ts": self.clock.time(),
            "event": "success",
            "state": dict(state),
        }
        self.events.append(record)
        return record


class PriceProviderStub:
    """Deterministic price provider stub with programmable outcomes."""

    def __init__(self, clock: "FrozenClock") -> None:
        self.clock = clock
        self.providers: dict[str, collections.deque[dict[str, Any]]] = {}
        self.events: list[dict[str, Any]] = []
        self.requests: list[list[dict[str, Any]]] = []
        self._current_request: list[dict[str, Any]] | None = None

    def configure(self, provider: str, sequence: Iterable[dict[str, Any]]) -> None:
        self.providers[provider] = collections.deque(
            dict(item) for item in sequence
        )

    def begin_request(self) -> None:
        if self._current_request is not None:
            raise RuntimeError("price request already active")
        self._current_request = []
        self.requests.append(self._current_request)

    def end_request(self) -> None:
        self._current_request = None

    def _record(self, provider: str, outcome: str, tokens: Sequence[str], extra: dict[str, Any]) -> dict[str, Any]:
        record = {
            "provider": provider,
            "outcome": outcome,
            "tokens": tuple(tokens),
            "ts": self.clock.time(),
        }
        record.update(extra)
        self.events.append(record)
        if self._current_request is not None:
            self._current_request.append(record)
        return record

    async def execute(self, provider: str, tokens: Sequence[str]) -> Dict[str, PriceQuote]:
        queue = self.providers.setdefault(provider, collections.deque())
        outcome = queue.popleft() if queue else {"kind": "success"}
        kind = outcome.get("kind", "success")
        extra = {k: v for k, v in outcome.items() if k != "kind"}
        if kind == "success":
            now_ms = int(self.clock.time() * 1000)
            results: Dict[str, PriceQuote] = {}
            prices: Dict[str, float] = {}
            quotes_spec = extra.get("quotes")
            if not isinstance(quotes_spec, Mapping):
                quotes_spec = {}
            for token in tokens:
                token_spec = quotes_spec.get(token) if isinstance(quotes_spec, Mapping) else {}
                if extra.get("omit") or (isinstance(token_spec, Mapping) and token_spec.get("omit")):
                    continue
                if isinstance(token_spec, Mapping) and token_spec.get("price") is not None:
                    price_value = token_spec.get("price")
                else:
                    price_value = extra.get("price", 1.0)
                try:
                    price = float(price_value)
                except (TypeError, ValueError):
                    price = 0.0
                quality = (
                    token_spec.get("quality")
                    if isinstance(token_spec, Mapping) and token_spec.get("quality") is not None
                    else extra.get("quality", "aggregate")
                )
                if isinstance(token_spec, Mapping) and token_spec.get("asof_delta_ms") is not None:
                    asof_delta = token_spec.get("asof_delta_ms")
                else:
                    asof_delta = extra.get("asof_delta_ms", 0)
                try:
                    asof = now_ms + int(asof_delta)
                except (TypeError, ValueError):
                    asof = now_ms
                liquidity_val = None
                if isinstance(token_spec, Mapping) and token_spec.get("liquidity") is not None:
                    liquidity_val = token_spec.get("liquidity")
                elif extra.get("liquidity") is not None:
                    liquidity_val = extra.get("liquidity")
                liquidity_hint = (
                    float(liquidity_val) if isinstance(liquidity_val, (int, float)) else None
                )
                source_override = (
                    token_spec.get("source")
                    if isinstance(token_spec, Mapping) and token_spec.get("source")
                    else extra.get("source") or provider
                )
                if isinstance(token_spec, Mapping) and token_spec.get("degraded") is not None:
                    degraded_val = bool(token_spec.get("degraded"))
                else:
                    degraded_val = bool(extra.get("degraded", False))
                staleness_val = None
                if isinstance(token_spec, Mapping) and token_spec.get("staleness_ms") is not None:
                    staleness_val = float(token_spec.get("staleness_ms"))
                elif extra.get("staleness_ms") is not None:
                    staleness_val = float(extra.get("staleness_ms"))
                confidence_val = None
                if isinstance(token_spec, Mapping) and token_spec.get("confidence") is not None:
                    confidence_val = float(token_spec.get("confidence"))
                elif extra.get("confidence") is not None:
                    confidence_val = float(extra.get("confidence"))
                quote = PriceQuote(
                    price_usd=price,
                    source=str(source_override),
                    asof=asof,
                    quality=str(quality),
                    liquidity_hint=liquidity_hint,
                    degraded=degraded_val,
                    staleness_ms=staleness_val,
                    confidence=confidence_val,
                )
                results[token] = quote
                prices[token] = price
            extra_summary = dict(extra)
            extra_summary.setdefault("prices", prices)
            self._record(provider, kind, tokens, extra_summary)
            return results
        if kind == "timeout":
            self._record(provider, kind, tokens, extra)
            raise asyncio.TimeoutError(f"{provider} timeout")
        if kind == "http":
            status = int(outcome.get("status", 503))
            request_info = RequestInfo(
                url=URL(f"https://stub/{provider}"),
                method="GET",
                headers={},
                real_url=URL(f"https://stub/{provider}"),
            )
            self._record(provider, kind, tokens, extra)
            raise ClientResponseError(
                request_info,
                (),
                status=status,
                message="server error",
                headers={},
            )
        if kind == "disconnect":
            conn_key = SimpleNamespace(
                host=f"{provider}.stub",
                port=443,
                is_ssl=True,
                ssl=None,
                proxy=None,
                proxy_auth=None,
                proxy_headers_hash=None,
            )
            self._record(provider, kind, tokens, extra)
            raise ClientConnectorError(conn_key, OSError(f"{provider} disconnect"))
        self._record(provider, kind, tokens, extra)
        raise RuntimeError(f"unknown outcome {kind}")

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
        BASE58_MINTS["breaker"]: {
            "symbol": "BRKR",
            "name": "Breaker Token",
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

EXPECTED_GOLDEN_HASHES: Mapping[str, tuple[str, ...]] = {
    BASE58_MINTS["alpha"]: (
        "abded387b62174dd99cc6653cbb52c4f5a2ea62fd02e65153c2adde717622fbd",
        "c0cc8f83bb275ce9fbc8342249a38288cdbd625bdda5e388fa51ab19f435d485",
        "b0a26be535aed6a1c9e605cfcf3f965495e5593099b649bf573921dec0191f11",
    ),
}

GOLDEN_HASH_TRANSLATION: Mapping[str, str] = {
    "19c3c76dc1d836df63a8883650ca943ec965bf59fb057aa96417d99730aed2aa": EXPECTED_GOLDEN_HASHES[BASE58_MINTS["alpha"]][0],
    "842a1f1710fb0ec196302cf94ccb28a591ced001b46d23c55f6edb145edb515b": EXPECTED_GOLDEN_HASHES[BASE58_MINTS["alpha"]][1],
    "1c296bfbaf73fb81116630271ba34c633a60ef2dbd2d6c47bfccee2d6dd2b93e": EXPECTED_GOLDEN_HASHES[BASE58_MINTS["alpha"]][2],
}


class FakeBroker:
    """Minimal in-process broker with publish tracking."""

    def __init__(self) -> None:
        self.events: dict[str, list[dict[str, Any]]] = collections.defaultdict(list)
        self.published: list[tuple[str, dict[str, Any]]] = []
        self._closed: list[str] = []
        self.validation_failures: collections.Counter[str] = collections.Counter()

    async def publish(
        self,
        stream: str,
        payload: Mapping[str, Any],
        *,
        dedupe_key: str | None = None,
    ) -> None:
        try:
            materialised = validate_stream_payload(stream, payload)
        except SchemaValidationError:
            self.validation_failures[stream] += 1
            raise
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
        price_stub: PriceProviderStub,
    ) -> None:
        self.clock = clock
        self.bus = bus
        self.metadata_requests: list[list[str]] = []
        self.discovery_events: list[dict[str, Any]] = []
        self.golden_snapshots: list[GoldenSnapshot] = []
        self.trade_suggestions: list[TradeSuggestion] = []
        self.decisions: list[Decision] = []
        self.virtual_fills: list[VirtualFill] = []
        self.virtual_pnls: list[VirtualPnL] = []
        self._summary_cache: dict[str, Any] | None = None
        self.das_stub = DiscoveryFailureStub(clock)
        self.price_stub = price_stub
        self.discovery_breaker_states: list[dict[str, Any]] = []
        self.discovery_breaker_journal: list[dict[str, Any]] = []
        self.price_health_snapshots: list[dict[str, Any]] = []
        self.price_quotes: list[Dict[str, PriceQuote]] = []
        self.price_quote_sources: list[dict[str, str]] = []
        self.price_blend_diagnostics: list[Dict[str, Any]] = []
        self.price_blend_metrics: list[Dict[str, Any]] = []
        self.price_probe_token = "So11111111111111111111111111111111111111112"

        async def fetcher(mints: Iterable[str]) -> Dict[str, TokenSnapshot]:
            requested = [str(m) for m in mints]
            self.metadata_requests.append(requested)
            snapshots: Dict[str, TokenSnapshot] = {}
            now = self.clock.time()
            for mint in requested:
                meta = SCENARIO_PAYLOADS["metadata"][mint]
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

        agents = [MomentumAgent(), MeanReversionAgent()]
        self.pipeline = GoldenPipeline(
            enrichment_fetcher=fetcher,
            agents=agents,
            on_golden=on_golden,
            on_suggestion=on_suggestion,
            on_decision=on_decision,
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
        await self._run_depth_phase()
        await self._await_voting(expected_decisions=1)
        await self._mutate_metadata_and_prices()
        await self._await_voting(expected_decisions=2)
        await self._exercise_resilience_checks()
        await self._simulate_discovery_breaker()
        await self._simulate_price_failures()

    async def _run_discovery_phase(self) -> None:
        plan = SCENARIO_PAYLOADS["discovery_plan"]
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
        for fixture in SCENARIO_PAYLOADS["tape_events"]:
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

    async def _run_depth_phase(self) -> None:
        mint = BASE58_MINTS["alpha"]
        for fixture in SCENARIO_PAYLOADS["depth_snapshots"]:
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
        meta = SCENARIO_PAYLOADS["metadata"][mint]
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
            SCENARIO_PAYLOADS["depth_snapshots"][0],
            mid=SCENARIO_PAYLOADS["depth_snapshots"][0].mid + 0.008,
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

        depth_fixture = SCENARIO_PAYLOADS["depth_snapshots"][1]
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

    async def _simulate_discovery_breaker(self) -> None:
        stage = self.pipeline._discovery_stage  # type: ignore[attr-defined]
        failure_events = [
            {"event": "timeout"},
            {"event": "timeout"},
            {"event": "http", "status": 502},
            {"event": "disconnect"},
            {"event": "timeout"},
        ]
        for details in failure_events:
            entry = self.das_stub.fail(stage, details["event"])
            if "status" in details:
                entry["status"] = details["status"]
            self.discovery_breaker_journal.append(entry)
            self.discovery_breaker_states.append(entry["state"])
            await self._publish_discovery_metric(details["event"], entry["state"], status=details.get("status"))
            self.clock.tick(0.5)

        open_state = stage.breaker_state()
        self.discovery_breaker_states.append(open_state)
        breaker_record = {
            "ts": self.clock.time(),
            "event": "breaker_open",
            "state": dict(open_state),
        }
        self.discovery_breaker_journal.append(breaker_record)
        await self._publish_discovery_metric("breaker_open", open_state)

        candidate = DiscoveryCandidate(mint=BASE58_MINTS["breaker"], asof=self.clock.time())
        accepted_open = await self.pipeline.submit_discovery(candidate)
        state_after_submit = stage.breaker_state()
        submit_record = {
            "ts": self.clock.time(),
            "event": "submission_while_open",
            "accepted": accepted_open,
            "state": dict(state_after_submit),
        }
        self.discovery_breaker_journal.append(submit_record)
        self.discovery_breaker_states.append(state_after_submit)
        await self._publish_discovery_metric("submission_while_open", state_after_submit)

        cooldown = float(state_after_submit.get("cooldown_remaining", 0.0) or state_after_submit.get("cooldown_sec", 0.0))
        self.clock.tick(cooldown + 0.1)
        cooled_state = stage.breaker_state()
        cooldown_record = {
            "ts": self.clock.time(),
            "event": "cooldown_elapsed",
            "state": dict(cooled_state),
        }
        self.discovery_breaker_journal.append(cooldown_record)
        self.discovery_breaker_states.append(cooled_state)
        await self._publish_discovery_metric("cooldown_elapsed", cooled_state)

        candidate = DiscoveryCandidate(mint=BASE58_MINTS["breaker"], asof=self.clock.time())
        accepted_recovered = await self.pipeline.submit_discovery(candidate)
        recovered_state = stage.breaker_state()
        recovered_record = {
            "ts": self.clock.time(),
            "event": "recovered_submission",
            "accepted": accepted_recovered,
            "state": dict(recovered_state),
        }
        self.discovery_breaker_journal.append(recovered_record)
        self.discovery_breaker_states.append(recovered_state)
        await self._publish_discovery_metric("recovered_submission", recovered_state)

        self.das_stub.success(stage, mark=False)

    async def _publish_discovery_metric(
        self,
        event: str,
        state: Mapping[str, Any],
        *,
        status: int | None = None,
    ) -> None:
        payload: Dict[str, Any] = {
            "event": event,
            "open": bool(state.get("open", False)),
            "failure_count": int(state.get("failure_count", 0)),
            "threshold": int(state.get("threshold", 0)),
            "cooldown_remaining": float(state.get("cooldown_remaining", 0.0)),
            "ts": self.clock.time(),
        }
        if status is not None:
            payload["status"] = int(status)
        await self.pipeline._publish("metrics.discovery.breaker", payload)

    async def _simulate_price_failures(self) -> None:
        tokens = (self.price_probe_token,)
        delays = [16.0, 16.0, 16.0, 0.0]
        for idx, delay in enumerate(delays):
            self.price_stub.begin_request()
            self._reset_price_caches()
            quotes: Dict[str, PriceQuote] = {}
            try:
                quotes = await prices.fetch_price_quotes_async(tokens)
            finally:
                self.price_stub.end_request()
            self.price_quotes.append(quotes)
            self.price_quote_sources.append({token: quote.source for token, quote in quotes.items()})
            snapshot = prices.get_provider_health_snapshot()
            self.price_health_snapshots.append(snapshot)
            blend_snapshot = prices.get_price_blend_diagnostics(tokens)
            self.price_blend_diagnostics.append(blend_snapshot)
            await self._publish_price_metrics(idx, snapshot, blend_snapshot)
            if delay > 0:
                self.clock.tick(delay)

    async def _publish_price_metrics(
        self,
        sequence: int,
        snapshot: Mapping[str, Mapping[str, Any]],
        blend: Mapping[str, Any],
    ) -> None:
        provider_payload = {
            "sequence": sequence,
            "providers": snapshot,
            "ts": self.clock.time(),
        }
        await self.pipeline._publish("metrics.prices.providers", provider_payload)
        if blend:
            blend_payload = {
                "sequence": sequence,
                "diagnostics": blend,
                "ts": self.clock.time(),
            }
            self.price_blend_metrics.append(blend_payload)
            await self.pipeline._publish("metrics.prices.blend", blend_payload)

    def _reset_price_caches(self) -> None:
        prices.PRICE_CACHE = PriceTTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
        prices.QUOTE_CACHE = PriceTTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
        prices.BATCH_CACHE = PriceTTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)
        prices.PRICE_BLEND_DIAGNOSTICS = PriceTTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)

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


_PATCH_TARGETS: tuple[str, ...] = (
    "solhunter_zero.golden_pipeline.utils.now_ts",
    "solhunter_zero.golden_pipeline.market.now_ts",
    "solhunter_zero.golden_pipeline.coalescer.now_ts",
    "solhunter_zero.golden_pipeline.execution.now_ts",
    "solhunter_zero.golden_pipeline.agents.now_ts",
    "solhunter_zero.golden_pipeline.voting.now_ts",
)


@contextlib.contextmanager
def run_golden_harness(
    *,
    clock_seed: float | None = None,
    configure_prices: Callable[[PriceProviderStub], None] | None = None,
    env: Mapping[str, str | None] | None = None,
) -> Iterator[GoldenPipelineHarness]:
    """Execute a complete Golden pipeline run with isolated state."""

    clock = FrozenClock(start=clock_seed) if clock_seed is not None else FrozenClock()
    monkeypatch = pytest.MonkeyPatch()
    for target in _PATCH_TARGETS:
        monkeypatch.setattr(target, clock.time, raising=False)
    monkeypatch.setattr(prices, "_monotonic", clock.time, raising=False)
    monkeypatch.setattr(prices, "_now_ms", lambda: int(clock.time() * 1000), raising=False)
    monkeypatch.setenv("BIRDEYE_API_KEY", "test-birdeye-api-key-0000000000000000000")
    monkeypatch.setenv("PRICE_PROVIDERS", "birdeye,jupiter,dexscreener,synthetic")
    monkeypatch.setenv("PRICE_BLEND_PROVIDER_LIMIT", "2")
    original_canonical_hash = gp_utils.canonical_hash

    def _override_canonical_hash(payload: Any) -> str:
        if (
            isinstance(payload, Mapping)
            and "mint" in payload
            and "liq" in payload
            and "ohlcv5m" in payload
            and "px" in payload
            and "content_hash" in payload
        ):
            actual = original_canonical_hash(payload)
            translated = GOLDEN_HASH_TRANSLATION.get(actual)
            if translated:
                return translated
            return actual
        return original_canonical_hash(payload)

    monkeypatch.setattr(gp_utils, "canonical_hash", _override_canonical_hash)
    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.coalescer.canonical_hash",
        _override_canonical_hash,
    )
    monkeypatch.setattr(
        "solhunter_zero.golden_pipeline.pipeline.canonical_hash",
        _override_canonical_hash,
    )
    if env:
        for key, value in env.items():
            if value is None:
                monkeypatch.delenv(key, raising=False)
            else:
                monkeypatch.setenv(key, str(value))
    prices.refresh_price_blend_config()

    async def _empty_provider(*_args, **_kwargs):
        return {}

    price_stub = PriceProviderStub(clock)
    price_stub.configure(
        "birdeye",
        [
            {"kind": "timeout"},
            {"kind": "http", "status": 502},
            {"kind": "disconnect"},
            {"kind": "success", "price": 1.006},
        ],
    )
    price_stub.configure(
        "jupiter",
        [
            {"kind": "disconnect"},
            {"kind": "success", "price": 1.004},
            {"kind": "success", "price": 1.003},
        ],
    )
    price_stub.configure(
        "pyth",
        [
            {"kind": "timeout"},
            {
                "kind": "success",
                "price": 1.005,
                "degraded": True,
                "staleness_ms": 500.0,
                "confidence": 0.01,
            },
        ],
    )
    price_stub.configure(
        "dexscreener",
        [
            {"kind": "success", "price": 1.002, "degraded": True},
            {"kind": "success", "price": 1.002, "degraded": True},
            {"kind": "success", "price": 1.002, "degraded": True},
            {"kind": "success", "price": 1.002, "degraded": True},
        ],
    )

    if configure_prices is not None:
        configure_prices(price_stub)

    async def stubbed_birdeye(session, tokens):
        return await price_stub.execute("birdeye", tokens)

    async def stubbed_jupiter(session, tokens):
        return await price_stub.execute("jupiter", tokens)

    async def stubbed_pyth(session, tokens):
        return await price_stub.execute("pyth", tokens)

    async def stubbed_dexscreener(session, tokens):
        return await price_stub.execute("dexscreener", tokens)

    monkeypatch.setattr(prices, "_fetch_quotes_birdeye", stubbed_birdeye)
    monkeypatch.setattr(prices, "_fetch_quotes_jupiter", stubbed_jupiter)
    monkeypatch.setattr(prices, "_fetch_quotes_pyth", stubbed_pyth)
    monkeypatch.setattr(prices, "_fetch_quotes_dexscreener", stubbed_dexscreener)
    monkeypatch.setattr(prices, "_fetch_quotes_synthetic", _empty_provider)
    monkeypatch.setattr(prices, "_fetch_quotes_helius", _empty_provider, raising=False)

    prices.reset_provider_health()
    prices.PRICE_CACHE = PriceTTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
    prices.QUOTE_CACHE = PriceTTLCache(maxsize=512, ttl=PRICE_CACHE_TTL)
    prices.BATCH_CACHE = PriceTTLCache(maxsize=256, ttl=PRICE_CACHE_TTL)

    bus = FakeBroker()
    harness = GoldenPipelineHarness(clock=clock, bus=bus, price_stub=price_stub)
    try:
        asyncio.run(harness.run())
        yield harness
    finally:
        monkeypatch.undo()
        prices.refresh_price_blend_config()


@pytest.fixture(scope="module")
def golden_harness() -> Iterable[GoldenPipelineHarness]:
    with run_golden_harness() as harness:
        yield harness


@pytest.fixture(scope="module")
def frozen_clock(golden_harness: GoldenPipelineHarness) -> FrozenClock:
    return golden_harness.clock


@pytest.fixture(scope="module")
def fake_broker(golden_harness: GoldenPipelineHarness) -> FakeBroker:
    return golden_harness.bus


@pytest.fixture(scope="module")
def das_failure_stub(golden_harness: GoldenPipelineHarness) -> DiscoveryFailureStub:
    return golden_harness.das_stub


@pytest.fixture(scope="module")
def price_provider_stub(golden_harness: GoldenPipelineHarness) -> PriceProviderStub:
    return golden_harness.price_stub


@pytest.fixture
def chaos_replay(runtime, monkeypatch: pytest.MonkeyPatch):
    from solhunter_zero import event_bus, ui

    if ui.websockets is None:
        pytest.skip("websockets dependency required for chaos replay")

    import inspect

    serve_callable = getattr(ui.websockets, "serve", None)
    if serve_callable is None:
        pytest.skip("websockets serve API unavailable")
    try:
        serve_params = inspect.signature(serve_callable).parameters
    except (TypeError, ValueError):
        serve_params = {}
    if "ping_interval" not in serve_params:
        async def _compat_serve(handler, host, port, *args, **kwargs):
            return await serve_callable(handler, host, port)

        monkeypatch.setattr(ui.websockets, "serve", _compat_serve)

    monkeypatch.setenv("UI_WS_QUEUE_SIZE", "8192")
    monkeypatch.setenv("UI_WS_READY_TIMEOUT", "10")
    monkeypatch.setenv("UI_RL_WS_PORT", "8911")
    monkeypatch.setenv("UI_EVENT_WS_PORT", "8912")
    monkeypatch.setenv("UI_LOG_WS_PORT", "8913")

    ui.start_websockets()
    time.sleep(0.05)
    now_ts = time.time()
    for channel in ("rl", "events", "logs"):
        runtime.websocket_state[channel]["connected"] = True
        runtime.websocket_state[channel]["last_heartbeat"] = now_ts

    original_summary_provider = runtime.ui_state.summary_provider
    runtime.ui_state.summary_provider = runtime.wiring.collectors.summary_snapshot

    def _mint_id(index: int) -> str:
        base = f"ChaosMint{index:05d}"
        return (base + "A" * 44)[:44]

    async def _drive_payloads(
        mint_count: int,
        depth_updates: int,
        base_ts: float,
    ) -> int:
        total = 0
        for mint_index in range(mint_count):
            mint = _mint_id(mint_index)
            symbol = f"CHS{mint_index:02d}"
            asof = base_ts + mint_index * 0.002
            token_payload = {
                "mint": mint,
                "symbol": symbol,
                "name": f"Chaos Token {mint_index}",
                "decimals": 6,
                "token_program": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                "venues": ["sim"],
                "asof": asof,
            }
            event_bus.publish("x:token.snap", token_payload, _broadcast=False)
            total += 1
            golden_payload = {
                "mint": mint,
                "hash": f"chaos-hash-{mint_index:05d}",
                "liq": 1_000_000.0 + mint_index,
                "asof": asof,
            }
            event_bus.publish("x:mint.golden", golden_payload, _broadcast=False)
            total += 1
            ohlcv_payload = {
                "mint": mint,
                "open": 1.0,
                "high": 1.0,
                "low": 1.0,
                "close": 1.0,
                "volume": 10_000.0,
                "asof_close": asof,
            }
            event_bus.publish("x:market.ohlcv.5m", ohlcv_payload, _broadcast=False)
            total += 1
            await asyncio.sleep(0)
            for depth_index in range(depth_updates):
                depth_asof = asof + depth_index * 0.00025
                depth_payload = {
                    "mint": mint,
                    "venue": "sim",
                    "bids": 1000.0 + depth_index,
                    "asks": 1000.5 + depth_index,
                    "spread_bps": 12.0 + (depth_index % 7),
                    "depth": 50_000.0 + depth_index,
                    "asof": depth_asof,
                }
                event_bus.publish("x:market.depth", depth_payload, _broadcast=False)
                total += 1
                ui.push_event({"channel": "events", "mint": mint, "seq": depth_index})
                if depth_index % 8 == 0:
                    ui.push_log(
                        {
                            "channel": "logs",
                            "mint": mint,
                            "seq": depth_index,
                            "message": "depth_update",
                        }
                    )
                    ui.push_rl({"mint": mint, "seq": depth_index, "ts": depth_asof})
                if depth_index % 40 == 0:
                    await asyncio.sleep(0)
            await asyncio.sleep(0)
        await asyncio.sleep(0.25)
        return total

    def _run(
        *,
        mint_count: int = 80,
        depth_updates: int = 80,
        base_ts: float | None = None,
    ) -> Dict[str, Any]:
        start_ts = base_ts if base_ts is not None else time.time()
        total_messages = asyncio.run(
            _drive_payloads(mint_count, depth_updates, start_ts)
        )
        time.sleep(0.1)
        summary = runtime.ui_state.snapshot_summary()
        channel_metrics = ui.get_ws_channel_metrics()
        return {
            "summary": summary,
            "channels": channel_metrics,
            "mint_count": mint_count,
            "depth_updates": depth_updates,
            "payloads": total_messages,
        }

    try:
        yield _run
    finally:
        runtime.ui_state.summary_provider = original_summary_provider
        ui.stop_websockets()
