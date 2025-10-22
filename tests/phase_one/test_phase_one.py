import asyncio
import time
import sys
import types
from pathlib import Path
from typing import Dict, List, Mapping

jsonschema = types.ModuleType("jsonschema")
Draft202012Validator = type(
    "Draft202012Validator",
    (),
    {
        "__init__": lambda self, schema: None,
        "validate": lambda self, obj: None,
    },
)


class _ValidationError(Exception):
    pass


exceptions_mod = types.ModuleType("jsonschema.exceptions")
exceptions_mod.ValidationError = _ValidationError

jsonschema.Draft202012Validator = Draft202012Validator
jsonschema.exceptions = exceptions_mod
sys.modules.setdefault("jsonschema", jsonschema)
sys.modules.setdefault("jsonschema.exceptions", exceptions_mod)

prices_mod = types.ModuleType("solhunter_zero.golden_pipeline.prices")


def _fake_parse_pyth_mapping():
    return ({}, [])


prices_mod._parse_pyth_mapping = _fake_parse_pyth_mapping  # type: ignore[attr-defined]
sys.modules.setdefault("solhunter_zero.golden_pipeline.prices", prices_mod)

from solhunter_zero.golden_pipeline.phase_one.cache import BloomFilter, TokenCache
from solhunter_zero.golden_pipeline.phase_one.circuit import HostCircuitBreaker
from solhunter_zero.golden_pipeline.phase_one.discovery import DiscoveryManager, RawCandidate
from solhunter_zero.golden_pipeline.phase_one.pipeline import CadenceController, StageAProcessor
from solhunter_zero.golden_pipeline.phase_one.pricing import PriceQuote
from solhunter_zero.golden_pipeline.types import CandidateStats, NormalizedCandidate


def test_cadence_controller_throttle_recovery() -> None:
    async def _run() -> None:
        controller = CadenceController(target_tps=6.0, max_delay=2.0)
        full_buckets = {"birdeye": {"capacity": 6.0, "tokens": 6.0}}
        drained_buckets = {"birdeye": {"capacity": 6.0, "tokens": 0.0}}
        recovered_buckets = {"birdeye": {"capacity": 6.0, "tokens": 5.5}}

        delay_full = controller.compute_delay(full_buckets)
        delay_drained = controller.compute_delay(drained_buckets)
        delay_recovered = controller.compute_delay(recovered_buckets)

        assert delay_drained > delay_full
        assert delay_recovered < delay_drained
        assert delay_recovered <= delay_full * 1.5

    asyncio.run(_run())


class _DummyHttp:
    pass


def test_discovery_replay(tmp_path: Path) -> None:
    async def _run() -> None:
        cache = TokenCache(tmp_path / "cache.db")
        manager = DiscoveryManager(
            http=_DummyHttp(),
            token_cache=cache,
            bloom=BloomFilter(),
            dedupe_ttl=3600.0,
            liquidity_floor=0.0,
            volume_floor=0.0,
        )
        first = RawCandidate(
            mint="TestMint11111111111111111111111111111",
            symbol="TEST",
            name="Test Token",
            decimals=6,
            first_seen=0.0,
            liquidity_usd=5000.0,
            volume_1h_usd=2000.0,
            volume_24h_usd=9000.0,
            sources=("helius",),
            payload={"version": 1},
        )
        await manager.reset_cycle()
        await manager.ingest([first])
        initial = await manager.emit()
        assert len(initial) == 1

        await manager.reset_cycle()
        await manager.ingest([first])
        repeat = await manager.emit()
        assert repeat == []

        updated = RawCandidate(
            mint="TestMint11111111111111111111111111111",
            symbol="TEST",
            name="Test Token",
            decimals=6,
            first_seen=0.0,
            liquidity_usd=7000.0,
            volume_1h_usd=2500.0,
            volume_24h_usd=12000.0,
            sources=("helius",),
            payload={"version": 2},
        )
        await manager.reset_cycle()
        await manager.ingest([updated])
        changed = await manager.emit()
        assert len(changed) == 1

        await cache.close()

    asyncio.run(_run())


class _StaticPricing:
    async def quote(self, mint: str) -> PriceQuote:
        return PriceQuote(
            mint=mint,
            mid_usd=1.25,
            asof=time.time(),
            source="jup_price",
        )


class _LiquidityGateScorer:
    def __init__(self, floor: float) -> None:
        self.floor = floor
        self.calls: List[Mapping[str, float]] = []

    def score(self, features: Mapping[str, float]) -> float:
        self.calls.append(dict(features))
        liquidity = float(features.get("liquidity_usd", 0.0))
        return 0.9 if liquidity >= self.floor else 0.1


def _candidate(liquidity: float, mint: str) -> NormalizedCandidate:
    return NormalizedCandidate(
        mint=mint,
        symbol="TOK",
        name="Token",
        decimals=6,
        first_seen_at="2024-01-01T00:00:00Z",
        sources=("helius", "birdeye"),
        stats=CandidateStats(
            liquidity_usd=liquidity,
            volume_1h_usd=500.0,
            volume_24h_usd=2500.0,
            pair_age_min=180.0,
        ),
        raw_payload_hash=f"hash-{mint}",
    )


def test_stage_a_scoring_thresholds() -> None:
    async def _run() -> None:
        scorer = _LiquidityGateScorer(5000.0)
        pricing = _StaticPricing()
        stage = StageAProcessor(
            scorer=scorer,
            pricing=pricing,
            score_threshold=0.5,
            cooldown_seconds=5.0,
            producer_service="test",
            producer_build="dev",
        )

        rich = _candidate(7000.0, "RichMint")
        poor = _candidate(1000.0, "PoorMint")

        rich_result = await stage.process(rich)
        assert rich_result.snapshot is not None
        assert rich_result.retry_at is None
        assert stage.cooldown_until(rich.mint) == 0.0
        assert any(snapshot.mint == rich.mint for snapshot in stage.high_score_seeds())

        poor_result = await stage.process(poor)
        assert poor_result.snapshot is None
        assert poor_result.retry_at is not None
        assert poor_result.retry_at > time.time()
        assert stage.cooldown_until(poor.mint) == poor_result.retry_at
        assert scorer.calls  # Features were passed to scorer

    asyncio.run(_run())


def test_host_circuit_breaker_open_and_restore() -> None:
    async def _run() -> None:
        breaker = HostCircuitBreaker(threshold=3, cooldown=0.2)
        host = "api.test"

        assert not await breaker.is_open(host)
        await breaker.record_failure(host)
        await breaker.record_failure(host)
        assert not await breaker.is_open(host)

        await breaker.record_failure(host)
        assert await breaker.is_open(host)

        snapshot = await breaker.snapshot()
        assert host in snapshot
        remaining = snapshot[host].get("cooldown_remaining", 0.0)
        assert remaining > 0.0

        restored = HostCircuitBreaker(threshold=3, cooldown=0.2)
        await restored.restore(snapshot)
        assert await restored.is_open(host)

        await asyncio.sleep(0.25)
        assert not await restored.is_open(host)

        await restored.record_success(host)
        assert not await restored.is_open(host)

    asyncio.run(_run())
