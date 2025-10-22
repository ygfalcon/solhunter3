"""Phase-One Golden Stream pipeline orchestration."""
from __future__ import annotations

import asyncio
import contextlib
import datetime as dt
import hashlib
import logging
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..contracts import STREAMS
from ..types import NormalizedCandidate, PreliminaryProvenance, PreliminarySnapshot
from .discovery import (
    BirdeyeDiscovery,
    DexScreenerDiscovery,
    DiscoveryManager,
    HeliusDiscovery,
    SolscanHydrator,
)
from .pricing import PricingEngine, PriceQuote
from .rate_limit import HostRateLimiter
from .scoring import LogisticScorer
from .http import HttpClient

log = logging.getLogger(__name__)


@dataclass(slots=True)
class StageAResult:
    snapshot: PreliminarySnapshot | None
    retry_at: float | None = None


class VolumeStats:
    """Rolling statistics used to compute 1h volume z-scores."""

    def __init__(self) -> None:
        self._count = 0
        self._mean = 0.0
        self._m2 = 0.0

    def update(self, value: float) -> float:
        self._count += 1
        if self._count == 1:
            self._mean = value
            self._m2 = 0.0
            return 0.0
        delta = value - self._mean
        self._mean += delta / self._count
        delta2 = value - self._mean
        self._m2 += delta * delta2
        return self.zscore(value)

    def zscore(self, value: float) -> float:
        if self._count < 2:
            return 0.0
        variance = self._m2 / (self._count - 1)
        if variance <= 0:
            return 0.0
        std = variance ** 0.5
        if std <= 1e-6:
            return 0.0
        return (value - self._mean) / std


class CadenceController:
    """Smoothly adjust cadence based on token bucket fill levels."""

    def __init__(self, *, target_tps: float = 6.0, max_delay: float = 2.0) -> None:
        self._max_delay = max_delay
        self._target_tps = max(target_tps, 0.1)
        self._base_delay = 1.0 / self._target_tps

    def set_target(self, target_tps: float) -> None:
        sanitized = max(target_tps, 0.1)
        self._target_tps = sanitized
        self._base_delay = 1.0 / sanitized

    @property
    def target_tps(self) -> float:
        return self._target_tps

    def compute_delay(self, buckets: Mapping[str, Mapping[str, float]]) -> float:
        if not buckets:
            return self._base_delay
        ratios: List[float] = []
        for meta in buckets.values():
            capacity = float(meta.get("capacity") or 1.0)
            tokens = float(meta.get("tokens") or 0.0)
            ratio = max(0.0, min(1.0, tokens / capacity if capacity else 0.0))
            ratios.append(ratio)
        if not ratios:
            return self._base_delay
        floor = min(ratios)
        if floor <= 0.05:
            return self._max_delay
        return min(self._max_delay, self._base_delay / max(0.2, floor))


class StageAProcessor:
    def __init__(
        self,
        *,
        scorer: LogisticScorer,
        pricing: PricingEngine,
        solscan: SolscanHydrator | None = None,
        score_threshold: float = 0.55,
        cooldown_seconds: float = 90.0,
        producer_service: str = "golden-stream",
        producer_build: str = "dev",
    ) -> None:
        self._scorer = scorer
        self._pricing = pricing
        self._solscan = solscan
        self._score_threshold = score_threshold
        self._cooldown = cooldown_seconds
        self._producer = {"service": producer_service, "build": producer_build}
        self._volume_stats = VolumeStats()
        self._recent: Dict[str, tuple[str, float]] = {}
        self._cooldowns: Dict[str, float] = {}
        self._high_scores: List[PreliminarySnapshot] = []

    def cooldown_until(self, mint: str) -> float:
        return self._cooldowns.get(mint, 0.0)

    def record_snapshot(self, snapshot: PreliminarySnapshot) -> None:
        self._high_scores.append(snapshot)
        self._high_scores = sorted(self._high_scores, key=lambda s: s.score, reverse=True)[
            :500
        ]

    def high_score_seeds(self) -> List[PreliminarySnapshot]:
        return list(self._high_scores)

    def load_seeds(self, seeds: Sequence[Mapping[str, Any]]) -> None:
        for entry in seeds:
            try:
                snapshot = PreliminarySnapshot(**entry)
            except TypeError:
                continue
            self.record_snapshot(snapshot)

    async def process(self, candidate: NormalizedCandidate) -> StageAResult:
        now = time.time()
        cooldown_at = self._cooldowns.get(candidate.mint)
        if cooldown_at and cooldown_at > now:
            return StageAResult(snapshot=None, retry_at=cooldown_at)
        await self._hydrate_candidate(candidate)
        quote = await self._pricing.quote(candidate.mint)
        if quote is None:
            retry = now + self._cooldown
            self._cooldowns[candidate.mint] = retry
            return StageAResult(snapshot=None, retry_at=retry)
        features = self._compute_features(candidate, quote)
        score = self._scorer.score(features)
        if score < self._score_threshold:
            retry = now + self._cooldown
            self._cooldowns[candidate.mint] = retry
            return StageAResult(snapshot=None, retry_at=retry)
        snapshot = self._build_snapshot(candidate, quote, score)
        self._cooldowns.pop(candidate.mint, None)
        if not self._should_emit(snapshot):
            return StageAResult(snapshot=None)
        self._recent[candidate.mint] = (snapshot.idempotency_key, now)
        self.record_snapshot(snapshot)
        return StageAResult(snapshot=snapshot)

    async def warm_seed(
        self, candidate: NormalizedCandidate, quote: PriceQuote | None = None
    ) -> None:
        if quote is None:
            quote = await self._pricing.quote(candidate.mint)
        if quote is None:
            return
        features = self._compute_features(candidate, quote)
        score = self._scorer.score(features)
        snapshot = self._build_snapshot(candidate, quote, score)
        self.record_snapshot(snapshot)

    async def _hydrate_candidate(self, candidate: NormalizedCandidate) -> None:
        if candidate.symbol and candidate.decimals is not None:
            return
        if not self._solscan:
            return
        meta = await self._solscan.hydrate(candidate.mint)
        if not meta:
            return
        info = meta.get("data") if isinstance(meta, Mapping) else meta
        if not isinstance(info, Mapping):
            return
        token_info = info.get("tokenInfo") or info.get("token_info") or info
        if isinstance(token_info, Mapping):
            symbol = token_info.get("symbol")
            name = token_info.get("name")
            decimals = token_info.get("decimals")
            if symbol and not candidate.symbol:
                candidate.symbol = str(symbol)
            if name and not candidate.name:
                candidate.name = str(name)
            if decimals is not None and candidate.decimals is None:
                try:
                    candidate.decimals = int(decimals)
                except (TypeError, ValueError):
                    pass

    def _compute_features(self, candidate: NormalizedCandidate, quote: PriceQuote) -> Dict[str, float]:
        volume_z = self._volume_stats.update(candidate.stats.volume_1h_usd)
        staleness_ms = quote.staleness_ms
        features = {
            "liquidity_usd": candidate.stats.liquidity_usd,
            "vol_1h_z": volume_z,
            "pair_age_min": candidate.stats.pair_age_min or 0.0,
            "source_diversity": float(len(candidate.sources)),
            "px_staleness_ms": staleness_ms,
        }
        return features

    def _build_snapshot(
        self, candidate: NormalizedCandidate, quote: PriceQuote, score: float
    ) -> PreliminarySnapshot:
        asof = dt.datetime.now(dt.timezone.utc).isoformat()
        idempotency_key = hashlib.sha256(
            f"{candidate.mint}|{asof}|{round(quote.mid_usd, 6)}|{round(candidate.stats.liquidity_usd, 2)}".encode(
                "utf-8"
            )
        ).hexdigest()
        provenance = PreliminaryProvenance(
            sources=tuple(candidate.sources),
            producer=dict(self._producer),
        )
        return PreliminarySnapshot(
            mint=candidate.mint,
            symbol=candidate.symbol or candidate.mint[:6].upper(),
            decimals=int(candidate.decimals or 0),
            asof=asof,
            px_mid_usd=quote.mid_usd,
            price_source=quote.source,
            staleness_ms=quote.staleness_ms,
            liquidity_usd=candidate.stats.liquidity_usd,
            volume_1h_usd=candidate.stats.volume_1h_usd,
            volume_24h_usd=candidate.stats.volume_24h_usd,
            pair_age_min=candidate.stats.pair_age_min,
            score=score,
            provenance=provenance,
            idempotency_key=idempotency_key,
            price_confidence=quote.confidence,
            name=candidate.name,
        )

    def _should_emit(self, snapshot: PreliminarySnapshot) -> bool:
        entry = self._recent.get(snapshot.mint)
        if not entry:
            return True
        last_key, last_ts = entry
        if last_key != snapshot.idempotency_key:
            return True
        return (time.time() - last_ts) > 180.0


class StageBProcessor:
    """Attach optional extras to Preliminary snapshots."""

    async def process(self, snapshot: PreliminarySnapshot) -> PreliminarySnapshot:
        return snapshot


class GoldenStreamPipeline:
    """Orchestrates discovery, enrichment, and emission of preliminary snapshots."""

    def __init__(
        self,
        *,
        discovery: DiscoveryManager,
        helius: HeliusDiscovery,
        birdeye: BirdeyeDiscovery,
        pricing: PricingEngine,
        limiter: HostRateLimiter,
        http: HttpClient,
        stage_a: StageAProcessor,
        stage_b: StageBProcessor,
        bus,
        cadencer: CadenceController | None = None,
        dex: DexScreenerDiscovery | None = None,
    ) -> None:
        self._discovery = discovery
        self._helius = helius
        self._birdeye = birdeye
        self._pricing = pricing
        self._limiter = limiter
        self._http = http
        self._stage_a = stage_a
        self._stage_b = stage_b
        self._bus = bus
        self._cadencer = cadencer or CadenceController()
        self._running = False
        self._task: asyncio.Task[None] | None = None
        self._dex = dex

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._run_loop(), name="golden_stream")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._task = None

    async def _run_loop(self) -> None:
        while self._running:
            try:
                await self.run_cycle()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("GoldenStream cycle failed: %s", exc)
            await asyncio.sleep(0.05)

    async def run_cycle(self) -> None:
        await self._discovery.reset_cycle()
        helius_records = await self._helius.fetch(limit=200)
        await self._discovery.ingest(helius_records)
        birdeye_records = await self._birdeye.fetch()
        await self._discovery.ingest(birdeye_records)
        if self._dex:
            mints = {record.mint for record in helius_records}
            mints.update(record.mint for record in birdeye_records)
            dex_records = await self._dex.fetch(sorted(mints))
            await self._discovery.ingest(dex_records)
        candidates = await self._discovery.emit()
        for candidate in candidates:
            result = await self._stage_a.process(candidate)
            snapshot = result.snapshot
            if snapshot is None:
                continue
            enriched = await self._stage_b.process(snapshot)
            await self._publish(enriched)
            buckets = await self._limiter.snapshot()
            delay = self._cadencer.compute_delay(buckets)
            await asyncio.sleep(delay)

    async def _publish(self, snapshot: PreliminarySnapshot) -> None:
        payload = {
            "mint": snapshot.mint,
            "symbol": snapshot.symbol,
            "decimals": snapshot.decimals,
            "name": snapshot.name,
            "asof": snapshot.asof,
            "px_mid_usd": snapshot.px_mid_usd,
            "price_source": snapshot.price_source,
            "staleness_ms": snapshot.staleness_ms,
            "price_confidence": snapshot.price_confidence,
            "liquidity_usd": snapshot.liquidity_usd,
            "volume_1h_usd": snapshot.volume_1h_usd,
            "volume_24h_usd": snapshot.volume_24h_usd,
            "pair_age_min": snapshot.pair_age_min,
            "score": snapshot.score,
            "provenance": {
                "sources": list(snapshot.provenance.sources),
                "producer": dict(snapshot.provenance.producer),
            },
            "idempotency_key": snapshot.idempotency_key,
            "schema_version": snapshot.schema_version,
        }
        await self._bus.publish(STREAMS.preliminary_snapshot, payload)

    @property
    def discovery(self) -> DiscoveryManager:
        return self._discovery

