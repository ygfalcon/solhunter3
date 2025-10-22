"""Service entry point wiring the Phase-One Golden Stream."""
from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlparse

from ..bus import MessageBus
from ..types import CandidateStats, NormalizedCandidate, PreliminarySnapshot
from ..utils import canonical_hash
from .cache import BloomFilter, TokenCache, WarmBundle
from .circuit import HostCircuitBreaker
from .discovery import BirdeyeDiscovery, DexScreenerDiscovery, DiscoveryManager, HeliusDiscovery, SolscanHydrator
from .http import HttpClient
from .pipeline import CadenceController, GoldenStreamPipeline, StageAProcessor, StageBProcessor
from .pricing import JupiterPriceClient, PricingEngine, PythHermesClient
from .rate_limit import HostRateLimiter
from .scoring import LogisticScorer

log = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        return default
    return value


def _host_from_url(url: str) -> str:
    parsed = urlparse(url)
    return parsed.hostname or url


class GoldenStreamService:
    """Configure and operate the Phase-One Golden Stream pipeline."""

    def __init__(self, *, bus: MessageBus, cache_path: str | None = None) -> None:
        self._bus = bus
        cache_root = Path(cache_path or "./artifacts/golden_stream")
        cache_root.mkdir(parents=True, exist_ok=True)
        db_path = cache_root / "golden_stream.db"
        self._bundle_path = cache_root / "warm_bundle.json"
        self._token_cache = TokenCache(db_path)
        self._limiter = HostRateLimiter()
        self._breaker = HostCircuitBreaker(threshold=3, cooldown=30.0)
        self._http = HttpClient(limiter=self._limiter, circuit=self._breaker)
        self._host_urls: dict[str, str] = {}
        self._configure_limits()
        self._pipeline: GoldenStreamPipeline | None = None
        self._stage_a: StageAProcessor | None = None
        self._cadencer: CadenceController | None = None
        self._ramp_task: asyncio.Task[None] | None = None
        self._running = False

    def _configure_limits(self) -> None:
        helius_url = _env("HELIUS_DAS_URL", "https://api.helius.xyz")
        birdeye_url = _env("BIRDEYE_URL", "https://public-api.birdeye.so")
        dexscreener_url = _env("DEXSCREENER_URL", "https://api.dexscreener.com")
        solscan_url = _env("SOLSCAN_URL", "https://public-api.solscan.io")
        jupiter_url = _env("JUPITER_PRICE_URL", "https://price.jup.ag")
        pyth_url = _env("PYTH_HERMES_URL", "https://hermes.pyth.network")
        self._limit_host(helius_url, concurrency=4, rps=4.0)
        self._limit_host(birdeye_url, concurrency=6, rps=5.0)
        self._limit_host(dexscreener_url, concurrency=8, rps=5.0)
        self._limit_host(solscan_url, concurrency=4, rps=2.0)
        self._limit_host(jupiter_url, concurrency=6, rps=6.0)
        self._limit_host(pyth_url, concurrency=6, rps=6.0)

    def _limit_host(self, url: str, *, concurrency: int, rps: float) -> None:
        host = _host_from_url(url)
        self._limiter.configure_host(host, concurrency=concurrency, rps=rps)
        self._host_urls[host] = url.rstrip("/")

    async def start(self) -> None:
        if self._running:
            return
        bundle = WarmBundle.from_path(self._bundle_path)
        bloom = bundle.bloom if bundle else BloomFilter()
        discovery = DiscoveryManager(
            http=self._http,
            token_cache=self._token_cache,
            bloom=bloom,
            dedupe_ttl=24 * 3600.0,
            liquidity_floor=float(os.getenv("GOLDEN_STREAM_LIQUIDITY_FLOOR", "1000")),
            volume_floor=float(os.getenv("GOLDEN_STREAM_VOLUME_FLOOR", "1000")),
        )
        helius = HeliusDiscovery(
            self._http,
            self._token_cache,
            base_url=_env("HELIUS_DAS_URL", "https://api.helius.xyz"),
            api_key=_env("HELIUS_API_KEY", ""),
            allow_program_prefixes=[p.strip() for p in _env("HELIUS_ALLOW_PREFIXES", "").split(",") if p.strip()],
            deny_program_prefixes=[p.strip() for p in _env("HELIUS_DENY_PREFIXES", "").split(",") if p.strip()],
        )
        birdeye = BirdeyeDiscovery(
            self._http,
            base_url=_env("BIRDEYE_URL", "https://public-api.birdeye.so"),
            api_key=os.getenv("BIRDEYE_API_KEY"),
        )
        dex = DexScreenerDiscovery(
            self._http,
            base_url=_env("DEXSCREENER_URL", "https://api.dexscreener.com/latest/dex"),
        )
        solscan = None
        if os.getenv("SOLSCAN_URL") is not None:
            solscan = SolscanHydrator(
                self._http,
                base_url=_env("SOLSCAN_URL", "https://public-api.solscan.io"),
                ttl_seconds=float(os.getenv("SOLSCAN_META_TTL", "3600")),
            )
        jupiter = JupiterPriceClient(self._http, base_url=_env("JUPITER_PRICE_URL", "https://price.jup.ag"))
        pyth = PythHermesClient(self._http, base_url=_env("PYTH_HERMES_URL", "https://hermes.pyth.network"))
        pricing = PricingEngine(jupiter, pyth)
        scorer_path = os.getenv("GOLDEN_STREAM_SCORER_WEIGHTS")
        scorer = LogisticScorer(scorer_path)
        self._stage_a = StageAProcessor(
            scorer=scorer,
            pricing=pricing,
            solscan=solscan,
            score_threshold=float(os.getenv("GOLDEN_STREAM_SCORE_THRESHOLD", "0.55")),
            cooldown_seconds=float(os.getenv("GOLDEN_STREAM_COOLDOWN", "90")),
            producer_service=os.getenv("GOLDEN_STREAM_SERVICE", "golden-stream"),
            producer_build=os.getenv("GOLDEN_STREAM_BUILD", "dev"),
        )
        if bundle and bundle.seeds:
            self._stage_a.load_seeds(bundle.seeds)
        stage_b = StageBProcessor()
        target_tps = float(os.getenv("GOLDEN_STREAM_TARGET_TPS", "6"))
        warm_initial = float(os.getenv("GOLDEN_STREAM_WARM_INITIAL_TPS", "2"))
        ramp_seconds = float(os.getenv("GOLDEN_STREAM_WARM_RAMP_SECONDS", "60"))
        self._cadencer = CadenceController(target_tps=warm_initial)
        self._pipeline = GoldenStreamPipeline(
            discovery=discovery,
            helius=helius,
            birdeye=birdeye,
            pricing=pricing,
            limiter=self._limiter,
            http=self._http,
            stage_a=self._stage_a,
            stage_b=stage_b,
            bus=self._bus,
            cadencer=self._cadencer,
            dex=dex,
        )
        if bundle:
            await self._restore_bundle_state(bundle)
        await self._http.start()
        await self._prewarm_hosts()
        if bundle:
            await self._warm_from_bundle(bundle, pricing)
        await self._pipeline.start()
        self._running = True
        log.info("GoldenStreamService started")

        if self._cadencer:
            if target_tps != warm_initial and ramp_seconds > 0:
                self._ramp_task = asyncio.create_task(
                    self._ramp_cadence(warm_initial, target_tps, ramp_seconds)
                )
            else:
                self._cadencer.set_target(target_tps)

    async def stop(self) -> None:
        if not self._running:
            return
        if self._ramp_task:
            self._ramp_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._ramp_task
            self._ramp_task = None
        if self._pipeline:
            await self._pipeline.stop()
        await self._http.close()
        await self._token_cache.close()
        await self._persist_bundle()
        self._running = False
        self._cadencer = None
        log.info("GoldenStreamService stopped")

    async def _persist_bundle(self) -> None:
        bloom = self._pipeline.discovery.bloom if self._pipeline else BloomFilter()
        meta_cache = await self._token_cache.load_meta_cache()
        cursors = await self._token_cache.dump_cursors()
        buckets = await self._limiter.snapshot()
        breakers = await self._breaker.snapshot()
        seeds: Sequence[Mapping[str, Any]] = []
        if self._stage_a is not None:
            seeds = [asdict(seed) for seed in self._stage_a.high_score_seeds()]
        bundle = WarmBundle(
            bloom=bloom,
            meta_cache=meta_cache,
            cursors=cursors,
            seeds=seeds,
            buckets=buckets,
            breakers=breakers,
        )
        bundle.persist(self._bundle_path)

    async def _restore_bundle_state(self, bundle: WarmBundle) -> None:
        cursor_tasks = []
        for source, cursor in bundle.cursors.items():
            if cursor is None or cursor == "":
                continue
            cursor_tasks.append(self._token_cache.set_cursor(source, str(cursor)))
        if cursor_tasks:
            await asyncio.gather(*cursor_tasks)
        if bundle.buckets:
            await self._limiter.restore(bundle.buckets)
        if bundle.breakers:
            await self._breaker.restore(bundle.breakers)

    async def _prewarm_hosts(self) -> None:
        if not self._host_urls:
            return
        tasks = [
            asyncio.create_task(self._safe_ping(url))
            for url in {value for value in self._host_urls.values() if value}
        ]
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _safe_ping(self, url: str) -> None:
        if not url:
            return
        for method in ("HEAD", "GET"):
            try:
                resp = await self._http.request(method, url, allow_retry=False, budget=0.2)
            except Exception:
                continue
            try:
                await resp.release()
            except Exception:
                pass
            break

    async def _warm_from_bundle(self, bundle: WarmBundle, pricing: PricingEngine) -> None:
        if self._stage_a is None:
            return
        seeds = self._stage_a.high_score_seeds()
        if not seeds:
            return
        try:
            limit = int(os.getenv("GOLDEN_STREAM_WARM_SEED_LIMIT", "80"))
        except (TypeError, ValueError):
            limit = 80
        limit = max(1, limit)
        seen: set[str] = set()
        candidates: list[NormalizedCandidate] = []
        for seed in seeds:
            if not isinstance(seed, PreliminarySnapshot):
                try:
                    seed = PreliminarySnapshot(**seed)  # type: ignore[arg-type]
                except Exception:
                    continue
            if seed.mint in seen:
                continue
            candidate = self._candidate_from_snapshot(seed, bundle.meta_cache)
            if candidate is None:
                continue
            candidates.append(candidate)
            seen.add(seed.mint)
            if len(candidates) >= limit:
                break
        if not candidates:
            return
        quotes = await pricing.quote_many([candidate.mint for candidate in candidates])
        for candidate in candidates:
            quote = quotes.get(candidate.mint)
            if not quote:
                continue
            await self._stage_a.warm_seed(candidate, quote)

    def _candidate_from_snapshot(
        self,
        snapshot: PreliminarySnapshot,
        meta_cache: Mapping[str, Mapping[str, Any]] | None,
    ) -> NormalizedCandidate | None:
        meta = meta_cache.get(snapshot.mint) if meta_cache else None
        symbol = snapshot.symbol
        name = snapshot.name
        decimals: int | None = snapshot.decimals
        if isinstance(meta, Mapping):
            symbol = symbol or meta.get("symbol")
            name = name or meta.get("name")
            meta_decimals = meta.get("decimals")
            if decimals is None and meta_decimals is not None:
                try:
                    decimals = int(meta_decimals)
                except (TypeError, ValueError):
                    decimals = None
        try:
            decimals_int = int(decimals) if decimals is not None else None
        except (TypeError, ValueError):
            decimals_int = None
        stats_meta = meta.get("stats") if isinstance(meta, Mapping) else None
        liquidity = self._coerce_float(snapshot.liquidity_usd)
        volume_1h = self._coerce_float(snapshot.volume_1h_usd)
        volume_24h = self._coerce_float(snapshot.volume_24h_usd)
        pair_age = self._coerce_optional(snapshot.pair_age_min)
        if isinstance(stats_meta, Mapping):
            liquidity = self._coerce_float(stats_meta.get("liquidity_usd"), liquidity)
            volume_1h = self._coerce_float(stats_meta.get("volume_1h_usd"), volume_1h)
            volume_24h = self._coerce_float(stats_meta.get("volume_24h_usd"), volume_24h)
            pair_age = self._coerce_optional(stats_meta.get("pair_age_min"), pair_age)
        stats = CandidateStats(
            liquidity_usd=liquidity,
            volume_1h_usd=volume_1h,
            volume_24h_usd=volume_24h,
            pair_age_min=pair_age,
        )
        provenance = snapshot.provenance
        sources: Iterable[str] = provenance.sources if provenance else ()
        if isinstance(meta, Mapping):
            meta_sources = meta.get("sources")
            if isinstance(meta_sources, Iterable):
                sources = tuple(str(value) for value in meta_sources)
        source_tuple = tuple(str(value) for value in sources)
        first_seen = snapshot.asof
        if isinstance(meta, Mapping):
            fallback_seen = meta.get("first_seen") or meta.get("first_seen_at")
            if fallback_seen:
                first_seen = str(fallback_seen)
        payload = {
            "mint": snapshot.mint,
            "symbol": symbol,
            "name": name,
            "decimals": decimals_int,
            "sources": list(source_tuple),
            "stats": {
                "liquidity_usd": liquidity,
                "volume_1h_usd": volume_1h,
                "volume_24h_usd": volume_24h,
                "pair_age_min": pair_age,
            },
        }
        raw_hash = canonical_hash(payload)
        return NormalizedCandidate(
            mint=snapshot.mint,
            symbol=str(symbol) if symbol else None,
            name=str(name) if name else None,
            decimals=decimals_int,
            first_seen_at=str(first_seen),
            sources=source_tuple,
            stats=stats,
            raw_payload_hash=raw_hash,
        )

    @staticmethod
    def _coerce_float(value: Any, fallback: float | None = None) -> float:
        try:
            if value is None:
                return float(fallback or 0.0)
            return float(value)
        except (TypeError, ValueError):
            return float(fallback or 0.0)

    @staticmethod
    def _coerce_optional(value: Any, fallback: float | None = None) -> float | None:
        if value is None:
            return fallback if fallback is None else float(fallback)
        try:
            return float(value)
        except (TypeError, ValueError):
            return fallback if fallback is None else float(fallback)

    async def _ramp_cadence(self, start: float, finish: float, duration: float) -> None:
        if self._cadencer is None:
            return
        if duration <= 0:
            self._cadencer.set_target(finish)
            return
        steps = max(1, int(duration // 5))
        interval = duration / steps if steps else duration
        for step in range(1, steps + 1):
            await asyncio.sleep(interval)
            progress = step / steps
            target = start + (finish - start) * progress
            self._cadencer.set_target(target)
        self._cadencer.set_target(finish)


