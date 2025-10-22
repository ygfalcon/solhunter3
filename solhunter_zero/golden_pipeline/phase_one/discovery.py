"""Discovery orchestrator for the Phase-One Golden Stream."""
from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Sequence

from ..types import CandidateStats, NormalizedCandidate
from ..utils import canonical_hash
from .cache import BloomFilter, TokenCache
from .http import CircuitOpen, HttpClient

log = logging.getLogger(__name__)

_HELIUS_SOURCE = "helius"
_BIRDEYE_SOURCE = "birdeye"
_DEXSCREENER_SOURCE = "dexscreener"
_SOLSCAN_SOURCE = "solscan"


@dataclass(slots=True)
class RawCandidate:
    """Intermediate record produced by individual discovery sources."""

    mint: str
    symbol: str | None = None
    name: str | None = None
    decimals: int | None = None
    first_seen: float | None = None
    liquidity_usd: float | None = None
    volume_1h_usd: float | None = None
    volume_24h_usd: float | None = None
    pair_age_min: float | None = None
    sources: Sequence[str] = field(default_factory=tuple)
    payload: Mapping[str, Any] = field(default_factory=dict)


def _coerce_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _to_rfc3339(ts: float | None) -> str:
    if ts is None:
        ts = time.time()
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).isoformat()


class CandidateAggregate:
    """Merge raw records from multiple discovery sources."""

    def __init__(self, mint: str) -> None:
        self.mint = mint
        self.symbol: str | None = None
        self.name: str | None = None
        self.decimals: int | None = None
        self.first_seen: float | None = None
        self.liquidity_usd: float = 0.0
        self.volume_1h_usd: float = 0.0
        self.volume_24h_usd: float = 0.0
        self.pair_age_min: float | None = None
        self.sources: MutableMapping[str, int] = {}
        self.payloads: List[Mapping[str, Any]] = []

    def ingest(self, record: RawCandidate) -> None:
        if record.symbol and not self.symbol:
            self.symbol = record.symbol
        if record.name and not self.name:
            self.name = record.name
        if record.decimals is not None:
            self.decimals = int(record.decimals)
        if record.first_seen is not None:
            if self.first_seen is None or record.first_seen < self.first_seen:
                self.first_seen = float(record.first_seen)
        if record.liquidity_usd is not None:
            self.liquidity_usd = max(self.liquidity_usd, float(record.liquidity_usd))
        if record.volume_1h_usd is not None:
            self.volume_1h_usd = max(self.volume_1h_usd, float(record.volume_1h_usd))
        if record.volume_24h_usd is not None:
            self.volume_24h_usd = max(self.volume_24h_usd, float(record.volume_24h_usd))
        if record.pair_age_min is not None:
            if self.pair_age_min is None:
                self.pair_age_min = float(record.pair_age_min)
            else:
                self.pair_age_min = min(self.pair_age_min, float(record.pair_age_min))
        for source in record.sources:
            self.sources[source] = self.sources.get(source, 0) + 1
        if record.payload:
            self.payloads.append(dict(record.payload))

    def finalise(self) -> NormalizedCandidate:
        first_seen = self.first_seen if self.first_seen is not None else time.time()
        stats = CandidateStats(
            liquidity_usd=float(self.liquidity_usd or 0.0),
            volume_1h_usd=float(self.volume_1h_usd or 0.0),
            volume_24h_usd=float(self.volume_24h_usd or 0.0),
            pair_age_min=self.pair_age_min,
        )
        payload_summary = {
            "mint": self.mint,
            "symbol": self.symbol,
            "name": self.name,
            "decimals": self.decimals,
            "first_seen": first_seen,
            "sources": sorted(self.sources.keys()),
            "stats": {
                "liquidity_usd": stats.liquidity_usd,
                "volume_1h_usd": stats.volume_1h_usd,
                "volume_24h_usd": stats.volume_24h_usd,
                "pair_age_min": stats.pair_age_min,
            },
            "payloads": self.payloads,
        }
        content_hash = canonical_hash(payload_summary)
        return NormalizedCandidate(
            mint=self.mint,
            symbol=self.symbol,
            name=self.name,
            decimals=self.decimals,
            first_seen_at=_to_rfc3339(first_seen),
            sources=tuple(sorted(self.sources.keys())),
            stats=stats,
            raw_payload_hash=content_hash,
        )


class DiscoveryManager:
    """Coordinate discovery sources, dedupe, and normalization."""

    def __init__(
        self,
        *,
        http: HttpClient,
        token_cache: TokenCache,
        bloom: BloomFilter | None = None,
        dedupe_ttl: float = 24 * 3600.0,
        liquidity_floor: float = 1_000.0,
        volume_floor: float = 1_000.0,
    ) -> None:
        self._http = http
        self._cache = token_cache
        self._bloom = bloom or BloomFilter()
        self._dedupe_ttl = dedupe_ttl
        self._liquidity_floor = liquidity_floor
        self._volume_floor = volume_floor
        self._aggregates: Dict[str, CandidateAggregate] = {}
        self._lock = asyncio.Lock()

    async def reset_cycle(self) -> None:
        self._bloom.reset()
        self._aggregates.clear()

    async def ingest(self, records: Iterable[RawCandidate]) -> None:
        async with self._lock:
            for record in records:
                if not record.mint:
                    continue
                mint = record.mint
                aggregate = self._aggregates.setdefault(mint, CandidateAggregate(mint))
                aggregate.ingest(record)

    async def emit(self) -> List[NormalizedCandidate]:
        async with self._lock:
            aggregates = list(self._aggregates.values())
            self._aggregates.clear()
        emitted: List[NormalizedCandidate] = []
        for aggregate in aggregates:
            candidate = aggregate.finalise()
            if not self._passes_edges(candidate):
                continue
            if not self._bloom.add(candidate.mint):
                log.debug("Bloom filter dropped %s", candidate.mint)
                continue
            cached = await self._cache.check_content(
                candidate.mint,
                candidate.raw_payload_hash,
                self._dedupe_ttl,
            )
            if cached:
                log.debug("L2 cache suppressed %s", candidate.mint)
                continue
            await self._cache.upsert(
                candidate.mint,
                candidate.raw_payload_hash,
                {
                    "mint": candidate.mint,
                    "symbol": candidate.symbol,
                    "name": candidate.name,
                    "decimals": candidate.decimals,
                    "sources": list(candidate.sources),
                    "stats": {
                        "liquidity_usd": candidate.stats.liquidity_usd,
                        "volume_1h_usd": candidate.stats.volume_1h_usd,
                        "volume_24h_usd": candidate.stats.volume_24h_usd,
                        "pair_age_min": candidate.stats.pair_age_min,
                    },
                },
            )
            emitted.append(candidate)
        return emitted

    def _passes_edges(self, candidate: NormalizedCandidate) -> bool:
        if candidate.stats.liquidity_usd < self._liquidity_floor:
            return False
        if candidate.stats.volume_24h_usd < self._volume_floor:
            return False
        return True

    @property
    def bloom(self) -> BloomFilter:
        return self._bloom


class HeliusDiscovery:
    """Pull candidate tokens from Helius DAS searchAssets."""

    def __init__(
        self,
        http: HttpClient,
        cache: TokenCache,
        *,
        base_url: str,
        api_key: str,
        page_size: int = 100,
        allow_program_prefixes: Sequence[str] | None = None,
        deny_program_prefixes: Sequence[str] | None = None,
    ) -> None:
        self._http = http
        self._cache = cache
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key
        self._page_size = max(1, int(page_size))
        self._allow_prefixes = {p.upper() for p in allow_program_prefixes or ()}
        self._deny_prefixes = {p.upper() for p in deny_program_prefixes or ()}
        self._cursor_key = "helius:cursor"

    async def fetch(self, limit: int = 200) -> Sequence[RawCandidate]:
        url = f"{self._base_url}/v0/assets/search"
        headers = {"Content-Type": "application/json"}
        params = {"api-key": self._api_key} if self._api_key else None
        cursor = await self._cache.get_cursor(self._cursor_key)
        remaining = max(1, int(limit))
        results: List[RawCandidate] = []
        while remaining > 0:
            payload = {
                "limit": min(self._page_size, remaining),
                "cursor": cursor,
                "sortBy": {"field": "last_action", "direction": "desc"},
                "condition": {"interface": "FungibleToken"},
            }
            try:
                resp = await self._http.request(
                    "POST",
                    url,
                    headers=headers,
                    params=params,
                    json_body=payload,
                )
            except CircuitOpen:
                break
            except Exception as exc:  # pragma: no cover - network failure
                log.warning("Helius searchAssets failed: %s", exc)
                break
            data = await resp.json(content_type=None)
            entries = data.get("items") or data.get("result") or []
            if not isinstance(entries, list) or not entries:
                break
            cursor = data.get("cursor") or data.get("next")
            remaining -= len(entries)
            for entry in entries:
                candidate = self._normalize(entry)
                if candidate is None:
                    continue
                results.append(candidate)
            if not cursor:
                break
        if cursor:
            await self._cache.set_cursor(self._cursor_key, str(cursor))
        return results

    def _normalize(self, payload: Mapping[str, Any]) -> RawCandidate | None:
        mint = payload.get("id") or payload.get("mint")
        if not isinstance(mint, str) or not mint:
            return None
        program = str(payload.get("token_program")) if payload.get("token_program") else ""
        program_upper = program.upper()
        if self._deny_prefixes and any(program_upper.startswith(p) for p in self._deny_prefixes):
            return None
        if self._allow_prefixes and not any(program_upper.startswith(p) for p in self._allow_prefixes):
            return None
        token_info = payload.get("token_info") or payload.get("content") or {}
        if not isinstance(token_info, Mapping):
            token_info = {}
        symbol = token_info.get("symbol") or payload.get("symbol")
        name = token_info.get("name") or payload.get("name")
        decimals = token_info.get("decimals") or payload.get("decimals")
        stats = payload.get("stats") or {}
        liquidity = _coerce_float(stats.get("liquidity_usd")) or _coerce_float(
            token_info.get("liquidity_usd")
        )
        vol_24h = _coerce_float(stats.get("volume_24h")) or _coerce_float(
            token_info.get("volume_24h_usd")
        )
        vol_1h = _coerce_float(stats.get("volume_1h"))
        first_seen = _coerce_float(payload.get("first_verified"))
        age_min = _coerce_float(stats.get("age_minutes"))
        return RawCandidate(
            mint=mint,
            symbol=str(symbol) if symbol else None,
            name=str(name) if name else None,
            decimals=int(decimals) if decimals is not None else None,
            first_seen=float(first_seen) if first_seen else None,
            liquidity_usd=liquidity,
            volume_1h_usd=vol_1h,
            volume_24h_usd=vol_24h,
            pair_age_min=age_min,
            sources=(_HELIUS_SOURCE,),
            payload=payload,
        )


class BirdeyeDiscovery:
    """Fetch liquid tokens from Birdeye public endpoints."""

    def __init__(
        self,
        http: HttpClient,
        *,
        base_url: str,
        api_key: str | None = None,
        volume_floor: float = 10_000.0,
    ) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._api_key = api_key or ""
        self._volume_floor = volume_floor

    async def fetch(self, page_size: int = 100) -> Sequence[RawCandidate]:
        params = {
            "offset": 0,
            "limit": max(1, min(page_size, 200)),
            "sortBy": "v24hUSD",
        }
        headers = {"Accept": "application/json"}
        if self._api_key:
            headers["X-API-KEY"] = self._api_key
        url = f"{self._base_url}/public/tokenlist"
        results: List[RawCandidate] = []
        trailing_low = 0
        while True:
            try:
                resp = await self._http.request("GET", url, headers=headers, params=params)
            except CircuitOpen:
                break
            except Exception as exc:  # pragma: no cover - network failure
                log.warning("Birdeye token list failed: %s", exc)
                break
            data = await resp.json(content_type=None)
            items = data.get("data") or {}
            tokens = items.get("tokens") if isinstance(items, Mapping) else None
            if not isinstance(tokens, list) or not tokens:
                break
            for entry in tokens:
                mint = entry.get("address") or entry.get("mint")
                if not isinstance(mint, str):
                    continue
                volume_24h = _coerce_float(entry.get("v24hUSD"))
                liquidity = _coerce_float(entry.get("liquidity"))
                if volume_24h is not None and volume_24h < self._volume_floor:
                    trailing_low += 1
                    if trailing_low >= 5:
                        return results
                else:
                    trailing_low = 0
                symbol = entry.get("symbol")
                name = entry.get("name")
                decimals = entry.get("decimals")
                first_seen = _coerce_float(entry.get("createdTime"))
                record = RawCandidate(
                    mint=mint,
                    symbol=str(symbol) if symbol else None,
                    name=str(name) if name else None,
                    decimals=int(decimals) if decimals is not None else None,
                    first_seen=float(first_seen) if first_seen else None,
                    liquidity_usd=liquidity,
                    volume_1h_usd=_coerce_float(entry.get("v1hUSD")),
                    volume_24h_usd=volume_24h,
                    pair_age_min=_coerce_float(entry.get("age")),
                    sources=(_BIRDEYE_SOURCE,),
                    payload=entry,
                )
                results.append(record)
            params["offset"] += params["limit"]
        return results


class DexScreenerDiscovery:
    """Pull batch token stats from DexScreener."""

    def __init__(self, http: HttpClient, *, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    async def fetch(self, mints: Sequence[str]) -> Sequence[RawCandidate]:
        if not mints:
            return []
        batches: List[Sequence[str]] = []
        chunk = []
        for mint in mints:
            chunk.append(mint)
            if len(chunk) >= 150:
                batches.append(tuple(chunk))
                chunk = []
        if chunk:
            batches.append(tuple(chunk))
        results: List[RawCandidate] = []
        for batch in batches:
            url = f"{self._base_url}/latest/dex/tokens/{','.join(batch)}"
            try:
                resp = await self._http.request("GET", url)
            except CircuitOpen:
                break
            except Exception as exc:  # pragma: no cover - network failure
                log.debug("DexScreener batch failed: %s", exc)
                continue
            data = await resp.json(content_type=None)
            pairs = data.get("pairs")
            if not isinstance(pairs, list):
                continue
            for pair in pairs:
                base = pair.get("baseToken") or {}
                if not isinstance(base, Mapping):
                    continue
                mint = base.get("address")
                if not isinstance(mint, str):
                    continue
                liquidity = _coerce_float(pair.get("liquidity", {}).get("usd"))
                volume_24h = _coerce_float(pair.get("volume", {}).get("h24"))
                volume_1h = _coerce_float(pair.get("volume", {}).get("h1"))
                age_min = _coerce_float(pair.get("pairCreatedAt"))
                symbol = base.get("symbol")
                record = RawCandidate(
                    mint=mint,
                    symbol=str(symbol) if symbol else None,
                    decimals=_coerce_float(base.get("decimals")),
                    liquidity_usd=liquidity,
                    volume_1h_usd=volume_1h,
                    volume_24h_usd=volume_24h,
                    pair_age_min=age_min,
                    sources=(_DEXSCREENER_SOURCE,),
                    payload=pair,
                )
                results.append(record)
        return results


class SolscanHydrator:
    """Optional metadata hydrator when symbol/decimals are missing."""

    def __init__(self, http: HttpClient, *, base_url: str, ttl_seconds: float = 3600.0) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._ttl = ttl_seconds
        self._cache: Dict[str, tuple[float, Mapping[str, Any]]] = {}

    async def hydrate(self, mint: str) -> Mapping[str, Any] | None:
        now = time.time()
        cached = self._cache.get(mint)
        if cached and now - cached[0] < self._ttl:
            return cached[1]
        url = f"{self._base_url}/account/{mint}"
        try:
            resp = await self._http.request("GET", url)
        except CircuitOpen:
            return None
        except Exception:  # pragma: no cover - network failure
            return None
        data = await resp.json(content_type=None)
        self._cache[mint] = (now, data)
        return data
