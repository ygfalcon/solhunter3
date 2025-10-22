"""Spot mid pricing helpers for the Golden Stream pipeline."""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Mapping, MutableMapping, Sequence

from .http import CircuitOpen, HttpClient
from ..prices import _parse_pyth_mapping  # type: ignore[attr-defined]


@dataclass(slots=True)
class PriceQuote:
    mint: str
    mid_usd: float
    asof: float
    source: str
    confidence: float | None = None

    @property
    def staleness_ms(self) -> float:
        return max(0.0, (time.time() - self.asof) * 1000.0)


class JupiterPriceClient:
    def __init__(self, http: HttpClient, *, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")

    async def fetch(self, mints: Sequence[str]) -> Dict[str, PriceQuote]:
        if not mints:
            return {}
        unique = []
        seen = set()
        for mint in mints:
            if mint in seen:
                continue
            seen.add(mint)
            unique.append(mint)
        ids = ",".join(unique)
        url = f"{self._base_url}/price/v3"
        params = {"ids": ids}
        try:
            resp = await self._http.request("GET", url, params=params)
        except CircuitOpen:
            return {}
        except Exception:
            return {}
        payload = await resp.json(content_type=None)
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, Mapping):
            return {}
        results: Dict[str, PriceQuote] = {}
        now = time.time()
        for mint, entry in data.items():
            if not isinstance(entry, Mapping):
                continue
            price = entry.get("price") or entry.get("midPrice")
            try:
                mid = float(price)
            except (TypeError, ValueError):
                continue
            ts_raw = entry.get("lastUpdatedAt") or entry.get("time")
            try:
                asof = float(ts_raw)
            except (TypeError, ValueError):
                asof = now
            results[mint] = PriceQuote(
                mint=mint,
                mid_usd=mid,
                asof=asof,
                source="jup_price",
            )
        return results


class PythHermesClient:
    def __init__(self, http: HttpClient, *, base_url: str) -> None:
        self._http = http
        self._base_url = base_url.rstrip("/")
        self._mapping, _ = _parse_pyth_mapping()
        self._cache: MutableMapping[str, PriceQuote] = {}

    async def fetch(self, mints: Sequence[str]) -> Dict[str, PriceQuote]:
        if not mints:
            return {}
        ids = []
        mint_lookup: Dict[str, str] = {}
        for mint in mints:
            identifier = self._mapping.get(mint)
            if not identifier:
                continue
            ids.append(identifier.feed_id)
            mint_lookup[identifier.feed_id] = mint
        if not ids:
            return {}
        params = [("ids[]", fid) for fid in ids]
        url = f"{self._base_url}/v2/updates/price/latest"
        try:
            resp = await self._http.request("GET", url, params=params)
        except CircuitOpen:
            return {}
        except Exception:
            return {}
        payload = await resp.json(content_type=None)
        data = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(data, list):
            return {}
        results: Dict[str, PriceQuote] = {}
        for entry in data:
            if not isinstance(entry, Mapping):
                continue
            feed_id = entry.get("id")
            mint = mint_lookup.get(feed_id)
            if not mint:
                continue
            price_info = entry.get("price") or {}
            if not isinstance(price_info, Mapping):
                continue
            try:
                mid = float(price_info.get("price"))
            except (TypeError, ValueError):
                continue
            conf_raw = price_info.get("conf")
            try:
                conf = float(conf_raw)
            except (TypeError, ValueError):
                conf = None
            ts_raw = entry.get("publishTime") or entry.get("timestamp")
            try:
                asof = float(ts_raw)
            except (TypeError, ValueError):
                asof = time.time()
            results[mint] = PriceQuote(
                mint=mint,
                mid_usd=mid,
                asof=asof,
                source="pyth_mid",
                confidence=conf,
            )
        return results


class PricingEngine:
    def __init__(
        self,
        jupiter: JupiterPriceClient,
        pyth: PythHermesClient,
    ) -> None:
        self._jupiter = jupiter
        self._pyth = pyth

    async def quote(self, mint: str) -> PriceQuote | None:
        quotes = await self.quote_many([mint])
        return quotes.get(mint)

    async def quote_many(self, mints: Sequence[str]) -> Dict[str, PriceQuote]:
        jupiter_quotes = await self._jupiter.fetch(mints)
        missing = [mint for mint in mints if mint not in jupiter_quotes]
        fallback = await self._pyth.fetch(missing) if missing else {}
        return {**jupiter_quotes, **fallback}

