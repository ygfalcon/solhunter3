"""Snapshot coalescer producing Golden Snapshots."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from typing import Any, Awaitable, Callable, Dict, Mapping

from .contracts import golden_hash_key
from .kv import KeyValueStore
from .types import (
    DepthSnapshot,
    GOLDEN_SNAPSHOT_SCHEMA_VERSION,
    GoldenSnapshot,
    OHLCVBar,
    TokenSnapshot,
)
from .utils import canonical_hash, now_ts
from ..lru import TTLCache


logger = logging.getLogger(__name__)


_GOLDEN_PREFIX = golden_hash_key("")


class SnapshotCoalescer:
    """Join token metadata, OHLCV, and depth into Golden Snapshots."""

    def __init__(
        self,
        emit: Callable[[GoldenSnapshot], Awaitable[None]],
        *,
        kv: KeyValueStore | None = None,
        hash_ttl: float = 90.0,
        hash_cache_size: int = 4096,
        depth_extensions_enabled: bool = False,
        depth_near_fresh_ms: float | None = None,
    ) -> None:
        self._emit = emit
        self._meta: Dict[str, TokenSnapshot] = {}
        self._bars: Dict[str, OHLCVBar] = {}
        self._depth: Dict[str, DepthSnapshot] = {}
        self._hash_cache: TTLCache[str, str] = TTLCache(
            maxsize=hash_cache_size,
            ttl=hash_ttl,
        )
        self._versions: Dict[str, int] = {}
        self._locks_guard = asyncio.Lock()
        self._mint_locks: Dict[str, asyncio.Lock] = {}
        self._kv = kv
        self._hash_ttl = hash_ttl
        self._prewarm_task: asyncio.Task[None] | None = None
        self._depth_extensions_enabled = depth_extensions_enabled
        normalized_near_fresh: float | None
        if depth_near_fresh_ms is None:
            normalized_near_fresh = None
        else:
            try:
                candidate = float(depth_near_fresh_ms)
            except Exception:
                candidate = None
            if candidate is not None and candidate > 0:
                normalized_near_fresh = candidate
            else:
                normalized_near_fresh = None
        self._depth_near_fresh_ms = normalized_near_fresh
        self._schedule_hash_prewarm()

    def _schedule_hash_prewarm(self) -> None:
        if not self._kv:
            return
        scan = getattr(self._kv, "scan_prefix", None)
        if not callable(scan):
            return
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        self._prewarm_task = loop.create_task(self._prewarm_hash_cache(scan))
        self._prewarm_task.add_done_callback(self._handle_prewarm_result)

    def _handle_prewarm_result(self, task: asyncio.Task[None]) -> None:
        try:
            task.result()
        except Exception:  # pragma: no cover - logging safeguard
            logger.exception("failed prewarming golden hash cache")

    async def _prewarm_hash_cache(self, scan: Callable[[str], Awaitable[Any]]) -> None:
        try:
            result = await scan(_GOLDEN_PREFIX)
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("error scanning persisted golden hashes")
            return

        if not result:
            return

        for key, value in result:  # type: ignore[type-var]
            self._store_hash_from_kv(key, value)

    def _store_hash_from_kv(self, key: str, value: str | None) -> None:
        if not key.startswith(_GOLDEN_PREFIX):
            return
        if not value:
            return
        mint = key[len(_GOLDEN_PREFIX) :]
        if not mint:
            return
        self._hash_cache.set(mint, value)

    async def update_metadata(self, snapshot: TokenSnapshot) -> None:
        await self._update_and_maybe_emit(snapshot.mint, meta=snapshot)

    async def update_bar(self, bar: OHLCVBar) -> None:
        await self._update_and_maybe_emit(bar.mint, bar=bar)

    async def update_depth(self, depth: DepthSnapshot) -> None:
        await self._update_and_maybe_emit(depth.mint, depth=depth)

    def get_metadata(self, mint: str) -> TokenSnapshot | None:
        return self._meta.get(mint)

    async def _get_mint_lock(self, mint: str) -> asyncio.Lock:
        async with self._locks_guard:
            lock = self._mint_locks.get(mint)
            if lock is None:
                lock = asyncio.Lock()
                self._mint_locks[mint] = lock
                self._versions.setdefault(mint, 0)
            return lock

    async def _update_and_maybe_emit(
        self,
        mint: str,
        *,
        meta: TokenSnapshot | None = None,
        bar: OHLCVBar | None = None,
        depth: DepthSnapshot | None = None,
    ) -> None:
        lock = await self._get_mint_lock(mint)
        async with lock:
            if meta is not None:
                self._meta[mint] = meta
            if bar is not None:
                self._bars[mint] = bar
            if depth is not None:
                self._depth[mint] = depth
            current_meta = self._meta.get(mint)
            current_bar = self._bars.get(mint)
            current_depth = self._depth.get(mint)
            version = self._versions.get(mint, 0) + 1
            self._versions[mint] = version
        await self._maybe_emit(
            mint,
            current_meta,
            current_bar,
            current_depth,
            version,
            lock,
        )

    async def _maybe_emit(
        self,
        mint: str,
        meta: TokenSnapshot | None,
        bar: OHLCVBar | None,
        depth: DepthSnapshot | None,
        version: int,
        lock: asyncio.Lock,
    ) -> None:
        if not (meta and bar and depth):
            return
        now = now_ts()
        depth_pct = dict(depth.depth_pct)

        def _normalize_pct(value: Any) -> float | None:
            if value is None:
                return None
            raw = str(value).strip().lower()
            if not raw:
                return None
            if raw.endswith("bps"):
                raw = raw[:-3]
                try:
                    return float(raw) / 100.0
                except Exception:
                    return None
            if raw.endswith("%"):
                raw = raw[:-1]
            try:
                numeric = float(raw)
            except Exception:
                return None
            if numeric > 1.0:
                numeric = numeric / 100.0
            if numeric < 0:
                return None
            return numeric

        bands = []
        depth_usd_by_pct: Dict[str, float] = {}
        for key, value in depth_pct.items():
            pct_value = _normalize_pct(key)
            if pct_value is None:
                continue
            try:
                usd_value = float(value)
            except Exception:
                continue
            bands.append({"pct": pct_value, "usd": usd_value})
            label = f"{pct_value:.3f}".rstrip("0").rstrip(".")
            depth_usd_by_pct[label] = usd_value
        bands.sort(key=lambda item: item["pct"])
        ohlcv_payload = {
            "o": bar.open,
            "h": bar.high,
            "l": bar.low,
            "c": bar.close,
            "close": bar.close,
            "vol_usd": bar.vol_usd,
            "volume": bar.vol_usd,
            "volume_usd": bar.vol_usd,
            "vol_base": bar.vol_base,
            "volume_base": bar.vol_base,
            "buyers": bar.buyers,
            "flow_usd": bar.flow_usd,
            "zret": bar.zret,
            "zvol": bar.zvol,
            "asof_close": bar.asof_close,
            "schema_version": bar.schema_version,
        }
        ohlcv_payload["content_hash"] = canonical_hash(dict(ohlcv_payload))
        depth_band_values: Dict[str, float] = {}
        if depth.depth_bands_usd:
            for key, value in depth.depth_bands_usd.items():
                try:
                    depth_band_values[str(key)] = float(value)
                except Exception:
                    continue
        liq_payload = {
            "depth_pct": depth_pct,
            "depth_usd_by_pct": {**depth_usd_by_pct, **depth_band_values} if depth_band_values else depth_usd_by_pct,
            "bands": bands,
            "asof": depth.asof,
        }
        if self._depth_extensions_enabled:
            liq_payload.setdefault("depth_usd_by_pct", depth_usd_by_pct)
            if depth_band_values:
                liq_payload["depth_usd_by_pct"].update(depth_band_values)
            liq_payload["degraded"] = bool(depth.degraded)
            if depth.source:
                liq_payload["source"] = depth.source
            if depth.route_meta:
                liq_payload["route_meta"] = depth.route_meta
        mid_price = float(depth.mid_usd)
        spread_bps = float(depth.spread_bps)
        half_spread = 0.0
        if mid_price > 0 and spread_bps > 0:
            half_spread = mid_price * (spread_bps / 20000.0)
        bid_usd = depth.px_bid_usd
        ask_usd = depth.px_ask_usd
        if bid_usd is None:
            bid_usd = max(0.0, mid_price - half_spread)
        if ask_usd is None:
            ask_usd = max(0.0, mid_price + half_spread)
        payload = {
            "mint": mint,
            "meta": {
                "symbol": meta.symbol,
                "decimals": meta.decimals,
                "token_program": meta.token_program,
                "asof": meta.asof,
            },
            "px": {
                "mid_usd": mid_price,
                "spread_bps": depth.spread_bps,
                "bid_usd": bid_usd,
                "ask_usd": ask_usd,
                "ts": depth.asof,
            },
            "liq": liq_payload,
            "ohlcv5m": ohlcv_payload,
            "schema_version": GOLDEN_SNAPSHOT_SCHEMA_VERSION,
        }
        if payload["px"]["ts"] > now:
            payload["px"]["ts"] = now
        if liq_payload["asof"] > now:
            liq_payload["asof"] = now
        payload["px_mid_usd"] = mid_price

        if self._depth_extensions_enabled:
            depth_lookup = liq_payload.get("depth_usd_by_pct") if isinstance(liq_payload, dict) else {}
            def _get_band(label: str) -> float | None:
                if not isinstance(depth_lookup, Mapping):
                    return None
                value = depth_lookup.get(label)
                if value is None and label.endswith("%"):
                    value = depth_lookup.get(label[:-1])
                if value is None:
                    return None
                try:
                    return float(value)
                except Exception:
                    return None

            payload["liq_depth_0_1pct_usd"] = _get_band("0.1")
            payload["liq_depth_0_5pct_usd"] = _get_band("0.5")
            payload["liq_depth_1_0pct_usd"] = _get_band("1.0") or _get_band("1")
            payload["px_bid_usd"] = bid_usd
            payload["px_ask_usd"] = ask_usd
            payload["degraded"] = bool(depth.degraded)
            source_value = depth.source or (
                liq_payload.get("source") if isinstance(liq_payload, Mapping) else None
            )
            if source_value:
                payload["source"] = str(source_value)
            if payload.get("liq_depth_0_5pct_usd") is not None and payload.get("liq_depth_0_1pct_usd") is not None:
                payload["liq_depth_0_5pct_usd"] = max(
                    float(payload["liq_depth_0_5pct_usd"]), float(payload["liq_depth_0_1pct_usd"])
                )
            if payload.get("liq_depth_1_0pct_usd") is not None and payload.get("liq_depth_0_5pct_usd") is not None:
                payload["liq_depth_1_0pct_usd"] = max(
                    float(payload["liq_depth_1_0pct_usd"]), float(payload["liq_depth_0_5pct_usd"])
                )
            if payload.get("px_bid_usd") is not None and payload.get("px_ask_usd") is not None:
                if payload["px_ask_usd"] < payload["px_bid_usd"]:
                    midpoint = (float(payload["px_bid_usd"]) + float(payload["px_ask_usd"])) / 2.0
                    payload["px_bid_usd"] = midpoint
                    payload["px_ask_usd"] = midpoint

        def _lookup_depth(target: float) -> float | None:
            for band in bands:
                if abs(band["pct"] - target) <= 1e-6:
                    return band["usd"]
            return None

        depth_1pct = _lookup_depth(1.0)
        payload["liq_depth_1pct_usd"] = depth_1pct
        payload["content_hash"] = canonical_hash({k: v for k, v in payload.items() if k != "content_hash"})
        band_rounds: Dict[str, float] | None = None
        if self._depth_extensions_enabled:
            band_rounds = {}
            for label, field_name in (
                ("0.1", "liq_depth_0_1pct_usd"),
                ("0.5", "liq_depth_0_5pct_usd"),
                ("1.0", "liq_depth_1_0pct_usd"),
            ):
                value = payload.get(field_name)
                if value is None:
                    continue
                try:
                    band_rounds[label] = round(float(value), 2)
                except Exception:
                    continue
            if not band_rounds:
                band_rounds = None

        idempotency_source = {
            "mint": mint,
            "meta": payload["meta"],
            "px": payload["px"],
            "liq": payload["liq"],
            "ohlcv5m": payload["ohlcv5m"],
            "schema_version": payload["schema_version"],
        }
        if band_rounds:
            idempotency_source["depth_band_cents"] = band_rounds
        payload["idempotency_key"] = hashlib.sha1(
            json.dumps(idempotency_source, sort_keys=True, separators=(",", ":")).encode("utf-8")
        ).hexdigest()
        snapshot_hash = canonical_hash(payload)
        async with lock:
            if self._versions.get(mint, 0) != version:
                return
            if self._hash_cache.get(mint) == snapshot_hash:
                return
            self._hash_cache[mint] = snapshot_hash
        if self._kv:
            key = golden_hash_key(mint)
            cached = await self._kv.get(key)
            if cached == snapshot_hash:
                return
            await self._kv.set(key, snapshot_hash, ttl=self._hash_ttl)
        asof = max(meta.asof, depth.asof, bar.asof_close)
        latency_ms = max(0.0, (now - asof) * 1000.0)
        depth_staleness_ms = max(0.0, (now - depth.asof) * 1000.0)
        candle_age_ms = max(0.0, (now - bar.asof_close) * 1000.0)
        near_fresh = None
        if self._depth_near_fresh_ms is not None:
            near_fresh = depth_staleness_ms <= self._depth_near_fresh_ms
        payload["liq"]["staleness_ms"] = depth_staleness_ms
        if self._depth_extensions_enabled:
            payload["staleness_ms"] = depth_staleness_ms
        golden = GoldenSnapshot(
            mint=mint,
            asof=asof,
            meta=payload["meta"],
            px=payload["px"],
            liq=payload["liq"],
            ohlcv5m=payload["ohlcv5m"],
            hash=snapshot_hash,
            content_hash=payload.get("content_hash", snapshot_hash),
            idempotency_key=payload.get("idempotency_key", ""),
            metrics={
                "emitted_at": now,
                "latency_ms": latency_ms,
                "depth_staleness_ms": depth_staleness_ms,
                "candle_age_ms": candle_age_ms,
            },
        )
        if near_fresh is not None:
            golden.metrics["depth_near_fresh"] = bool(near_fresh)
            golden.metrics["depth_near_fresh_window_ms"] = self._depth_near_fresh_ms
        await self._emit(golden)
