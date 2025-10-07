from __future__ import annotations

import asyncio
import contextlib
import logging
import math
import os
from typing import Any, Dict, List

from . import onchain_metrics
from .mempool_scanner import stream_ranked_mempool_tokens_with_depth
from .scanner_onchain import scan_tokens_onchain
from .token_scanner import TRENDING_METADATA, enrich_tokens_async, scan_tokens_async

logger = logging.getLogger(__name__)

_DEFAULT_LIMIT = int(os.getenv("DISCOVERY_MAX_TOKENS", "50") or 50)
_DEFAULT_MEMPOOL_THRESHOLD = float(os.getenv("MEMPOOL_SCORE_THRESHOLD", "0") or 0.0)
_MEMPOOL_TIMEOUT = float(os.getenv("DISCOVERY_MEMPOOL_TIMEOUT", "2.5") or 2.5)
_MEMPOOL_TIMEOUT_RETRIES = max(
    1, int(os.getenv("DISCOVERY_MEMPOOL_TIMEOUT_RETRIES", "3") or 3)
)


def _coerce_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return default
        return float(text)
    except Exception:
        return default


def _score_component(value: float) -> float:
    if value <= 0:
        return 0.0
    return math.log1p(value)


async def fetch_trending_tokens_async(limit: int | None = None) -> List[str]:
    """Return a deduplicated list of trending token addresses."""

    size = max(1, int(limit or _DEFAULT_LIMIT))
    tokens = await scan_tokens_async(limit=size)
    try:
        enriched = await enrich_tokens_async(tokens)
    except Exception as exc:  # pragma: no cover - defensive fallback
        logger.debug("RPC enrichment failed: %s", exc)
        enriched = [tok for tok in tokens if isinstance(tok, str)]
    return enriched[:size]


async def _collect_mempool_candidates(
    rpc_url: str,
    *,
    limit: int,
    threshold: float,
    ws_url: str | None = None,
) -> List[Dict[str, Any]]:
    """Collect a handful of ranked mempool events with depth information."""

    if not rpc_url:
        return []

    results: List[Dict[str, Any]] = []
    gen = None
    timeouts = 0
    timeout = max(_MEMPOOL_TIMEOUT, 0.1)
    target_url = ws_url or rpc_url
    try:
        gen = stream_ranked_mempool_tokens_with_depth(target_url, threshold=threshold)
        while len(results) < limit:
            try:
                item = await asyncio.wait_for(anext(gen), timeout=timeout)
            except asyncio.TimeoutError:
                timeouts += 1
                if timeouts >= _MEMPOOL_TIMEOUT_RETRIES:
                    logger.debug(
                        "Mempool stream timed out after %d attempts", timeouts
                    )
                    break
                continue
            except StopAsyncIteration:
                break
            except Exception as exc:  # pragma: no cover - defensive
                logger.debug("Mempool stream yielded error: %s", exc)
                break
            else:
                timeouts = 0
            if not isinstance(item, dict):
                continue
            address = item.get("address")
            if not isinstance(address, str) or not address.strip():
                continue
            results.append(item)
            if len(results) >= limit:
                break
    finally:
        if gen is not None:
            with contextlib.suppress(Exception):
                await gen.aclose()
    return results


def _update_entry(entry: Dict[str, Any], payload: Dict[str, Any]) -> None:
    for key in ("symbol", "name", "market_cap", "price", "price_change"):
        value = payload.get(key)
        if value and key not in entry:
            entry[key] = value


def _finalise_sources(entry: Dict[str, Any]) -> None:
    sources = entry.get("sources") or set()
    if not isinstance(sources, set):
        sources = set(sources)
    entry["sources"] = sorted(sources)


def _combine_score(entry: Dict[str, Any]) -> float:
    volume = _coerce_float(entry.get("volume"))
    liquidity = _coerce_float(entry.get("liquidity"))
    price_change = _coerce_float(entry.get("price_change"))
    mempool_score = _coerce_float(entry.get("mempool_score"))
    helius_score = _coerce_float(entry.get("helius_score"))
    base = _score_component(liquidity) * 0.55 + _score_component(volume) * 0.45
    if price_change:
        base *= max(0.5, 1.0 + (price_change / 100.0) * 0.1)
    if mempool_score:
        base += mempool_score * 5.0
    if helius_score:
        base += helius_score
    return base


async def merge_sources(
    rpc_url: str,
    *,
    limit: int | None = None,
    mempool_threshold: float | None = None,
    ws_url: str | None = None,
) -> List[Dict[str, Any]]:
    """Merge BirdEye/Helius trending tokens with on-chain metrics and mempool data.

    When ``ws_url`` is provided it will be used for websocket mempool discovery,
    allowing callers to supply a pre-validated websocket endpoint separate from
    the HTTP RPC URL.
    """

    size = max(1, int(limit or _DEFAULT_LIMIT))
    threshold = (
        float(mempool_threshold)
        if mempool_threshold is not None
        else _DEFAULT_MEMPOOL_THRESHOLD
    )

    trending_task = asyncio.create_task(fetch_trending_tokens_async(limit=size))
    onchain_task = asyncio.create_task(
        scan_tokens_onchain(rpc_url, return_metrics=True)
    )
    mempool_candidates = await _collect_mempool_candidates(
        rpc_url, limit=size, threshold=threshold, ws_url=ws_url
    )

    trending_result, onchain_result = await asyncio.gather(
        trending_task, onchain_task, return_exceptions=True
    )

    trending_tokens: List[str] = []
    if isinstance(trending_result, Exception):
        logger.debug("Trending discovery failed: %s", trending_result)
    else:
        trending_tokens = [
            tok for tok in trending_result if isinstance(tok, str)
        ][:size]

    onchain_tokens: List[Dict[str, Any]] = []
    if isinstance(onchain_result, Exception):
        logger.debug("On-chain discovery failed: %s", onchain_result)
    else:
        onchain_tokens = [
            tok for tok in onchain_result if isinstance(tok, dict)
        ]

    volume_tasks = [
        asyncio.create_task(onchain_metrics.fetch_volume_onchain_async(tok, rpc_url))
        for tok in trending_tokens
    ]
    liquidity_tasks = [
        asyncio.create_task(
            onchain_metrics.fetch_liquidity_onchain_async(tok, rpc_url)
        )
        for tok in trending_tokens
    ]

    volumes = await asyncio.gather(*volume_tasks, return_exceptions=True) if volume_tasks else []
    liquidities = (
        await asyncio.gather(*liquidity_tasks, return_exceptions=True)
        if liquidity_tasks
        else []
    )

    combined: Dict[str, Dict[str, Any]] = {}

    def _entry(address: str) -> Dict[str, Any]:
        entry = combined.setdefault(address, {"address": address, "sources": set()})
        if not isinstance(entry.get("sources"), set):
            entry["sources"] = set(entry.get("sources", []))
        return entry

    for idx, address in enumerate(trending_tokens):
        entry = _entry(address)
        entry["sources"].add("trending")
        meta = TRENDING_METADATA.get(address) or {}
        _update_entry(entry, meta)
        entry.setdefault("rank", meta.get("rank", idx + 1))
        entry["helius_score"] = max(
            _coerce_float(entry.get("helius_score")),
            _coerce_float(meta.get("score")),
        )
        entry["volume"] = max(
            _coerce_float(entry.get("volume")),
            _coerce_float(volumes[idx] if idx < len(volumes) else 0.0),
            _coerce_float(meta.get("volume")),
        )
        entry["liquidity"] = max(
            _coerce_float(entry.get("liquidity")),
            _coerce_float(liquidities[idx] if idx < len(liquidities) else 0.0),
            _coerce_float(meta.get("liquidity")),
        )
        entry.setdefault("price_change", meta.get("price_change", 0.0))

    for payload in onchain_tokens:
        address = payload.get("address") or payload.get("mint")
        if not isinstance(address, str):
            continue
        entry = _entry(address)
        entry["sources"].add("onchain")
        entry["volume"] = max(
            _coerce_float(entry.get("volume")),
            _coerce_float(payload.get("volume")),
        )
        entry["liquidity"] = max(
            _coerce_float(entry.get("liquidity")),
            _coerce_float(payload.get("liquidity")),
        )
        entry.setdefault("symbol", payload.get("symbol"))
        entry.setdefault("name", payload.get("name", address))

    for payload in mempool_candidates:
        address = payload.get("address")
        if not isinstance(address, str):
            continue
        entry = _entry(address)
        entry["sources"].add("mempool")
        entry["mempool_score"] = max(
            _coerce_float(entry.get("mempool_score")),
            _coerce_float(payload.get("combined_score", payload.get("score"))),
        )
        entry["volume"] = max(
            _coerce_float(entry.get("volume")),
            _coerce_float(payload.get("volume")),
        )
        entry["liquidity"] = max(
            _coerce_float(entry.get("liquidity")),
            _coerce_float(payload.get("liquidity")),
        )
        _update_entry(entry, payload)

    final: List[Dict[str, Any]] = []
    for address, entry in combined.items():
        _finalise_sources(entry)
        entry["score"] = _combine_score(entry)
        final.append(entry)

    final.sort(key=lambda item: (_coerce_float(item.get("score")), _coerce_float(item.get("volume"))), reverse=True)

    return final[:size]


__all__ = [
    "fetch_trending_tokens_async",
    "merge_sources",
]
