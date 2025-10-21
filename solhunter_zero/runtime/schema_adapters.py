"""Adapters that normalise runtime-facing market and snapshot payloads."""

from __future__ import annotations

import logging
import math
import time
from typing import Any, Dict, Iterable, Mapping, MutableMapping, Optional, Sequence

from ..event_bus import publish

log = logging.getLogger(__name__)

_SCHEMA_WARNING_INTERVAL = 60.0
_SCHEMA_WARNING_STATE: Dict[tuple[str, str], float] = {}


def _maybe_float(value: Any) -> Optional[float]:
    if value in (None, "", "null"):
        return None
    try:
        result = float(value)
    except Exception:
        return None
    if not math.isfinite(result):
        return None
    return result


def _maybe_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        result = int(value)
    except Exception:
        try:
            result = int(float(value))
        except Exception:
            return None
    return result


def _rate_limited_warning(key: tuple[str, str], message: str) -> None:
    now = time.time()
    last = _SCHEMA_WARNING_STATE.get(key)
    if last is not None and now - last < _SCHEMA_WARNING_INTERVAL:
        return
    _SCHEMA_WARNING_STATE[key] = now
    log.warning(message)


def _record_schema_mismatch(
    *,
    stream: str,
    reader: str,
    missing: Sequence[str],
    fallback: Sequence[str],
) -> None:
    if not missing and not fallback:
        return
    payload = {
        "stream": stream,
        "reader": reader,
        "missing": sorted(set(missing)),
        "fallback": sorted(set(fallback)),
        "ts": time.time(),
    }
    try:
        publish("metrics.schema_mismatch_total", payload)
    except Exception:  # pragma: no cover - telemetry best effort
        log.debug("failed to publish schema mismatch metric", exc_info=True)
    warn_key = (stream, reader)
    parts: list[str] = []
    if missing:
        parts.append(f"missing={sorted(set(missing))}")
    if fallback:
        parts.append(f"fallback={sorted(set(fallback))}")
    message = f"schema mismatch for {stream} reader={reader}: " + ", ".join(parts)
    _rate_limited_warning(warn_key, message)


def _extract_numeric(
    payload: Mapping[str, Any],
    keys: Sequence[str],
    *,
    parser: Any,
    required: bool,
    track_fallback: bool,
    missing: set[str],
    fallback: set[str],
) -> Optional[float]:
    for idx, key in enumerate(keys):
        if key not in payload:
            continue
        value = parser(payload.get(key))
        if value is not None:
            if idx > 0 and track_fallback:
                fallback.add(keys[0])
            return value
    if required:
        missing.add(keys[0])
    return None


def read_ohlcv(
    payload: Mapping[str, Any] | None,
    *,
    reader: str,
    stream: str = "x:market.ohlcv.5m",
) -> Dict[str, Any]:
    if isinstance(payload, Mapping):
        cached = payload.get("_normalized_ohlcv")
        if isinstance(cached, Mapping):
            return dict(cached)
    data: Mapping[str, Any] = payload or {}
    missing: set[str] = set()
    fallback: set[str] = set()
    open_value = _extract_numeric(
        data,
        ("o", "open"),
        parser=_maybe_float,
        required=True,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    high_value = _extract_numeric(
        data,
        ("h", "high"),
        parser=_maybe_float,
        required=True,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    low_value = _extract_numeric(
        data,
        ("l", "low"),
        parser=_maybe_float,
        required=True,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    close_value = _extract_numeric(
        data,
        ("c", "close"),
        parser=_maybe_float,
        required=True,
        track_fallback=True,
        missing=missing,
        fallback=fallback,
    )
    volume_usd = _extract_numeric(
        data,
        ("vol_usd", "volume", "volume_usd"),
        parser=_maybe_float,
        required=True,
        track_fallback=True,
        missing=missing,
        fallback=fallback,
    )
    volume_base = _extract_numeric(
        data,
        ("vol_base", "volume_base"),
        parser=_maybe_float,
        required=False,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    trades = _extract_numeric(
        data,
        ("trades", "trade_count"),
        parser=_maybe_int,
        required=False,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    buyers = _extract_numeric(
        data,
        ("buyers", "buyer_count"),
        parser=_maybe_int,
        required=False,
        track_fallback=True,
        missing=missing,
        fallback=fallback,
    )
    sellers = _extract_numeric(
        data,
        ("sellers", "seller_count"),
        parser=_maybe_int,
        required=False,
        track_fallback=True,
        missing=missing,
        fallback=fallback,
    )
    zret = _extract_numeric(
        data,
        ("zret",),
        parser=_maybe_float,
        required=False,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    zvol = _extract_numeric(
        data,
        ("zvol",),
        parser=_maybe_float,
        required=False,
        track_fallback=False,
        missing=missing,
        fallback=fallback,
    )
    normalized = {
        "mint": data.get("mint"),
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume_usd": volume_usd,
        "volume_base": volume_base,
        "trades": trades,
        "buyers": buyers,
        "sellers": sellers,
        "zret": zret,
        "zvol": zvol,
        "schema_version": data.get("schema_version"),
        "content_hash": data.get("content_hash"),
        "asof_close": data.get("asof_close"),
    }
    if isinstance(payload, MutableMapping):
        payload["_normalized_ohlcv"] = dict(normalized)
    _record_schema_mismatch(
        stream=stream,
        reader=reader,
        missing=sorted(missing),
        fallback=sorted(fallback),
    )
    return normalized


def _extract_nested_float(value: Any, preferred_keys: Iterable[str]) -> Optional[float]:
    if isinstance(value, Mapping):
        for key in preferred_keys:
            if key in value:
                numeric = _maybe_float(value.get(key))
                if numeric is not None:
                    return numeric
        for candidate in value.values():
            numeric = _extract_nested_float(candidate, preferred_keys)
            if numeric is not None:
                return numeric
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            numeric = _extract_nested_float(item, preferred_keys)
            if numeric is not None:
                return numeric
        return None
    return _maybe_float(value)


def read_golden(
    payload: Mapping[str, Any] | None,
    *,
    reader: str,
    stream: str = "x:mint.golden",
) -> Dict[str, Any]:
    if isinstance(payload, Mapping):
        cached = payload.get("_normalized_golden")
        if isinstance(cached, Mapping):
            return dict(cached)
    data: Mapping[str, Any] = payload or {}
    missing: set[str] = set()
    fallback: set[str] = set()
    mid_usd = _maybe_float(data.get("px_mid_usd"))
    if mid_usd is None:
        px = data.get("px")
        mid_usd = _extract_nested_float(
            px,
            (
                "mid_usd",
                "midUsd",
                "mid",
                "mid_price_usd",
                "price_usd",
                "fair_price",
                "price",
            ),
        )
        if mid_usd is not None:
            fallback.add("px_mid_usd")
        else:
            missing.add("px_mid_usd")
    depth_value = _maybe_float(data.get("liq_depth_1pct_usd"))
    if depth_value is None:
        liq = data.get("liq")
        depth_source: Any = liq
        if isinstance(liq, Mapping):
            depth_source = liq.get("depth_usd_by_pct") or liq.get("depth_pct") or liq
        depth_value = _extract_nested_float(depth_source, ("1", "1.0", "100", "100bps"))
        if depth_value is not None:
            fallback.add("liq_depth_1pct_usd")
        else:
            missing.add("liq_depth_1pct_usd")
    ohlcv_payload = data.get("ohlcv5m")
    if not isinstance(ohlcv_payload, Mapping):
        ohlcv_payload = data.get("ohlcv") if isinstance(data.get("ohlcv"), Mapping) else {}
        if ohlcv_payload:
            fallback.add("ohlcv5m")
        else:
            missing.add("ohlcv5m")
    normalized_ohlcv = read_ohlcv(
        ohlcv_payload if isinstance(ohlcv_payload, Mapping) else {},
        reader=f"{reader}.ohlcv",
        stream="x:market.ohlcv.5m",
    )
    normalized = {
        "mint": data.get("mint"),
        "hash": data.get("hash"),
        "content_hash": data.get("content_hash"),
        "schema_version": data.get("schema_version"),
        "mid_usd": mid_usd,
        "depth_1pct_usd": depth_value,
        "px": data.get("px"),
        "liq": data.get("liq"),
        "ohlcv": normalized_ohlcv,
        "asof": data.get("asof"),
    }
    if isinstance(payload, MutableMapping):
        payload["_normalized_golden"] = dict(normalized)
    _record_schema_mismatch(
        stream=stream,
        reader=reader,
        missing=sorted(missing),
        fallback=sorted(fallback),
    )
    return normalized


__all__ = ["read_ohlcv", "read_golden"]
