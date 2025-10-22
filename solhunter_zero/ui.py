from __future__ import annotations

import asyncio
import contextlib
import errno
import functools
import hashlib
import json
import logging
import math
import os
import subprocess
import sys
import threading
import time
import types
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from queue import Queue
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse, urlunparse

from flask import Flask, Request, Response, jsonify, render_template, request
from werkzeug.serving import BaseWSGIServer, make_server

from . import event_bus
from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    DISCOVERY_METHODS,
    resolve_discovery_method,
)
from .util import parse_bool_env


log = logging.getLogger(__name__)


UI_SCHEMA_VERSION: int = 3
_UI_META_CACHE_TTL = 1.0
_ui_meta_cache: tuple[float, Dict[str, Any]] | None = None
_active_ui_state: "UIState" | None = None


def _set_active_ui_state(state: "UIState" | None) -> None:
    global _active_ui_state
    _active_ui_state = state


def _get_active_ui_state() -> "UIState" | None:
    return _active_ui_state


def _select_first_url(*candidates: Any) -> str | None:
    for candidate in candidates:
        if not candidate:
            continue
        if isinstance(candidate, (list, tuple, set)):
            selected = _select_first_url(*candidate)
            if selected:
                return selected
            continue
        text = str(candidate).strip()
        if text:
            return text
    return None


def _discover_broker_url() -> str | None:
    urls = getattr(event_bus, "_BROKER_URLS", None) or []
    selected = _select_first_url(urls)
    if selected:
        return selected
    env_urls = os.getenv("BROKER_URLS") or os.getenv("BROKER_URLS_JSON")
    if env_urls:
        parts = [part.strip() for part in env_urls.replace("[", "").replace("]", "").split(",")]
        selected = _select_first_url([part for part in parts if part])
        if selected:
            return selected
    single = os.getenv("BROKER_URL")
    if single and single.strip():
        return single.strip()
    redis_env = os.getenv("REDIS_URL")
    if redis_env and redis_env.strip():
        return redis_env.strip()
    return None


def _parse_redis_url(url: str | None) -> Dict[str, Any]:
    if not url:
        return {}
    parsed = urlparse(url)
    info: Dict[str, Any] = {"url": url}
    if parsed.hostname:
        info["host"] = parsed.hostname
    if parsed.port is not None:
        info["port"] = parsed.port
    path = (parsed.path or "").strip("/")
    if path:
        try:
            info["db"] = int(path)
        except ValueError:
            pass
    return info


def _maybe_float(value: Any) -> Optional[float]:
    if value in (None, "", False):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(number):
        return None
    return number


def _maybe_int(value: Any) -> Optional[int]:
    try:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        text = str(value).strip()
        if not text:
            return None
        return int(float(text))
    except (TypeError, ValueError):
        return None


def _env_or_default(name: str) -> Optional[str]:
    value = os.getenv(name)
    if value is not None:
        return value
    try:
        from .env_defaults import DEFAULTS
    except Exception:
        return None
    return DEFAULTS.get(name)


@functools.lru_cache(maxsize=1)
def _resolve_app_version() -> Optional[str]:
    try:
        from . import __version__ as version  # type: ignore[attr-defined]
    except Exception:
        return None
    return str(version)


@functools.lru_cache(maxsize=1)
def _resolve_build_git() -> Optional[str]:
    for key in ("BUILD_GIT", "GIT_COMMIT", "HEROKU_SLUG_COMMIT", "SOURCE_COMMIT"):
        candidate = os.getenv(key)
        if candidate:
            text = candidate.strip()
            if text:
                return text
    git_dir = Path(__file__).resolve().parents[1] / ".git"
    if not git_dir.exists():
        return None
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None
    text = (result.stdout or "").strip()
    return text or None


@functools.lru_cache(maxsize=1)
def _compute_schema_hash() -> str:
    try:
        from .schemas.golden_pipeline import STREAM_SCHEMAS
    except Exception:
        return "unknown"
    records: List[Dict[str, Any]] = []
    for topic in sorted(STREAM_SCHEMAS.keys()):
        schema = STREAM_SCHEMAS.get(topic) or {}
        schema_version = None
        properties = schema.get("properties")
        if isinstance(properties, dict):
            version_prop = properties.get("schema_version")
            if isinstance(version_prop, dict):
                schema_version = version_prop.get("const")
        required = schema.get("required")
        if isinstance(required, (list, tuple)):
            required_fields = sorted(str(field) for field in required)
        else:
            required_fields = []
        records.append(
            {
                "topic": topic,
                "schema_version": schema_version,
                "required": required_fields,
            }
        )
    payload = {"ui_schema_version": UI_SCHEMA_VERSION, "schemas": records}
    digest = hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()
    return digest


def _price_provider_order() -> List[str]:
    try:
        from . import prices
    except Exception:
        return []
    try:
        providers = [str(name) for name in prices.PROVIDER_CONFIGS.keys()]
    except Exception:
        providers = []
    preferred = ["pyth", "dexscreener", "birdeye", "synthetic"]
    ordered: List[str] = []
    for name in preferred:
        if name in providers and name not in ordered:
            ordered.append(name)
    for name in providers:
        if name not in ordered:
            ordered.append(name)
    return ordered


def _discover_seed_tokens() -> List[str]:
    try:
        from . import seed_token_publisher
    except Exception:
        seed_tokens: Sequence[str] = ()
    else:
        try:
            seed_tokens = seed_token_publisher.configured_seed_tokens()
        except Exception:
            seed_tokens = ()
    tokens: List[str] = [str(token) for token in (seed_tokens or ()) if token]
    if not tokens:
        fallback = _env_or_default("SEED_TOKENS") or ""
        if fallback:
            for raw in str(fallback).split(","):
                candidate = raw.strip()
                if candidate:
                    tokens.append(candidate)
    return tokens


def _pyth_price_hints() -> List[Dict[str, Any]]:
    try:
        from . import prices
    except Exception:
        return []
    try:
        mapping, extras = prices._parse_pyth_mapping()  # type: ignore[attr-defined]
    except Exception:
        return []
    hints: List[Dict[str, Any]] = []
    for mint, identifier in mapping.items():
        if identifier is None:
            continue
        hint: Dict[str, Any] = {
            "mint": str(mint),
            "kind": getattr(identifier, "kind", None),
            "id": getattr(identifier, "feed_id", None),
        }
        account = getattr(identifier, "account", None)
        if account:
            hint["account"] = account
        hint = {key: value for key, value in hint.items() if value}
        if hint:
            hints.append(hint)
    for identifier in extras:
        if identifier is None:
            continue
        hint = {
            "mint": getattr(identifier, "canonical_mint", None) or getattr(identifier, "mint", None),
            "kind": getattr(identifier, "kind", None),
            "id": getattr(identifier, "feed_id", None),
        }
        account = getattr(identifier, "account", None)
        if account:
            hint["account"] = account
        hint = {key: value for key, value in hint.items() if value}
        if hint:
            hints.append(hint)
    return hints


def _discovery_hint_sources(state: "UIState" | None) -> List[str]:
    if state is None:
        return []
    try:
        console = state.snapshot_discovery_console()
    except Exception:
        return []
    candidates = console.get("candidates") if isinstance(console, Mapping) else None
    sources: Set[str] = set()
    if isinstance(candidates, Iterable):
        for candidate in candidates:
            if not isinstance(candidate, Mapping):
                continue
            origin = candidate.get("source") or candidate.get("origin")
            if isinstance(origin, str) and origin:
                sources.add(origin)
            elif isinstance(origin, (list, tuple, set)):
                for item in origin:
                    if isinstance(item, str) and item:
                        sources.add(item)
            extra_sources = candidate.get("sources")
            if isinstance(extra_sources, (list, tuple, set)):
                for item in extra_sources:
                    if isinstance(item, str) and item:
                        sources.add(item)
    if not sources:
        sources.update(["pump_fun", "birdeye_trending", "raydium_new_pairs"])
    return sorted(sources)


def _resolve_depth_ttl() -> float:
    for key in ("GOLDEN_DEPTH_TTL_SECONDS", "GOLDEN_DEPTH_CACHE_TTL"):
        raw = os.getenv(key)
        if not raw:
            continue
        try:
            value = float(raw)
        except Exception:
            continue
        if value > 0:
            return max(0.5, value)
    return 10.0


def _install_jsonschema_stub() -> None:
    """Install a minimal ``jsonschema`` module when the dependency is missing."""

    if "jsonschema" in sys.modules:
        return

    module = types.ModuleType("jsonschema")

    class _DummyValidator:  # pragma: no cover - only exercised in dependency-lite envs
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.schema = args[0] if args else kwargs.get("schema")

        def iter_errors(self, *args: Any, **kwargs: Any) -> list[Any]:
            return []

        def validate(self, *args: Any, **kwargs: Any) -> None:
            return None

    exceptions_module = types.ModuleType("jsonschema.exceptions")

    class _ValidationError(Exception):  # pragma: no cover - mirrors external API
        def __init__(self, message: str = "", *, instance: Any = None, schema: Any = None) -> None:
            super().__init__(message)
            self.message = message
            self.instance = instance
            self.schema = schema

    module.Draft202012Validator = _DummyValidator  # type: ignore[attr-defined]
    module.exceptions = exceptions_module  # type: ignore[attr-defined]
    exceptions_module.ValidationError = _ValidationError  # type: ignore[attr-defined]

    sys.modules["jsonschema"] = module
    sys.modules["jsonschema.exceptions"] = exceptions_module


def _schema_entries_fallback(ttl_info: Mapping[str, float]) -> List[Dict[str, Any]]:
    return [
        {
            "name": "discovery",
            "topics": ["x:discovery.candidates", "x:mint.discovered"],
            "schema_id": "discovery@1",
            "required": ["mint", "asof"],
            "_field_types": {"mint": "string", "asof": "number"},
        },
        {
            "name": "market_ohlcv",
            "topics": ["x:market.ohlcv.5m"],
            "schema_id": "ohlcv_5m@2.0",
            "required": [
                "mint",
                "o",
                "h",
                "l",
                "c",
                "close",
                "vol_usd",
                "volume",
                "volume_usd",
                "trades",
                "buyers",
                "zret",
                "zvol",
                "asof_close",
                "schema_version",
                "content_hash",
            ],
            "aliases": {
                "open": "o",
                "high": "h",
                "low": "l",
                "close": "c",
                "volume_usd": "vol_usd",
                "volume_base": "vol_base",
            },
            "ttl_seconds": ttl_info.get("ohlcv_5m_s"),
            "_field_types": {
                "mint": "string",
                "o": "number",
                "h": "number",
                "l": "number",
                "c": "number",
                "close": "number",
                "vol_usd": "number",
                "volume": "number",
                "volume_usd": "number",
                "trades": "integer",
                "buyers": "integer",
                "zret": "number",
                "zvol": "number",
                "asof_close": "number",
                "content_hash": "string",
            },
        },
        {
            "name": "market_depth",
            "topics": ["x:market.depth"],
            "schema_id": "depth_snapshot@1.0",
            "required": ["mint", "venue", "mid_usd", "spread_bps", "depth_pct", "asof", "schema_version"],
            "ttl_seconds": ttl_info.get("depth_s"),
            "_field_types": {
                "mint": "string",
                "venue": "string",
                "mid_usd": "number",
                "spread_bps": "number",
                "depth_pct": "object",
                "asof": "number",
            },
        },
        {
            "name": "golden_snapshot",
            "topics": ["x:mint.golden"],
            "schema_id": "golden_snapshot@2.0",
            "required": [
                "mint",
                "asof",
                "meta",
                "px",
                "liq",
                "ohlcv5m",
                "hash",
                "content_hash",
                "idempotency_key",
                "schema_version",
            ],
            "ttl_seconds": ttl_info.get("golden_s"),
            "_field_types": {
                "mint": "string",
                "asof": "number",
                "meta": "object",
                "px": "object",
                "liq": "object",
                "ohlcv5m": "object",
                "hash": "string",
                "content_hash": "string",
                "idempotency_key": "string",
            },
        },
        {
            "name": "agent_suggestions",
            "topics": ["x:trade.suggested"],
            "schema_id": "trade_suggested@1.0",
            "required": [
                "agent",
                "mint",
                "side",
                "notional_usd",
                "max_slippage_bps",
                "risk",
                "confidence",
                "inputs_hash",
                "ttl_sec",
                "generated_at",
                "sequence",
                "schema_version",
            ],
            "ttl_seconds": ttl_info.get("suggestions_s"),
            "_field_types": {
                "agent": "string",
                "mint": "string",
                "side": "string",
                "notional_usd": "number",
                "max_slippage_bps": "number",
                "risk": "object",
                "confidence": "number",
                "inputs_hash": "string",
                "ttl_sec": "number",
                "generated_at": "number",
                "sequence": "integer",
            },
        },
        {
            "name": "vote_decisions",
            "topics": ["x:vote.decisions"],
            "schema_id": "vote_decision@1.0",
            "required": [
                "mint",
                "side",
                "notional_usd",
                "score",
                "snapshot_hash",
                "client_order_id",
                "agents",
                "ts",
                "sequence",
                "schema_version",
            ],
            "ttl_seconds": ttl_info.get("votes_s"),
            "_field_types": {
                "mint": "string",
                "side": "string",
                "notional_usd": "number",
                "score": "number",
                "snapshot_hash": "string",
                "client_order_id": "string",
                "agents": "array",
                "ts": "number",
                "sequence": "integer",
            },
        },
        {
            "name": "shadow_execution",
            "topics": ["x:virt.fills"],
            "schema_id": "shadow_fill@1.0",
            "required": [
                "order_id",
                "mint",
                "side",
                "qty_base",
                "price_usd",
                "fees_usd",
                "slippage_bps",
                "snapshot_hash",
                "route",
                "ts",
                "schema_version",
            ],
            "_field_types": {
                "order_id": "string",
                "mint": "string",
                "side": "string",
                "qty_base": "number",
                "price_usd": "number",
                "fees_usd": "number",
                "slippage_bps": "number",
                "snapshot_hash": "string",
                "route": "string",
                "ts": "number",
            },
        },
        {
            "name": "rl_weights",
            "topics": ["rl_weights", "rl:weights.applied"],
            "schema_id": "rl_weights@1",
            "required": ["weights"],
            "_field_types": {"weights": "array"},
        },
    ]


def _schema_entries(ttl_info: Mapping[str, float]) -> List[Dict[str, Any]]:
    def _load_pipeline_contracts() -> Tuple[Any, ...]:
        from .golden_pipeline.contracts import STREAMS
        from .golden_pipeline.types import (
            DECISION_SCHEMA_VERSION,
            DEPTH_SNAPSHOT_SCHEMA_VERSION,
            GOLDEN_SNAPSHOT_SCHEMA_VERSION,
            OHLCV_BAR_SCHEMA_VERSION,
            TRADE_SUGGESTION_SCHEMA_VERSION,
            VIRTUAL_FILL_SCHEMA_VERSION,
        )
        from .schemas.golden_pipeline import STREAM_SCHEMAS

        return (
            STREAMS,
            DECISION_SCHEMA_VERSION,
            DEPTH_SNAPSHOT_SCHEMA_VERSION,
            GOLDEN_SNAPSHOT_SCHEMA_VERSION,
            OHLCV_BAR_SCHEMA_VERSION,
            TRADE_SUGGESTION_SCHEMA_VERSION,
            VIRTUAL_FILL_SCHEMA_VERSION,
            STREAM_SCHEMAS,
        )

    try:
        (
            STREAMS,
            DECISION_SCHEMA_VERSION,
            DEPTH_SNAPSHOT_SCHEMA_VERSION,
            GOLDEN_SNAPSHOT_SCHEMA_VERSION,
            OHLCV_BAR_SCHEMA_VERSION,
            TRADE_SUGGESTION_SCHEMA_VERSION,
            VIRTUAL_FILL_SCHEMA_VERSION,
            STREAM_SCHEMAS,
        ) = _load_pipeline_contracts()
    except ModuleNotFoundError as exc:
        missing = exc.name.split(".", 1)[0] if exc.name else ""
        if missing == "jsonschema":
            _install_jsonschema_stub()
            (
                STREAMS,
                DECISION_SCHEMA_VERSION,
                DEPTH_SNAPSHOT_SCHEMA_VERSION,
                GOLDEN_SNAPSHOT_SCHEMA_VERSION,
                OHLCV_BAR_SCHEMA_VERSION,
                TRADE_SUGGESTION_SCHEMA_VERSION,
                VIRTUAL_FILL_SCHEMA_VERSION,
                STREAM_SCHEMAS,
            ) = _load_pipeline_contracts()
        else:
            return _schema_entries_fallback(ttl_info)
    except Exception:
        return _schema_entries_fallback(ttl_info)

    def _required(stream: str) -> List[str]:
        schema = STREAM_SCHEMAS.get(stream)
        if not isinstance(schema, Mapping):
            return []
        required = schema.get("required")
        if isinstance(required, (list, tuple)):
            return sorted(str(field) for field in required)
        return []

    def _field_types(stream: str | None, fields: Sequence[str]) -> Dict[str, Any]:
        if not stream:
            return {}
        schema = STREAM_SCHEMAS.get(stream)
        if not isinstance(schema, Mapping):
            return {}
        properties = schema.get("properties")
        if not isinstance(properties, Mapping):
            return {}
        result: Dict[str, Any] = {}
        for field in fields:
            prop = properties.get(field) if isinstance(properties, Mapping) else None
            if not isinstance(prop, Mapping):
                continue
            types: list[str] = []
            primary = prop.get("type")
            if isinstance(primary, str):
                types.append(primary)
            elif isinstance(primary, (list, tuple)):
                types.extend(str(item) for item in primary if item)
            for alt_key in ("anyOf", "oneOf", "allOf"):
                alternatives = prop.get(alt_key)
                if not isinstance(alternatives, (list, tuple)):
                    continue
                for option in alternatives:
                    if not isinstance(option, Mapping):
                        continue
                    opt_type = option.get("type")
                    if isinstance(opt_type, str):
                        types.append(opt_type)
                    elif isinstance(opt_type, (list, tuple)):
                        types.extend(str(item) for item in opt_type if item)
            if not types:
                continue
            normalized = sorted({candidate for candidate in types if candidate})
            if not normalized:
                continue
            result[field] = normalized[0] if len(normalized) == 1 else normalized
        return result

    entries: List[Dict[str, Any]] = []
    discovery_topics = [STREAMS.discovery_candidates]
    mint_discovered = getattr(STREAMS, "mint_discovered", "x:mint.discovered")
    if mint_discovered not in discovery_topics:
        discovery_topics.append(mint_discovered)
    entries.append(
        {
            "name": "discovery",
            "topics": discovery_topics,
            "schema_id": "discovery@1",
            "required": _required(STREAMS.discovery_candidates),
            "_schema_key": STREAMS.discovery_candidates,
        }
    )
    entries.append(
        {
            "name": "market_ohlcv",
            "topics": [STREAMS.market_ohlcv],
            "schema_id": f"ohlcv_5m@{OHLCV_BAR_SCHEMA_VERSION}",
            "required": _required(STREAMS.market_ohlcv),
            "aliases": {
                "open": "o",
                "high": "h",
                "low": "l",
                "close": "c",
                "volume_usd": "vol_usd",
                "volume_base": "vol_base",
            },
            "ttl_seconds": ttl_info.get("ohlcv_5m_s"),
            "_schema_key": STREAMS.market_ohlcv,
        }
    )
    entries.append(
        {
            "name": "market_depth",
            "topics": [STREAMS.market_depth],
            "schema_id": f"depth_snapshot@{DEPTH_SNAPSHOT_SCHEMA_VERSION}",
            "required": _required(STREAMS.market_depth),
            "ttl_seconds": ttl_info.get("depth_s"),
            "_schema_key": STREAMS.market_depth,
        }
    )
    entries.append(
        {
            "name": "golden_snapshot",
            "topics": [STREAMS.golden_snapshot],
            "schema_id": f"golden_snapshot@{GOLDEN_SNAPSHOT_SCHEMA_VERSION}",
            "required": _required(STREAMS.golden_snapshot),
            "ttl_seconds": ttl_info.get("golden_s"),
            "_schema_key": STREAMS.golden_snapshot,
        }
    )
    entries.append(
        {
            "name": "agent_suggestions",
            "topics": [STREAMS.trade_suggested],
            "schema_id": f"trade_suggested@{TRADE_SUGGESTION_SCHEMA_VERSION}",
            "required": _required(STREAMS.trade_suggested),
            "ttl_seconds": ttl_info.get("suggestions_s"),
            "_schema_key": STREAMS.trade_suggested,
        }
    )
    entries.append(
        {
            "name": "vote_decisions",
            "topics": [STREAMS.vote_decisions],
            "schema_id": f"vote_decision@{DECISION_SCHEMA_VERSION}",
            "required": _required(STREAMS.vote_decisions),
            "ttl_seconds": ttl_info.get("votes_s"),
            "_schema_key": STREAMS.vote_decisions,
        }
    )
    entries.append(
        {
            "name": "shadow_execution",
            "topics": [STREAMS.virtual_fills],
            "schema_id": f"shadow_fill@{VIRTUAL_FILL_SCHEMA_VERSION}",
            "required": _required(STREAMS.virtual_fills),
            "_schema_key": STREAMS.virtual_fills,
        }
    )
    entries.append(
        {
            "name": "rl_weights",
            "topics": ["rl_weights", "rl:weights.applied"],
            "schema_id": "rl_weights@1",
            "required": ["weights"],
            "_schema_key": None,
            "_field_types": {"weights": "array"},
        }
    )
    for entry in entries:
        schema_key = entry.get("_schema_key")
        required_fields = entry.get("required") or []
        if "_field_types" not in entry:
            entry["_field_types"] = _field_types(schema_key, required_fields)
    return entries


def _provider_status_snapshot() -> Dict[str, Dict[str, Any]]:
    providers: Dict[str, Dict[str, Any]] = {}
    try:
        from . import prices
    except Exception:
        prices = None  # type: ignore
    if prices is not None:
        try:
            health = prices.get_provider_health_snapshot()
        except Exception:
            health = {}
        try:
            active = {str(name) for name in prices.PROVIDER_CONFIGS.keys()}
        except Exception:
            active = set()
    else:
        health = {}
        active = set()

    def _health_status(name: str) -> str:
        info = health.get(name) or {}
        if info.get("healthy") is True:
            return "ok"
        failures = _maybe_int(info.get("consecutive_failures")) or 0
        cooldown = _maybe_float(info.get("cooldown_remaining")) or 0.0
        if info.get("last_error_status") or info.get("last_error"):
            return "down"
        if failures > 0 or cooldown > 0:
            return "degraded"
        if info.get("healthy") is False:
            return "down"
        return "degraded" if name in health else "ok"

    def _entry(
        *,
        enabled: bool,
        status: str,
        auth_mode: str,
        base_url: str | None = None,
        ws_url: str | None = None,
    ) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "enabled": bool(enabled),
            "status": status,
            "auth_mode": auth_mode or "none",
        }
        if base_url:
            payload["base_url"] = base_url
        if ws_url:
            payload["ws_url"] = ws_url
        return payload

    solana_url = (_env_or_default("SOLANA_RPC_URL") or "").strip()
    solana_enabled = bool(solana_url)
    solana_status = "ok" if solana_enabled else "down"
    if not solana_enabled and os.getenv("SOLANA_RPC_URL") is None:
        fallback = _env_or_default("HELIUS_RPC_URL") or ""
        if fallback:
            solana_url = fallback.strip()
            solana_enabled = bool(solana_url)
            solana_status = "ok" if solana_enabled else "down"
    solana_auth = "api-key" if ("api-key" in solana_url.lower()) or os.getenv("HELIUS_API_KEY") else "none"
    providers["solana_rpc"] = _entry(
        enabled=solana_enabled,
        status=solana_status,
        auth_mode=solana_auth,
        base_url=solana_url or None,
    )

    try:
        from .clients import helius_das as helius_client
    except Exception:
        helius_client = None
    if helius_client is not None:
        das_base = getattr(helius_client, "DAS_BASE", None)
    else:
        das_base = None
    if not das_base:
        das_base = (_env_or_default("DAS_BASE_URL") or "").strip()
    das_enabled = parse_bool_env("USE_DAS_DISCOVERY", True)
    try:
        from . import token_scanner
    except Exception:
        circuit_until = 0.0
    else:
        circuit_until = float(getattr(token_scanner, "_DAS_CIRCUIT_OPEN_UNTIL", 0.0) or 0.0)
    now = time.monotonic()
    if not das_enabled:
        das_status = "down"
    elif circuit_until > now:
        das_status = "degraded"
    else:
        das_status = "ok"
    das_auth = "api-key" if os.getenv("HELIUS_API_KEY") or (das_base and "api-key" in das_base.lower()) else "none"
    providers["helius_das"] = _entry(
        enabled=das_enabled,
        status=das_status,
        auth_mode=das_auth,
        base_url=das_base or None,
    )

    try:
        from .scanner_common import JUPITER_WS_URL as DEFAULT_JUPITER_WS_URL
    except Exception:
        DEFAULT_JUPITER_WS_URL = None
    jupiter_url = (os.getenv("JUPITER_WS_URL") or DEFAULT_JUPITER_WS_URL or "").strip()
    jupiter_enabled = bool(jupiter_url)
    jupiter_status = _health_status("jupiter") if jupiter_enabled else "down"
    providers["jupiter_ws"] = _entry(
        enabled=jupiter_enabled,
        status=jupiter_status,
        auth_mode="none",
        ws_url=jupiter_url or None,
    )

    pyth_enabled = "pyth" in active or bool(health.get("pyth"))
    pyth_status = _health_status("pyth") if pyth_enabled else "down"
    providers["pyth_prices"] = _entry(
        enabled=pyth_enabled,
        status=pyth_status,
        auth_mode="none",
    )

    birdeye_url = None
    if prices is not None:
        birdeye_url = getattr(prices, "BIRDEYE_PRICE_URL", None)
    if not birdeye_url:
        birdeye_url = (_env_or_default("BIRDEYE_PRICE_URL") or "").strip()
    birdeye_enabled = parse_bool_env("BIRDEYE_ENABLED", True) and ("birdeye" in active or bool(birdeye_url))
    birdeye_status = _health_status("birdeye") if birdeye_enabled else "down"
    birdeye_auth = "api-key" if os.getenv("BIRDEYE_API_KEY") else "none"
    providers["bird_eye"] = _entry(
        enabled=birdeye_enabled,
        status=birdeye_status,
        auth_mode=birdeye_auth,
        base_url=birdeye_url or None,
    )

    dexscreener_enabled = ("dexscreener" in active) and not parse_bool_env("DEXSCREENER_DISABLED", False)
    dexscreener_status = _health_status("dexscreener") if dexscreener_enabled else "down"
    providers["dexscreener"] = _entry(
        enabled=dexscreener_enabled,
        status=dexscreener_status,
        auth_mode="none",
    )

    for provider_name, env_key in (
        ("raydium", "RAYDIUM_API_URL"),
        ("meteora", "METEORA_API_URL"),
        ("phoenix", "PHOENIX_API_URL"),
    ):
        url = (_env_or_default(env_key) or "").strip()
        enabled = bool(url)
        providers[provider_name] = _entry(
            enabled=enabled,
            status="ok" if enabled else "down",
            auth_mode="none",
            base_url=url or None,
        )

    pump_url = (_env_or_default("PUMP_LEADERBOARD_URL") or "").strip()
    enabled = bool(pump_url)
    providers["pump_fun"] = _entry(
        enabled=enabled,
        status="ok" if enabled else "down",
        auth_mode="none",
        base_url=pump_url or None,
    )

    return providers


def _resolve_keypair_mode(_paper_mode: bool) -> str:
    explicit_path = os.getenv("KEYPAIR_PATH") or os.getenv("SOLANA_KEYPAIR")
    if explicit_path:
        candidate = Path(explicit_path).expanduser()
        if candidate.exists():
            return "persistent"
        return "missing"
    # Without an explicit keypair path treat the runtime as ephemeral/paper
    # so the UI can describe the reduced risk posture.
    return "ephemeral"


def _depth_service_info(
    status: Mapping[str, Any], *, keypair_mode: str | None = None, paper_mode: bool = False
) -> Dict[str, Any]:
    requested = parse_bool_env("DEPTH_SERVICE", True) and parse_bool_env("USE_DEPTH_STREAM", True)
    ready = bool(status.get("depth_service"))
    info: Dict[str, Any] = {"enabled": bool(requested), "ready": ready}
    version = os.getenv("DEPTH_SERVICE_VERSION")
    if version:
        info["version"] = version
    rpc_url = (
        os.getenv("DEPTH_SERVICE_RPC_URL")
        or os.getenv("DEPTH_SERVICE_URL")
        or os.getenv("SOLANA_RPC_URL")
    )
    if rpc_url:
        info["rpc_url"] = rpc_url
    addr = (os.getenv("DEPTH_WS_ADDR") or "").strip()
    port = (os.getenv("DEPTH_WS_PORT") or "").strip()
    if addr and port:
        info["ws_url"] = f"ws://{addr}:{port}"
    if not ready:
        reason = None
        if not requested:
            reason = "disabled"
        elif paper_mode or (keypair_mode in {"ephemeral", "missing"}):
            reason = "paper-mode"
        else:
            reason = "starting"
        info["reason"] = reason
    return info


def _resolve_execution_snapshot(status: Mapping[str, Any]) -> Dict[str, Any]:
    paper = bool(status.get("paper_mode"))
    rate_interval = _maybe_float(os.getenv("EVENT_EXECUTOR_RATE_LIMIT"))
    if rate_interval is None or rate_interval <= 0:
        rate_interval = 0.05
    rate_limit_per_s = 1.0 / max(rate_interval, 1e-6)
    concurrency = _maybe_int(os.getenv("EVENT_EXECUTOR_LIMIT")) or 64
    priority_env = os.getenv("PRIORITY_RPC") or ""
    priority_rpc = [value.strip() for value in priority_env.split(",") if value.strip()]
    keypair_mode = _resolve_keypair_mode(paper)
    effective_paper = paper or keypair_mode in {"ephemeral", "missing"}
    if keypair_mode == "missing" and effective_paper:
        keypair_mode = "ephemeral"
    depth_info = _depth_service_info(status, keypair_mode=keypair_mode, paper_mode=effective_paper)
    return {
        "paper": effective_paper,
        "rate_limit_per_s": rate_limit_per_s,
        "concurrency": concurrency,
        "priority_rpc": priority_rpc,
        "keypair_mode": keypair_mode,
        "depth_service": depth_info,
    }


def _resilience_snapshot() -> Dict[str, Any]:
    resilience: Dict[str, Any] = {}
    try:
        from .clients import helius_das as helius_client
    except Exception:
        helius_client = None
    if helius_client is not None:
        rate_limiter = getattr(helius_client, "_rl", None)
        das_rps = getattr(rate_limiter, "rps", None) if rate_limiter else None
        timeout_total = getattr(helius_client, "_SESSION_TIMEOUT", None)
        timeout_connect = getattr(helius_client, "_CONNECT_TIMEOUT", None)
    else:
        das_rps = None
        timeout_total = None
        timeout_connect = None
    try:
        from . import token_scanner
    except Exception:
        degraded_cooldown = None
        timeout_threshold = None
        backoff_base = None
        backoff_cap = None
        request_interval = None
        max_attempts = None
    else:
        degraded_cooldown = getattr(token_scanner, "_DAS_DEGRADED_COOLDOWN", None)
        timeout_threshold = getattr(token_scanner, "_DAS_TIMEOUT_THRESHOLD", None)
        backoff_base = getattr(token_scanner, "_DAS_BACKOFF_BASE", None)
        backoff_cap = getattr(token_scanner, "_DAS_BACKOFF_CAP", None)
        request_interval = getattr(token_scanner, "_DAS_REQUEST_INTERVAL", None)
        max_attempts = getattr(token_scanner, "_DAS_MAX_ATTEMPTS", None)
    env_rps = _maybe_float(os.getenv("DAS_RPS"))
    if env_rps is not None:
        das_rps = env_rps
    else:
        das_rps = 1.0
    env_timeout_total = _maybe_float(os.getenv("DAS_TIMEOUT_TOTAL"))
    if env_timeout_total is not None:
        timeout_total = env_timeout_total
    else:
        timeout_total = 9.0
    if timeout_connect is None:
        timeout_connect = _maybe_float(os.getenv("DAS_TIMEOUT_CONNECT")) or 1.5
    if timeout_threshold is None:
        timeout_threshold = _maybe_float(os.getenv("DAS_TIMEOUT_THRESHOLD")) or 3.0
    if degraded_cooldown is None:
        degraded_cooldown = _maybe_float(os.getenv("DAS_DEGRADED_COOLDOWN")) or 90.0
    resilience["das"] = {
        "rps": das_rps,
        "timeout_connect_s": timeout_connect,
        "timeout_total_s": timeout_total,
        "timeout_threshold": timeout_threshold,
        "backoff_base_s": backoff_base,
        "backoff_cap_s": backoff_cap,
        "circuit_open_s": degraded_cooldown,
        "request_interval_s": request_interval,
        "max_attempts": max_attempts,
    }
    try:
        from . import prices
    except Exception:
        price_attempts = None
        price_backoff = None
    else:
        price_attempts = getattr(prices, "PRICE_RETRY_ATTEMPTS", None)
        price_backoff = getattr(prices, "PRICE_RETRY_BACKOFF", None)
    resilience["birdeye_retry"] = {
        "attempts": price_attempts,
        "backoff_s": price_backoff,
    }
    ping_interval = _maybe_float(os.getenv("UI_WS_PING_INTERVAL") or os.getenv("WS_PING_INTERVAL"))
    if ping_interval is None or ping_interval <= 0:
        ping_interval = 20.0
    ping_timeout = _maybe_float(os.getenv("UI_WS_PING_TIMEOUT") or os.getenv("WS_PING_TIMEOUT"))
    if ping_timeout is None or ping_timeout <= 0:
        ping_timeout = 20.0
    resilience["ws"] = {
        "ping_interval_s": ping_interval,
        "ping_timeout_s": ping_timeout,
    }
    return resilience

def _build_ui_meta_snapshot(state: "UIState" | None = None) -> Dict[str, Any]:
    state = state or _get_active_ui_state()
    summary: Dict[str, Any] = {}
    status: Dict[str, Any] = {}
    settings: Dict[str, Any] = {}
    rl_status: Mapping[str, Any] | None = None
    if state is not None:
        try:
            summary = dict(state.snapshot_summary())
        except Exception:
            log.debug("Failed to build summary snapshot for UI_META", exc_info=True)
        try:
            status = dict(state.snapshot_status())
        except Exception:
            log.debug("Failed to build status snapshot for UI_META", exc_info=True)
        try:
            settings = dict(state.snapshot_settings())
        except Exception:
            log.debug("Failed to build settings snapshot for UI_META", exc_info=True)
        try:
            rl_status = state.snapshot_rl_status()
        except Exception:
            rl_status = None
    if rl_status is None and isinstance(status.get("rl_daemon_status"), Mapping):
        rl_status = status.get("rl_daemon_status")

    lag_snapshot = summary.get("lag") if isinstance(summary.get("lag"), dict) else {}
    golden_info = summary.get("golden") if isinstance(summary.get("golden"), dict) else {}
    lag_metrics: Dict[str, Optional[float]] = {
        "bus_ms": _maybe_float(status.get("bus_latency_ms")),
        "ohlcv_ms": _maybe_float(lag_snapshot.get("ohlcv_ms")),
        "depth_ms": _maybe_float(lag_snapshot.get("depth_ms")),
        "golden_ms": _maybe_float(golden_info.get("lag_ms")),
        "suggestions_ms": _maybe_float(lag_snapshot.get("suggestion_ms")),
        "votes_ms": _maybe_float(lag_snapshot.get("decision_ms")),
    }
    rl_updated = None
    if isinstance(rl_status, Mapping):
        rl_updated = _maybe_float(rl_status.get("updated_at"))
    if rl_updated is not None:
        lag_metrics["rl_ms"] = max(0.0, (time.time() - rl_updated) * 1000.0)
    else:
        lag_metrics["rl_ms"] = None

    execution = _resolve_execution_snapshot(status)
    paper_mode = bool(execution.get("paper"))

    redis_url = _discover_broker_url() or (_env_or_default("REDIS_URL") or "redis://localhost:6379/1")
    channel = (
        getattr(event_bus, "_BROKER_CHANNEL", None)
        or os.getenv("BROKER_CHANNEL")
        or _env_or_default("BROKER_CHANNEL")
        or "solhunter-events-v3"
    )

    depth_info = execution.get("depth_service") if isinstance(execution.get("depth_service"), Mapping) else {}
    features: Dict[str, Any] = {
        "sentiment_enabled": parse_bool_env("SENTIMENT_ENABLED", False),
        "rl_shadow_enabled": str(status.get("rl_mode") or "").lower() == "shadow",
        "depth_service_enabled": bool(depth_info.get("enabled")),
        "golden_pipeline_enabled": parse_bool_env("GOLDEN_PIPELINE", True),
        "discovery_enabled": parse_bool_env("MINT_STREAM_ENABLE", True),
        "use_mev_bundles": parse_bool_env("USE_MEV_BUNDLES", False),
        "golden_depth": bool(getattr(state, "golden_depth_enabled", False)) if state else False,
        "golden_momentum": bool(getattr(state, "golden_momentum_enabled", False)) if state else False,
    }
    # Backwards compatibility for legacy keys used by older dashboards
    features["sentiment"] = features["sentiment_enabled"]

    staleness = {}
    if isinstance(settings.get("staleness"), dict):
        staleness = dict(settings["staleness"])

    ttl_info = {
        "depth_s": _resolve_depth_ttl(),
        "ohlcv_5m_s": 600.0,
        "golden_s": 60.0,
        "suggestions_s": 15.0,
        "votes_s": 30.0,
    }

    workflow = (
        status.get("workflow")
        or os.getenv("SOLHUNTER_WORKFLOW")
        or os.getenv("RUNTIME_WORKFLOW")
    )
    version_info: Dict[str, Any] = {
        "app_version": _resolve_app_version(),
        "schema_version": UI_SCHEMA_VERSION,
        "schema_hash": _compute_schema_hash(),
        "build_git": _resolve_build_git(),
        "workflow": workflow,
        "mode": "paper" if paper_mode else "live",
    }
    version_info = {key: value for key, value in version_info.items() if value is not None}

    redis_info = _parse_redis_url(redis_url)
    broker_kind = "redis"
    try:
        broker_types = getattr(event_bus, "_BROKER_TYPES", None)
    except Exception:
        broker_types = None
    if broker_types:
        try:
            broker_kind = str(broker_types[0])
        except Exception:
            broker_kind = "redis"
    broker_info: Dict[str, Any] = {
        "kind": broker_kind,
        "channel": channel,
    }
    broker_url = redis_info.get("url") if isinstance(redis_info, Mapping) else None
    if not broker_url:
        broker_url = redis_url
    if broker_url:
        broker_info["url"] = broker_url
    if isinstance(redis_info, Mapping) and redis_info.get("db") is not None:
        broker_info["db"] = redis_info.get("db")

    event_bus_url = (
        os.getenv("EVENT_BUS_URL")
        or getattr(event_bus, "DEFAULT_WS_URL", None)
        or "ws://127.0.0.1:8779"
    )
    event_bus_payload = {"url_ws": event_bus_url}

    raw_schema_entries = _schema_entries(ttl_info)
    streams_contracts: List[Dict[str, Any]] = []
    schemas_listing: List[Dict[str, Any]] = []
    for raw_entry in raw_schema_entries:
        schema_key = raw_entry.get("_schema_key")
        field_types = raw_entry.get("_field_types")
        entry = {
            key: value
            for key, value in raw_entry.items()
            if key not in {"_schema_key", "_field_types"}
        }
        streams_contracts.append(entry)
        topics = entry.get("topics") or []
        topic = topics[0] if topics else None
        required_fields = entry.get("required") or []
        schema_item: Dict[str, Any] = {
            "topic": topic,
            "schema_id": entry.get("schema_id"),
            "required": required_fields,
        }
        if isinstance(field_types, Mapping) and field_types:
            schema_item["types"] = dict(field_types)
        if entry.get("aliases"):
            schema_item["aliases"] = entry["aliases"]
        schema_item = {key: value for key, value in schema_item.items() if value}
        if schema_item:
            schemas_listing.append(schema_item)

    bootstrap = {
        "seed_tokens": _discover_seed_tokens(),
        "pyth_price_hints": _pyth_price_hints(),
        "price_providers": _price_provider_order(),
        "discovery": {"sources": _discovery_hint_sources(state)},
    }

    execution = _resolve_execution_snapshot(status)

    try:
        from .golden_pipeline.contracts import STREAMS
    except Exception:
        pipeline_topics = None
    else:
        pipeline_topics = STREAMS

    stages: Dict[str, Any] = {}
    if pipeline_topics is not None:
        discovery_topics = [pipeline_topics.discovery_candidates]
        mint_discovered = getattr(pipeline_topics, "mint_discovered", "x:mint.discovered")
        if mint_discovered not in discovery_topics:
            discovery_topics.append(mint_discovered)
        stages["discovery"] = {
            "enabled": bool(features.get("discovery_enabled")),
            "topics": discovery_topics,
        }
        stages["golden"] = {
            "enabled": bool(features.get("golden_pipeline_enabled")),
            "topics": [pipeline_topics.golden_snapshot],
        }
        stages["agents"] = {
            "enabled": bool(features.get("golden_pipeline_enabled")),
            "topics": [pipeline_topics.trade_suggested],
        }
    else:
        stages["discovery"] = {
            "enabled": bool(features.get("discovery_enabled")),
            "topics": ["x:discovery.candidates", "x:mint.discovered"],
        }
        stages["golden"] = {
            "enabled": bool(features.get("golden_pipeline_enabled")),
            "topics": ["x:mint.golden"],
        }
        stages["agents"] = {
            "enabled": bool(features.get("golden_pipeline_enabled")),
            "topics": ["x:trade.suggested"],
        }
    pipeline_payload = {
        "stages": stages,
        "expectations": "discovery → golden → suggestions → votes",
    }

    heartbeat_ts = _maybe_float(status.get("heartbeat"))

    payload: Dict[str, Any] = {
        "type": "UI_META",
        "v": UI_SCHEMA_VERSION,
        "channel": channel,
        "redis": redis_info,
        "broker": broker_info,
        "event_bus": event_bus_payload,
        "version": version_info,
        "lag": lag_metrics,
        "features": features,
        "staleness": staleness,
        "heartbeat_epoch": time.monotonic(),
        "ttl": ttl_info,
        "bootstrap": bootstrap,
        "providers": _provider_status_snapshot(),
        "streams": streams_contracts,
        "schemas": schemas_listing,
        "execution": execution,
        "pipeline": pipeline_payload,
        "resilience": _resilience_snapshot(),
        "test_contracts": {
            "handshake": "tests/test_ui_meta_ws.py",
            "golden_pipeline": [
                "tests/golden_pipeline/test_bootstrap.py",
                "tests/golden_pipeline/test_discovery.py",
                "tests/golden_pipeline/test_ui_smoke_synth.py",
                "tests/golden_pipeline/test_validation.py",
            ],
        },
        "generated_ts": time.time(),
    }

    if heartbeat_ts is not None:
        payload["heartbeat_ts"] = heartbeat_ts

    if status.get("environment"):
        payload["environment"] = status.get("environment")
    if version_info.get("workflow"):
        payload["workflow"] = version_info.get("workflow")
    return payload


def get_ui_meta_snapshot(force: bool = False) -> Dict[str, Any]:
    global _ui_meta_cache
    now = time.monotonic()
    if not force and _ui_meta_cache is not None:
        cached_ts, cached_payload = _ui_meta_cache
        if now - cached_ts <= _UI_META_CACHE_TTL:
            return dict(cached_payload)
    payload = _build_ui_meta_snapshot()
    _ui_meta_cache = (now, dict(payload))
    return payload


try:  # pragma: no cover - imported lazily in tests
    import websockets
except ImportError:  # pragma: no cover - optional dependency
    websockets = None  # type: ignore[assignment]


_WS_HOST_ENV_KEYS = ("UI_WS_HOST", "UI_HOST")
_RL_WS_PORT_DEFAULT = 0
_EVENT_WS_PORT_DEFAULT = 0
_LOG_WS_PORT_DEFAULT = 0
_WS_QUEUE_DEFAULT = 512
_backlog_env = os.getenv("UI_WS_BACKLOG_MAX", "64")
try:
    BACKLOG_MAX = int(_backlog_env or 64)
except (TypeError, ValueError):
    log.warning("Invalid backlog value %r; using default 64", _backlog_env)
    BACKLOG_MAX = 64
else:
    if BACKLOG_MAX < 0:
        log.warning("Negative backlog %s not allowed; using default 64", BACKLOG_MAX)
        BACKLOG_MAX = 64
_ADDR_IN_USE_ERRNOS = {errno.EADDRINUSE}


rl_ws_loop: asyncio.AbstractEventLoop | None = None
event_ws_loop: asyncio.AbstractEventLoop | None = None
log_ws_loop: asyncio.AbstractEventLoop | None = None

_RL_WS_PORT = _RL_WS_PORT_DEFAULT
_EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
_LOG_WS_PORT = _LOG_WS_PORT_DEFAULT


class _WebsocketState:
    __slots__ = (
        "loop",
        "server",
        "clients",
        "queue",
        "task",
        "thread",
        "port",
        "name",
        "host",
        "queue_max",
        "queue_depth",
        "queue_high",
        "drop_count",
        "recent_close_codes",
        "lock",
    )

    def __init__(self, name: str) -> None:
        self.loop: asyncio.AbstractEventLoop | None = None
        self.server: Any | None = None
        self.clients: set[Any] = set()
        self.queue: asyncio.Queue[str] | None = None
        self.task: asyncio.Task[Any] | None = None
        self.thread: threading.Thread | None = None
        self.port: int = 0
        self.name = name
        self.host: str | None = None
        self.queue_max: int = 0
        self.queue_depth: int = 0
        self.queue_high: int = 0
        self.drop_count: int = 0
        self.recent_close_codes: Deque[int | None] = deque(maxlen=10)
        self.lock = threading.Lock()


_WS_CHANNELS: dict[str, _WebsocketState] = {
    "rl": _WebsocketState("rl"),
    "events": _WebsocketState("events"),
    "logs": _WebsocketState("logs"),
}


def _resolve_host() -> str:
    for key in _WS_HOST_ENV_KEYS:
        host = os.getenv(key)
        if host:
            return host
    return "127.0.0.1"


def _parse_port(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        port = int(value)
    except (TypeError, ValueError):
        log.warning("Invalid port value %r; using default %s", value, default)
        return default
    if port < 0:
        log.warning("Negative port %s not allowed; using default %s", port, default)
        return default
    return port


def _resolve_port(*keys: str, default: int) -> int:
    for key in keys:
        env_value = os.getenv(key)
        if env_value is not None and env_value != "":
            return _parse_port(env_value, default)
    return default


def _parse_positive_int(value: str | None, default: int) -> int:
    if value is None or value == "":
        return default
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        log.warning("Invalid integer value %r; using default %s", value, default)
        return default
    if parsed <= 0:
        log.warning("Non-positive integer %s not allowed; using default %s", parsed, default)
        return default
    return parsed


def _format_payload(payload: Any) -> str:
    if isinstance(payload, bytes):
        return payload.decode("utf-8", errors="replace")
    if isinstance(payload, str):
        return payload
    try:
        return json.dumps(payload)
    except TypeError:
        return str(payload)


def _enqueue_message(channel: str, payload: Any) -> bool:
    state = _WS_CHANNELS.get(channel)
    if not state or state.loop is None or state.queue is None:
        return False

    message = _format_payload(payload)

    def _put() -> None:
        if state.queue is None:
            return
        try:
            state.queue.put_nowait(message)
        except asyncio.QueueFull:
            try:
                state.queue.get_nowait()
                state.queue.task_done()
            except asyncio.QueueEmpty:  # pragma: no cover - race
                pass
            state.queue.put_nowait(message)
            with state.lock:
                state.drop_count += 1
                depth = state.queue.qsize()
                state.queue_depth = depth
                if depth > state.queue_high:
                    state.queue_high = depth
        else:
            with state.lock:
                depth = state.queue.qsize()
                state.queue_depth = depth
                if depth > state.queue_high:
                    state.queue_high = depth

    state.loop.call_soon_threadsafe(_put)
    return True


def push_event(payload: Any) -> bool:
    """Broadcast *payload* to UI event websocket listeners."""

    return _enqueue_message("events", payload)


def push_rl(payload: Any) -> bool:
    """Broadcast *payload* to RL websocket listeners."""

    return _enqueue_message("rl", payload)


def push_log(payload: Any) -> bool:
    """Broadcast *payload* to log websocket listeners."""

    return _enqueue_message("logs", payload)


def get_ws_client_counts() -> Dict[str, int]:
    """Return the number of connected websocket clients per channel."""

    counts: Dict[str, int] = {}
    for name, state in _WS_CHANNELS.items():
        with state.lock:
            counts[name] = len(state.clients)
    return counts


def get_ws_channel_metrics() -> Dict[str, Dict[str, Any]]:
    """Return queue depth/backpressure metrics for each websocket channel."""

    metrics: Dict[str, Dict[str, Any]] = {}
    for name, state in _WS_CHANNELS.items():
        with state.lock:
            metrics[name] = {
                "queue_max": state.queue_max,
                "queue_depth": state.queue_depth,
                "queue_high": state.queue_high,
                "drops": state.drop_count,
                "clients": len(state.clients),
                "recent_close_codes": list(state.recent_close_codes),
            }
    return metrics


def _normalize_ws_url(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if not candidate:
        return None
    if candidate.startswith(("ws://", "wss://")):
        return candidate
    return None


def _infer_ws_scheme(request_scheme: str | None = None) -> str:
    override = (os.getenv("UI_WS_SCHEME") or os.getenv("WS_SCHEME") or "").strip().lower()
    if override in {"ws", "wss"}:
        return override
    if request_scheme and request_scheme.lower() in {"https", "wss"}:
        return "wss"
    return "ws"


def _split_netloc(netloc: str | None) -> tuple[str | None, int | None]:
    if not netloc:
        return None, None
    parsed = urlparse(f"//{netloc}", scheme="http")
    return parsed.hostname, parsed.port


def _channel_path(channel: str) -> str:
    override_map = {
        "rl": "UI_RL_WS_PATH",
        "events": "UI_EVENTS_WS_PATH",
        "logs": "UI_LOGS_WS_PATH",
    }
    override_key = override_map.get(channel)
    override = os.getenv(override_key) if override_key else None
    if override:
        candidate = override
    else:
        template = os.getenv("UI_WS_PATH_TEMPLATE")
        if template:
            try:
                candidate = template.format(channel=channel)
            except Exception:
                log.warning(
                    "Invalid UI_WS_PATH_TEMPLATE %r; falling back to default",
                    template,
                )
                candidate = f"/ws/{channel}"
        else:
            candidate = f"/ws/{channel}"
    if not candidate:
        candidate = f"/ws/{channel}"
    if not candidate.startswith("/"):
        candidate = "/" + candidate.lstrip("/")
    return candidate


def get_ws_urls() -> dict[str, str]:
    """Return websocket URLs for RL, events, and logs channels."""

    channel_env_keys: dict[str, tuple[str, ...]] = {
        "events": ("UI_EVENTS_WS", "UI_EVENTS_WS_URL", "UI_WS_URL"),
        "rl": ("UI_RL_WS", "UI_RL_WS_URL"),
        "logs": ("UI_LOGS_WS", "UI_LOG_WS_URL"),
    }
    urls: dict[str, str] = {}
    for channel, env_keys in channel_env_keys.items():
        resolved: str | None = None
        for env_key in env_keys:
            resolved = _normalize_ws_url(os.environ.get(env_key))
            if resolved:
                break
        if not resolved:
            state = _WS_CHANNELS.get(channel)
            host = state.host if state and state.host else _resolve_host()
            url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
            if channel == "rl":
                port = state.port or _RL_WS_PORT
            elif channel == "events":
                port = state.port or _EVENT_WS_PORT
            else:
                port = state.port or _LOG_WS_PORT
            path = _channel_path(channel)
            scheme = _infer_ws_scheme()
            resolved = f"{scheme}://{url_host}:{port}{path}"
        urls[channel] = resolved
    return urls


def build_ui_manifest(req: Request | None = None) -> Dict[str, Any]:
    urls = get_ws_urls()
    scheme_hint = _infer_ws_scheme(getattr(req, "scheme", None))
    public_host_env = os.getenv("UI_PUBLIC_HOST") or os.getenv("UI_EXTERNAL_HOST")
    public_host, _ = _split_netloc(public_host_env)
    request_host, _ = _split_netloc(getattr(req, "host", None))

    manifest: Dict[str, Any] = {}
    for channel in ("rl", "events", "logs"):
        raw_url = urls.get(channel, "")
        parsed = urlparse(raw_url)
        host = public_host or parsed.hostname or request_host or _resolve_host()
        port = parsed.port
        path = parsed.path or ""
        if path in {"", "/", "/ws"}:
            path = _channel_path(channel)
        if not path.startswith("/"):
            path = "/" + path.lstrip("/")
        scheme = parsed.scheme or scheme_hint
        netloc = host or ""
        if port:
            netloc = f"{host}:{port}"
        manifest[f"{channel}_ws"] = urlunparse((scheme, netloc, path, "", "", ""))

    ui_port_value = os.getenv("UI_PORT") or os.getenv("PORT")
    manifest["ui_port"] = _parse_port(ui_port_value, 5000)
    return manifest
def _shutdown_state(state: _WebsocketState) -> None:
    loop = state.loop
    if loop is None:
        return

    def _stop_loop() -> None:
        loop.stop()

    loop.call_soon_threadsafe(_stop_loop)
    thread = state.thread
    if thread is not None:
        thread.join(timeout=2)
    state.thread = None
    state.host = None


def _close_server(loop: asyncio.AbstractEventLoop, state: _WebsocketState) -> None:
    server = state.server
    if server is not None:
        server.close()
        with contextlib.suppress(Exception):
            loop.run_until_complete(server.wait_closed())
    state.server = None

    with state.lock:
        clients_snapshot = list(state.clients)
    for ws in clients_snapshot:
        with contextlib.suppress(Exception):
            loop.run_until_complete(ws.close(code=1012, reason="server shutdown"))
    with state.lock:
        state.clients.clear()

    if state.task is not None:
        state.task.cancel()
        with contextlib.suppress(BaseException):
            loop.run_until_complete(state.task)
    state.task = None

    if state.queue is not None:
        with contextlib.suppress(Exception):
            loop.run_until_complete(state.queue.join())
    state.queue = None

    with contextlib.suppress(Exception):
        loop.run_until_complete(loop.shutdown_asyncgens())
    loop.close()
    state.loop = None
    state.thread = None
    state.port = 0
    state.host = None
    with state.lock:
        state.queue = None
        state.queue_max = 0
        state.queue_depth = 0
        state.queue_high = 0
        state.drop_count = 0
        state.recent_close_codes.clear()


def _start_channel(
    channel: str,
    *,
    host: str,
    port: int,
    queue_size: int,
    ping_interval: float,
    ping_timeout: float,
) -> threading.Thread:
    state = _WS_CHANNELS[channel]
    ready: Queue[Any] = Queue(maxsize=1)

    def _run() -> None:
        nonlocal port
        global rl_ws_loop, event_ws_loop, log_ws_loop
        global _RL_WS_PORT, _EVENT_WS_PORT, _LOG_WS_PORT
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        state.loop = loop
        state.port = 0
        state.host = host

        if channel == "rl":
            rl_ws_loop = loop
        elif channel == "events":
            event_ws_loop = loop
        else:
            log_ws_loop = loop

        queue = asyncio.Queue[str](maxsize=queue_size)
        state.queue = queue
        backlog: deque[str] = deque()
        with state.lock:
            state.queue_max = queue_size
            state.queue_depth = 0
            state.queue_high = 0
            state.drop_count = 0
            state.recent_close_codes = deque(maxlen=10)

        async def _broadcast_loop() -> None:
            while True:
                message = await queue.get()
                stale_clients: list[Any] = []
                with state.lock:
                    clients_snapshot = list(state.clients)
                for ws in clients_snapshot:
                    try:
                        await ws.send(message)
                    except Exception:
                        stale_clients.append(ws)
                if stale_clients:
                    with state.lock:
                        for ws in stale_clients:
                            state.clients.discard(ws)
                backlog.append(message)
                if len(backlog) > BACKLOG_MAX:
                    backlog.popleft()
                queue.task_done()
                with state.lock:
                    depth = queue.qsize()
                    state.queue_depth = depth

        async def _handler(websocket, path) -> None:  # type: ignore[override]
            parsed = urlparse(path or "")
            req_path = parsed.path or "/"
            template_path = _channel_path(channel)
            allowed_paths = {
                "",
                "/",
                "/ws",
                f"/{channel}",
                f"/{channel}/",
                f"/ws/{channel}",
                f"/ws/{channel}/",
                template_path,
                template_path.rstrip("/") + "/",
            }
            if req_path not in allowed_paths:
                await websocket.close(code=1008, reason="invalid path")
                return
            with state.lock:
                state.clients.add(websocket)

            handshake_event = asyncio.Event()
            send_lock = asyncio.Lock()
            meta_sent = False
            backlog_snapshot = list(backlog)

            async def _send_meta(reason: str = "hello") -> None:
                nonlocal meta_sent
                if meta_sent:
                    handshake_event.set()
                    return
                async with send_lock:
                    if meta_sent:
                        handshake_event.set()
                        return
                    payload = get_ui_meta_snapshot()
                    meta_frame = json.dumps(payload)
                    try:
                        await websocket.send(meta_frame)
                    except Exception:
                        meta_sent = True
                        handshake_event.set()
                        return
                    meta_sent = True
                    handshake_event.set()
                    for cached in backlog_snapshot:
                        try:
                            await websocket.send(cached)
                        except Exception:
                            break

            async def _handshake_timeout() -> None:
                try:
                    await asyncio.wait_for(handshake_event.wait(), timeout=1.0)
                except asyncio.TimeoutError:
                    await _send_meta("timeout")

            timeout_task = asyncio.create_task(_handshake_timeout())

            try:
                await _send_meta("connect")
            except Exception:
                with state.lock:
                    state.clients.discard(websocket)
                timeout_task.cancel()
                with contextlib.suppress(Exception):
                    await timeout_task
                return

            hello = json.dumps({"channel": channel, "event": "hello", "schema": UI_SCHEMA_VERSION})
            try:
                await websocket.send(hello)
            except Exception:
                with state.lock:
                    state.clients.discard(websocket)
                timeout_task.cancel()
                with contextlib.suppress(Exception):
                    await timeout_task
                return

            async def _decode_client_message(raw: Any) -> Dict[str, Any] | None:
                if isinstance(raw, bytes):
                    try:
                        raw = raw.decode("utf-8", errors="ignore")
                    except Exception:
                        return None
                if isinstance(raw, str):
                    candidate = raw.strip()
                    if not candidate:
                        return None
                    if candidate.startswith("{"):
                        try:
                            obj = json.loads(candidate)
                        except Exception:
                            return None
                        if isinstance(obj, dict):
                            return obj
                    return None
                if isinstance(raw, dict):
                    return raw
                return None

            try:
                async for incoming in websocket:
                    message_obj = await _decode_client_message(incoming)
                    if message_obj is None:
                        continue
                    event_name = str(message_obj.get("event") or message_obj.get("type") or "").lower()
                    if event_name in {"hello", "ui_hello", "client_hello"}:
                        await _send_meta("client")
                    elif event_name == "request_meta":
                        await _send_meta("request")
                await _send_meta("close")
            except Exception:
                await _send_meta("error")
            finally:
                handshake_event.set()
                timeout_task.cancel()
                with contextlib.suppress(Exception):
                    await timeout_task
                with state.lock:
                    state.clients.discard(websocket)
                    state.recent_close_codes.append(getattr(websocket, "close_code", None))

        server = None
        last_exc: Exception | None = None

        for candidate_port in (port, 0) if port != 0 else (0,):
            try:
                server = loop.run_until_complete(
                    websockets.serve(  # type: ignore[call-arg]
                        _handler,
                        host,
                        candidate_port,
                        ping_interval=ping_interval,
                        ping_timeout=ping_timeout,
                    )
                )
            except OSError as exc:
                last_exc = exc
                if exc.errno in _ADDR_IN_USE_ERRNOS and candidate_port != 0:
                    log.warning(
                        "%s websocket port %s unavailable; retrying with automatic port",
                        channel,
                        candidate_port,
                    )
                    continue
                ready.put(exc)
                state.loop = None
                state.host = None
                if channel == "rl":
                    rl_ws_loop = None
                    _RL_WS_PORT = _RL_WS_PORT_DEFAULT
                elif channel == "events":
                    event_ws_loop = None
                    _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
                else:
                    log_ws_loop = None
                    _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
                try:
                    loop.close()
                except Exception:
                    pass
                return
            except Exception as exc:  # pragma: no cover - unexpected startup failure
                ready.put(exc)
                state.loop = None
                state.host = None
                if channel == "rl":
                    rl_ws_loop = None
                    _RL_WS_PORT = _RL_WS_PORT_DEFAULT
                elif channel == "events":
                    event_ws_loop = None
                    _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
                else:
                    log_ws_loop = None
                    _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
                try:
                    loop.close()
                except Exception:
                    pass
                return
            else:
                bound_port = candidate_port
                if getattr(server, "sockets", None):
                    try:
                        sockname = server.sockets[0].getsockname()
                        bound_port = sockname[1] if isinstance(sockname, tuple) else bound_port
                    except Exception:
                        pass
                previous_port = port
                bound_port = bound_port or previous_port
                if candidate_port == 0 and bound_port == previous_port:
                    log.error(
                        "Unable to determine dynamically assigned port for %s websocket",
                        channel,
                    )
                    if server is not None:
                        with contextlib.suppress(Exception):
                            server.close()
                            loop.run_until_complete(server.wait_closed())
                    server = None
                    continue
                port = bound_port
                state.port = bound_port
                if channel == "rl":
                    _RL_WS_PORT = bound_port
                elif channel == "events":
                    _EVENT_WS_PORT = bound_port
                else:
                    _LOG_WS_PORT = bound_port
                if candidate_port == 0 and bound_port != previous_port:
                    log.info(
                        "%s websocket using dynamically assigned port %s", channel, bound_port
                    )
                break

        if server is None:
            ready.put(last_exc or RuntimeError(f"Unable to start {channel} websocket"))
            state.loop = None
            state.host = None
            if channel == "rl":
                rl_ws_loop = None
                _RL_WS_PORT = _RL_WS_PORT_DEFAULT
            elif channel == "events":
                event_ws_loop = None
                _EVENT_WS_PORT = _EVENT_WS_PORT_DEFAULT
            else:
                log_ws_loop = None
                _LOG_WS_PORT = _LOG_WS_PORT_DEFAULT
            try:
                loop.close()
            except Exception:
                pass
            return

        state.server = server
        state.task = loop.create_task(_broadcast_loop())
        ready.put(None)

        try:
            loop.run_forever()
        finally:
            _close_server(loop, state)

    thread = threading.Thread(target=_run, name=f"ui-ws-{channel}", daemon=True)
    thread.start()
    state.thread = thread

    ready_timeout_raw = os.getenv("UI_WS_READY_TIMEOUT", "5")
    try:
        ready_timeout = float(ready_timeout_raw or 5)
    except (TypeError, ValueError):
        log.warning(
            "Invalid UI_WS_READY_TIMEOUT value %r; using default 5 seconds",
            ready_timeout_raw,
        )
        ready_timeout = 5.0
    try:
        result = ready.get(timeout=ready_timeout)
    except Exception as exc:  # pragma: no cover - unexpected queue failure
        _shutdown_state(state)
        raise RuntimeError(f"Timeout starting {channel} websocket") from exc

    if isinstance(result, Exception):
        _shutdown_state(state)
        raise RuntimeError(
            f"{channel} websocket failed to bind on {host}:{port}: {result}"
        ) from result

    return thread


def start_websockets() -> dict[str, threading.Thread]:
    """Launch UI websocket endpoints for RL, runtime events, and logs."""

    if websockets is None:
        log.warning("UI websockets unavailable: install the 'websockets' package")
        return {}

    if all(state.loop is not None for state in _WS_CHANNELS.values()):
        return {
            name: state.thread
            for name, state in _WS_CHANNELS.items()
            if state.thread is not None
        }

    threads: dict[str, threading.Thread] = {}
    host = _resolve_host()
    queue_size = _parse_positive_int(os.getenv("UI_WS_QUEUE_SIZE"), _WS_QUEUE_DEFAULT)
    ping_interval = float(os.getenv("UI_WS_PING_INTERVAL", os.getenv("WS_PING_INTERVAL", "20")))
    ping_timeout = float(os.getenv("UI_WS_PING_TIMEOUT", os.getenv("WS_PING_TIMEOUT", "20")))
    url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
    scheme = _infer_ws_scheme()

    rl_port = _resolve_port("UI_RL_WS_PORT", "RL_WS_PORT", default=_RL_WS_PORT_DEFAULT)
    log_port = _resolve_port("UI_LOG_WS_PORT", default=_LOG_WS_PORT_DEFAULT)
    event_port = _resolve_port("UI_EVENT_WS_PORT", "EVENT_WS_PORT", default=_EVENT_WS_PORT_DEFAULT)

    try:
        threads["rl"] = _start_channel(
            "rl",
            host=host,
            port=rl_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
        threads["events"] = _start_channel(
            "events",
            host=host,
            port=event_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
        threads["logs"] = _start_channel(
            "logs",
            host=host,
            port=log_port,
            queue_size=queue_size,
            ping_interval=ping_interval,
            ping_timeout=ping_timeout,
        )
    except Exception:
        for state in _WS_CHANNELS.values():
            _shutdown_state(state)
        raise

    events_url = f"{scheme}://{url_host}:{_EVENT_WS_PORT}{_channel_path('events')}"
    rl_url = f"{scheme}://{url_host}:{_RL_WS_PORT}{_channel_path('rl')}"
    logs_url = f"{scheme}://{url_host}:{_LOG_WS_PORT}{_channel_path('logs')}"

    defaults = {
        "UI_WS_URL": events_url,
        "UI_EVENTS_WS_URL": events_url,
        "UI_EVENTS_WS": events_url,
        "UI_RL_WS_URL": rl_url,
        "UI_RL_WS": rl_url,
        "UI_LOG_WS_URL": logs_url,
        "UI_LOGS_WS": logs_url,
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)
    log.info(
        "UI websockets listening on rl=%s events=%s logs=%s",
        _RL_WS_PORT,
        _EVENT_WS_PORT,
        _LOG_WS_PORT,
    )
    return threads


def stop_websockets() -> None:
    """Shut down all websocket channels."""

    for state in _WS_CHANNELS.values():
        _shutdown_state(state)


StatusProvider = Callable[[], Dict[str, Any]]
ListProvider = Callable[[], Iterable[Dict[str, Any]]]
DictProvider = Callable[[], Dict[str, Any]]


@dataclass
class UIState:
    """Holds callables that provide live data to the UI endpoints."""

    status_provider: StatusProvider = field(
        default=lambda: {"event_bus": False, "trading_loop": False}
    )
    activity_provider: ListProvider = field(default=lambda: [])
    trades_provider: ListProvider = field(default=lambda: [])
    weights_provider: DictProvider = field(default=lambda: {})
    rl_status_provider: DictProvider = field(default=lambda: {})
    logs_provider: ListProvider = field(default=lambda: [])
    summary_provider: DictProvider = field(default=lambda: {})
    discovery_provider: DictProvider = field(default=lambda: {"recent": []})
    config_provider: DictProvider = field(default=lambda: {})
    actions_provider: ListProvider = field(default=lambda: [])
    history_provider: ListProvider = field(default=lambda: [])
    discovery_console_provider: DictProvider = field(
        default=lambda: {"candidates": [], "stats": {}}
    )
    token_facts_provider: DictProvider = field(
        default=lambda: {"tokens": {}, "selected": None}
    )
    market_state_provider: DictProvider = field(
        default=lambda: {"markets": [], "updated_at": None}
    )
    golden_snapshot_provider: DictProvider = field(
        default=lambda: {"snapshots": [], "hash_map": {}}
    )
    suggestions_provider: DictProvider = field(
        default=lambda: {"suggestions": [], "metrics": {}}
    )
    exit_provider: DictProvider = field(
        default=lambda: {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}
    )
    vote_windows_provider: DictProvider = field(
        default=lambda: {"windows": [], "decisions": []}
    )
    shadow_provider: DictProvider = field(
        default=lambda: {"virtual_fills": [], "paper_positions": [], "live_fills": []}
    )
    rl_provider: DictProvider = field(
        default=lambda: {"weights": {}, "uplift": {}}
    )
    settings_provider: DictProvider = field(
        default=lambda: {"controls": {}, "overrides": {}, "staleness": {}}
    )
    health_provider: DictProvider = field(default=lambda: {})
    golden_depth_enabled: bool = False
    golden_momentum_enabled: bool = False

    def snapshot_status(self) -> Dict[str, Any]:
        try:
            return dict(self.status_provider())
        except Exception:  # pragma: no cover - defensive coding
            log.exception("UI status provider failed")
            return {"event_bus": False, "trading_loop": False}

    def snapshot_activity(self) -> List[Dict[str, Any]]:
        try:
            return list(self.activity_provider())
        except Exception:  # pragma: no cover
            log.exception("UI activity provider failed")
            return []

    def snapshot_trades(self) -> List[Dict[str, Any]]:
        try:
            return list(self.trades_provider())
        except Exception:  # pragma: no cover
            log.exception("UI trades provider failed")
            return []

    def snapshot_weights(self) -> Dict[str, Any]:
        try:
            return dict(self.weights_provider())
        except Exception:  # pragma: no cover
            log.exception("UI weights provider failed")
            return {}

    def snapshot_rl(self) -> Dict[str, Any]:
        try:
            return dict(self.rl_status_provider())
        except Exception:  # pragma: no cover
            log.exception("UI RL status provider failed")
            return {}

    def snapshot_logs(self) -> List[Dict[str, Any]]:
        try:
            return list(self.logs_provider())
        except Exception:  # pragma: no cover
            log.exception("UI log provider failed")
            return []

    def snapshot_summary(self) -> Dict[str, Any]:
        try:
            return dict(self.summary_provider())
        except Exception:  # pragma: no cover
            log.exception("UI summary provider failed")
            return {}

    def snapshot_discovery(self) -> Dict[str, Any]:
        try:
            data = self.discovery_provider()
            if isinstance(data, dict):
                return dict(data)
            return {"recent": list(data)}
        except Exception:  # pragma: no cover
            log.exception("UI discovery provider failed")
            return {"recent": []}

    def snapshot_config(self) -> Dict[str, Any]:
        try:
            return dict(self.config_provider())
        except Exception:  # pragma: no cover
            log.exception("UI config provider failed")
            return {}

    def snapshot_history(self) -> List[Dict[str, Any]]:
        try:
            return list(self.history_provider())
        except Exception:  # pragma: no cover
            log.exception("UI history provider failed")
            return []

    def snapshot_actions(self) -> List[Dict[str, Any]]:
        try:
            return list(self.actions_provider())
        except Exception:  # pragma: no cover
            log.exception("UI actions provider failed")
            return []

    def snapshot_discovery_console(self) -> Dict[str, Any]:
        try:
            return dict(self.discovery_console_provider())
        except Exception:  # pragma: no cover
            log.exception("UI discovery console provider failed")
            return {"candidates": [], "stats": {}}

    def snapshot_token_facts(self) -> Dict[str, Any]:
        try:
            return dict(self.token_facts_provider())
        except Exception:  # pragma: no cover
            log.exception("UI token facts provider failed")
            return {"tokens": {}, "selected": None}

    def snapshot_market_state(self) -> Dict[str, Any]:
        try:
            return dict(self.market_state_provider())
        except Exception:  # pragma: no cover
            log.exception("UI market state provider failed")
            return {"markets": [], "updated_at": None}

    def snapshot_golden_snapshots(self) -> Dict[str, Any]:
        try:
            return dict(self.golden_snapshot_provider())
        except Exception:  # pragma: no cover
            log.exception("UI golden snapshot provider failed")
            return {"snapshots": [], "hash_map": {}}

    def snapshot_suggestions(self) -> Dict[str, Any]:
        try:
            return dict(self.suggestions_provider())
        except Exception:  # pragma: no cover
            log.exception("UI suggestions provider failed")
            return {"suggestions": [], "rejections": [], "metrics": {}}

    def snapshot_exit(self) -> Dict[str, Any]:
        try:
            data = self.exit_provider()
            if not isinstance(data, dict):
                data = {"hot_watch": list(data)}
            payload = {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}
            payload.update(data)
            return payload
        except Exception:  # pragma: no cover
            log.exception("UI exit provider failed")
            return {"hot_watch": [], "diagnostics": [], "closed": [], "queue": [], "missed_exits": []}

    def snapshot_vote_windows(self) -> Dict[str, Any]:
        try:
            return dict(self.vote_windows_provider())
        except Exception:  # pragma: no cover
            log.exception("UI vote windows provider failed")
            return {"windows": [], "decisions": []}

    def snapshot_shadow(self) -> Dict[str, Any]:
        try:
            return dict(self.shadow_provider())
        except Exception:  # pragma: no cover
            log.exception("UI shadow provider failed")
            return {"virtual_fills": [], "paper_positions": [], "live_fills": []}

    def snapshot_rl_panel(self) -> Dict[str, Any]:
        try:
            return dict(self.rl_provider())
        except Exception:  # pragma: no cover
            log.exception("UI RL panel provider failed")
            return {"weights": {}, "uplift": {}}

    def snapshot_settings(self) -> Dict[str, Any]:
        try:
            return dict(self.settings_provider())
        except Exception:  # pragma: no cover
            log.exception("UI settings provider failed")
            return {"controls": {}, "overrides": {}, "staleness": {}}

    def snapshot_health(self) -> Dict[str, Any]:
        try:
            return dict(self.health_provider())
        except Exception:  # pragma: no cover
            log.exception("UI health provider failed")
            return {"ok": False}




def _json_ready(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {str(k): _json_ready(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_json_ready(v) for v in obj]
    return obj


def create_app(state: UIState | None = None) -> Flask:
    """Return a configured Flask application bound to *state*."""

    if state is None:
        state = UIState()
    _set_active_ui_state(state)

    app = Flask(__name__)  # type: ignore[arg-type]

    @app.after_request
    def _no_store(response: Response) -> Response:
        response.headers.setdefault("X-Content-Type-Options", "nosniff")
        response.headers.setdefault("Referrer-Policy", "no-referrer")
        response.headers.setdefault("X-Frame-Options", "DENY")
        content_type = response.content_type or ""
        if "application/json" in content_type.lower():
            response.headers["Cache-Control"] = "no-store"
        return response

    def _status_cards(status: Dict[str, Any]) -> List[Dict[str, Any]]:
        cards: List[Dict[str, Any]] = []
        cards.append(
            {
                "label": "Event Bus",
                "state": "ok" if status.get("event_bus") else "fail",
                "caption": "connected" if status.get("event_bus") else "offline",
            }
        )
        cards.append(
            {
                "label": "Trading Loop",
                "state": "ok" if status.get("trading_loop") else "fail",
                "caption": "running" if status.get("trading_loop") else "stopped",
            }
        )
        if status.get("depth_service") is not None:
            cards.append(
                {
                    "label": "Depth",
                    "state": "ok" if status.get("depth_service") else "warn",
                    "caption": "streaming" if status.get("depth_service") else "idle",
                }
            )
        if status.get("rl_daemon") is not None:
            cards.append(
                {
                    "label": "RL Daemon",
                    "state": "ok" if status.get("rl_daemon") else "warn",
                    "caption": "healthy" if status.get("rl_daemon") else "degraded",
                }
            )
        heartbeat = status.get("heartbeat") or status.get("heartbeat_ts")
        if heartbeat:
            cards.append(
                {
                    "label": "Heartbeat",
                    "state": "ok",
                    "caption": str(heartbeat),
                }
            )
        return cards

    @app.get("/")
    def index() -> Any:
        if request.args.get("format", "").lower() == "json":
            payload = {
                "message": "SolHunter Zero Swarm UI",
                "status": state.snapshot_status(),
                "summary": state.snapshot_summary(),
                "discovery": state.snapshot_discovery_console(),
                "token_facts": state.snapshot_token_facts(),
                "market": state.snapshot_market_state(),
                "golden": state.snapshot_golden_snapshots(),
                "suggestions": state.snapshot_suggestions(),
                "votes": state.snapshot_vote_windows(),
                "shadow": state.snapshot_shadow(),
                "rl": state.snapshot_rl_panel(),
                "settings": state.snapshot_settings(),
                "activity": state.snapshot_activity(),
                "logs": state.snapshot_logs(),
                "weights": state.snapshot_weights(),
                "config_overview": state.snapshot_config(),
                "history": state.snapshot_history(),
                "exits": state.snapshot_exit(),
            }
            return jsonify(_json_ready(payload))

        status = state.snapshot_status()
        status_cards = _status_cards(status)
        discovery_console = state.snapshot_discovery_console()
        token_facts = state.snapshot_token_facts()
        market_state = state.snapshot_market_state()
        golden_detail = state.snapshot_golden_snapshots()
        suggestions = state.snapshot_suggestions()
        exit_panel = state.snapshot_exit()
        vote_windows = state.snapshot_vote_windows()
        shadow = state.snapshot_shadow()
        rl_panel = state.snapshot_rl_panel()
        settings = state.snapshot_settings()
        summary = state.snapshot_summary()

        golden_snapshots = golden_detail.get("snapshots", [])
        golden_summary = {
            "count": len(golden_snapshots),
        }
        suggestion_metrics = suggestions.get("metrics", {})
        rl_summary = {
            "weights_applied": len(rl_panel.get("weights", [])),
        }
        swarm_overall = {
            "stale": suggestions.get("metrics", {}).get("stale", False),
            "age_label": suggestions.get("metrics", {}).get("updated_label", "n/a"),
        }
        stream_lag = {
            "ohlcv": (market_state.get("lag_ms") or {}).get("ohlcv_ms"),
            "depth": (market_state.get("lag_ms") or {}).get("depth_ms"),
            "golden": golden_detail.get("lag_ms"),
        }
        golden_lag_value = None
        golden_candidates = [
            status.get("golden_lag_ms"),
            summary.get("golden", {}).get("lag_ms")
            if isinstance(summary, dict)
            else None,
            stream_lag.get("golden"),
        ]
        for candidate in golden_candidates:
            try:
                numeric = float(candidate)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(numeric):
                continue
            golden_lag_value = numeric
            break
        agent_is_stale = bool(suggestion_metrics.get("stale"))
        agent_age_label = suggestion_metrics.get("updated_label") or swarm_overall.get(
            "age_label", "n/a"
        )
        if not isinstance(summary, dict):
            summary = {}
        evaluation_summary = summary.get("evaluation")
        if not isinstance(evaluation_summary, dict):
            evaluation_summary = {}
        execution_summary = summary.get("execution")
        if not isinstance(execution_summary, dict):
            execution_summary = {}
        paper_summary = summary.get("paper_pnl")
        if not isinstance(paper_summary, dict):
            paper_summary = {}
        golden_meta = summary.get("golden")
        if not isinstance(golden_meta, dict):
            golden_meta = {}

        session_summary = {
            "suggestions_5m": evaluation_summary.get("suggestions_5m"),
            "acceptance_rate": evaluation_summary.get("acceptance_rate"),
            "open_vote_windows": evaluation_summary.get("open_vote_windows"),
            "golden_hashes": golden_meta.get("count"),
            "shadow_fills": execution_summary.get("count"),
            "paper_unrealized_usd": paper_summary.get("latest_unrealized"),
            "lag_bus_ms": status.get("bus_latency_ms"),
            "lag_ohlcv_ms": status.get("ohlcv_lag_ms")
            if status.get("ohlcv_lag_ms") is not None
            else stream_lag.get("ohlcv"),
            "lag_depth_ms": status.get("depth_lag_ms")
            if status.get("depth_lag_ms") is not None
            else stream_lag.get("depth"),
            "lag_golden_ms": golden_lag_value,
        }

        def _badge_css(status_value: str) -> str:
            mapping = {
                "open": "status-ok",
                "ok": "status-ok",
                "streaming": "status-ok",
                "warn": "status-warn",
                "warning": "status-warn",
                "error": "status-danger",
                "fail": "status-danger",
                "danger": "status-danger",
                "connecting": "status-pending",
                "pending": "status-pending",
                "idle": "status-idle",
            }
            return mapping.get(status_value, "status-idle")

        def _lag_badge(value: Any, fallback: str) -> tuple[str, str]:
            if value is None:
                return "connecting", fallback
            try:
                numeric = float(value)
            except (TypeError, ValueError):
                return "connecting", fallback
            if not math.isfinite(numeric):
                return "connecting", fallback
            if numeric > 10_000.0:
                status_value = "error"
            elif numeric > 3_000.0:
                status_value = "warn"
            else:
                status_value = "open"
            detail = f"Lag {int(numeric + 0.5)} ms"
            return status_value, detail

        def _make_badge(name: str, label: str, status_value: str, detail: str) -> Dict[str, str]:
            return {
                "name": name,
                "label": label,
                "status": status_value,
                "detail": detail,
                "css_class": _badge_css(status_value),
            }

        market_lag_value: float | None = None
        for candidate in (
            session_summary.get("lag_ohlcv_ms"),
            session_summary.get("lag_depth_ms"),
        ):
            try:
                numeric = float(candidate)
            except (TypeError, ValueError):
                continue
            if not math.isfinite(numeric):
                continue
            if market_lag_value is None or numeric > market_lag_value:
                market_lag_value = numeric
        market_status, market_detail = _lag_badge(market_lag_value, "Awaiting market data")
        golden_status, golden_detail = _lag_badge(
            golden_lag_value, "Awaiting golden stream"
        )
        agent_label = agent_age_label or "n/a"
        agent_prefix = "Stale" if agent_is_stale else "Fresh"
        agent_detail = f"{agent_prefix} · {agent_label}"

        connection_badges = [
            _make_badge("events", "Events", "connecting", "Waiting for metadata…"),
            _make_badge("market", "Market", market_status, market_detail),
            _make_badge("golden", "Golden", golden_status, golden_detail),
            _make_badge("agents", "Agents", "warn" if agent_is_stale else "open", agent_detail),
            _make_badge("rl", "RL", "connecting", "Waiting for metadata…"),
            _make_badge("logs", "Logs", "connecting", "Waiting for metadata…"),
        ]
        stream_lag["golden"] = golden_lag_value
        header_signals = {
            "environment": (status.get("environment") or "dev").upper(),
            "paper_mode": bool(status.get("paper_mode")),
            "paused": bool(status.get("paused")),
            "rl_mode": status.get("rl_mode", "shadow"),
            "bus_latency_ms": status.get("bus_latency_ms"),
            "stream_lag": stream_lag,
        }
        kpis = {
            "suggestions_per_5m": suggestions.get("metrics", {}).get("rate_per_min", 0)
            * 5.0,
            "acceptance_rate": suggestions.get("metrics", {}).get("acceptance_rate", 0),
            "golden_hashes": golden_summary.get("count", 0),
            "open_windows": len(vote_windows.get("windows", [])),
            "paper_pnl": summary.get("execution", {}).get("pnl_1d"),
            "drawdown": summary.get("execution", {}).get("drawdown"),
            "turnover": summary.get("execution", {}).get("turnover"),
        }

        return render_template(
            "ui.html",
            status_cards=status_cards,
            discovery_console=discovery_console,
            token_facts=token_facts,
            market_state=market_state,
            golden_snapshots=golden_snapshots,
            golden_summary=golden_summary,
            suggestions=suggestions,
            suggestion_metrics=suggestion_metrics,
            exit_panel=exit_panel,
            vote_windows=vote_windows,
            shadow=shadow,
            rl_panel=rl_panel,
            rl_summary=rl_summary,
            settings=settings,
            swarm_overall=swarm_overall,
            header_signals=header_signals,
            kpis=kpis,
            session_summary=session_summary,
            connection_badges=connection_badges,
            golden_lag_value=golden_lag_value,
            agent_is_stale=agent_is_stale,
            agent_age_label=agent_age_label,
            golden_depth_enabled=state.golden_depth_enabled,
            golden_momentum_enabled=state.golden_momentum_enabled,
            ui_schema_version=UI_SCHEMA_VERSION,
        )

    def _ws_config_payload() -> Dict[str, str]:
        manifest = build_ui_manifest(request)
        return {
            "rl_ws": manifest["rl_ws"],
            "events_ws": manifest["events_ws"],
            "logs_ws": manifest["logs_ws"],
        }

    def _ui_meta_payload() -> Dict[str, Any]:
        manifest = build_ui_manifest(request)
        base_url = (request.url_root or "").rstrip("/")
        if not base_url:
            scheme = getattr(request, "scheme", "http") or "http"
            host = request.host or ""
            if not host:
                host = f"127.0.0.1:{manifest.get('ui_port', 5000)}"
            base_url = f"{scheme}://{host}"
        meta_snapshot = get_ui_meta_snapshot()
        return {
            "url": base_url,
            "rl_ws": manifest["rl_ws"],
            "events_ws": manifest["events_ws"],
            "logs_ws": manifest["logs_ws"],
            "meta": meta_snapshot,
        }

    def _probe_ws(url: str | None, *, timeout: float = 1.5) -> tuple[str, Optional[str]]:
        if not url:
            return "fail", "missing endpoint"
        if websockets is None:
            return "fail", "websockets module unavailable"

        async def _check() -> tuple[str, Optional[str]]:
            try:
                async with websockets.connect(url, ping_timeout=timeout, close_timeout=timeout):
                    return "ok", None
            except Exception as exc:  # pragma: no cover - network probe failures
                log.debug("UI websocket probe failed for %s: %s", url, exc)
                return "fail", f"{type(exc).__name__}: {exc}"

        try:
            return asyncio.run(_check())
        except RuntimeError as exc:  # pragma: no cover - unexpected event loop state
            log.debug("UI websocket probe unavailable for %s: %s", url, exc)
            return "fail", f"RuntimeError: {exc}"
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("UI websocket probe crashed for %s: %s", url, exc)
            return "fail", f"{type(exc).__name__}: {exc}"

    @app.get("/api/manifest")
    def api_manifest() -> Any:
        return jsonify(build_ui_manifest(request))

    @app.get("/ui/meta")
    def ui_meta() -> Any:
        return jsonify(_ui_meta_payload())

    @app.get("/ui/ws-config")
    def ui_ws_config() -> Any:
        return jsonify(_ws_config_payload())

    @app.get("/ws-config")
    def ws_config() -> Any:
        return jsonify(_ws_config_payload())

    @app.get("/ui/health")
    def ui_health() -> Any:
        urls = get_ws_urls()
        rl_status, rl_detail = _probe_ws(urls.get("rl"))
        events_status, events_detail = _probe_ws(urls.get("events"))
        logs_status, logs_detail = _probe_ws(urls.get("logs"))
        payload: Dict[str, Any] = {
            "ui": "ok",
            "rl_ws": rl_status,
            "events_ws": events_status,
            "logs_ws": logs_status,
        }
        details = {
            key: detail
            for key, detail in {
                "rl_ws": rl_detail,
                "events_ws": events_detail,
                "logs_ws": logs_detail,
            }.items()
            if detail
        }
        if details:
            payload["details"] = details
        return jsonify(payload)

    @app.get("/health")
    def health() -> Any:
        status = state.snapshot_status()
        ok = bool(status.get("event_bus")) and bool(status.get("trading_loop"))
        return jsonify({"ok": ok, "status": status})

    @app.get("/health/runtime")
    def health_runtime_view() -> Any:
        payload = state.snapshot_health()
        if "ok" not in payload:
            event_bus_ok = bool(payload.get("event_bus", {}).get("connected"))
            heartbeat_ok = bool(payload.get("heartbeat", {}).get("ok", True))
            resource_ok = not bool(payload.get("resource", {}).get("exit_active"))
            payload["ok"] = event_bus_ok and heartbeat_ok and resource_ok
        return jsonify(_json_ready(payload))

    @app.get("/status")
    def status_view() -> Any:
        return jsonify(state.snapshot_status())

    @app.get("/summary")
    def summary() -> Any:
        return jsonify(state.snapshot_summary())

    @app.get("/tokens")
    def tokens() -> Any:
        return jsonify(state.snapshot_token_facts())

    @app.get("/actions")
    def actions() -> Any:
        return jsonify({"actions": state.snapshot_actions()})

    @app.get("/activity")
    def activity() -> Any:
        return jsonify({"entries": state.snapshot_activity()})

    @app.get("/trades")
    def trades() -> Any:
        return jsonify(list(state.snapshot_trades()))

    @app.get("/weights")
    def weights() -> Any:
        return jsonify(state.snapshot_weights())

    @app.get("/rl/status")
    def rl_status() -> Any:
        return jsonify(state.snapshot_rl())

    @app.get("/config")
    def config() -> Any:
        return jsonify(state.snapshot_config())

    @app.get("/logs")
    def logs() -> Any:
        return jsonify({"entries": state.snapshot_logs()})

    @app.get("/discovery")
    def discovery_settings() -> Any:
        method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if method is None:
            method = DEFAULT_DISCOVERY_METHOD
        return jsonify(
            {
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.post("/discovery")
    def update_discovery() -> Any:
        payload = request.get_json(silent=True) or {}
        raw_method = payload.get("method")
        if not isinstance(raw_method, str) or not raw_method.strip():
            return (
                jsonify(
                    {
                        "error": "method must be a non-empty string",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        method = resolve_discovery_method(raw_method)
        if method is None:
            return (
                jsonify(
                    {
                        "error": f"Invalid discovery method: {raw_method}",
                        "allowed_methods": sorted(DISCOVERY_METHODS),
                    }
                ),
                400,
            )
        os.environ["DISCOVERY_METHOD"] = method
        return jsonify(
            {
                "status": "ok",
                "method": method,
                "allowed_methods": sorted(DISCOVERY_METHODS),
            }
        )

    @app.get("/swarm/discovery")
    def swarm_discovery() -> Any:
        return jsonify(_json_ready(state.snapshot_discovery_console()))

    @app.get("/swarm/market")
    def swarm_market() -> Any:
        return jsonify(_json_ready(state.snapshot_market_state()))

    @app.get("/swarm/golden")
    def swarm_golden() -> Any:
        return jsonify(_json_ready(state.snapshot_golden_snapshots()))

    @app.get("/swarm/suggestions")
    def swarm_suggestions() -> Any:
        return jsonify(_json_ready(state.snapshot_suggestions()))

    @app.get("/swarm/exits")
    def swarm_exits() -> Any:
        return jsonify(_json_ready(state.snapshot_exit()))

    @app.get("/swarm/votes")
    def swarm_votes() -> Any:
        return jsonify(_json_ready(state.snapshot_vote_windows()))

    @app.get("/swarm/shadow")
    def swarm_shadow() -> Any:
        return jsonify(_json_ready(state.snapshot_shadow()))

    @app.get("/swarm/rl")
    def swarm_rl() -> Any:
        return jsonify(_json_ready(state.snapshot_rl_panel()))

    @app.post("/actions/flatten")
    def action_flatten() -> Any:
        payload = request.get_json(silent=True) or {}
        mint = payload.get("mint")
        if not isinstance(mint, str) or not mint.strip():
            return jsonify({"ok": False, "error": "mint must be a non-empty string"}), 400

        must = bool(payload.get("must", False))
        must_exit = bool(payload.get("must_exit", False))
        action = {
            "type": "flatten",
            "mint": mint,
            "must": must,
            "must_exit": must_exit,
        }
        log.info(
            "UI flatten requested mint=%s must=%s must_exit=%s",
            mint,
            must,
            must_exit,
        )
        push_event({"ui_action": action})
        return jsonify({"ok": True, "action": action})

    @app.get("/__shutdown__")
    def _shutdown() -> Any:  # pragma: no cover - invoked via HTTP
        func = request.environ.get("werkzeug.server.shutdown")
        if func is None:
            raise RuntimeError("Not running with the Werkzeug Server")
        func()
        return {"ok": True}

    return app

class UIServer:
    """Utility wrapper that runs the Flask app in a background thread."""

    def __init__(
        self,
        state: UIState,
        *,
        host: str = "127.0.0.1",
        port: int = 5000,
    ) -> None:
        self.state = state
        self.host = host
        self.port = int(port)
        self.app = create_app(state)
        self._thread: Optional[threading.Thread] = None
        self._server: Optional[BaseWSGIServer] = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return

        def _serve() -> None:
            try:
                server = make_server(self.host, self.port, self.app)
                server.daemon_threads = True
                self._server = server
                server.serve_forever()
            except Exception:  # pragma: no cover - best effort logging
                log.exception("UI server crashed")
            finally:
                self._server = None

        self._thread = threading.Thread(target=_serve, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        server = self._server
        if server is not None:
            with contextlib.suppress(Exception):
                server.shutdown()
            with contextlib.suppress(Exception):
                server.server_close()
        if self._thread:
            self._thread.join(timeout=2)
        self._thread = None
        self._server = None
        stop_websockets()
