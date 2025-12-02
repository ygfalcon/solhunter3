"""Integration layer wiring the Golden pipeline into the runtime event flow."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import math
import os
import random
import time
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from ..agent_manager import AgentManager
from ..event_bus import BUS as RUNTIME_EVENT_BUS, EventBus
from ..portfolio import Portfolio
from ..token_aliases import canonical_mint
from ..token_scanner import TRENDING_METADATA

from .agents import BaseAgent
from .bus import EventBusAdapter, MessageBus
from .contracts import STREAMS
from .depth_adapter import GoldenDepthAdapter
from .flags import resolve_depth_flag, resolve_momentum_flag
from .momentum import MomentumAgent
from .pipeline import GoldenPipeline
from .types import (
    Decision,
    DepthSnapshot,
    DiscoveryCandidate,
    GoldenSnapshot,
    TapeEvent,
    TokenSnapshot,
    TradeSuggestion,
    TRADE_REJECTION_SCHEMA_VERSION,
    VirtualFill,
    VirtualPnL,
)

log = logging.getLogger(__name__)


def _parse_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _resolve_depth_cache_ttl(config: Mapping[str, Any] | None) -> float:
    default = 10.0
    env_name = None
    env_value = None
    for candidate_env in ("GOLDEN_DEPTH_TTL_SECONDS", "GOLDEN_DEPTH_CACHE_TTL"):
        value = os.getenv(candidate_env)
        if value is not None:
            env_name = candidate_env
            env_value = value
            break
    if env_value is not None:
        parsed_env = _parse_float(env_value)
        if parsed_env is not None and parsed_env > 0:
            return max(0.5, parsed_env)
        log.warning(
            "Invalid %s=%r; falling back to default %.1f s",
            env_name,
            env_value,
            default,
        )
    candidate: Any = None
    if isinstance(config, Mapping):
        golden_cfg = config.get("golden")
        if isinstance(golden_cfg, Mapping):
            depth_cfg = golden_cfg.get("depth")
            if isinstance(depth_cfg, Mapping):
                candidate = depth_cfg.get("cache_ttl") or depth_cfg.get("depth_cache_ttl")
            if candidate is None:
                candidate = golden_cfg.get("depth_cache_ttl")
        legacy_cfg = config.get("golden_pipeline")
        if candidate is None and isinstance(legacy_cfg, Mapping):
            depth_cfg = legacy_cfg.get("depth")
            if isinstance(depth_cfg, Mapping):
                candidate = depth_cfg.get("cache_ttl") or depth_cfg.get("depth_cache_ttl")
    parsed = _parse_float(candidate)
    if parsed is not None and parsed > 0:
        return max(0.5, parsed)
    if candidate is not None:
        log.warning(
            "Invalid depth_cache_ttl=%r in config; using default %.1f s",
            candidate,
            default,
        )
    return default


_GLOBAL_CAP_ENV = "SWARM_AGENT_GLOBAL_CAP_USD"
_AGENT_CAPS_ENV = "SWARM_AGENT_AGENT_CAPS_USD"


_MAX_IN_FLIGHT_SPAWN_TASKS = 32


def _coerce_float(value: Any) -> Optional[float]:
    """Best-effort conversion of ``value`` to ``float``."""

    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        text = str(value).strip()
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def _configured_seed_tokens() -> tuple[str, ...]:
    raw = os.getenv("SEED_TOKENS")
    if raw is None or not raw.strip():
        return tuple()
    tokens: list[str] = []
    for part in raw.split(","):
        candidate = part.strip()
        if candidate:
            tokens.append(candidate)
    return tuple(tokens)


def _normalize_source_values(*values: Any) -> tuple[str, ...]:
    """Flatten arbitrary ``values`` into a normalised tuple of source slugs."""

    collected: list[str] = []

    def _collect(value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str):
            text = value.strip()
            if text:
                collected.append(text)
            return
        if isinstance(value, Mapping):
            for key in ("source", "sources", "origin", "method", "provider", "providers", "tag", "tags"):
                if key in value:
                    _collect(value.get(key))
            if "discovery" in value:
                _collect(value.get("discovery"))
            return
        if isinstance(value, (list, tuple, set)):
            for item in value:
                _collect(item)

    for entry in values:
        _collect(entry)

    normalised: list[str] = []
    seen: set[str] = set()
    for raw in collected:
        candidate = str(raw).strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        if lowered not in seen:
            seen.add(lowered)
            normalised.append(lowered)
    return tuple(normalised)


def _default_symbol(mint: str) -> str:
    return mint[:6].upper()


def _normalize_depth_key(raw: Any) -> Optional[str]:
    """Normalise a depth percentage key into the canonical string form."""

    if isinstance(raw, (int, float)):
        value = float(raw)
    else:
        text = str(raw).strip()
        if not text:
            return None
        if text.endswith("%"):
            text = text[:-1].strip()
        if not text:
            return None
        try:
            value = float(text)
        except ValueError:
            return text
    if not math.isfinite(value):
        return None
    return f"{value:g}"


def _normalize_depth_pct_map(raw: Mapping[Any, Any]) -> Dict[str, float]:
    normalized: Dict[str, float] = {}
    for key, value in raw.items():
        amount = _coerce_float(value)
        if amount is None:
            continue
        normalized_key = _normalize_depth_key(key)
        if not normalized_key:
            continue
        normalized[normalized_key] = float(amount)
    return normalized


@dataclass(slots=True)
class _AgentAction:
    side: str
    notional: float
    price: float
    max_slippage_bps: float
    confidence: float
    ttl_sec: float
    risk: Dict[str, float]


class AgentManagerAgent(BaseAgent):
    """Adapter that turns ``AgentManager`` actions into Golden suggestions."""

    def __init__(
        self,
        manager: AgentManager,
        portfolio: Portfolio,
        *,
        name: str = "swarm",
        default_notional: float = 1_000.0,
        default_slippage_bps: float = 60.0,
        default_ttl: float = 1.5,
        allowed_patterns: Sequence[str] | None = None,
        min_burst_zscore: float = 2.0,
        max_micro_spread_bps: float = 40.0,
        min_depth_usd: float = 15_000.0,
        buffer_bps: float = 20.0,
        agent_notional_caps: Mapping[str, float] | None = None,
        global_notional_cap: float | None = None,
        bus: MessageBus | None = None,
        rejection_stream: str | None = None,
    ) -> None:
        super().__init__(name)
        self.manager = manager
        self.portfolio = portfolio
        self.default_notional = float(default_notional)
        self.default_slippage_bps = float(default_slippage_bps)
        self.default_ttl = float(default_ttl)
        patterns = allowed_patterns or (
            "first_pullback",
            "first_pullback_breakout",
            "breakout_pullback",
        )
        self._allowed_patterns = {str(p).lower() for p in patterns if str(p).strip()}
        self._last_buyers: Dict[str, int] = {}
        self._min_burst_zscore = float(min_burst_zscore)
        self._max_micro_spread_bps = float(max_micro_spread_bps)
        self._min_depth_usd = float(min_depth_usd)
        self._edge_buffer_bps = float(buffer_bps)
        self._bus = bus
        self._rejection_stream = rejection_stream or STREAMS.trade_rejected
        self._global_notional_cap = self._resolve_global_cap(global_notional_cap)
        self._agent_caps = self._resolve_agent_caps(agent_notional_caps)

    def _resolve_global_cap(self, override: float | None) -> float | None:
        value = _coerce_float(override)
        if value is not None and value > 0:
            return float(value)
        env_value = _coerce_float(os.getenv(_GLOBAL_CAP_ENV))
        if env_value is not None and env_value > 0:
            return float(env_value)
        return None

    def _resolve_agent_caps(
        self, provided: Mapping[str, float] | None
    ) -> Dict[str, float]:
        caps: Dict[str, float] = {}
        env_raw = os.getenv(_AGENT_CAPS_ENV)
        if env_raw:
            caps.update(self._parse_agent_caps(env_raw))
        if provided:
            for key, value in provided.items():
                amount = _coerce_float(value)
                if amount is None or amount <= 0:
                    continue
                name = str(key).strip().lower()
                if not name:
                    continue
                caps[name] = float(amount)
        return caps

    @staticmethod
    def _parse_agent_caps(raw: str) -> Dict[str, float]:
        caps: Dict[str, float] = {}
        try:
            payload = json.loads(raw)
        except Exception:
            payload = None
        if isinstance(payload, Mapping):
            for key, value in payload.items():
                amount = _coerce_float(value)
                if amount is None or amount <= 0:
                    continue
                name = str(key).strip().lower()
                if not name:
                    continue
                caps[name] = float(amount)
            if caps:
                return caps
        entries = raw.replace(";", ",").split(",") if raw else []
        for entry in entries:
            if not entry.strip():
                continue
            if "=" in entry:
                key, value = entry.split("=", 1)
            elif ":" in entry:
                key, value = entry.split(":", 1)
            else:
                continue
            amount = _coerce_float(value)
            if amount is None or amount <= 0:
                continue
            name = key.strip().lower()
            if not name:
                continue
            caps[name] = float(amount)
        return caps

    def _resolve_actor(self, raw_action: Mapping[str, Any]) -> str:
        keys = (
            "agent",
            "agent_name",
            "strategy",
            "source",
            "name",
            "family",
            "lane",
        )
        for key in keys:
            value = raw_action.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        for nested_key in ("metadata", "meta"):
            nested = raw_action.get(nested_key)
            if isinstance(nested, Mapping):
                for key in keys:
                    value = nested.get(key)
                    if isinstance(value, str) and value.strip():
                        return value.strip()
        return self.name

    async def generate(self, snapshot: GoldenSnapshot) -> List[TradeSuggestion]:
        try:
            ctx = await self.manager.evaluate_with_swarm(snapshot.mint, self.portfolio)
        except Exception:
            log.exception("AgentManager evaluation failed for %s", snapshot.mint)
            return []

        suggestions: List[TradeSuggestion] = []
        actions = getattr(ctx, "actions", [])
        for raw in actions or []:
            if not isinstance(raw, dict):
                continue
            action = self._normalise_action(snapshot, raw)
            if not action:
                continue
            gating_report = self._apply_entry_gates(snapshot, raw, action)
            if not gating_report.get("ruthless_filter", {}).get("passed"):
                continue
            if not gating_report.get("edge_pass"):
                continue
            if not await self._guard_suggestion(snapshot, raw, action, gating_report):
                continue
            suggestion = self.build_suggestion(
                snapshot=snapshot,
                side=action.side,
                notional_usd=action.notional,
                max_slippage_bps=action.max_slippage_bps,
                risk=self._augment_risk(action, gating_report),
                confidence=action.confidence,
                ttl_sec=action.ttl_sec,
                gating=gating_report,
            )
            suggestions.append(suggestion)
        return suggestions

    def _normalise_action(
        self, snapshot: GoldenSnapshot, action: Dict[str, Any]
    ) -> Optional[_AgentAction]:
        side = str(action.get("side", "")).lower()
        if side not in {"buy", "sell"}:
            return None

        price_candidates = [
            action.get("price"),
            action.get("entry_price"),
            snapshot.px.get("mid_usd"),
            snapshot.ohlcv5m.get("c"),
        ]
        price = next(
            (float(val) for val in price_candidates if _coerce_float(val)),
            None,
        )
        if price is None or price <= 0:
            return None

        amount = _coerce_float(action.get("amount"))
        if amount is None or amount <= 0:
            amount = _coerce_float(action.get("size"))
        if amount is None or amount <= 0:
            notional_hint = _coerce_float(action.get("notional_usd"))
            if notional_hint is not None and notional_hint > 0:
                amount = notional_hint / price

        notional = _coerce_float(action.get("notional_usd"))
        if notional is None or notional <= 0:
            if amount is not None and amount > 0:
                notional = amount * price
        if notional is None or notional <= 0:
            depth1 = _coerce_float(snapshot.liq.get("depth_pct", {}).get("1"))
            if depth1 is not None and depth1 > 0:
                notional = depth1 * 0.1
            else:
                notional = self.default_notional

        max_slippage = _coerce_float(action.get("max_slippage_bps"))
        if max_slippage is None or max_slippage <= 0:
            max_slippage = self.default_slippage_bps

        confidence = _coerce_float(action.get("confidence"))
        if confidence is None:
            expected = _coerce_float(action.get("expected_roi")) or 0.0
            success = _coerce_float(action.get("success_prob")) or 0.0
            confidence = max(0.0, expected) * max(0.0, success)
        if confidence <= 0:
            confidence = 0.05

        ttl = _coerce_float(action.get("ttl_sec"))
        if ttl is None or ttl <= 0:
            ttl = _coerce_float(action.get("ttl"))
        if ttl is None or ttl <= 0:
            ttl = self.default_ttl

        risk: Dict[str, float] = {}
        for key, alias in (
            ("stop_bps", "stop_loss"),
            ("take_bps", "take_profit"),
            ("time_stop_sec", "time_stop"),
        ):
            value = _coerce_float(action.get(key))
            if value is None or value <= 0:
                value = _coerce_float(action.get(alias))
            if value is not None and value > 0:
                risk[key] = float(value)

        return _AgentAction(
            side=side,
            notional=float(notional),
            price=float(price),
            max_slippage_bps=float(max_slippage),
            confidence=float(confidence),
            ttl_sec=float(ttl),
            risk=risk,
        )

    def _augment_risk(
        self, action: _AgentAction, gating: Dict[str, Any]
    ) -> Dict[str, float]:
        risk = dict(action.risk)
        breakeven = gating.get("breakeven_bps")
        if isinstance(breakeven, (int, float)) and breakeven > 0:
            risk.setdefault("breakeven_bps", float(breakeven))
        return risk

    async def _guard_suggestion(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Mapping[str, Any],
        action: _AgentAction,
        gating: Mapping[str, Any],
    ) -> bool:
        reason = self._budget_guard(raw_action, action)
        if reason:
            await self._emit_rejection(snapshot, action, raw_action, gating, reason)
            return False
        reason = self._slippage_guard(snapshot, raw_action, action)
        if reason:
            await self._emit_rejection(snapshot, action, raw_action, gating, reason)
            return False
        return True

    def _budget_guard(
        self, raw_action: Mapping[str, Any], action: _AgentAction
    ) -> Dict[str, Any] | None:
        notional = float(action.notional)
        actor = self._resolve_actor(raw_action)
        actor_key = actor.strip().lower()
        if self._global_notional_cap is not None and notional > self._global_notional_cap:
            return {
                "guard": "budget",
                "code": "budget_cap",
                "message": (
                    f"Requested notional {notional:.2f} USD exceeds global cap "
                    f"{self._global_notional_cap:.2f} USD"
                ),
                "scope": "global",
                "cap_usd": float(self._global_notional_cap),
                "requested_usd": notional,
                "actor": actor,
            }
        if actor_key and actor_key in self._agent_caps:
            cap = self._agent_caps[actor_key]
            if notional > cap:
                return {
                    "guard": "budget",
                    "code": "budget_cap",
                    "message": (
                        f"Requested notional {notional:.2f} USD exceeds cap {cap:.2f} USD"
                        f" for agent {actor}"
                    ),
                    "scope": "agent",
                    "cap_usd": float(cap),
                    "requested_usd": notional,
                    "actor": actor,
                }
        return None

    def _slippage_guard(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Mapping[str, Any],
        action: _AgentAction,
    ) -> Dict[str, Any] | None:
        depth = snapshot.liq or {}
        depth_map = depth.get("depth_pct") if isinstance(depth.get("depth_pct"), Mapping) else {}
        max_slippage = max(float(action.max_slippage_bps), 0.0)
        capacity, depth_points = self._synthetic_capacity(depth_map, max_slippage)
        if capacity is None:
            return None
        if action.notional <= capacity:
            return None
        actor = self._resolve_actor(raw_action)
        return {
            "guard": "slippage",
            "code": "slippage_cap",
            "message": (
                f"Max slippage {max_slippage:.1f} bps supports {capacity:.2f} USD "
                f"but {action.notional:.2f} USD was requested"
            ),
            "available_notional_usd": float(capacity),
            "requested_usd": float(action.notional),
            "max_slippage_bps": max_slippage,
            "actor": actor,
            "depth_points": depth_points,
        }

    def _synthetic_capacity(
        self, depth_map: Mapping[str, Any], max_slippage_bps: float
    ) -> tuple[float | None, List[Dict[str, float]]]:
        points: List[tuple[float, float]] = []
        for bucket, value in (depth_map or {}).items():
            pct = _coerce_float(bucket)
            notional = _coerce_float(value)
            if pct is None or pct <= 0:
                continue
            if notional is None or notional <= 0:
                continue
            bps = float(pct) * 100.0
            points.append((bps, float(notional)))
        points.sort(key=lambda entry: entry[0])
        depth_points = [
            {"slippage_bps": bps, "notional_usd": cap} for bps, cap in points
        ]
        if not points:
            return None, depth_points
        limit = max(float(max_slippage_bps), 0.0)
        if limit <= 0:
            return None, depth_points
        prev_bps = 0.0
        prev_cap = 0.0
        for bps, cap in points:
            if limit <= bps:
                span = bps - prev_bps
                if span <= 0:
                    return max(prev_cap, 0.0), depth_points
                ratio = (limit - prev_bps) / span
                capacity = prev_cap + ratio * (cap - prev_cap)
                return max(capacity, 0.0), depth_points
            prev_bps = bps
            prev_cap = cap
        return max(prev_cap, 0.0), depth_points

    async def _emit_rejection(
        self,
        snapshot: GoldenSnapshot,
        action: _AgentAction,
        raw_action: Mapping[str, Any],
        gating: Mapping[str, Any],
        reason: Mapping[str, Any],
    ) -> None:
        actor = str(reason.get("actor") or self._resolve_actor(raw_action))
        payload: Dict[str, Any] = {
            "agent": self.name,
            "source_agent": actor,
            "mint": snapshot.mint,
            "side": action.side,
            "notional_usd": float(action.notional),
            "max_slippage_bps": float(action.max_slippage_bps),
            "confidence": float(action.confidence),
            "ttl_sec": float(action.ttl_sec),
            "snapshot_hash": snapshot.hash,
            "asof": snapshot.asof,
            "reason": reason.get("message"),
            "reason_code": reason.get("code"),
            "guard": reason.get("guard"),
            "scope": reason.get("scope"),
            "requested_notional_usd": reason.get(
                "requested_usd", float(action.notional)
            ),
            "detected_at": time.time(),
            "gating": dict(gating or {}),
            "schema_version": TRADE_REJECTION_SCHEMA_VERSION,
        }
        cap = reason.get("cap_usd")
        if cap is not None:
            payload["cap_usd"] = float(cap)
        available = reason.get("available_notional_usd")
        if available is not None:
            payload["available_notional_usd"] = float(available)
        slippage = reason.get("max_slippage_bps")
        if slippage is not None:
            payload["guard_slippage_bps"] = float(slippage)
        depth_points = reason.get("depth_points")
        if isinstance(depth_points, list):
            payload["depth_points"] = depth_points
        if self._bus and self._rejection_stream:
            await self._bus.publish(self._rejection_stream, payload)
        log.info(
            "Agent suggestion rejected",
            extra={
                "guard": reason.get("guard"),
                "scope": reason.get("scope"),
                "reason": reason.get("message"),
                "mint": snapshot.mint,
                "notional_usd": action.notional,
                "source_agent": actor,
            },
        )

    def _apply_entry_gates(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Dict[str, Any],
        action: _AgentAction,
    ) -> Dict[str, Any]:
        ruthless_report = self._ruthless_filter(snapshot, raw_action)
        friction_report = self._friction_floor(snapshot, raw_action, action)
        gating: Dict[str, Any] = {
            "ruthless_filter": ruthless_report,
            "friction_floor": friction_report,
            "breakeven_bps": friction_report.get("breakeven_bps"),
            "expected_edge_bps": friction_report.get("expected_edge_bps"),
            "edge_buffer_bps": friction_report.get("edge_buffer_bps"),
            "edge_pass": friction_report.get("passed"),
        }
        return gating

    def _ruthless_filter(
        self, snapshot: GoldenSnapshot, action: Dict[str, Any]
    ) -> Dict[str, Any]:
        ohlcv = snapshot.ohlcv5m or {}
        px = snapshot.px or {}
        liq = snapshot.liq or {}
        zret = _coerce_float(ohlcv.get("zret")) or 0.0
        zvol = _coerce_float(ohlcv.get("zvol")) or 0.0
        buyers = int(_coerce_float(ohlcv.get("buyers")) or 0)
        prev_buyers = self._last_buyers.get(snapshot.mint)
        buyers_uptick = bool(prev_buyers is not None and buyers > prev_buyers)
        spread_bps = _coerce_float(px.get("spread_bps")) or 0.0
        depth = _coerce_float(liq.get("depth_pct", {}).get("1")) or 0.0
        pattern = str(action.get("pattern") or "").strip().lower()
        pattern_allowed = bool(pattern and pattern in self._allowed_patterns)
        passed = (
            zret > self._min_burst_zscore
            and zvol > self._min_burst_zscore
            and buyers_uptick
            and spread_bps <= self._max_micro_spread_bps
            and depth >= self._min_depth_usd
            and pattern_allowed
        )
        report = {
            "zret": float(zret),
            "zvol": float(zvol),
            "buyers": buyers,
            "previous_buyers": prev_buyers,
            "buyers_uptick": buyers_uptick,
            "spread_bps": float(spread_bps),
            "depth_1pct_usd": float(depth),
            "pattern": pattern or None,
            "pattern_allowed": pattern_allowed,
            "passed": passed,
        }
        self._last_buyers[snapshot.mint] = buyers
        return report

    def _friction_floor(
        self,
        snapshot: GoldenSnapshot,
        raw_action: Dict[str, Any],
        action: _AgentAction,
    ) -> Dict[str, Any]:
        liq = snapshot.liq or {}
        depth = _coerce_float(liq.get("depth_pct", {}).get("1")) or 0.0
        notional = max(action.notional, 0.0)
        depth = max(depth, 1e-9)
        size_fraction = min(notional / depth, 1.0)
        impact_bps = min(size_fraction * 50.0, 200.0)
        fees_bps = (
            _coerce_float(raw_action.get("fees_bps"))
            or _coerce_float(raw_action.get("fee_bps"))
            or 4.0
        )
        latency_bps = _coerce_float(raw_action.get("latency_bps")) or 2.0
        breakeven = float(fees_bps + latency_bps + impact_bps)
        expected_edge = _coerce_float(raw_action.get("expected_edge_bps"))
        if expected_edge is None:
            expected_edge = _coerce_float(raw_action.get("edge_bps"))
        if expected_edge is None:
            expected_roi = _coerce_float(raw_action.get("expected_roi"))
            if expected_roi is not None:
                expected_edge = expected_roi * 10_000.0
        buffer_requirement = self._edge_buffer_bps
        edge_pass = False
        edge_buffer = None
        if expected_edge is not None:
            edge_buffer = float(expected_edge - breakeven)
            edge_pass = expected_edge >= breakeven + buffer_requirement
        report: Dict[str, Any] = {
            "fees_bps": float(fees_bps),
            "latency_bps": float(latency_bps),
            "impact_bps": float(impact_bps),
            "breakeven_bps": float(breakeven),
            "expected_edge_bps": expected_edge,
            "required_buffer_bps": float(buffer_requirement),
            "edge_buffer_bps": edge_buffer,
            "passed": edge_pass,
        }
        return report


class GoldenPipelineService:
    """Orchestrate Golden pipeline ingestion from existing runtime events."""

    def __init__(
        self,
        *,
        agent_manager: AgentManager,
        portfolio: Portfolio,
        enrichment_fetcher: Optional[Callable[[Iterable[str]], Awaitable[Dict[str, TokenSnapshot]]]] = None,
        event_bus: EventBus | None = None,
        config: Mapping[str, Any] | None = None,
    ) -> None:
        self.agent_manager = agent_manager
        self.portfolio = portfolio
        self._event_bus = event_bus or RUNTIME_EVENT_BUS
        self._config = config
        self._depth_flag = resolve_depth_flag(config)
        self._momentum_flag = resolve_momentum_flag(config)
        if enrichment_fetcher is None:
            enrichment_fetcher = self._default_enrichment_fetcher
        depth_cache_ttl = _resolve_depth_cache_ttl(config)
        self._depth_cache_ttl = depth_cache_ttl
        self._depth_near_fresh_ms: float | None
        if depth_cache_ttl > 0:
            self._depth_near_fresh_ms = depth_cache_ttl * 2000.0
        else:
            self._depth_near_fresh_ms = None
        shared_bus = EventBusAdapter(self._event_bus)
        manager_agent = AgentManagerAgent(
            agent_manager,
            portfolio,
            bus=shared_bus,
            rejection_stream=STREAMS.trade_rejected,
        )
        self.pipeline = GoldenPipeline(
            enrichment_fetcher=enrichment_fetcher,
            agents=[manager_agent],
            on_decision=self._handle_decision,
            on_golden=self._handle_snapshot,
            on_suggestion=self._handle_suggestion,
            on_virtual_fill=self._handle_virtual_fill,
            on_virtual_pnl=self._handle_virtual_pnl,
            bus=shared_bus,
            depth_extensions_enabled=self._depth_flag,
            depth_near_fresh_ms=self._depth_near_fresh_ms,
        )
        self._momentum_agent: MomentumAgent | None = None
        if self._momentum_flag:
            self._momentum_agent = MomentumAgent(
                pipeline=self.pipeline,
                publish=self.pipeline.publish_momentum,
                config=config,
            )
        self._depth_adapter = GoldenDepthAdapter(
            enabled=self._depth_flag,
            submit_depth=self.pipeline.submit_depth,
            decimals_resolver=self._resolve_decimals,
            cache_ttl=depth_cache_ttl,
        )

        self._subscriptions: List[Callable[[], None]] = []
        self._tasks: List[asyncio.Task] = []
        self._pending: set[asyncio.Task] = set()
        self._pending_gate = asyncio.Semaphore(_MAX_IN_FLIGHT_SPAWN_TASKS)
        self._running = False
        self._last_price: Dict[str, float] = {}

    def _resolve_decimals(self, mint: str) -> int:
        coalescer = getattr(self.pipeline, "_coalescer", None)
        if coalescer is not None:
            snapshot = coalescer.get_metadata(mint)  # type: ignore[attr-defined]
            if snapshot is not None:
                return int(getattr(snapshot, "decimals", 6))
        return 6

    def _seed_price_hint(self, token: str, canonical: str) -> float:
        price_candidates: list[Any] = []
        price_candidates.append(self._last_price.get(canonical))
        for key in (canonical, token):
            meta = TRENDING_METADATA.get(key)
            if not isinstance(meta, Mapping):
                continue
            for field in (
                "mid_usd",
                "midPrice",
                "mid_price",
                "usd_price",
                "usdPrice",
                "price_usd",
                "priceUsd",
                "price",
                "close",
            ):
                if field in meta:
                    price_candidates.append(meta.get(field))
        for candidate in price_candidates:
            hint = _coerce_float(candidate)
            if hint is not None and hint > 0:
                return float(hint)
        return 0.0

    async def _publish_warm_start_depth(self) -> None:
        if not self._running:
            return
        tokens = _configured_seed_tokens()
        if not tokens:
            log.debug("Warm-start depth skipped (no seed tokens configured)")
            return
        now = time.time()
        seen: set[str] = set()
        published = 0
        for token in tokens:
            canonical = canonical_mint(str(token))
            if not canonical or canonical in seen:
                continue
            seen.add(canonical)
            mid = self._seed_price_hint(str(token), canonical)
            depth_pct = {"1": 0.0}
            snapshot = DepthSnapshot(
                mint=canonical,
                venue="warm_start",
                mid_usd=float(mid),
                spread_bps=0.0,
                depth_pct=depth_pct,
                asof=now,
                px_bid_usd=float(mid) if mid > 0 else None,
                px_ask_usd=float(mid) if mid > 0 else None,
                degraded=True,
                source="warm_start",
                staleness_ms=0.0,
            )
            try:
                await self.pipeline.submit_depth(snapshot)
            except Exception:  # pragma: no cover - defensive
                log.exception("Failed to publish warm-start depth for %s", canonical)
                continue
            published += 1
        if published:
            log.info(
                "Warm-started depth snapshots for %s seed tokens", published
            )

    async def _scan_discovery_cache(self) -> int:
        kv = getattr(self.pipeline, "_kv", None)
        if kv is None:
            return 0
        try:
            entries = await kv.scan_prefix("discovery:")
        except Exception:
            log.debug("Discovery preflight cache scan failed", exc_info=True)
            return 0
        return len(entries)

    @staticmethod
    def _extract_discovery_tokens(payload: Any) -> list[str]:
        tokens: list[str] = []
        if isinstance(payload, Mapping):
            for key in ("tokens", "mints", "mint", "token", "address"):
                value = payload.get(key)
                if isinstance(value, str):
                    tokens.append(value)
                elif isinstance(value, (list, tuple)):
                    tokens.extend(str(v) for v in value if v)
            nested = payload.get("entry")
            if isinstance(nested, Mapping):
                tokens.extend(
                    t
                    for t in GoldenPipelineService._extract_discovery_tokens(nested)
                    if t
                )
        elif isinstance(payload, DiscoveryCandidate):
            tokens.append(payload.mint)
        return [tok for tok in tokens if tok]

    async def _ensure_discovery_flow(self) -> None:
        try:
            timeout = float(os.getenv("DISCOVERY_PREFLIGHT_TIMEOUT", "8") or 8.0)
        except Exception:
            timeout = 8.0
        timeout = max(0.5, timeout)

        cached = await self._scan_discovery_cache()
        if cached:
            log.info(
                "Discovery preflight satisfied by %d cached discovery key(s)", cached
            )
            return

        observed: list[str] = []
        ready = asyncio.Event()

        async def _capture(payload: Any) -> None:
            tokens = self._extract_discovery_tokens(payload)
            if not tokens:
                return
            observed.extend(tokens)
            ready.set()

        unsub_tokens = self._event_bus.subscribe("token_discovered", _capture)
        unsub_candidates = self._event_bus.subscribe(
            STREAMS.discovery_candidates, _capture
        )
        try:
            try:
                await asyncio.wait_for(ready.wait(), timeout=timeout)
            except asyncio.TimeoutError as exc:
                guidance = (
                    "Discovery preflight failed: no discovery messages observed within "
                    f"{timeout:.1f}s. Ensure the discovery publisher is running and that "
                    "Redis discovery:* keys or the token_discovered/discovery_candidates "
                    "topics are producing fresh data."
                )
                log.error(guidance)
                raise RuntimeError(guidance) from exc
        finally:
            if callable(unsub_tokens):
                unsub_tokens()
            if callable(unsub_candidates):
                unsub_candidates()
        log.info(
            "Discovery preflight observed %d discovery token(s)", len(observed)
        )

    async def start(self) -> None:
        if self._running:
            return
        await self._ensure_bus_visible()
        await self._ensure_discovery_flow()
        self._running = True
        self._log_bus_configuration()
        bootstrapped = await self._bootstrap_trending_metadata()
        self._subscriptions.append(
            self._event_bus.subscribe("token_discovered", self._on_discovery)
        )
        self._subscriptions.append(
            self._event_bus.subscribe("price_update", self._on_price)
        )
        self._subscriptions.append(
            self._event_bus.subscribe("depth_update", self._on_depth)
        )
        await self.pipeline.flush_market()
        await self._depth_adapter.start()
        await self._publish_warm_start_depth()
        if self._momentum_agent:
            await self._momentum_agent.start()
        self._tasks.append(asyncio.create_task(self._market_flush_loop(), name="golden_market_flush"))
        self._tasks.append(asyncio.create_task(self._heartbeat_loop(), name="golden_heartbeat"))
        if bootstrapped:
            log.info(
                "GoldenPipelineService started (subscriptions=token_discovered, price_update, depth_update; bootstrapped=%d)",
                bootstrapped,
            )
        else:
            log.info(
                "GoldenPipelineService started (subscriptions=token_discovered, price_update, depth_update)"
            )

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        for unsub in self._subscriptions:
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()

        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._tasks.clear()
        await self._depth_adapter.stop()
        if self._momentum_agent:
            await self._momentum_agent.stop()

        pending = list(self._pending)
        for task in pending:
            task.cancel()
        if pending:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.gather(*pending, return_exceptions=True)
        self._pending.clear()
        log.info("GoldenPipelineService stopped")

    async def _market_flush_loop(self) -> None:
        try:
            while self._running:
                await asyncio.sleep(5.0)
                await self.pipeline.flush_market()
        except asyncio.CancelledError:
            raise

    async def _heartbeat_loop(self) -> None:
        base_interval = 5.0
        max_interval = 60.0
        interval = base_interval
        try:
            while self._running:
                try:
                    heartbeat = {"type": "golden_heartbeat", "ts": time.time()}
                    self._event_bus.publish("x:mint.golden.__meta", heartbeat)
                except Exception:
                    interval = min(max_interval, interval * 2.0)
                    log.warning(
                        "GoldenPipelineService heartbeat publish failed; backing off to %.2fs",
                        interval,
                        exc_info=True,
                    )
                else:
                    interval = base_interval
                jitter_multiplier = random.uniform(0.8, 1.2)
                await asyncio.sleep(interval * jitter_multiplier)
        except asyncio.CancelledError:
            raise

    def _spawn(self, coro: Awaitable[Any]) -> None:
        if not self._running:
            return
        gate = getattr(self, "_pending_gate", None)
        if gate is None:
            gate = asyncio.Semaphore(_MAX_IN_FLIGHT_SPAWN_TASKS)
            setattr(self, "_pending_gate", gate)
        if not hasattr(self, "_pending"):
            self._pending = set()  # type: ignore[attr-defined]
        async def _runner() -> Any:
            acquired = False
            try:
                await gate.acquire()
                acquired = True
                return await coro
            finally:
                if acquired:
                    gate.release()

        task = asyncio.create_task(_runner())
        self._pending.add(task)

        def _on_done(completed: asyncio.Task) -> None:
            self._pending.discard(completed)
            if completed.cancelled():
                return
            try:
                exc = completed.exception()
            except Exception:
                log.exception("GoldenPipelineService background task failed during exception retrieval")
                return
            if exc:
                log.exception("GoldenPipelineService background task failed", exc_info=exc)

        task.add_done_callback(_on_done)

    async def _ensure_bus_visible(self) -> None:
        channel = os.getenv("BROKER_CHANNEL", "solhunter-events-v3")
        max_attempts = 5
        base_delay = 0.5
        ack = asyncio.Event()

        async def _on_meta(payload: Any) -> None:
            nonlocal ack
            if isinstance(payload, dict) and payload.get("type") == "golden_heartbeat":
                ack.set()

        unsubscribe = self._event_bus.subscribe("x:mint.golden.__meta", _on_meta)
        try:
            for attempt in range(1, max_attempts + 1):
                ack = asyncio.Event()
                heartbeat = {"type": "golden_heartbeat", "ts": time.time()}
                self._event_bus.publish("x:mint.golden.__meta", heartbeat)
                try:
                    await asyncio.wait_for(ack.wait(), timeout=2.0)
                except asyncio.TimeoutError as exc:
                    if attempt == max_attempts:
                        log.error(
                            "GoldenPipelineService bus self-check failed after %s attempts (channel=%s)",
                            attempt,
                            channel,
                        )
                        raise RuntimeError("golden pipeline event bus heartbeat failed") from exc
                    delay = base_delay * (2 ** (attempt - 1))
                    log.warning(
                        "GoldenPipelineService bus self-check attempt %s/%s did not observe heartbeat; retrying in %.2fs",
                        attempt,
                        max_attempts,
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    if attempt > 1:
                        log.info(
                            "GoldenPipelineService bus self-check succeeded after %s attempts",
                            attempt,
                        )
                    return
        finally:
            unsubscribe()

    def _log_bus_configuration(self) -> None:
        channel = os.getenv("BROKER_CHANNEL", "solhunter-events-v3")
        url = (
            os.getenv("EVENT_BUS_URL")
            or os.getenv("BROKER_URL")
            or os.getenv("REDIS_URL")
        )
        broker_urls = getattr(self._event_bus, "broker_urls", None)
        if not url and broker_urls:
            url = broker_urls[0] if broker_urls else ""
        transport = "local"
        if url and ":" in url:
            transport = url.split(":", 1)[0].lower() or transport
        elif url:
            transport = url.lower()
        log.info(
            "golden.bus=%s channel=%s url=%s (shared)",
            transport,
            channel,
            url or "n/a",
        )

    async def _bootstrap_trending_metadata(self) -> int:
        """Seed cached trending metadata into the discovery pipeline."""

        snapshot = list(TRENDING_METADATA.items())
        seeds = _configured_seed_tokens()
        for token in seeds:
            canonical = canonical_mint(str(token))
            if not canonical:
                continue
            meta = TRENDING_METADATA.get(canonical)
            if isinstance(meta, Mapping):
                seed_meta: Dict[str, Any] = dict(meta)
                existing_sources = list(seed_meta.get("sources") or [])
                existing_sources.append("seeded")
                seed_meta["sources"] = existing_sources
            else:
                seed_meta = {"sources": ["seeded"]}
            snapshot.append((canonical, seed_meta))
        if not snapshot:
            return 0

        seen: set[str] = set()
        bootstrapped = 0
        now = time.time()

        for key, meta in snapshot:
            meta_dict = dict(meta) if isinstance(meta, Mapping) else {}
            candidates: list[str] = []
            if isinstance(meta, Mapping):
                for field in ("address", "mint"):
                    raw = meta.get(field)
                    if isinstance(raw, str) and raw.strip():
                        candidates.append(raw)
            candidates.append(key)

            canonical = ""
            for raw in candidates:
                try:
                    candidate = canonical_mint(str(raw))
                except Exception:
                    continue
                if not candidate:
                    continue
                if candidate in seen:
                    continue
                canonical = candidate
                break

            if not canonical:
                continue

            seen.add(canonical)
            candidate_sources = _normalize_source_values(meta_dict)
            if not candidate_sources:
                candidate_sources = ("trending",)
            try:
                accepted = await self.pipeline.submit_discovery(
                    DiscoveryCandidate(
                        mint=canonical,
                        asof=now,
                        source=candidate_sources[0] if candidate_sources else None,
                        sources=candidate_sources,
                    )
                )
            except Exception:
                log.exception(
                    "Failed to bootstrap cached trending mint %s", canonical
                )
                continue

            if accepted:
                bootstrapped += 1
                if self._momentum_agent:
                    self._momentum_agent.record_candidate(canonical, ts=now)

        return bootstrapped

    def _on_discovery(self, payload: Any) -> None:
        if not self._running:
            return
        now = time.time()
        root_context = payload if isinstance(payload, Mapping) else None

        def _iter_entries() -> Iterable[tuple[str, Any]]:
            if isinstance(payload, Mapping) and "tokens" in payload:
                tokens = payload.get("tokens") or []
                for entry in tokens:
                    yield entry, payload
                return
            if isinstance(payload, Mapping):
                yield payload, None
                return
            if isinstance(payload, (list, tuple, set)):
                for entry in payload:
                    yield entry, None
                return
            yield payload, None

        for entry, context in _iter_entries():
            raw_mint: Any
            if isinstance(entry, Mapping):
                for key in ("mint", "token", "address"):
                    raw_mint = entry.get(key)
                    if isinstance(raw_mint, str) and raw_mint.strip():
                        break
                else:
                    raw_mint = None
            else:
                raw_mint = entry
            if not raw_mint:
                continue
            try:
                mint = canonical_mint(str(raw_mint))
            except Exception:
                continue
            if not mint:
                continue
            candidate_sources = _normalize_source_values(
                entry,
                context,
                root_context,
                TRENDING_METADATA.get(mint),
            )
            if not candidate_sources and root_context is not None:
                candidate_sources = ("discovery",)
            def _first_value(*keys: str) -> Any:
                for container in (entry, context, root_context):
                    if isinstance(container, Mapping):
                        for key in keys:
                            if key in container:
                                value = container.get(key)
                                if value is not None:
                                    return value
                return None

            ts_candidate = _first_value("ts", "timestamp", "discovered_at")
            asof = _coerce_float(ts_candidate)
            if asof is None:
                asof = now

            hints: Dict[str, Any] = {}
            score_value = _coerce_float(_first_value("score", "confidence"))
            if score_value is not None:
                hints["score"] = score_value
            tx_value = _first_value("tx", "signature")
            if isinstance(tx_value, str) and tx_value:
                hints["tx"] = tx_value
            tags_value = _first_value("tags")
            if isinstance(tags_value, (list, tuple, set)):
                tags = [str(tag) for tag in tags_value if str(tag)]
                if tags:
                    hints["tags"] = tags
            interface_value = _first_value("interface")
            if isinstance(interface_value, str) and interface_value:
                hints["interface"] = interface_value
            discovery_value = _first_value("discovery")
            if isinstance(discovery_value, Mapping):
                hints["discovery"] = dict(discovery_value)
            attributes_value = _first_value("attributes")
            if isinstance(attributes_value, Mapping):
                hints["attributes"] = dict(attributes_value)
            candidate = DiscoveryCandidate(
                mint=mint,
                asof=asof,
                source=candidate_sources[0] if candidate_sources else None,
                sources=candidate_sources,
                hints=hints,
            )
            self._spawn(self.pipeline.submit_discovery(candidate))
            if self._momentum_agent and mint:
                self._momentum_agent.record_candidate(mint, ts=now)

    def _on_price(self, payload: Any) -> None:
        if not self._running or not isinstance(payload, dict):
            return
        token = payload.get("token")
        price = _coerce_float(payload.get("price"))
        if not token or price is None or price <= 0:
            return
        mint = canonical_mint(str(token))
        if getattr(self, "_depth_flag", False):
            self._depth_adapter.record_activity(mint)
        self._last_price[mint] = price
        try:
            self.portfolio.record_prices({mint: float(price)})
        except Exception:
            pass
        event = TapeEvent(
            mint_base=mint,
            mint_quote="USD",
            amount_base=0.0,
            amount_quote=0.0,
            route=str(payload.get("venue") or ""),
            program_id=str(payload.get("venue") or ""),
            pool=str(payload.get("pool") or ""),
            signer="",
            signature="",
            slot=0,
            ts=time.time(),
            fees_base=0.0,
            price_usd=float(price),
            fees_usd=0.0,
            is_self=False,
            buyer=None,
        )
        self._spawn(self.pipeline.submit_market_event(event))

    def _on_depth(self, payload: Any) -> None:
        if not self._running or not isinstance(payload, dict):
            return
        now = time.time()
        for token, entry in payload.items():
            if not isinstance(entry, Mapping):
                continue
            try:
                mint = canonical_mint(str(token))
            except Exception:
                continue
            if getattr(self, "_depth_flag", False):
                self._depth_adapter.record_activity(mint, weight=2.0)
            bids = _coerce_float(entry.get("bids")) or 0.0
            asks = _coerce_float(entry.get("asks")) or 0.0
            depth_val = _coerce_float(entry.get("depth")) or max(bids + asks, 0.0)
            depth_pct_entry = entry.get("depth_pct")
            depth_pct: Dict[str, float]
            if isinstance(depth_pct_entry, Mapping):
                depth_pct = _normalize_depth_pct_map(depth_pct_entry)
            else:
                depth_pct = {}
            if not depth_pct:
                depth_pct = {
                    "1": float(depth_val),
                    "2": float(depth_val * 1.5),
                    "5": float(depth_val * 2.0),
                }
            mid = self._last_price.get(mint) or _coerce_float(entry.get("mid")) or 0.0
            spread_bps = _coerce_float(entry.get("spread_bps"))
            if spread_bps is None:
                spread_bps = 40.0 if bids and asks else 100.0
            snapshot = DepthSnapshot(
                mint=mint,
                venue=str(entry.get("venue") or "depth_service"),
                mid_usd=float(mid) if mid else float(self._last_price.get(mint, 0.0) or 0.0),
                spread_bps=float(spread_bps),
                depth_pct=depth_pct,
                asof=now,
            )
            self._spawn(self.pipeline.submit_depth(snapshot))

    async def _handle_decision(self, decision: Decision) -> None:
        snapshot = self.pipeline.context.get(decision.snapshot_hash)
        mid = float(snapshot.px.get("mid_usd", 0.0)) if snapshot else 0.0
        if mid <= 0:
            mid = float(self._last_price.get(decision.mint, 0.0) or 0.0)
        if mid <= 0:
            log.debug("Dropping decision for %s due to missing mid price", decision.mint)
            return
        size = decision.notional_usd / mid if mid > 0 else 0.0
        payload = {
            "token": decision.mint,
            "side": decision.side,
            "size": float(max(size, 0.0)),
            "price": float(mid),
            "rationale": {
                "agents": list(decision.agents),
                "confidence": float(decision.score),
                "snapshot_hash": decision.snapshot_hash,
            },
        }
        dedupe_key = None
        try:
            seq_value = int(getattr(decision, "sequence", 0))
        except Exception:
            seq_value = 0
        if seq_value > 0:
            dedupe_key = f"action_decision:{decision.mint}:{seq_value}"
        self._event_bus.publish("action_decision", payload, dedupe_key=dedupe_key)

    async def _handle_virtual_fill(self, fill: VirtualFill) -> None:
        self._event_bus.publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"virtual_fill:{fill.mint}:{fill.side}:{fill.price_usd:.4f}",
            },
        )

    async def _handle_virtual_pnl(self, pnl: VirtualPnL) -> None:
        self._event_bus.publish(
            "virtual_pnl",
            {
                "order_id": pnl.order_id,
                "mint": pnl.mint,
                "snapshot_hash": pnl.snapshot_hash,
                "realized_usd": pnl.realized_usd,
                "unrealized_usd": pnl.unrealized_usd,
                "ts": pnl.ts,
            },
        )

    async def _handle_snapshot(self, snapshot: GoldenSnapshot) -> None:
        self._event_bus.publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"snapshot:{snapshot.mint}:{snapshot.hash[:6]}",
            },
        )
        if self._momentum_agent:
            self._momentum_agent.record_snapshot(snapshot)

    async def _handle_suggestion(self, suggestion: TradeSuggestion) -> None:
        self._event_bus.publish(
            "runtime.log",
            {
                "stage": "golden",
                "detail": f"suggestion:{suggestion.agent}:{suggestion.mint}",
            },
        )

    async def _default_enrichment_fetcher(
        self, mints: Iterable[str]
    ) -> Dict[str, TokenSnapshot]:
        snapshots: Dict[str, TokenSnapshot] = {}
        now = time.time()
        for mint in mints:
            canonical = canonical_mint(str(mint))
            meta = TRENDING_METADATA.get(canonical) or {}
            symbol = meta.get("symbol") or meta.get("ticker") or _default_symbol(canonical)
            name = meta.get("name") or symbol
            decimals = _coerce_float(meta.get("decimals"))
            if decimals is None:
                nested = meta.get("metadata")
                if isinstance(nested, dict):
                    decimals = _coerce_float(nested.get("decimals"))
            try:
                decimals_int = int(decimals) if decimals is not None else 6
            except Exception:
                decimals_int = 6
            decimals_int = max(0, min(decimals_int, 12))
            token_program = str(meta.get("token_program") or "Tokenkeg")
            venues = None
            raw_venues = meta.get("venues")
            if isinstance(raw_venues, (list, tuple)):
                venues = [str(v) for v in raw_venues if isinstance(v, str)]
            flags = meta.get("flags") if isinstance(meta.get("flags"), dict) else {}
            snapshots[canonical] = TokenSnapshot(
                mint=canonical,
                symbol=str(symbol),
                name=str(name),
                decimals=decimals_int,
                token_program=token_program,
                venues=venues,
                flags=flags if isinstance(flags, dict) else {},
                asof=now,
            )
        return snapshots


__all__ = [
    "GoldenPipelineService",
    "AgentManagerAgent",
]
