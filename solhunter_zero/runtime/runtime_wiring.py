from __future__ import annotations

import asyncio
import copy
import math
import os
import threading
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..event_bus import subscribe
from ..ui import UIState, get_ws_channel_metrics
from .schema_adapters import read_golden, read_ohlcv
from ..util import parse_bool_env


DEFAULT_RUNTIME_WORKFLOW = "golden-multi-stage-golden-stream"
_WORKFLOW_ENV_CANDIDATES = ("RUNTIME_WORKFLOW", "SOLHUNTER_WORKFLOW", "WORKFLOW")


def _resolve_runtime_workflow(env: Mapping[str, str] | None = None) -> str:
    env_map: Mapping[str, str] = env or os.environ
    for key in _WORKFLOW_ENV_CANDIDATES:
        raw = env_map.get(key)
        if not raw:
            continue
        value = str(raw).strip()
        if value:
            return value
    return DEFAULT_RUNTIME_WORKFLOW


log = __import__("logging").getLogger(__name__)


def _normalize_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"", "auto", "default"}:
            return None
        if lowered in {"0", "false", "off", "no"}:
            return False
        if lowered in {"1", "true", "on", "yes"}:
            return True
    return bool(value)


def resolve_golden_enabled(
    cfg: Mapping[str, Any] | None,
    *,
    env: Mapping[str, str] | None = None,
    default: Optional[bool] = None,
) -> bool:
    """Return whether the Golden pipeline should start for the given config."""

    env_map = env or os.environ
    if "GOLDEN_PIPELINE" in env_map:
        override = parse_bool_env("GOLDEN_PIPELINE", True)
        return bool(override)

    golden_cfg: Mapping[str, Any] | None = None
    if cfg:
        value = cfg.get("golden")
        if isinstance(value, Mapping):
            golden_cfg = value
        legacy_key = cfg.get("golden_pipeline")
        if isinstance(legacy_key, Mapping) and golden_cfg is None:
            golden_cfg = legacy_key
    if golden_cfg:
        explicit = _normalize_bool(golden_cfg.get("enabled"))
        if explicit is not None:
            return explicit

    if cfg:
        for legacy_key in ("use_golden_pipeline", "golden_pipeline_enabled"):
            if legacy_key in cfg:
                flag = _normalize_bool(cfg.get(legacy_key))
                if flag is not None:
                    return flag

    mode: Optional[str] = None
    if cfg:
        raw_mode = cfg.get("mode")
        if isinstance(raw_mode, str):
            mode = raw_mode
    if not mode:
        for key in ("MODE", "RUNTIME_MODE", "TRADING_MODE"):
            value = env_map.get(key)
            if value:
                mode = value
                break
    if mode and mode.lower() in {"live", "paper", "paper_trading", "paper-trading"}:
        return True

    if default is not None:
        return default

    return True


def _int_env(name: str, default: int) -> int:
    try:
        raw = os.getenv(name, str(default))
        if raw is None or raw == "":
            return default
        return max(1, int(raw))
    except Exception:
        return default


def _serialize(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (list, tuple)):
        return [_serialize(v) for v in value]
    if isinstance(value, dict):
        return {str(k): _serialize(v) for k, v in value.items()}
    if hasattr(value, "_asdict"):
        return {str(k): _serialize(v) for k, v in value._asdict().items()}
    if hasattr(value, "__dict__"):
        return {str(k): _serialize(v) for k, v in value.__dict__.items()}
    return str(value)


def _maybe_float(value: Any, default: Optional[float] = None) -> Optional[float]:
    if value in (None, "", "null"):
        return default
    try:
        result = float(value)
    except Exception:
        return default
    if not math.isfinite(result):
        return default
    return result


def _extract_nested_float(
    value: Any, preferred_keys: Iterable[str] = ()
) -> Optional[float]:
    if isinstance(value, Mapping):
        for key in preferred_keys:
            if key in value:
                numeric = _maybe_float(value.get(key))
                if numeric is not None:
                    return numeric
        for candidate in value.values():
            numeric = _extract_nested_float(candidate)
            if numeric is not None:
                return numeric
        return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            numeric = _extract_nested_float(item)
            if numeric is not None:
                return numeric
        return None
    return _maybe_float(value)


def _extract_golden_spread(snapshot: Mapping[str, Any]) -> Optional[float]:
    return _extract_nested_float(
        snapshot.get("px"),
        (
            "spread_bps",
            "spreadBps",
            "spread",
            "spread_pct",
            "spreadPct",
        ),
    )


def _extract_golden_depths(snapshot: Mapping[str, Any]) -> Dict[str, float]:
    liq = snapshot.get("liq")
    depth_source: Any = None
    if isinstance(liq, Mapping):
        depth_source = liq.get("depth_pct") or liq.get("depth")
    result: Dict[str, float] = {}
    if isinstance(depth_source, Mapping):
        for key, raw in depth_source.items():
            numeric = _maybe_float(raw)
            if numeric is None:
                continue
            label = str(key).strip()
            if label.endswith("bps"):
                label = label[: -3]
            label = label.strip("% ")
            if not label:
                continue
            result[label] = numeric
    return result
def _maybe_int(value: Any, default: Optional[int] = None) -> Optional[int]:
    numeric = _maybe_float(value)
    if numeric is None:
        return default
    try:
        return int(numeric)
    except Exception:
        return default


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text)
        except Exception:
            return None
    return None


def _entry_timestamp(entry: Dict[str, Any], field: str) -> Optional[datetime]:
    ts = _parse_timestamp(entry.get(field))
    if ts is not None:
        return ts
    received = entry.get("_received")
    if received is not None:
        try:
            return datetime.fromtimestamp(float(received), tz=timezone.utc)
        except Exception:
            return None
    return None


def _age_seconds(timestamp: Optional[datetime], now: Optional[float] = None) -> Optional[float]:
    if timestamp is None:
        return None
    if now is None:
        now = time.time()
    try:
        return max(0.0, now - timestamp.timestamp())
    except Exception:
        return None


def _format_age(age: Optional[float]) -> str:
    if age is None:
        return "n/a"
    if age < 0.5:
        return "<1s ago"
    total = int(age)
    minutes, seconds = divmod(total, 60)
    hours, minutes = divmod(minutes, 60)
    parts: List[str] = []
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if not parts or seconds:
        parts.append(f"{seconds}s")
    return " ".join(parts) + " ago"


def _format_countdown(seconds: float) -> str:
    if seconds <= 0:
        return "expired"
    total = int(round(seconds))
    minutes, secs = divmod(total, 60)
    if minutes:
        return f"{minutes}m {secs}s"
    return f"{secs}s"


_EXIT_PANEL_KEYS: tuple[str, ...] = (
    "hot_watch",
    "diagnostics",
    "queue",
    "closed",
    "missed_exits",
)


def _sanitize_exit_payload(data: Optional[Mapping[str, Any]]) -> Dict[str, Any]:
    sanitized: Dict[str, Any] = {}
    if isinstance(data, Mapping):
        for key, value in data.items():
            try:
                sanitized[key] = copy.deepcopy(value)
            except Exception:
                sanitized[key] = value
    for key in _EXIT_PANEL_KEYS:
        value = sanitized.get(key)
        if isinstance(value, list):
            try:
                sanitized[key] = copy.deepcopy(value)
            except Exception:
                sanitized[key] = list(value)
        elif value is None:
            sanitized[key] = []
        else:
            sanitized[key] = [value]
    for key in _EXIT_PANEL_KEYS:
        sanitized.setdefault(key, [])
    return sanitized


def _compute_rl_uplift(
    decisions: List[Dict[str, Any]],
    suggestions: List[Dict[str, Any]],
    now: float,
) -> Dict[str, Any]:
    window = 300.0
    decision_scores: List[float] = []
    for decision in decisions:
        ts = _entry_timestamp(decision, "ts")
        age = _age_seconds(ts, now)
        if age is None and decision.get("_received") is not None:
            try:
                age = max(0.0, now - float(decision["_received"]))
            except Exception:
                age = None
        if age is not None and age <= window:
            score = _maybe_float(decision.get("score"))
            if score is not None:
                decision_scores.append(score)
    suggestion_scores: List[float] = []
    for suggestion in suggestions:
        ts = _entry_timestamp(suggestion, "asof")
        age = _age_seconds(ts, now)
        if age is None and suggestion.get("_received") is not None:
            try:
                age = max(0.0, now - float(suggestion["_received"]))
            except Exception:
                age = None
        if age is not None and age <= window:
            edge = _maybe_float(suggestion.get("edge"))
            if edge is not None:
                suggestion_scores.append(edge)
    rl_avg = sum(decision_scores) / len(decision_scores) if decision_scores else 0.0
    plain_avg = (
        sum(suggestion_scores) / len(suggestion_scores)
        if suggestion_scores
        else 0.0
    )
    rolling = rl_avg - plain_avg if decision_scores and suggestion_scores else 0.0
    last_delta = 0.0
    if decisions:
        last_decision = decisions[0]
        score_val = _maybe_float(last_decision.get("score"))
        if score_val is not None:
            related = [
                _maybe_float(suggestion.get("edge"))
                for suggestion in suggestions
                if suggestion.get("mint") == last_decision.get("mint")
                and (
                    last_decision.get("snapshot_hash") is None
                    or suggestion.get("inputs_hash")
                    == last_decision.get("snapshot_hash")
                )
            ]
            related = [value for value in related if value is not None]
            if related:
                last_delta = score_val - (sum(related) / len(related))
    uplift_pct = 0.0
    if plain_avg:
        uplift_pct = (rl_avg - plain_avg) / abs(plain_avg) * 100.0
    return {
        "rolling_5m": rolling,
        "last_decision_delta": last_delta,
        "score_rl": rl_avg,
        "score_plain": plain_avg,
        "uplift_pct": uplift_pct,
    }


def _short_hash(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    text = str(value)
    if len(text) <= 10:
        return text
    return f"{text[:6]}…{text[-4:]}"


def _format_ttl(remaining: Optional[float], original: Optional[float]) -> str:
    if remaining is None:
        if original is None:
            return "n/a"
        try:
            base = float(original)
        except Exception:
            return "n/a"
        if base <= 0:
            return "expired"
        return f"{int(base)}s"
    if remaining <= 0:
        return "expired"
    return _format_countdown(remaining)


class RuntimeEventCollectors:
    """Mirror TradingRuntime's event subscriptions for UI hydration."""

    def __init__(self) -> None:
        self._swarm_lock = threading.Lock()
        self._discovery_lock = threading.Lock()
        self._recent_tokens: Deque[str] = deque()
        self._recent_tokens_limit = int(os.getenv("UI_DISCOVERY_LIMIT", "200") or 200)
        self._discovery_seen: set[str] = set()
        self._discovery_candidates: Deque[Dict[str, Any]] = deque(maxlen=500)
        self._token_facts: Dict[str, Dict[str, Any]] = {}
        self._market_ohlcv: Dict[str, Dict[str, Any]] = {}
        self._market_depth: Dict[str, Dict[str, Any]] = {}
        self._golden_snapshots: Dict[str, Dict[str, Any]] = {}
        self._latest_golden_hash: Dict[str, str] = {}
        self._last_golden_update: Optional[float] = None
        self._agent_suggestions: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_SUGGESTIONS_LIMIT", 600)
        )
        self._vote_decisions: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_VOTE_LIMIT", 400)
        )
        self._decision_counts: Dict[str, int] = {}
        self._decision_recent: Deque[Tuple[str, float]] = deque()
        self._decision_first_seen: Dict[str, float] = {}
        self._mint_sequences: Dict[str, int] = {}
        fills_limit = _int_env("UI_FILLS_LIMIT", 400)
        self._virtual_fills: Deque[Dict[str, Any]] = deque(maxlen=fills_limit)
        self._live_fills: Deque[Dict[str, Any]] = deque(maxlen=fills_limit)
        self._subscriptions: List[Callable[[], None]] = []
        self._exit_summary: Dict[str, Any] = _sanitize_exit_payload(None)
        self._exit_lock = threading.Lock()
        self._rl_weights_windows: Deque[Dict[str, Any]] = deque(
            maxlen=_int_env("UI_RL_WEIGHTS_LIMIT", 240)
        )
        self._rl_status_info: Dict[str, Any] = {
            "vote_window_ms": None,
            "updated_at": None,
        }
        pnl_limit = _int_env("UI_VIRTUAL_PNL_LIMIT", 400)
        self._virtual_pnls: Deque[Dict[str, Any]] = deque(maxlen=pnl_limit)
        self._status_lock = threading.Lock()
        self._status_info: Dict[str, Any] = {
            "event_bus": False,
            "trading_loop": False,
            "heartbeat_ts": None,
            "last_stage": None,
            "last_stage_ok": None,
            "last_stage_detail": None,
            "last_stage_ts": None,
        }
        self._environment = (
            os.getenv("SOLHUNTER_ENV")
            or os.getenv("DEPLOY_ENV")
            or os.getenv("RUNTIME_ENV")
            or os.getenv("APP_ENV")
            or os.getenv("ENVIRONMENT")
            or os.getenv("ENV")
        )
        self._workflow = _resolve_runtime_workflow()
        self._control_status: Dict[str, Any] = {
            "paused": False,
            "paper_mode": parse_bool_env("PAPER_TRADING", False),
            "rl_mode": "shadow" if parse_bool_env("RL_WEIGHTS_DISABLED", True) else "applied",
        }

    def _parse_sequence(self, value: Any) -> Optional[int]:
        if value in (None, "", False):
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _accept_sequence(self, mint: str, sequence: Optional[int]) -> bool:
        if sequence is None:
            return True
        last = self._mint_sequences.get(mint)
        if last is not None and sequence <= last:
            return False
        self._mint_sequences[mint] = sequence
        return True

    def start(self) -> None:
        def _normalize_event(event: Any) -> Dict[str, Any]:
            payload = getattr(event, "payload", event)
            data = _serialize(payload)
            if isinstance(data, dict):
                return dict(data)
            return {"value": data}

        async def _on_discovery_candidate(event: Any) -> None:
            payload = _normalize_event(event)
            tokens = payload.get("tokens") or payload.get("recent")
            if isinstance(tokens, dict):
                tokens = tokens.get("mints") or tokens.get("tokens")
            if not tokens and "mint" in payload:
                tokens = [payload.get("mint")]
            if not tokens:
                return
            now = time.time()
            parsed: List[str] = []
            for raw in tokens:
                if raw is None:
                    continue
                mint = str(raw).strip()
                if not mint:
                    continue
                parsed.append(mint)
                with self._swarm_lock:
                    self._token_facts.setdefault(mint, {"seen": now})
            if parsed:
                self._record_discovery(parsed)
                entry = {"tokens": parsed, "asof": now}
                with self._discovery_lock:
                    self._discovery_candidates.appendleft(entry)

        async def _on_token_snapshot(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            self._record_discovery([mint])
            with self._swarm_lock:
                self._token_facts[str(mint)] = payload

        async def _on_market_ohlcv(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint") or payload.get("token")
            if not mint:
                return
            payload["_received"] = time.time()
            read_ohlcv(payload, reader="runtime_wiring")
            with self._swarm_lock:
                self._market_ohlcv[str(mint)] = payload

        async def _on_market_depth(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint") or payload.get("token")
            if not mint:
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._market_depth[str(mint)] = payload

        async def _on_golden_snapshot(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            read_golden(payload, reader="runtime_wiring")
            hash_value = payload.get("hash")
            with self._swarm_lock:
                self._golden_snapshots[str(mint)] = payload
                if hash_value:
                    self._latest_golden_hash[str(mint)] = str(hash_value)
                self._last_golden_update = time.time()

        async def _on_suggestion(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            sequence = self._parse_sequence(payload.get("sequence"))
            mint_key = str(mint)
            with self._swarm_lock:
                if not self._accept_sequence(mint_key, sequence):
                    return
                self._agent_suggestions.appendleft(payload)

        async def _on_vote_decision(event: Any) -> None:
            payload = _normalize_event(event)
            mint = payload.get("mint")
            if not mint:
                return
            payload["_received"] = time.time()
            sequence = self._parse_sequence(payload.get("sequence"))
            client_id = payload.get("clientOrderId") or payload.get("client_order_id")
            mint_key = str(mint)
            with self._swarm_lock:
                if not self._accept_sequence(mint_key, sequence):
                    return
                if client_id:
                    now_ts = time.time()
                    client_id_str = str(client_id)
                    self._decision_recent.append((client_id_str, now_ts))
                    cutoff = now_ts - 300.0
                    while self._decision_recent and self._decision_recent[0][1] < cutoff:
                        old_id, _old_ts = self._decision_recent.popleft()
                        current = self._decision_counts.get(old_id, 0)
                        if current <= 1:
                            self._decision_counts.pop(old_id, None)
                            self._decision_first_seen.pop(old_id, None)
                        else:
                            self._decision_counts[old_id] = current - 1
                    count = self._decision_counts.get(client_id_str, 0) + 1
                    self._decision_counts[client_id_str] = count
                    self._decision_first_seen.setdefault(client_id_str, now_ts)
                    payload["_duplicate_count"] = count
                    payload["_idempotent"] = count <= 1
                    payload["_first_seen"] = self._decision_first_seen.get(client_id_str)
                self._vote_decisions.appendleft(payload)

        async def _on_virtual_fill(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload.get("mint"):
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._virtual_fills.appendleft(payload)

        async def _on_live_fill(event: Any) -> None:
            payload = _normalize_event(event)
            if not payload.get("mint"):
                return
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._live_fills.appendleft(payload)

        async def _on_virtual_pnl(event: Any) -> None:
            payload = _normalize_event(event)
            payload["_received"] = time.time()
            with self._swarm_lock:
                self._virtual_pnls.appendleft(payload)

        async def _on_rl_weights(event: Any) -> None:
            payload = _normalize_event(event)
            payload["_received"] = time.time()
            vote_window = payload.get("vote_window_ms") or payload.get("vote_window")
            with self._swarm_lock:
                self._rl_weights_windows.appendleft(payload)
                if vote_window is not None:
                    try:
                        window_ms = float(vote_window)
                    except Exception:
                        window_ms = None
                    else:
                        self._rl_status_info["vote_window_ms"] = window_ms
                self._rl_status_info["updated_at"] = time.time()

        async def _on_exit_panel(event: Any) -> None:
            payload = _normalize_event(event)
            if not isinstance(payload, dict):
                payload = {"hot_watch": payload}
            self._set_exit_summary(payload)

        async def _on_stage(event: Any) -> None:
            payload = _normalize_event(event)
            stage = str(payload.get("stage") or "")
            ok = bool(payload.get("ok"))
            detail = payload.get("detail")
            now = time.time()
            with self._status_lock:
                info = self._status_info
                info["last_stage"] = stage or None
                info["last_stage_ok"] = ok
                info["last_stage_detail"] = detail
                info["last_stage_ts"] = now
                if stage.startswith("bus:"):
                    if ok:
                        info["event_bus"] = True
                    elif stage in {"bus:ws", "bus:verify"}:
                        info["event_bus"] = False
                if stage in {"agents:loop", "agents:event_runtime", "runtime:ready"}:
                    if ok:
                        info["trading_loop"] = True
                if stage in {"runtime:stopping", "runtime:stopped"}:
                    info["trading_loop"] = False
                if stage in {"agents:loop", "agents:event_runtime"} and not ok:
                    info["trading_loop"] = False

        async def _on_heartbeat(event: Any) -> None:
            payload = _normalize_event(event)
            service = str(payload.get("service") or "")
            now = time.time()
            with self._status_lock:
                info = self._status_info
                info["heartbeat_ts"] = now
                if service == "trading_loop":
                    info["trading_loop"] = True

        for topic, handler in (
            ("x:discovery.candidates", _on_discovery_candidate),
            ("x:token.snap", _on_token_snapshot),
            ("x:market.ohlcv.5m", _on_market_ohlcv),
            ("x:market.depth", _on_market_depth),
            ("x:mint.golden", _on_golden_snapshot),
            ("x:trade.suggested", _on_suggestion),
            ("x:vote.decisions", _on_vote_decision),
            ("x:virt.fills", _on_virtual_fill),
            ("x:live.fills", _on_live_fill),
            ("virtual_pnl", _on_virtual_pnl),
            ("rl:weights.applied", _on_rl_weights),
            ("rl_weights", _on_rl_weights),
            ("x:swarm.exits", _on_exit_panel),
            ("x:exit.panel", _on_exit_panel),
            ("runtime.stage_changed", _on_stage),
            ("heartbeat", _on_heartbeat),
        ):
            unsub = subscribe(topic, handler)
            self._subscriptions.append(unsub)

    def stop(self) -> None:
        for unsub in self._subscriptions:
            try:
                unsub()
            except Exception:
                pass
        self._subscriptions.clear()

    # ------------------------------------------------------------------
    # Provider snapshots
    # ------------------------------------------------------------------

    def _record_discovery(self, tokens: Iterable[str]) -> None:
        with self._discovery_lock:
            for token in tokens:
                if token is None:
                    continue
                token_str = str(token).strip()
                if not token_str or token_str in self._discovery_seen:
                    continue
                self._recent_tokens.appendleft(token_str)
                self._discovery_seen.add(token_str)
                while len(self._recent_tokens) > self._recent_tokens_limit:
                    removed = self._recent_tokens.pop()
                    self._discovery_seen.discard(removed)

    def _set_exit_summary(self, payload: Optional[Mapping[str, Any]]) -> None:
        sanitized = _sanitize_exit_payload(payload)
        with self._exit_lock:
            self._exit_summary = sanitized

    def exit_snapshot(self) -> Dict[str, Any]:
        with self._exit_lock:
            return copy.deepcopy(self._exit_summary)

    def token_facts_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            facts = dict(self._token_facts)
        ordered: Dict[str, Dict[str, Any]] = {}
        for mint in sorted(facts.keys()):
            payload = dict(facts[mint] or {})
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            missing_meta = not payload.get("symbol") or not payload.get("name")
            stale = (age is not None and age > 300.0) or missing_meta
            ordered[mint] = {
                "symbol": payload.get("symbol"),
                "name": payload.get("name"),
                "decimals": payload.get("decimals"),
                "token_program": payload.get("token_program"),
                "flags": payload.get("flags") or [],
                "venues": payload.get("venues") or [],
                "asof": payload.get("asof"),
                "age_seconds": age,
                "age_label": _format_age(age),
                "stale": stale,
            }
        return {"tokens": ordered, "selected": None}

    def rl_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            weights = list(self._rl_weights_windows)
            decisions = list(self._vote_decisions)
            suggestions = list(self._agent_suggestions)
        entries: List[Dict[str, Any]] = []
        for payload in weights:
            mint = payload.get("mint")
            if not mint:
                continue
            ts = _entry_timestamp(payload, "asof")
            if ts is None:
                ts = _entry_timestamp(payload, "timestamp")
            age = _age_seconds(ts, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            multipliers = (
                payload.get("multipliers")
                or payload.get("weights")
                or payload.get("agents")
            )
            if isinstance(multipliers, Mapping):
                parts: List[str] = []
                multiplier_map: Dict[str, Optional[float]] = {}
                for key, value in multipliers.items():
                    val = _maybe_float(value)
                    multiplier_map[str(key)] = val
                    if val is not None and len(parts) < 3:
                        parts.append(f"{key}:{val:.2f}")
                summary = ", ".join(parts)
                if len(multipliers) > 3:
                    summary = summary + " …" if summary else "…"
            else:
                val = _maybe_float(payload.get("multiplier"))
                multiplier_map = {"value": val} if val is not None else {}
                summary = f"{val:.2f}" if val is not None else "n/a"
            entries.append(
                {
                    "mint": mint,
                    "window_hash": payload.get("window_hash") or payload.get("hash"),
                    "window_hash_short": _short_hash(
                        payload.get("window_hash") or payload.get("hash")
                    ),
                    "multiplier": summary,
                    "age_label": _format_age(age),
                    "stale": age is not None and age > 600.0,
                    "age_seconds": age,
                    "multipliers": multiplier_map,
                }
            )
        uplift = _compute_rl_uplift(decisions, suggestions, now)
        return {"weights": entries[:120], "uplift": uplift}

    def rl_status_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            updated_at = self._rl_status_info.get("updated_at")
            vote_window_ms = self._rl_status_info.get("vote_window_ms")
            weights_count = len(self._rl_weights_windows)
        age = None
        if updated_at is not None:
            try:
                age = max(0.0, now - float(updated_at))
            except Exception:
                age = None
        return {
            "weights_applied": weights_count,
            "vote_window_ms": vote_window_ms,
            "updated_at": updated_at,
            "age_label": _format_age(age),
        }

    def _max_age_ms(
        self,
        entries: Iterable[Mapping[str, Any]],
        *,
        fields: Sequence[str],
        now: float,
    ) -> Optional[float]:
        max_age: Optional[float] = None
        for entry in entries:
            timestamp = None
            for field in fields:
                timestamp = _entry_timestamp(entry, field)
                if timestamp is not None:
                    break
            age = _age_seconds(timestamp, now)
            if age is None and entry.get("_received") is not None:
                try:
                    age = max(0.0, now - float(entry["_received"]))
                except Exception:
                    age = None
            if age is not None:
                if max_age is None or age > max_age:
                    max_age = age
        if max_age is None:
            return None
        return max_age * 1000.0

    def lag_snapshot(self) -> Dict[str, Optional[float]]:
        now = time.time()
        with self._swarm_lock:
            depth_entries = list(self._market_depth.values())
            ohlcv_entries = list(self._market_ohlcv.values())
            golden_entries = list(self._golden_snapshots.values())
            suggestion_entries = list(self._agent_suggestions)
            decision_entries = list(self._vote_decisions)
        depth_ms = self._max_age_ms(depth_entries, fields=("asof", "ts"), now=now)
        ohlcv_ms = self._max_age_ms(
            ohlcv_entries,
            fields=("asof_close", "asof", "ts"),
            now=now,
        )
        golden_ms = self._max_age_ms(golden_entries, fields=("asof", "ts"), now=now)
        suggestion_ms = self._max_age_ms(
            suggestion_entries,
            fields=("asof", "timestamp", "ts"),
            now=now,
        )
        decision_ms = self._max_age_ms(decision_entries, fields=("ts",), now=now)
        candidates = [
            value
            for value in (
                depth_ms,
                ohlcv_ms,
                golden_ms,
                suggestion_ms,
                decision_ms,
            )
            if value is not None
        ]
        bus_ms = max(candidates) if candidates else None
        return {
            "bus_ms": bus_ms,
            "depth_ms": depth_ms,
            "ohlcv_ms": ohlcv_ms,
            "golden_ms": golden_ms,
            "suggestion_ms": suggestion_ms,
            "decision_ms": decision_ms,
        }

    def summary_snapshot(self) -> Dict[str, Any]:
        suggestions = self.suggestions_snapshot()
        votes = self.vote_snapshot()
        golden = self.golden_snapshot()
        with self._swarm_lock:
            virtual_fills = list(self._virtual_fills)
            virtual_pnls = list(self._virtual_pnls)
        turnover = 0.0
        for fill in virtual_fills:
            qty = _maybe_float(fill.get("qty_base")) or 0.0
            price = _maybe_float(fill.get("price_usd")) or 0.0
            turnover += abs(qty * price)
        realized = 0.0
        latest_unrealized = None
        if virtual_pnls:
            latest = virtual_pnls[0]
            latest_unrealized = _maybe_float(latest.get("unrealized_usd"))
        for entry in virtual_pnls:
            realized += _maybe_float(entry.get("realized_usd")) or 0.0
        pnl_1d = realized + (latest_unrealized or 0.0)
        evaluation = {
            "suggestions_5m": suggestions.get("metrics", {}).get("rate_per_min", 0.0)
            * 5.0,
            "acceptance_rate": suggestions.get("metrics", {}).get("acceptance_rate"),
            "open_vote_windows": len(votes.get("windows", [])),
        }
        execution = {
            "turnover": turnover or 0.0,
            "pnl_1d": pnl_1d,
            "drawdown": None,
            "latest_unrealized": latest_unrealized,
            "count": len(virtual_fills),
        }
        paper_summary = {
            "count": len(virtual_pnls),
            "latest_unrealized": latest_unrealized,
            "turnover_usd": turnover,
        }
        lag_snapshot = self.lag_snapshot()
        websocket_metrics = get_ws_channel_metrics()
        seeded_summary: Dict[str, Any] = {
            "count": 0,
            "mapped": 0,
            "missing": [],
            "tokens": [],
        }
        try:
            from .. import seed_token_publisher
        except Exception as exc:  # pragma: no cover - defensive import guard
            seeded_summary["error"] = str(exc)
        else:
            try:
                seed_tokens = seed_token_publisher.configured_seed_tokens()
            except Exception as exc:
                seeded_summary["error"] = str(exc)
                seed_tokens = tuple()
            if seed_tokens:
                try:
                    metadata = seed_token_publisher.build_seeded_token_metadata(seed_tokens)
                except Exception as exc:
                    seeded_summary["error"] = str(exc)
                else:
                    entries: List[Dict[str, Any]] = []
                    missing: List[str] = []
                    mapped = 0
                    for token in seed_tokens:
                        info = metadata.get(token, {})
                        status = str(info.get("status") or "missing")
                        entry = {
                            "mint": token,
                            "canonical_mint": info.get("canonical_mint", token),
                            "status": status,
                            "pyth_feed_id": info.get("feed_id"),
                            "pyth_account": info.get("account"),
                            "pyth_kind": info.get("kind"),
                        }
                        entries.append(entry)
                        if status == "available":
                            mapped += 1
                        else:
                            missing.append(token)
                    seeded_summary.update(
                        {
                            "count": len(seed_tokens),
                            "mapped": mapped,
                            "tokens": entries,
                            "missing": missing,
                        }
                    )
        return {
            "evaluation": evaluation,
            "execution": execution,
            "paper_pnl": paper_summary,
            "golden": {
                "count": len(golden.get("snapshots", [])),
                "lag_ms": golden.get("lag_ms"),
            },
            "lag": lag_snapshot,
            "backpressure": {"websockets": websocket_metrics},
            "seeded": seeded_summary,
        }

    def settings_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            golden_ts = self._last_golden_update
            suggestion_ts = (
                self._agent_suggestions[0].get("_received")
                if self._agent_suggestions
                else None
            )
            decision_ts = (
                self._vote_decisions[0].get("_received")
                if self._vote_decisions
                else None
            )
            rl_updated = self._rl_status_info.get("updated_at")
            vote_window_ms = self._rl_status_info.get("vote_window_ms")

        def _control_entry(
            label: str,
            endpoint: str,
            timestamp: Optional[float],
            warn_seconds: Optional[float],
        ) -> Dict[str, Any]:
            age = None
            if timestamp is not None:
                try:
                    age = max(0.0, now - float(timestamp))
                except Exception:
                    age = None
            if age is None:
                state = "idle"
            elif warn_seconds is not None and age > warn_seconds:
                state = "stale"
            else:
                state = "active"
            remaining = None
            if warn_seconds is not None and age is not None:
                remaining = max(warn_seconds - age, 0.0)
            return {
                "label": label,
                "endpoint": endpoint,
                "state": state,
                "ttl_label": _format_ttl(remaining, warn_seconds),
                "age_label": _format_age(age),
            }

        controls = [
            _control_entry("Golden Stream", "x:mint.golden", golden_ts, 60.0),
            _control_entry("Suggestions Stream", "x:trade.suggested", suggestion_ts, 15.0),
            _control_entry("Vote Decisions", "x:vote.decisions", decision_ts, 30.0),
        ]
        if rl_updated is not None:
            warn_window = None
            if vote_window_ms is not None:
                try:
                    warn_window = max(float(vote_window_ms) / 1000.0 * 2.0, 30.0)
                except Exception:
                    warn_window = 120.0
            controls.append(
                _control_entry("RL Vote Window", "rl:weights.applied", rl_updated, warn_window)
            )
        staleness = {
            "golden_age_s": None
            if golden_ts is None
            else max(0.0, now - float(golden_ts)),
            "suggestions_age_s": None
            if suggestion_ts is None
            else max(0.0, now - float(suggestion_ts)),
            "votes_age_s": None
            if decision_ts is None
            else max(0.0, now - float(decision_ts)),
        }
        return {
            "controls": controls,
            "overrides": {"vote_window_ms": vote_window_ms},
            "staleness": staleness,
        }

    def status_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._status_lock:
            info = dict(self._status_info)
            control = dict(self._control_status)
        workflow = _resolve_runtime_workflow()
        if workflow != self._workflow:
            self._workflow = workflow
        heartbeat_ts = info.get("heartbeat_ts")
        latency_ms: Optional[float] = None
        if heartbeat_ts is not None:
            try:
                latency_ms = max(0.0, (now - float(heartbeat_ts)) * 1000.0)
            except Exception:
                latency_ms = None
        return {
            "event_bus": bool(info.get("event_bus")),
            "trading_loop": bool(info.get("trading_loop")),
            "heartbeat": heartbeat_ts,
            "bus_latency_ms": latency_ms,
            "last_stage": info.get("last_stage"),
            "last_stage_ok": info.get("last_stage_ok"),
            "last_stage_detail": info.get("last_stage_detail"),
            "last_stage_ts": info.get("last_stage_ts"),
            "environment": (self._environment or "dev"),
            "workflow": workflow,
            "paused": bool(control.get("paused")),
            "paper_mode": bool(control.get("paper_mode")),
            "rl_mode": control.get("rl_mode", "shadow"),
        }

    # Public provider accessors ------------------------------------------------

    def discovery_snapshot(self) -> Dict[str, Any]:
        with self._discovery_lock:
            recent = list(self._recent_tokens)
        return {
            "recent": recent[:50],
            "recent_count": len(recent),
        }

    def discovery_console_snapshot(self) -> Dict[str, Any]:
        with self._discovery_lock:
            candidates = list(self._discovery_candidates)
        return {"candidates": candidates, "stats": {}}

    def market_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            ohlcv = dict(self._market_ohlcv)
            depth = dict(self._market_depth)
        markets: List[Dict[str, Any]] = []
        ohlcv_lags: List[float] = []
        depth_lags: List[float] = []
        for mint in sorted(set(ohlcv) | set(depth)):
            candle = dict(ohlcv.get(mint) or {})
            depth_entry = dict(depth.get(mint) or {})
            ts_close = _entry_timestamp(candle, "asof_close")
            ts_depth = _entry_timestamp(depth_entry, "asof")
            age_close = _age_seconds(ts_close, now)
            age_depth = _age_seconds(ts_depth, now)
            if age_close is None and candle.get("_received") is not None:
                try:
                    age_close = max(0.0, now - float(candle["_received"]))
                except Exception:
                    age_close = None
            if age_depth is None and depth_entry.get("_received") is not None:
                try:
                    age_depth = max(0.0, now - float(depth_entry["_received"]))
                except Exception:
                    age_depth = None
            depth_pct_raw = depth_entry.get("depth_pct") or depth_entry.get("depth") or {}
            depth_pct: Dict[str, Optional[float]] = {}
            if isinstance(depth_pct_raw, dict):
                for key, value in depth_pct_raw.items():
                    depth_pct[str(key).strip("% ")] = _maybe_float(value)
            depth_pct = {k: v for k, v in depth_pct.items() if v is not None}
            combined_age = None
            for value in (age_close, age_depth):
                if value is None:
                    continue
                if combined_age is None or value < combined_age:
                    combined_age = value
            stale = False
            if age_close is not None:
                ohlcv_lags.append(age_close * 1000.0)
            if age_depth is not None:
                depth_lags.append(age_depth * 1000.0)
            if age_close is not None and age_close > 120.0:
                stale = True
            if age_depth is not None and age_depth > 6.0:
                stale = True
            normalized_candle = read_ohlcv(candle, reader="runtime_wiring")
            close_value = normalized_candle.get("close")
            volume_value = normalized_candle.get("volume_usd")
            volume_base_value = normalized_candle.get("volume_base")
            buyers_value = _maybe_int(depth_entry.get("buyers"))
            if buyers_value is None:
                buyers_value = _maybe_int(depth_entry.get("buyer_count"))
            if buyers_value is None:
                buyers_value = _maybe_int(depth_entry.get("num_buyers"))
            if buyers_value is None:
                normalized_buyers = normalized_candle.get("buyers")
                if isinstance(normalized_buyers, (int, float)):
                    buyers_value = _maybe_int(normalized_buyers)
                else:
                    buyers_value = normalized_buyers
            sellers_value = _maybe_int(depth_entry.get("sellers"))
            if sellers_value is None:
                sellers_value = _maybe_int(depth_entry.get("seller_count"))
            if sellers_value is None:
                sellers_value = _maybe_int(depth_entry.get("num_sellers"))
            if sellers_value is None:
                normalized_sellers = normalized_candle.get("sellers")
                if isinstance(normalized_sellers, (int, float)):
                    sellers_value = _maybe_int(normalized_sellers)
                else:
                    sellers_value = normalized_sellers
            markets.append(
                {
                    "mint": mint,
                    "close": close_value,
                    "volume": volume_value,
                    "spread_bps": _maybe_float(depth_entry.get("spread_bps")),
                    "depth_pct": depth_pct,
                    "age_close": age_close,
                    "age_depth": age_depth,
                    "lag_close_ms": age_close * 1000.0 if age_close is not None else None,
                    "lag_depth_ms": age_depth * 1000.0 if age_depth is not None else None,
                    "stale": stale,
                    "updated_label": _format_age(combined_age),
                    "buyers": buyers_value,
                    "sellers": sellers_value,
                    "volume_base": volume_base_value,
                }
            )
        summary = {
            "ohlcv_ms": max(ohlcv_lags) if ohlcv_lags else None,
            "depth_ms": max(depth_lags) if depth_lags else None,
        }
        return {"markets": markets, "updated_at": None, "lag_ms": summary}

    def golden_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            golden = dict(self._golden_snapshots)
            hash_map = dict(self._latest_golden_hash)
        snapshots: List[Dict[str, Any]] = []
        lag_samples: List[float] = []

        def _normalize_momentum_breakdown(value: Any) -> Any:
            if isinstance(value, bool) or value is None:
                return value
            if isinstance(value, Mapping):
                return {
                    str(key): _normalize_momentum_breakdown(val)
                    for key, val in value.items()
                }
            if isinstance(value, (list, tuple, set)):
                return [_normalize_momentum_breakdown(val) for val in value]
            numeric = _maybe_float(value)
            if numeric is not None:
                return numeric
            return value

        def _as_bool(value: Any) -> bool:
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                return value.strip().lower() in {"1", "true", "yes", "on", "enabled"}
            if isinstance(value, (int, float)):
                return bool(value)
            return False

        for mint in sorted(golden.keys()):
            payload = dict(golden[mint])
            normalized_golden = read_golden(payload, reader="runtime_wiring")
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            hash_value = payload.get("hash")
            hash_text = str(hash_value) if hash_value is not None else None
            if age is not None:
                lag_samples.append(age * 1000.0)
            coalesce = payload.get("coalesce_window_s")
            if coalesce is None:
                coalesce = payload.get("coalesce_s")
            if coalesce is None:
                window_ms = payload.get("coalesce_ms")
                if window_ms is not None:
                    try:
                        coalesce = float(window_ms) / 1000.0
                    except Exception:
                        coalesce = None
            if coalesce is None:
                try:
                    coalesce = float(payload.get("coalesce"))
                except Exception:
                    coalesce = None
            stale_threshold = None
            if coalesce is not None:
                try:
                    stale_threshold = max(0.0, float(coalesce) * 2.0 + 5.0)
                except Exception:
                    stale_threshold = None
                else:
                    stale_threshold = min(600.0, max(5.0, stale_threshold))
            stale_flag = False
            if age is not None:
                if stale_threshold is not None:
                    stale_flag = age > stale_threshold
                else:
                    stale_flag = age > 60.0
            momentum_score = _maybe_float(payload.get("momentum_score"))
            pump_intensity = _maybe_float(payload.get("pump_intensity"))
            pump_score = _maybe_float(payload.get("pump_score"))
            if pump_intensity is None and pump_score is not None:
                pump_intensity = pump_score
            social_score = _maybe_float(payload.get("social_score"))
            social_sentiment = _maybe_float(payload.get("social_sentiment"))
            tweets_per_min = _maybe_float(payload.get("tweets_per_min"))
            buyers_last_hour = _maybe_int(payload.get("buyers_last_hour"))
            momentum_latency = _maybe_float(payload.get("momentum_latency_ms"))
            raw_sources = payload.get("momentum_sources")
            if isinstance(raw_sources, (list, tuple, set)):
                candidate_sources = list(raw_sources)
            elif isinstance(raw_sources, str):
                candidate_sources = [raw_sources]
            else:
                candidate_sources = []
            momentum_sources: List[str] = []
            seen_sources: set[str] = set()
            for source in candidate_sources:
                if source is None:
                    continue
                source_text = str(source).strip()
                if not source_text or source_text in seen_sources:
                    continue
                seen_sources.add(source_text)
                momentum_sources.append(source_text)
            raw_breakdown = payload.get("momentum_breakdown")
            momentum_breakdown: Dict[str, Any] = {}
            if isinstance(raw_breakdown, Mapping):
                momentum_breakdown = {
                    str(key): _normalize_momentum_breakdown(value)
                    for key, value in raw_breakdown.items()
                }
            momentum_stale = _as_bool(payload.get("momentum_stale"))
            momentum_partial = _as_bool(payload.get("momentum_partial"))
            px_mid = normalized_golden.get("mid_usd")
            spread_bps = _extract_golden_spread(payload)
            px_payload = payload.get("px")
            px_detail = _serialize(px_payload) if isinstance(px_payload, Mapping) else None
            liq_payload = payload.get("liq")
            liq_detail = _serialize(liq_payload) if isinstance(liq_payload, Mapping) else None
            px_bid = _maybe_float(payload.get("px_bid_usd"))
            px_ask = _maybe_float(payload.get("px_ask_usd"))
            liq_total_usd = None
            if isinstance(liq_payload, Mapping):
                liq_total_usd = _extract_nested_float(
                    liq_payload,
                    (
                        "liquidity_usd",
                        "usd_total",
                        "usd",
                        "notional_usd",
                        "notional",
                    ),
                )
            if liq_total_usd is None:
                liq_total_usd = _maybe_float(liq_payload)
            primary_liq = normalized_golden.get("depth_1pct_usd")
            if primary_liq is None:
                primary_liq = liq_total_usd
            depth_pct = _extract_golden_depths(payload)
            depth_0_1 = _maybe_float(payload.get("liq_depth_0_1pct_usd"))
            depth_0_5 = _maybe_float(payload.get("liq_depth_0_5pct_usd"))
            depth_1_0 = _maybe_float(payload.get("liq_depth_1_0pct_usd"))
            degraded_flag = bool(payload.get("degraded"))
            if not degraded_flag and isinstance(liq_payload, Mapping):
                degraded_flag = bool(liq_payload.get("degraded"))
            source_label = payload.get("source")
            if not source_label and isinstance(liq_payload, Mapping):
                source_label = liq_payload.get("source")
            staleness_ms = _maybe_float(payload.get("staleness_ms"))
            if staleness_ms is None and isinstance(liq_payload, Mapping):
                staleness_ms = _maybe_float(liq_payload.get("staleness_ms"))
            route_meta = None
            if isinstance(liq_payload, Mapping):
                raw_meta = liq_payload.get("route_meta")
                if isinstance(raw_meta, Mapping):
                    hops_value = _maybe_float(raw_meta.get("hops"))
                    route_meta = {
                        "hops": int(hops_value or 0),
                        "dexes": [str(item) for item in raw_meta.get("dexes") or []],
                    }
                    latency_value = _maybe_float(raw_meta.get("latency_ms"))
                    if latency_value is not None:
                        route_meta["latency_ms"] = latency_value
                    sweeps_value = raw_meta.get("sweeps")
                    if isinstance(sweeps_value, list):
                        sweeps: list[Dict[str, Any]] = []
                        for sweep in sweeps_value:
                            if not isinstance(sweep, Mapping):
                                continue
                            entry: Dict[str, Any] = {}
                            direction = sweep.get("direction")
                            if isinstance(direction, str):
                                entry["direction"] = direction
                            usd_val = _maybe_float(sweep.get("usd"))
                            if usd_val is not None:
                                entry["usd"] = usd_val
                            impact_val = _maybe_float(sweep.get("impact_bps"))
                            if impact_val is not None:
                                entry["impact_bps"] = impact_val
                            if entry:
                                sweeps.append(entry)
                        if sweeps:
                            route_meta["sweeps"] = sweeps
            snapshots.append(
                {
                    "mint": mint,
                    "hash": hash_text,
                    "hash_short": _short_hash(hash_text),
                    "px": px_mid,
                    "px_mid_usd": px_mid,
                    "px_bid_usd": px_bid,
                    "px_ask_usd": px_ask,
                    "px_detail": px_detail,
                    "spread_bps": spread_bps,
                    "liq": primary_liq,
                    "liq_total_usd": liq_total_usd,
                    "liq_depth_pct": depth_pct if depth_pct else None,
                    "liq_detail": liq_detail,
                    "liq_depth_0_1pct_usd": depth_0_1,
                    "liq_depth_0_5pct_usd": depth_0_5,
                    "liq_depth_1_0pct_usd": depth_1_0,
                    "degraded": degraded_flag,
                    "source": source_label,
                    "staleness_ms": staleness_ms,
                    "route_meta": route_meta,
                    "age_seconds": age,
                    "age_label": _format_age(age),
                    "stale": stale_flag,
                    "coalesce_window_s": coalesce,
                    "lag_ms": age * 1000.0 if age is not None else None,
                    "stale_threshold_s": stale_threshold,
                    "momentum_score": momentum_score,
                    "momentum_stale": momentum_stale,
                    "momentum_partial": momentum_partial,
                    "momentum_sources": momentum_sources,
                    "momentum_breakdown": momentum_breakdown,
                    "pump_intensity": pump_intensity,
                    "pump_score": pump_score,
                    "social_score": social_score,
                    "social_sentiment": social_sentiment,
                    "tweets_per_min": tweets_per_min,
                    "buyers_last_hour": buyers_last_hour,
                    "momentum_latency_ms": momentum_latency,
                }
            )
        return {
            "snapshots": snapshots,
            "hash_map": hash_map,
            "lag_ms": max(lag_samples) if lag_samples else None,
        }

    async def wait_for_golden(self, timeout: float = 5.0) -> bool:
        deadline = time.time() + max(0.0, timeout)
        while time.time() < deadline:
            with self._swarm_lock:
                if self._golden_snapshots:
                    return True
                ts = self._last_golden_update
            if ts is not None and ts > 0:
                return True
            await asyncio.sleep(0.1)
        return False

    def last_golden_timestamp(self) -> Optional[float]:
        with self._swarm_lock:
            return self._last_golden_update

    def suggestions_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        window = 300.0
        with self._swarm_lock:
            suggestions = list(self._agent_suggestions)
            golden_hashes = dict(self._latest_golden_hash)
            decisions = list(self._vote_decisions)
        items: List[Dict[str, Any]] = []
        recent_count = 0
        for payload in suggestions:
            mint = payload.get("mint")
            if not mint:
                continue
            timestamp = _entry_timestamp(payload, "asof")
            age = _age_seconds(timestamp, now)
            if age is None and payload.get("_received") is not None:
                try:
                    age = max(0.0, now - float(payload["_received"]))
                except Exception:
                    age = None
            ttl = _maybe_float(payload.get("ttl_sec"))
            remaining = None
            if ttl is not None and age is not None:
                remaining = max(0.0, ttl - age)
            stale = False
            if ttl is not None and age is not None:
                stale = age > ttl
            elif age is not None and age > window:
                stale = True
            inputs_hash = payload.get("inputs_hash")
            golden_hash = golden_hashes.get(str(mint))
            mismatch = bool(inputs_hash and golden_hash and str(inputs_hash) != golden_hash)
            gating = payload.get("gating") or {}
            breakeven = _maybe_float(gating.get("breakeven_bps"))
            edge_buffer = _maybe_float(gating.get("edge_buffer_bps"))
            expected_edge = _maybe_float(gating.get("expected_edge_bps"))
            raw_edge_pass = gating.get("edge_pass")
            edge_pass = raw_edge_pass if isinstance(raw_edge_pass, bool) else None
            items.append(
                {
                    "agent": payload.get("agent"),
                    "mint": mint,
                    "side": (payload.get("side") or "").lower() or None,
                    "notional_usd": _maybe_float(payload.get("notional_usd")),
                    "edge": _maybe_float(payload.get("edge")),
                    "breakeven_bps": breakeven,
                    "edge_buffer_bps": edge_buffer,
                    "expected_edge_bps": expected_edge,
                    "edge_pass": edge_pass,
                    "risk": payload.get("risk") or {},
                    "max_slippage_bps": _maybe_float(payload.get("max_slippage_bps")),
                    "inputs_hash": inputs_hash,
                    "inputs_hash_short": _short_hash(inputs_hash),
                    "golden_hash": golden_hash,
                    "golden_hash_short": _short_hash(golden_hash),
                    "ttl_label": _format_ttl(remaining, ttl),
                    "ttl_seconds": ttl,
                    "age_label": _format_age(age),
                    "stale": stale,
                    "must": bool(payload.get("must")),
                    "age_seconds": age,
                    "hash_mismatch": mismatch,
                    "gating": gating,
                }
            )
            if age is not None and age <= window:
                recent_count += 1
        latest_age = None
        for item in items:
            age = item.get("age_seconds")
            if age is None:
                continue
            if latest_age is None or age < latest_age:
                latest_age = age
        recent_decisions = 0
        for decision in decisions:
            ts = _entry_timestamp(decision, "ts")
            age = _age_seconds(ts, now)
            if age is None and decision.get("_received") is not None:
                try:
                    age = max(0.0, now - float(decision["_received"]))
                except Exception:
                    age = None
            if age is not None and age <= window:
                recent_decisions += 1
        acceptance = 0.0
        if recent_count:
            acceptance = min(1.0, recent_decisions / max(recent_count, 1))
        metrics = {
            "count": len(items),
            "rate_per_min": recent_count / max(window / 60.0, 1e-6),
            "acceptance_rate": acceptance,
            "updated_label": _format_age(latest_age),
            "stale": latest_age is not None and latest_age > window,
            "golden_tracked": len(golden_hashes),
        }
        return {"suggestions": items[:200], "metrics": metrics}

    def vote_snapshot(self) -> Dict[str, Any]:
        now = time.time()
        with self._swarm_lock:
            suggestions = list(self._agent_suggestions)
            decisions = list(self._vote_decisions)
        window_ms = os.getenv("VOTE_WINDOW_MS", "15000")
        try:
            window_duration = float(window_ms) / 1000.0
        except Exception:
            window_duration = 15.0
        windows_map: Dict[Tuple[str, str, Any], Dict[str, Any]] = {}
        for suggestion in suggestions:
            mint = suggestion.get("mint")
            if not mint:
                continue
            side = (suggestion.get("side") or "").lower()
            key = (str(mint), side, suggestion.get("inputs_hash"))
            bucket = windows_map.setdefault(
                key,
                {
                    "mint": str(mint),
                    "side": side or "buy",
                    "quorum": 0,
                    "scores": [],
                    "first_ts": None,
                    "hash": suggestion.get("inputs_hash"),
                    "decision": None,
                },
            )
            bucket["quorum"] += 1
            score = _maybe_float(suggestion.get("edge"))
            if score is not None:
                bucket["scores"].append(score)
            ts = _entry_timestamp(suggestion, "asof")
            if ts is None and suggestion.get("_received") is not None:
                try:
                    ts = datetime.fromtimestamp(float(suggestion["_received"]), tz=timezone.utc)
                except Exception:
                    ts = None
            if ts is not None:
                current = bucket.get("first_ts")
                if current is None or ts < current:
                    bucket["first_ts"] = ts
        for decision in decisions:
            mint = decision.get("mint")
            if not mint:
                continue
            side = (decision.get("side") or "").lower()
            snapshot_hash = decision.get("snapshot_hash")
            key = (str(mint), side, snapshot_hash)
            if key in windows_map:
                windows_map[key]["decision"] = decision
        windows: List[Dict[str, Any]] = []
        for data in windows_map.values():
            first_ts = data.get("first_ts")
            age = _age_seconds(first_ts, now)
            if age is None:
                age = 0.0
            countdown = max(0.0, window_duration - age)
            decision = data.get("decision")
            expired = age > window_duration and not decision
            if data["scores"]:
                score_value = sum(data["scores"]) / max(len(data["scores"]), 1)
            elif decision is not None:
                score_value = _maybe_float(decision.get("score"))
            else:
                score_value = None
            idempotent = True
            if decision is not None:
                idempotent = bool(decision.get("_idempotent", False))
            windows.append(
                {
                    "mint": data.get("mint"),
                    "side": data.get("side"),
                    "quorum": data.get("quorum"),
                    "score": score_value,
                    "hash": data.get("hash"),
                    "countdown": countdown,
                    "countdown_label": _format_countdown(countdown),
                    "expired": expired,
                    "decision": decision,
                    "idempotent": idempotent,
                }
            )
        decisions_copy = []
        for decision in decisions[:200]:
            decisions_copy.append(dict(decision))
        return {"windows": windows[:200], "decisions": decisions_copy}

    def shadow_snapshot(self) -> Dict[str, Any]:
        with self._swarm_lock:
            virtual = list(self._virtual_fills)
            live = list(self._live_fills)
        return {
            "virtual_fills": virtual[:200],
            "paper_positions": [],
            "live_fills": live[:200],
        }


@dataclass
class RuntimeWiring:
    collectors: RuntimeEventCollectors

    def wire_ui_state(self, ui_state: UIState) -> None:
        ui_state.status_provider = self.collectors.status_snapshot
        ui_state.golden_snapshot_provider = self.collectors.golden_snapshot
        ui_state.discovery_provider = self.collectors.discovery_snapshot
        ui_state.discovery_console_provider = self.collectors.discovery_console_snapshot
        ui_state.market_state_provider = self.collectors.market_snapshot
        ui_state.suggestions_provider = self.collectors.suggestions_snapshot
        ui_state.vote_windows_provider = self.collectors.vote_snapshot
        ui_state.shadow_provider = self.collectors.shadow_snapshot
        ui_state.exit_provider = self.collectors.exit_snapshot
        ui_state.token_facts_provider = self.collectors.token_facts_snapshot
        ui_state.summary_provider = self.collectors.summary_snapshot
        ui_state.rl_provider = self.collectors.rl_snapshot
        ui_state.rl_status_provider = self.collectors.rl_status_snapshot
        ui_state.settings_provider = self.collectors.settings_snapshot

    def close(self) -> None:
        self.collectors.stop()

    async def wait_for_topic(self, topic: str, timeout: float = 5.0) -> bool:
        if topic == "x:mint.golden":
            return await self.collectors.wait_for_golden(timeout)
        return True


def initialise_runtime_wiring(ui_state: UIState) -> RuntimeWiring:
    collectors = RuntimeEventCollectors()
    collectors.start()
    wiring = RuntimeWiring(collectors=collectors)
    wiring.wire_ui_state(ui_state)
    return wiring
