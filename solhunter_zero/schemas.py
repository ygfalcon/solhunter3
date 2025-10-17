"""Typed event payload schemas used with the event bus."""
from __future__ import annotations

import base64
import datetime as _dt
from dataclasses import asdict, dataclass, is_dataclass
from typing import Any, Dict, List, Optional, Type, Union, cast


# ─────────────────────────────
# Event payload dataclasses
# ─────────────────────────────


@dataclass
class ActionExecuted:
    """Payload for an executed trading action."""

    action: Dict[str, Any]
    result: Any


@dataclass
class WeightsUpdated:
    """Payload for updated agent weights."""

    weights: Dict[str, float]


@dataclass
class RLWeights:
    """Payload for RL-generated weights and risk parameters."""

    weights: Dict[str, float]
    risk: Optional[Dict[str, float]] = None


@dataclass
class RLCheckpoint:
    """Payload emitted when RL daemon saves a checkpoint."""

    time: float
    path: str


@dataclass
class PortfolioUpdated:
    """Payload emitted whenever portfolio balances change."""

    balances: Dict[str, float]


@dataclass
class Heartbeat:
    """Heartbeat message payload."""

    service: str


@dataclass
class TradeLogged:
    """Payload emitted when a trade is written to memory."""

    token: str
    direction: str
    amount: float
    price: float
    reason: Optional[str] = None
    context: Optional[str] = None
    emotion: Optional[str] = None
    simulation_id: Optional[int] = None
    uuid: Optional[str] = None
    trade_id: Optional[int] = None
    # Optional timestamp when present from storage layer (unix seconds)
    created_at: Optional[float] = None


@dataclass
class MemorySyncRequest:
    """Request snapshot of trades since ``last_id``."""

    last_id: int = 0


@dataclass
class MemorySyncResponse:
    """Response carrying new trades and optional index bytes (base64 or bytes)."""

    trades: Optional[List[TradeLogged]] = None
    index: Optional[Union[bytes, str]] = None  # str is base64


@dataclass
class SystemMetrics:
    """Payload with system CPU and memory usage."""

    cpu: float
    memory: float
    proc_cpu: Optional[float] = None


@dataclass
class RuntimeLog:
    """
    Lightweight diagnostic log for UI/event bus.

    Fields:
      stage: e.g. 'loop', 'discovery', 'execute', 'order', 'rl'
      detail: free-form short message
      ts: optional unix timestamp (seconds)
      level: optional log level string, e.g. 'INFO', 'WARN'
      actions: optional count of actions considered/executed for the stage
    """

    stage: str
    detail: str
    ts: Optional[float] = None
    level: Optional[str] = None
    actions: Optional[int] = None


# ─────────────────────────────
# Topic constants & registry
# ─────────────────────────────

TOPIC_ACTION_EXECUTED = "action_executed"
TOPIC_WEIGHTS_UPDATED = "weights_updated"
TOPIC_RL_WEIGHTS = "rl_weights"
TOPIC_RL_CHECKPOINT = "rl_checkpoint"
TOPIC_PORTFOLIO_UPDATED = "portfolio_updated"
TOPIC_TRADE_LOGGED = "trade_logged"
TOPIC_MEMORY_SYNC_REQUEST = "memory_sync_request"
TOPIC_MEMORY_SYNC_RESPONSE = "memory_sync_response"
TOPIC_HEARTBEAT = "heartbeat"
TOPIC_SYSTEM_METRICS = "system_metrics"
TOPIC_RUNTIME_LOG = "runtime.log"  # canonical
TOPIC_RUNTIME_LOG_LEGACY = "runtime_log"  # alias

_EVENT_SCHEMAS: Dict[str, Type[Any]] = {
    TOPIC_ACTION_EXECUTED: ActionExecuted,
    TOPIC_WEIGHTS_UPDATED: WeightsUpdated,
    TOPIC_RL_WEIGHTS: RLWeights,
    TOPIC_RL_CHECKPOINT: RLCheckpoint,
    TOPIC_PORTFOLIO_UPDATED: PortfolioUpdated,
    TOPIC_TRADE_LOGGED: TradeLogged,
    TOPIC_MEMORY_SYNC_REQUEST: MemorySyncRequest,
    TOPIC_MEMORY_SYNC_RESPONSE: MemorySyncResponse,
    TOPIC_HEARTBEAT: Heartbeat,
    TOPIC_SYSTEM_METRICS: SystemMetrics,
    TOPIC_RUNTIME_LOG: RuntimeLog,
    TOPIC_RUNTIME_LOG_LEGACY: RuntimeLog,
}


# ─────────────────────────────
# Normalizers
# ─────────────────────────────


def _to_unix_ts(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return _dt.datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    if isinstance(value, _dt.datetime):
        if value.tzinfo is None:
            return value.timestamp()
        return value.astimezone(_dt.timezone.utc).timestamp()
    return None


def _to_float(value: Any) -> float:
    try:
        return float(value)
    except Exception:
        return 0.0


def _b64_to_bytes(maybe_str: Union[str, bytes]) -> bytes:
    if isinstance(maybe_str, bytes):
        return maybe_str
    try:
        return base64.b64decode(maybe_str)
    except Exception:
        return maybe_str.encode("utf-8", errors="ignore")


# ─────────────────────────────
# Helpers
# ─────────────────────────────


def validate_message(topic: str, payload: Any) -> Any:
    """
    Validate ``payload`` for ``topic`` returning a dataclass instance.

    Unknown topics pass through unmodified.
    Raises ``ValueError`` if validation fails.

    Special case: for runtime log topics a plain string payload is accepted and
    wrapped as RuntimeLog(stage='misc', detail='<string>').
    """

    schema = _EVENT_SCHEMAS.get(topic)
    if schema is None:
        return payload

    if isinstance(payload, schema):
        return payload

    if schema is RuntimeLog and isinstance(payload, str):
        return RuntimeLog(stage="misc", detail=payload)

    if isinstance(payload, dict):
        if schema is TradeLogged:
            payload = dict(payload)
            payload["amount"] = _to_float(payload.get("amount"))
            payload["price"] = _to_float(payload.get("price"))
            if "created_at" in payload:
                payload["created_at"] = _to_unix_ts(payload.get("created_at"))
        elif schema is MemorySyncResponse:
            trades = payload.get("trades")
            if trades:
                coerced: List[TradeLogged] = []
                for trade in trades:
                    if isinstance(trade, TradeLogged):
                        coerced.append(trade)
                    elif isinstance(trade, dict):
                        coerced.append(validate_message(TOPIC_TRADE_LOGGED, trade))
                payload = dict(payload)
                payload["trades"] = coerced
            if "index" in payload and payload["index"] is not None:
                payload = dict(payload)
                payload["index"] = _b64_to_bytes(cast(Union[str, bytes], payload["index"]))
        elif schema is RuntimeLog and "ts" in payload:
            payload = dict(payload)
            payload["ts"] = _to_unix_ts(payload.get("ts"))
        try:
            return schema(**payload)  # type: ignore[arg-type]
        except Exception as exc:
            raise ValueError(f"Invalid payload for {topic}: {exc}") from exc

    raise ValueError(f"Invalid payload for {topic}")


def to_dict(payload: Any) -> Any:
    """
    Convert dataclass ``payload`` to a plain dictionary, making it JSON/bus safe.

    - Nested dataclasses are expanded (via asdict).
    - MemorySyncResponse.index is base64-encoded if bytes.
    """

    if not is_dataclass(payload):
        return payload

    data = asdict(payload)

    if isinstance(payload, MemorySyncResponse) and data.get("index") is not None:
        idx = data["index"]
        if isinstance(idx, (bytes, bytearray)):
            data["index"] = base64.b64encode(bytes(idx)).decode("ascii")

    return data


__all__ = [
    "TOPIC_ACTION_EXECUTED",
    "TOPIC_WEIGHTS_UPDATED",
    "TOPIC_RL_WEIGHTS",
    "TOPIC_RL_CHECKPOINT",
    "TOPIC_PORTFOLIO_UPDATED",
    "TOPIC_TRADE_LOGGED",
    "TOPIC_MEMORY_SYNC_REQUEST",
    "TOPIC_MEMORY_SYNC_RESPONSE",
    "TOPIC_HEARTBEAT",
    "TOPIC_SYSTEM_METRICS",
    "TOPIC_RUNTIME_LOG",
    "TOPIC_RUNTIME_LOG_LEGACY",
    "ActionExecuted",
    "WeightsUpdated",
    "RLWeights",
    "RLCheckpoint",
    "PortfolioUpdated",
    "Heartbeat",
    "TradeLogged",
    "MemorySyncRequest",
    "MemorySyncResponse",
    "SystemMetrics",
    "RuntimeLog",
    "validate_message",
    "to_dict",
]
