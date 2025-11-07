"""Typed event payload schemas used with the event bus."""
from __future__ import annotations

from dataclasses import dataclass, asdict, is_dataclass
from typing import Any, Dict, Type, Optional


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
    # Optional timestamp when present from storage layer
    created_at: Optional[object] = None


@dataclass
class MemorySyncRequest:
    """Request snapshot of trades since ``last_id``."""
    last_id: int = 0


@dataclass
class MemorySyncResponse:
    """Response carrying new trades and optional index bytes."""
    trades: Optional[list[TradeLogged]] = None
    index: Optional[bytes] = None


@dataclass
class TokenDiscovered:
    """Token discovery event payload including metadata refresh signals."""

    tokens: list[str]
    metadata_refresh: bool = False
    changed_tokens: Optional[list[str]] = None
    metadata: Optional[Dict[str, Dict[str, Any]]] = None


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
# Topic → schema registry
# ─────────────────────────────

_EVENT_SCHEMAS: Dict[str, Type] = {
    "action_executed": ActionExecuted,
    "weights_updated": WeightsUpdated,
    "rl_weights": RLWeights,
    "rl_checkpoint": RLCheckpoint,
    "portfolio_updated": PortfolioUpdated,
    "trade_logged": TradeLogged,
    "memory_sync_request": MemorySyncRequest,
    "memory_sync_response": MemorySyncResponse,
    "heartbeat": Heartbeat,
    "system_metrics": SystemMetrics,
    "token_discovered": TokenDiscovered,
    # Add runtime log mapping so validate_message accepts it
    "runtime.log": RuntimeLog,
}


# ─────────────────────────────
# Helpers
# ─────────────────────────────

def validate_message(topic: str, payload: Any) -> Any:
    """
    Validate ``payload`` for ``topic`` returning a dataclass instance.

    Unknown topics pass through unmodified.
    Raises ``ValueError`` if validation fails.

    Special case: for 'runtime.log' a plain string payload is accepted and
    wrapped as RuntimeLog(stage='misc', detail='<string>').
    """
    schema = _EVENT_SCHEMAS.get(topic)
    if schema is None:
        return payload

    # Accept already-typed dataclass
    if isinstance(payload, schema):
        return payload

    # Special convenience: allow a bare string for runtime.log
    if schema is RuntimeLog and isinstance(payload, str):
        return RuntimeLog(stage="misc", detail=payload)

    # Dict → dataclass
    if isinstance(payload, dict):
        try:
            return schema(**payload)  # type: ignore[arg-type]
        except Exception as exc:
            raise ValueError(f"Invalid payload for {topic}: {exc}") from exc

    raise ValueError(f"Invalid payload for {topic}")


def to_dict(payload: Any) -> Any:
    """Convert dataclass ``payload`` to a plain dictionary."""
    if is_dataclass(payload):
        return asdict(payload)
    return payload
