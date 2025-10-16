from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass(slots=True)
class TokenCandidate:
    """Discovery stage output describing a potential token."""

    token: str
    source: str
    discovered_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScoredToken:
    """Scoring stage output that ranks a :class:`TokenCandidate`."""

    token: str
    score: float
    rank: int
    candidate: TokenCandidate
    profile: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EvaluationResult:
    """Result of running the evaluation agents for a token."""

    token: str
    actions: List[Dict[str, Any]]
    latency: float
    cached: bool = False
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ActionBundle:
    """Bundle of actions destined for execution."""

    token: str
    actions: List[Dict[str, Any]]
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionReceipt:
    """Outcome of executing an :class:`ActionBundle`."""

    token: str
    success: bool
    results: List[Any]
    errors: List[str]
    started_at: float
    finished_at: float
    lane: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class TelemetrySample:
    """Structured telemetry emitted by the pipeline."""

    timestamp: datetime
    stage: str
    detail: str
    payload: Dict[str, Any] = field(default_factory=dict)
