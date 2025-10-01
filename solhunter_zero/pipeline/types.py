from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from datetime import datetime


@dataclass(slots=True)
class TokenCandidate:
    token: str
    source: str
    discovered_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ScoredToken:
    token: str
    score: float
    rank: int
    candidate: TokenCandidate
    profile: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class EvaluationResult:
    token: str
    actions: List[Dict[str, Any]]
    latency: float
    cached: bool = False
    errors: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ActionBundle:
    token: str
    actions: List[Dict[str, Any]]
    created_at: float
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionReceipt:
    token: str
    success: bool
    results: List[Any]
    errors: List[str]
    started_at: float
    finished_at: float
    lane: Optional[str] = None


@dataclass(slots=True)
class TelemetrySample:
    timestamp: datetime
    stage: str
    detail: str
    payload: Dict[str, Any] = field(default_factory=dict)
