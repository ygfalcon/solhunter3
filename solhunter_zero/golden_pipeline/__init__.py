"""Event-driven Golden Snapshot trading pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .agents import BaseAgent
from .pipeline import GoldenPipeline
from .types import (
    Decision,
    DepthSnapshot,
    DiscoveryCandidate,
    GoldenSnapshot,
    LiveFill,
    OHLCVBar,
    TapeEvent,
    TokenSnapshot,
    TradeSuggestion,
    VirtualFill,
    VirtualPnL,
)

__all__ = [
    "GoldenPipeline",
    "BaseAgent",
    "DiscoveryCandidate",
    "TokenSnapshot",
    "TapeEvent",
    "DepthSnapshot",
    "OHLCVBar",
    "GoldenSnapshot",
    "TradeSuggestion",
    "Decision",
    "VirtualFill",
    "VirtualPnL",
    "LiveFill",
    "GoldenPipelineService",
    "AgentManagerAgent",
]


if TYPE_CHECKING:  # pragma: no cover - type checkers only
    from .service import AgentManagerAgent, GoldenPipelineService


def __getattr__(name: str) -> Any:  # pragma: no cover - exercised implicitly
    if name in {"GoldenPipelineService", "AgentManagerAgent"}:
        from .service import AgentManagerAgent, GoldenPipelineService

        mapping = {
            "GoldenPipelineService": GoldenPipelineService,
            "AgentManagerAgent": AgentManagerAgent,
        }
        return mapping[name]
    raise AttributeError(name)
