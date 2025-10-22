"""Event-driven Golden Snapshot trading pipeline."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from .agents import BaseAgent
from .metrics import GoldenMetrics
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
    "GoldenStreamService",
    "AgentManagerAgent",
    "GoldenMetrics",
]


if TYPE_CHECKING:  # pragma: no cover - type checkers only
    from .service import AgentManagerAgent, GoldenPipelineService
    from .phase_one.service import GoldenStreamService


def __getattr__(name: str) -> Any:  # pragma: no cover - exercised implicitly
    if name in {"GoldenPipelineService", "AgentManagerAgent", "GoldenStreamService"}:
        from .service import AgentManagerAgent, GoldenPipelineService
        from .phase_one.service import GoldenStreamService

        mapping = {
            "GoldenPipelineService": GoldenPipelineService,
            "AgentManagerAgent": AgentManagerAgent,
            "GoldenStreamService": GoldenStreamService,
        }
        return mapping[name]
    raise AttributeError(name)
