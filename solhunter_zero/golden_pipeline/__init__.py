"""Event-driven Golden Snapshot trading pipeline."""

from .pipeline import GoldenPipeline
from .agents import BaseAgent
from .service import GoldenPipelineService, AgentManagerAgent
from .types import (
    DiscoveryCandidate,
    TokenSnapshot,
    TapeEvent,
    DepthSnapshot,
    OHLCVBar,
    GoldenSnapshot,
    TradeSuggestion,
    Decision,
    VirtualFill,
    LiveFill,
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
    "LiveFill",
    "GoldenPipelineService",
    "AgentManagerAgent",
]
