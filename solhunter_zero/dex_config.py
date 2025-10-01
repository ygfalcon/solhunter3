from dataclasses import dataclass, field
from typing import Dict

@dataclass
class DEXConfig:
    """DEX endpoint and cost configuration."""

    base_url: str
    testnet_url: str
    venue_urls: Dict[str, str] = field(default_factory=dict)
    fees: Dict[str, float] = field(default_factory=dict)
    gas: Dict[str, float] = field(default_factory=dict)
    latency: Dict[str, float] = field(default_factory=dict)
