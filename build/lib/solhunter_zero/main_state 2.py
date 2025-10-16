from __future__ import annotations

from dataclasses import dataclass, field
import datetime
from typing import Dict, List


@dataclass
class TradingState:
    """Container for runtime trading state.

    Keeps track of tokens processed in the last iteration and the timestamp of
    the most recent trade per token. This replaces previous module level global
    variables.
    """

    last_tokens: List[str] = field(default_factory=list)
    last_trade_times: Dict[str, datetime.datetime] = field(default_factory=dict)
