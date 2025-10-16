"""Common report schema for demo and paper trading outputs.

This module defines dataclasses representing the JSON artifacts written by
:mod:`solhunter_zero.investor_demo`. Tests for both the demo and paper
workflows import these types to ensure a consistent structure across the two
entry points.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import json

# Filenames produced by the demo and paper trading helpers
SUMMARY_FILE = "summary.json"
TRADE_HISTORY_FILE = "trade_history.json"
HIGHLIGHTS_FILE = "highlights.json"

# Set of JSON artifacts expected from the workflows
REQUIRED_JSON = {SUMMARY_FILE, TRADE_HISTORY_FILE, HIGHLIGHTS_FILE}


@dataclass
class StrategySummary:
    """Performance metrics for a single strategy/token pair."""

    token: str
    config: str
    roi: float
    sharpe: float
    drawdown: float
    volatility: float
    trades: int
    wins: int
    losses: int
    win_rate: float
    final_capital: float


@dataclass
class TradeRecord:
    """Recorded capital and action for a strategy at a given period."""

    token: str
    strategy: str
    period: int
    date: str
    action: str
    price: float
    capital: float


@dataclass
class Highlights:
    """High level outcomes and trade type results from a run."""

    top_strategy: str
    top_final_capital: float
    top_roi: float
    top_token: Optional[str] = None
    cpu_usage: Optional[float] = None
    memory_percent: Optional[float] = None
    arbitrage_path: Optional[List[str]] = None
    arbitrage_profit: Optional[float] = None
    route_ffi_path: Optional[List[str]] = None
    route_ffi_profit: Optional[float] = None
    flash_loan_signature: Optional[str] = None
    sniper_tokens: Optional[List[str]] = None
    dex_new_pools: Optional[List[str]] = None
    rl_reward: Optional[float] = None
    jito_swaps: Optional[List[str]] = None
    key_correlations: Optional[List[str]] = None
    hedged_weights: Optional[Dict[str, float]] = None


def load_reports(path: Path) -> Tuple[List[StrategySummary], List[TradeRecord], Highlights]:
    """Load report artifacts from ``path`` and parse them into dataclasses."""

    summary_data = json.loads((path / SUMMARY_FILE).read_text())
    trade_data = json.loads((path / TRADE_HISTORY_FILE).read_text())
    highlights_data = json.loads((path / HIGHLIGHTS_FILE).read_text())

    summary = [StrategySummary(**row) for row in summary_data]
    trade_hist = [TradeRecord(**row) for row in trade_data]
    highlights = Highlights(**highlights_data)
    return summary, trade_hist, highlights


__all__ = [
    "StrategySummary",
    "TradeRecord",
    "Highlights",
    "SUMMARY_FILE",
    "TRADE_HISTORY_FILE",
    "HIGHLIGHTS_FILE",
    "REQUIRED_JSON",
    "load_reports",
]
