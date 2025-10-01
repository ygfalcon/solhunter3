from __future__ import annotations

import random
from typing import Any, Dict


def _volatility(window: list[float]) -> float:
    if len(window) < 2:
        return 0.0
    mean = sum(window) / len(window)
    if mean == 0:
        return 0.0
    var = sum((x - mean) ** 2 for x in window) / len(window)
    return (var ** 0.5) / abs(mean)


def heuristic_score(token: str, portfolio: Any, profile: Dict[str, Any] | None = None) -> float:
    """Compute a score for ``token`` combining heuristics and cached profile."""
    score = 0.0
    balances = getattr(portfolio, "balances", {}) or {}
    if token in balances:
        score += 12.0

    history = getattr(portfolio, "price_history", {}).get(token) or []
    window = history[-min(len(history), 10) :]
    if len(window) >= 2:
        first = window[0] or 1.0
        last = window[-1]
        delta = (last - first) / (abs(first) or 1.0)
        score += delta * 6.0
        score += _volatility(window) * 3.0

    if profile:
        score += float(profile.get("trend_score", 0.0))
        score += float(profile.get("volume_score", 0.0))
        if profile.get("recent_execution_error"):
            score -= 5.0

    return score + random.random() * 0.01
