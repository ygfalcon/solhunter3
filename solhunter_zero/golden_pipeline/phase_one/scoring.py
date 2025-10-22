"""Logistic scoring utilities used by Stage-A of the Golden Stream."""
from __future__ import annotations

import math
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Mapping, Optional

import yaml


DEFAULT_WEIGHTS: Dict[str, float] = {
    "bias": -4.0,
    "liquidity_usd": 0.00008,
    "vol_1h_z": 0.35,
    "pair_age_min": 0.004,
    "source_diversity": 0.6,
    "px_staleness_ms": -0.0004,
}


@dataclass(slots=True)
class LogisticWeights:
    bias: float
    coefficients: Dict[str, float]


class LogisticScorer:
    """Compute logistic scores from feature dictionaries."""

    def __init__(self, weights_path: str | None = None) -> None:
        self._weights_path = Path(weights_path) if weights_path else None
        self._weights = self._load()

    def _load(self) -> LogisticWeights:
        if self._weights_path and self._weights_path.exists():
            payload = yaml.safe_load(self._weights_path.read_text()) or {}
        else:
            payload = dict(DEFAULT_WEIGHTS)
        bias = float(payload.pop("bias", 0.0))
        coefficients = {str(key): float(value) for key, value in payload.items()}
        return LogisticWeights(bias=bias, coefficients=coefficients)

    def reload(self) -> None:
        self._weights = self._load()

    def score(self, features: Mapping[str, float]) -> float:
        total = self._weights.bias
        for key, weight in self._weights.coefficients.items():
            value = float(features.get(key, 0.0))
            total += weight * value
        return 1.0 / (1.0 + math.exp(-total))

    @property
    def weights(self) -> LogisticWeights:
        return self._weights


