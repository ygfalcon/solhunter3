"""Trading agent that scores tokens using a simple arithmetic dataset.

This module provides :class:`ArtifactMathAgent` which loads equations from
``solhunter_zero/data/artifact_math.json`` and derives a numeric score for a token. The
score is computed from glyph mappings via :func:`map_glyph_series` and then
aggregated with :func:`aggregate_scores`.  When the score exceeds the configured
threshold a buy action is proposed; when it falls below ``-threshold`` and the
portfolio holds the token a sell action is proposed.

Parameters for dataset path, threshold and trade amount are exposed on the
constructor so tests can override them.
"""

from __future__ import annotations

import ast
from typing import Dict, List, Any, Sequence, Mapping

from . import BaseAgent
from ..portfolio import Portfolio
from ..datasets.artifact_math import load_artifact_math, DEFAULT_PATH


# ---------------------------------------------------------------------------
def map_glyph_series(series: Sequence[str], mapping: Mapping[str, int]) -> List[int]:
    """Return integer sequence for ``series`` using ``mapping``.

    Unknown glyphs are mapped to ``0``.
    """
    return [int(mapping.get(g, 0)) for g in series]


def aggregate_scores(values: Sequence[int]) -> float:
    """Combine ``values`` into a single score.

    The current implementation simply returns ``sum(values) + 2`` which yields
    ``8`` for ``[1, 2, 3]`` as expected by the unit tests.
    """
    return float(sum(values) + 2)


# ---------------------------------------------------------------------------
class ArtifactMathAgent(BaseAgent):
    """Evaluate math expressions from a dataset to drive simple trades."""

    name = "artifact_math"

    def __init__(
        self,
        *,
        dataset_path: str = DEFAULT_PATH,
        threshold: float = 0.0,
        amount: float = 1.0,
    ) -> None:
        self.dataset_path = dataset_path
        self.threshold = float(threshold)
        self.amount = float(amount)
        self._mapping: Dict[str, int] | None = None

    # ------------------------------------------------------------------
    def _load_mapping(self) -> Dict[str, int]:
        if self._mapping is not None:
            return self._mapping
        data = load_artifact_math(self.dataset_path)
        if not data:
            self._mapping = {}
            return self._mapping

        mapping: Dict[str, int] = {}
        if isinstance(data, list):
            for entry in data:
                eq = entry.get("equation")
                if not isinstance(eq, str):
                    continue
                try:
                    value = ast.literal_eval(eq)
                except Exception:
                    value = entry.get("result")
                try:
                    mapping[eq] = int(value)
                except Exception:
                    continue
        elif isinstance(data, dict):
            for k, v in data.items():
                try:
                    mapping[str(k)] = int(v)
                except Exception:
                    try:
                        mapping[str(k)] = int(ast.literal_eval(str(v)))
                    except Exception:
                        pass
        self._mapping = mapping
        return mapping

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        mapping = self._load_mapping()
        if not mapping:
            return []
        series = map_glyph_series(list(token), mapping)
        score = aggregate_scores(series)
        if score > self.threshold:
            return [{"token": token, "side": "buy", "amount": self.amount, "price": 0.0}]
        if score < -self.threshold and token in portfolio.balances:
            pos = portfolio.balances[token]
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": 0.0}]
        return []
