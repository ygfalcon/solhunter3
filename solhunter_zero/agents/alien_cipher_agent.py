from __future__ import annotations

import hashlib
from typing import List, Dict, Any

from . import BaseAgent
from ..portfolio import Portfolio
from ..datasets.alien_cipher import load_alien_cipher, DEFAULT_PATH


class AlienCipherAgent(BaseAgent):
    """Logistic-map trading decisions from predefined coefficients."""

    name = "alien_cipher"

    def __init__(
        self,
        threshold: float = 0.5,
        amount: float = 1.0,
        dataset_path: str = DEFAULT_PATH,
    ) -> None:
        self.threshold = float(threshold)
        self.amount = float(amount)
        self.dataset_path = dataset_path
        self._coeffs: Dict[str, tuple[float, int]] | None = None

    # ------------------------------------------------------------------
    def _load_coeffs(self) -> Dict[str, tuple[float, int]]:
        if self._coeffs is not None:
            return self._coeffs

        data = load_alien_cipher(self.dataset_path)
        mapping: Dict[str, tuple[float, int]] = {}
        if isinstance(data, dict):
            for tok, info in data.items():
                if not isinstance(info, dict):
                    continue
                try:
                    r = float(info.get("r", 0.0))
                    iterations = int(info.get("iterations", 0))
                except Exception:
                    continue
                mapping[str(tok)] = (r, iterations)

        self._coeffs = mapping
        return mapping

    # ------------------------------------------------------------------
    def _initial_value(self, token: str) -> float:
        digest = int(hashlib.sha256(token.encode()).hexdigest(), 16)
        return digest / float(1 << 256)

    # ------------------------------------------------------------------
    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
    ) -> List[Dict[str, Any]]:
        coeffs = self._load_coeffs()
        if token not in coeffs:
            return []
        r, iterations = coeffs[token]
        x = self._initial_value(token)
        for _ in range(max(0, iterations)):
            x = r * x * (1.0 - x)

        if x >= self.threshold:
            return [{"token": token, "side": "buy", "amount": self.amount, "price": 0.0}]
        if x <= 1.0 - self.threshold and token in portfolio.balances:
            pos = portfolio.balances[token]
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": 0.0}]
        return []
