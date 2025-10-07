from __future__ import annotations

import hashlib
from typing import List, Dict, Any

import logging

from . import BaseAgent
from ..portfolio import Portfolio
from ..datasets.alien_cipher import load_alien_cipher, DEFAULT_PATH
from .price_utils import resolve_price

logger = logging.getLogger(__name__)


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
            price, context = await resolve_price(token, portfolio)
            metadata = {"price_context": context}
            if price <= 0:
                fallback = None
                for key in ("history_price", "cached_price", "fetched_price"):
                    value = context.get(key)
                    if isinstance(value, (int, float)) and value > 0:
                        fallback = float(value)
                        metadata["price_fallback"] = key
                        break
                if fallback is not None:
                    price = fallback
                else:
                    metadata["price_missing"] = True
                    logger.info(
                        "%s agent proceeding without live price for %s", self.name, token
                    )
            return [
                {
                    "token": token,
                    "side": "buy",
                    "amount": self.amount,
                    "price": price,
                    "metadata": metadata,
                }
            ]
        if x <= 1.0 - self.threshold and token in portfolio.balances:
            pos = portfolio.balances[token]
            price, context = await resolve_price(token, portfolio)
            metadata = {"price_context": context}
            if price <= 0:
                entry = getattr(pos, "entry_price", 0.0)
                if isinstance(entry, (int, float)) and entry > 0:
                    price = float(entry)
                    metadata["price_fallback"] = "entry_price"
                else:
                    metadata["price_missing"] = True
                    logger.info(
                        "%s agent proceeding without live price for %s", self.name, token
                    )
            return [
                {
                    "token": token,
                    "side": "sell",
                    "amount": pos.amount,
                    "price": price,
                    "metadata": metadata,
                }
            ]
        return []
