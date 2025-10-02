from __future__ import annotations

import logging

from typing import List, Dict, Any

from collections.abc import Iterable as IterableABC, Mapping as MappingABC
from datetime import timedelta

from . import BaseAgent
from ..portfolio import Portfolio
from .price_utils import resolve_price


logger = logging.getLogger(__name__)


class ExitAgent(BaseAgent):
    """Propose exit trades using ROI thresholds or trailing stops."""

    name = "exit"

    def __init__(self, trailing: float = 0.0, stop_loss: float = 0.0, take_profit: float = 0.0):
        self.trailing = trailing
        self.stop_loss = stop_loss
        self.take_profit = take_profit

    def _normalize_metric(self, value: Any) -> float | None:
        """Best-effort conversion of portfolio metrics to ``float``."""

        if value is None:
            return None
        if isinstance(value, timedelta):
            try:
                value = value.total_seconds()
            except Exception:
                return None
        if hasattr(value, "total_seconds") and not isinstance(value, (int, float)):
            try:
                value = value.total_seconds()
            except Exception:
                return None
        try:
            return float(value)
        except Exception:
            return None

    def _resolve_holding_duration(
        self,
        portfolio: Portfolio,
        token: str,
        provided: Any,
    ) -> float | None:
        if provided is not None:
            return self._normalize_metric(provided)
        helper = getattr(portfolio, "holding_duration", None)
        if callable(helper):
            try:
                duration = helper(token)
            except Exception:
                return None
            return self._normalize_metric(duration)
        return None

    def _resolve_realized_roi(
        self,
        portfolio: Portfolio,
        token: str,
        price: float,
        provided: Any,
    ) -> float | None:
        if provided is not None:
            return self._normalize_metric(provided)
        helper = getattr(portfolio, "realized_roi", None)
        if callable(helper):
            try:
                if price > 0:
                    roi = helper(token, price)  # type: ignore[misc]
                else:
                    roi = helper(token)  # type: ignore[misc]
            except TypeError:
                try:
                    roi = helper(token)
                except Exception:
                    roi = None
            except Exception:
                roi = None
            if roi is not None:
                return self._normalize_metric(roi)
        if price > 0:
            try:
                roi = portfolio.position_roi(token, price)
            except Exception:
                roi = None
            return self._normalize_metric(roi)
        return None

    def _resolve_drawdown(
        self,
        portfolio: Portfolio,
        token: str,
        price: float,
        provided: Any,
    ) -> float | None:
        if provided is not None:
            return self._normalize_metric(provided)
        helper = getattr(portfolio, "current_drawdown", None)
        if callable(helper):
            price_map: Dict[str, float] = {}
            if price > 0:
                price_map[token] = price
            else:
                history = getattr(portfolio, "price_history", {})
                if isinstance(history, MappingABC):
                    hist = history.get(token)
                    if isinstance(hist, IterableABC):
                        last_val = None
                        try:
                            if hasattr(hist, "__getitem__"):
                                last_val = hist[-1]  # type: ignore[index]
                            else:
                                last_val = list(hist)[-1]
                        except Exception:
                            last_val = None
                        if last_val is not None:
                            try:
                                last = float(last_val)
                            except Exception:
                                last = None
                            else:
                                if last > 0:
                                    price_map[token] = last
            try:
                drawdown = helper(price_map)
            except Exception:
                drawdown = None
            else:
                return self._normalize_metric(drawdown)
        return None

    async def propose_trade(
        self,
        token: str,
        portfolio: Portfolio,
        *,
        depth: float | None = None,
        imbalance: float | None = None,
        holding_duration: Any | None = None,
        realized_roi: Any | None = None,
        drawdown: Any | None = None,
    ) -> List[Dict[str, Any]]:
        if token not in portfolio.balances:
            return []
        price, price_context = await resolve_price(token, portfolio)
        if price <= 0:
            logger.warning(
                "exit agent could not obtain a valid price for %s; aborting proposal",
                token,
                extra={"price_context": price_context},
            )
            return []
        pos = portfolio.balances[token]

        roi = portfolio.position_roi(token, price)
        metrics = {
            "holding_duration": self._resolve_holding_duration(
                portfolio, token, holding_duration
            ),
            "realized_roi": self._resolve_realized_roi(
                portfolio, token, price, realized_roi
            ),
            "drawdown": self._resolve_drawdown(portfolio, token, price, drawdown),
        }

        def _sell_action() -> Dict[str, Any]:
            action: Dict[str, Any] = {
                "token": token,
                "side": "sell",
                "amount": pos.amount,
                "price": price,
            }
            for key, value in metrics.items():
                if value is not None:
                    action[key] = value
            return action

        if self.stop_loss and roi <= -self.stop_loss:
            return [_sell_action()]
        if self.take_profit and roi >= self.take_profit:
            return [_sell_action()]

        if price > 0 and self.trailing and portfolio.trailing_stop_triggered(
            token, price, self.trailing
        ):
            return [_sell_action()]
        return []
