from __future__ import annotations
from .jsonutil import loads, dumps
import os
import logging
from dataclasses import dataclass, field
from typing import Dict, Optional, Mapping

import time

from .risk import RiskManager

try:  # Optional dependency
    import aiofiles  # type: ignore
except ImportError:  # pragma: no cover - fallback when aiofiles is missing
    aiofiles = None  # type: ignore

from .event_bus import publish
from .schemas import PortfolioUpdated

logger = logging.getLogger(__name__)


@dataclass
class Position:
    token: str
    amount: float
    entry_price: float
    high_price: float = 0.0
    breakeven_bps: float = 0.0
    breakeven_components: Dict[str, float] = field(default_factory=dict)
    opened_at: float = 0.0
    realized_pnl_usd: float = 0.0
    unrealized_pnl_usd: float = 0.0
    attribution: Dict[str, float] = field(default_factory=dict)


@dataclass
class Portfolio:
    path: Optional[str] = "portfolio.json"
    balances: Dict[str, Position] = field(default_factory=dict)
    max_value: float = 0.0
    price_history: Dict[str, list[float]] = field(default_factory=dict)
    history_window: int = 50
    risk_metrics: Dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.load()

    # persistence helpers -------------------------------------------------
    def load(self) -> None:
        if not self.path or not os.path.exists(self.path):
            return
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = loads(f.read())
        except Exception:  # pragma: no cover - invalid file
            return
        balances: Dict[str, Position] = {}
        for token, info in data.items():
            if not isinstance(info, dict):
                continue
            balances[token] = Position(
                token,
                float(info.get("amount", 0.0)),
                float(info.get("entry_price", 0.0)),
                float(info.get("high_price", info.get("entry_price", 0.0))),
                float(info.get("breakeven_bps", 0.0)),
                {
                    str(k): float(v)
                    for k, v in (info.get("breakeven_components") or {}).items()
                    if isinstance(v, (int, float))
                },
                float(info.get("opened_at", 0.0)),
                float(info.get("realized_pnl_usd", 0.0)),
                float(info.get("unrealized_pnl_usd", 0.0)),
                {
                    str(k): float(v)
                    for k, v in (info.get("attribution") or {}).items()
                    if isinstance(v, (int, float))
                },
            )
        self.balances = balances

    def save(self) -> None:
        if not self.path:
            return
        data = {
            token: {
                "amount": pos.amount,
                "entry_price": pos.entry_price,
                "high_price": pos.high_price,
                "breakeven_bps": pos.breakeven_bps,
                "breakeven_components": dict(pos.breakeven_components),
                "opened_at": pos.opened_at,
                "realized_pnl_usd": pos.realized_pnl_usd,
                "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                "attribution": dict(pos.attribution),
            }
            for token, pos in self.balances.items()
        }
        with open(self.path, "w", encoding="utf-8") as f:
            f.write(dumps(data))
        publish(
            "portfolio_updated",
            PortfolioUpdated(
                balances={t: p.amount for t, p in self.balances.items()}
            ),
        )

    async def save_async(self) -> None:
        if not self.path:
            return

        if aiofiles is None:  # pragma: no cover - fallback to sync save
            self.save()
            return

        data = {
            token: {
                "amount": pos.amount,
                "entry_price": pos.entry_price,
                "high_price": pos.high_price,
                "breakeven_bps": pos.breakeven_bps,
                "breakeven_components": dict(pos.breakeven_components),
                "opened_at": pos.opened_at,
                "realized_pnl_usd": pos.realized_pnl_usd,
                "unrealized_pnl_usd": pos.unrealized_pnl_usd,
                "attribution": dict(pos.attribution),
            }
            for token, pos in self.balances.items()
        }
        async with aiofiles.open(self.path, "w", encoding="utf-8") as f:  # type: ignore
            await f.write(dumps(data))
        publish(
            "portfolio_updated",
            PortfolioUpdated(
                balances={t: p.amount for t, p in self.balances.items()}
            ),
        )

    # position management -------------------------------------------------
    def add(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        breakeven_bps: float | None = None,
        breakeven_components: Mapping[str, float] | None = None,
    ) -> None:
        self.update(
            token,
            amount,
            price,
            breakeven_bps=breakeven_bps,
            breakeven_components=breakeven_components,
        )

    async def add_async(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        breakeven_bps: float | None = None,
        breakeven_components: Mapping[str, float] | None = None,
    ) -> None:
        await self.update_async(
            token,
            amount,
            price,
            breakeven_bps=breakeven_bps,
            breakeven_components=breakeven_components,
        )

    def update(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        breakeven_bps: float | None = None,
        breakeven_components: Mapping[str, float] | None = None,
        attribution: Mapping[str, float] | None = None,
    ) -> None:
        pos = self.balances.get(token)
        if pos is None:
            components = dict(breakeven_components or {})
            self.balances[token] = Position(
                token,
                amount,
                price,
                price,
                float(breakeven_bps or 0.0),
                components,
                time.time(),
            )
        else:
            total_cost = pos.amount * pos.entry_price + amount * price
            new_amount = pos.amount + amount
            if new_amount <= 0:
                self.balances.pop(token, None)
            else:
                pos.amount = new_amount
                if amount > 0:
                    pos.entry_price = total_cost / new_amount
                    if breakeven_bps is not None:
                        pos.breakeven_bps = float(breakeven_bps)
                        pos.breakeven_components = dict(breakeven_components or {})
                    if pos.opened_at == 0.0:
                        pos.opened_at = time.time()
                if price > pos.high_price:
                    pos.high_price = price
                if attribution:
                    for agent, value in attribution.items():
                        pos.attribution[str(agent)] = pos.attribution.get(
                            str(agent), 0.0
                        ) + float(value)
        self.save()
        publish(
            "portfolio_updated",
            PortfolioUpdated(
                balances={t: p.amount for t, p in self.balances.items()}
            ),
        )

    async def update_async(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        breakeven_bps: float | None = None,
        breakeven_components: Mapping[str, float] | None = None,
        attribution: Mapping[str, float] | None = None,
    ) -> None:
        pos = self.balances.get(token)
        if pos is None:
            components = dict(breakeven_components or {})
            self.balances[token] = Position(
                token,
                amount,
                price,
                price,
                float(breakeven_bps or 0.0),
                components,
                time.time(),
            )
        else:
            total_cost = pos.amount * pos.entry_price + amount * price
            new_amount = pos.amount + amount
            if new_amount <= 0:
                self.balances.pop(token, None)
            else:
                pos.amount = new_amount
                if amount > 0:
                    pos.entry_price = total_cost / new_amount
                    if breakeven_bps is not None:
                        pos.breakeven_bps = float(breakeven_bps)
                        pos.breakeven_components = dict(breakeven_components or {})
                    if pos.opened_at == 0.0:
                        pos.opened_at = time.time()
                if price > pos.high_price:
                    pos.high_price = price
                if attribution:
                    for agent, value in attribution.items():
                        pos.attribution[str(agent)] = pos.attribution.get(
                            str(agent), 0.0
                        ) + float(value)
        await self.save_async()
        publish(
            "portfolio_updated",
            PortfolioUpdated(
                balances={t: p.amount for t, p in self.balances.items()}
            ),
        )

    def remove(self, token: str) -> None:
        if token in self.balances:
            self.balances.pop(token)
            self.save()
            publish(
                "portfolio_updated",
                PortfolioUpdated(
                    balances={t: p.amount for t, p in self.balances.items()}
                ),
            )

    def update_breakeven(
        self,
        token: str,
        *,
        breakeven_bps: float,
        components: Mapping[str, float] | None = None,
    ) -> None:
        pos = self.balances.get(token)
        if pos is None:
            return
        pos.breakeven_bps = float(breakeven_bps)
        pos.breakeven_components = dict(components or {})
        self.save()

    # analytics -----------------------------------------------------------
    def unrealized_pnl(self, prices: Dict[str, float]) -> float:
        pnl = 0.0
        for token, pos in self.balances.items():
            if token in prices:
                pnl += (prices[token] - pos.entry_price) * pos.amount
        return pnl

    def position_roi(self, token: str, price: float) -> float:
        """Return the return-on-investment for ``token`` at ``price``.

        The ROI is expressed as a fraction of the entry price.  If the token
        is not held in the portfolio or the entry price is zero, ``0.0`` is
        returned.
        """
        pos = self.balances.get(token)
        if pos is None or pos.entry_price == 0:
            return 0.0
        return (price - pos.entry_price) / pos.entry_price

    def total_value(self, prices: Dict[str, float]) -> float:
        """Return portfolio value using ``prices`` or entry prices."""
        value = 0.0
        for token, pos in self.balances.items():
            price = prices.get(token, pos.entry_price)
            value += pos.amount * price
        return value

    def update_drawdown(self, prices: Dict[str, float]) -> None:
        """Update maximum portfolio value for drawdown calculations."""
        value = self.total_value(prices)
        if value > self.max_value:
            self.max_value = value

    def current_drawdown(self, prices: Dict[str, float]) -> float:
        """Return current drawdown fraction based on ``prices``."""
        value = self.total_value(prices)
        if self.max_value == 0:
            self.max_value = value
            return 0.0
        return (self.max_value - value) / self.max_value

    def update_highs(self, prices: Dict[str, float]) -> None:
        """Update high water marks for held tokens."""
        for token, pos in self.balances.items():
            price = prices.get(token)
            if price is not None and price > pos.high_price:
                pos.high_price = price
        self.save()

    async def update_highs_async(self, prices: Dict[str, float]) -> None:
        for token, pos in self.balances.items():
            price = prices.get(token)
            if price is not None and price > pos.high_price:
                pos.high_price = price
        await self.save_async()

    # ------------------------------------------------------------------
    def record_prices(
        self, prices: Dict[str, float], *, window: int | None = None
    ) -> None:
        """Record latest prices for correlation tracking."""

        limit = window or self.history_window
        for token, price in prices.items():
            hist = self.price_history.setdefault(token, [])
            hist.append(float(price))
            if len(hist) > limit:
                del hist[0]

        if prices:
            self.update_highs(prices)
            self.update_drawdown(prices)

    def correlations(self) -> Dict[tuple[str, str], float]:
        """Return pairwise correlations between tracked tokens."""

        import numpy as np

        tokens = list(self.price_history.keys())
        corr: Dict[tuple[str, str], float] = {}
        for i, a in enumerate(tokens):
            seq_a = self.price_history[a]
            for b in tokens[i + 1 :]:
                seq_b = self.price_history[b]
                if len(seq_a) < 2 or len(seq_b) < 2:
                    continue
                n = min(len(seq_a), len(seq_b))
                arr_a = np.asarray(seq_a[-n:], dtype=float)
                arr_b = np.asarray(seq_b[-n:], dtype=float)
                if np.std(arr_a) == 0 or np.std(arr_b) == 0:
                    c = 0.0
                else:
                    c = float(np.corrcoef(arr_a, arr_b)[0, 1])
                corr[(a, b)] = c
        return corr

    def update_risk_metrics(self) -> None:
        """Compute and store portfolio-wide risk metrics."""

        if not self.price_history or not self.balances:
            self.risk_metrics = {}
            publish("risk_metrics", self.risk_metrics)
            return

        from .risk import (
            compute_live_covariance,
            compute_live_correlation,
            portfolio_variance,
            portfolio_cvar,
            portfolio_evar,
            average_correlation,
        )

        weights = self.weights()
        if not weights:
            self.risk_metrics = {}
            publish("risk_metrics", self.risk_metrics)
            return

        hist = {t: self.price_history.get(t, []) for t in weights}
        cov_matrix = compute_live_covariance(hist)
        corr_matrix = compute_live_correlation(hist)
        if cov_matrix.size == 0 or not getattr(cov_matrix, "shape", None):
            logger.debug("Risk metrics skipped: insufficient covariance data")
            return
        cov = portfolio_variance(cov_matrix, weights.values())
        cvar = portfolio_cvar(hist, weights)
        evar = portfolio_evar(hist, weights)
        corr = average_correlation(hist)

        self.risk_metrics = {
            "covariance": cov,
            "portfolio_cvar": cvar,
            "portfolio_evar": evar,
            "correlation": corr,
            "cov_matrix": cov_matrix,
            "corr_matrix": corr_matrix,
        }
        payload = {
            **self.risk_metrics,
            "cov_matrix": cov_matrix.tolist(),
            "corr_matrix": corr_matrix.tolist(),
        }
        publish("risk_metrics", payload)

    def hedged_weights(self, prices: Dict[str, float]) -> Dict[str, float]:
        """Return allocation weights adjusted for correlations."""

        weights = self.weights(prices)
        if not weights:
            return {}
        corrs = self.correlations()
        return hedge_allocation(weights, corrs)

    def trailing_stop_triggered(
        self, token: str, price: float, trailing: float
    ) -> bool:
        """Return ``True`` if ``price`` hits the trailing stop for ``token``."""
        pos = self.balances.get(token)
        if pos is None:
            return False
        if price > pos.high_price:
            pos.high_price = price
            self.save()
            return False
        return price <= pos.high_price * (1 - trailing)

    def percent_allocated(
        self, token: str, prices: Dict[str, float] | None = None
    ) -> float:
        """Return the fraction of portfolio value allocated to ``token``."""
        prices = prices or {}
        total = self.total_value(prices)
        if total <= 0:
            return 0.0
        pos = self.balances.get(token)
        if pos is None:
            return 0.0
        price = prices.get(token, pos.entry_price)
        return (pos.amount * price) / total

    def weights(self, prices: Dict[str, float] | None = None) -> Dict[str, float]:
        """Return portfolio allocation weights for each token."""

        prices = prices or {}
        total = self.total_value(prices)
        if total <= 0:
            return {}
        weights: Dict[str, float] = {}
        for token, pos in self.balances.items():
            price = prices.get(token, pos.entry_price)
            weights[token] = (pos.amount * price) / total
        return weights


def hedge_allocation(
    weights: Dict[str, float], correlations: Mapping[tuple[str, str], float]
) -> Dict[str, float]:
    """Return hedged weights using ``correlations`` between tokens."""

    hedged = dict(weights)
    for (a, b), corr in correlations.items():
        if a not in hedged or b not in hedged:
            continue
        c = max(-1.0, min(1.0, corr))
        if c > 0:
            adjustment = hedged[a] * c
            hedged[a] -= adjustment
            hedged[b] += adjustment
    return hedged


def calculate_order_size(
    balance: float,
    expected_roi: float,
    volatility: float = 0.0,
    drawdown: float = 0.0,
    *,
    risk_tolerance: float = 0.1,
    max_allocation: float = 0.2,
    max_risk_per_token: float = 0.1,
    max_drawdown: float = 1.0,
    volatility_factor: float = 1.0,
    gas_cost: float | None = None,
    current_allocation: float = 0.0,
    min_portfolio_value: float = 0.0,
    correlation: float | None = None,
    var: float | None = None,
    var_threshold: float | None = None,
    predicted_var: float | None = None,
    risk_manager: "RiskManager" | None = None,
    risk_metrics: Mapping[str, float] | None = None,
) -> float:
    """Return trade size based on ``balance`` and expected ROI.

    The position size grows with the expected return but is limited by the
    ``risk_tolerance`` fraction of the balance.  ``current_allocation``
    represents the portion of the portfolio already allocated to the token and
    ensures the total allocation never exceeds ``max_allocation``.  Negative or
    zero expected returns yield a size of ``0.0``.

    ``min_portfolio_value`` sets a lower bound on the balance used for sizing,
    preventing portfolios that dip below this value from generating trades that
    cannot cover network fees.  When ``var`` exceeds ``var_threshold`` the size
    is reduced proportionally.  ``predicted_var`` scales risk based on the
    forecast ROI variance, while ``risk_manager`` metrics can override
    ``risk_tolerance`` and ``max_allocation`` for dynamic control.
    """

    if balance <= 0 or expected_roi <= 0:
        return 0.0

    if drawdown >= max_drawdown:
        return 0.0

    if risk_manager is not None:
        try:
            rm_params = risk_manager.adjusted(
                drawdown,
                volatility,
                correlation=correlation,
                portfolio_metrics=risk_metrics,
            )
            risk_tolerance = rm_params.risk_tolerance
            max_allocation = rm_params.max_allocation
        except Exception:
            pass

    if correlation is None and risk_metrics is not None:
        correlation = risk_metrics.get("correlation")

    adj_risk = (
        risk_tolerance
        * (1 - drawdown / max_drawdown)
        / (1 + volatility * volatility_factor)
    )
    if correlation is not None:
        corr = max(-1.0, min(1.0, correlation))
        adj_risk *= max(0.0, 1 - corr)
    if predicted_var is not None and predicted_var > 0:
        adj_risk /= 1 + predicted_var

    fraction = expected_roi * adj_risk
    remaining = max_allocation - current_allocation
    max_fraction = min(max_risk_per_token, remaining)
    fraction = min(fraction, max_fraction)
    if fraction <= 0:
        return 0.0
    effective_balance = max(balance, min_portfolio_value)
    size = min(effective_balance * fraction, balance)
    if (
        var is not None
        and var_threshold is not None
        and var > var_threshold
        and var > 0
    ):
        size *= var_threshold / var
    if gas_cost is None:
        try:
            from .gas import get_current_fee

            gas_cost = get_current_fee()
        except Exception:
            gas_cost = 0.0
    if size <= gas_cost:
        return 0.0
    return size - gas_cost


def dynamic_order_size(
    balance: float,
    expected_roi: float,
    predicted_roi: float | None = None,
    volatility: float = 0.0,
    drawdown: float = 0.0,
    *,
    risk_tolerance: float = 0.1,
    max_allocation: float = 0.2,
    max_risk_per_token: float = 0.1,
    max_drawdown: float = 1.0,
    volatility_factor: float = 1.0,
    gas_cost: float | None = None,
    current_allocation: float = 0.0,
    min_portfolio_value: float = 0.0,
    correlation: float | None = None,
    var: float | None = None,
    var_threshold: float | None = None,
) -> float:
    """Return trade size using Kelly criterion with dynamic adjustments."""

    if balance <= 0:
        return 0.0

    roi = expected_roi
    if predicted_roi is not None:
        roi = (expected_roi + predicted_roi) / 2
    if roi <= 0:
        return 0.0

    if drawdown >= max_drawdown:
        return 0.0

    risk = volatility if volatility > 0 else 1.0
    kelly_fraction = roi / (risk * risk)

    adj_risk = (
        risk_tolerance
        * (1 - drawdown / max_drawdown)
        / (1 + volatility * volatility_factor)
    )
    if correlation is not None:
        corr = max(-1.0, min(1.0, correlation))
        adj_risk *= max(0.0, 1 - corr)

    fraction = kelly_fraction * adj_risk

    remaining = max_allocation - current_allocation
    max_fraction = min(max_risk_per_token, remaining)
    fraction = min(max(fraction, 0.0), max_fraction)
    if fraction <= 0:
        return 0.0

    effective_balance = max(balance, min_portfolio_value)
    size = min(effective_balance * fraction, balance)

    if (
        var is not None
        and var_threshold is not None
        and var > var_threshold
        and var > 0
    ):
        size *= var_threshold / var

    if gas_cost is None:
        try:
            from .gas import get_current_fee

            gas_cost = get_current_fee()
        except Exception:
            gas_cost = 0.0

    if size <= gas_cost:
        return 0.0
    return size - gas_cost
