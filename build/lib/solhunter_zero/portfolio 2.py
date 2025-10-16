from __future__ import annotations
from .jsonutil import loads, dumps
import os
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Mapping
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
    breakeven_price: float = 0.0
    breakeven_bps: float = 0.0
    lifecycle: str = "open"
    attribution: Dict[str, Any] = field(default_factory=dict)
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    last_mark: float = 0.0


@dataclass
class Portfolio:
    path: Optional[str] = "portfolio.json"
    balances: Dict[str, Position] = field(default_factory=dict)
    max_value: float = 0.0
    price_history: Dict[str, list[float]] = field(default_factory=dict)
    history_window: int = 50
    risk_metrics: Dict[str, float] = field(default_factory=dict)
    closed_positions: Dict[str, Position] = field(default_factory=dict, init=False)

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
        self.balances = {}
        for token, info in data.items():
            if not isinstance(info, dict):
                continue
            amount = float(info.get("amount", 0.0))
            entry_price = float(info.get("entry_price", 0.0))
            high_price = float(info.get("high_price", entry_price))
            breakeven = float(info.get("breakeven_price", entry_price))
            breakeven_bps = float(info.get("breakeven_bps", 0.0))
            lifecycle = str(info.get("lifecycle", "open"))
            attribution = info.get("attribution")
            if not isinstance(attribution, dict):
                attribution = {}
            realized = float(info.get("realized_pnl", 0.0))
            unrealized = float(info.get("unrealized_pnl", 0.0))
            last_mark = float(info.get("last_mark", entry_price))
            self.balances[token] = Position(
                token=token,
                amount=amount,
                entry_price=entry_price,
                high_price=high_price,
                breakeven_price=breakeven,
                breakeven_bps=breakeven_bps,
                lifecycle=lifecycle,
                attribution=attribution,
                realized_pnl=realized,
                unrealized_pnl=unrealized,
                last_mark=last_mark,
            )

    def save(self) -> None:
        if not self.path:
            return
        data = {
            token: {
                "amount": pos.amount,
                "entry_price": pos.entry_price,
                "high_price": pos.high_price,
                "breakeven_price": pos.breakeven_price,
                "breakeven_bps": pos.breakeven_bps,
                "lifecycle": pos.lifecycle,
                "attribution": pos.attribution,
                "realized_pnl": pos.realized_pnl,
                "unrealized_pnl": pos.unrealized_pnl,
                "last_mark": pos.last_mark,
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
                "breakeven_price": pos.breakeven_price,
                "breakeven_bps": pos.breakeven_bps,
                "lifecycle": pos.lifecycle,
                "attribution": pos.attribution,
                "realized_pnl": pos.realized_pnl,
                "unrealized_pnl": pos.unrealized_pnl,
                "last_mark": pos.last_mark,
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
    def add(self, token: str, amount: float, price: float) -> None:
        self.update(token, amount, price)

    async def add_async(self, token: str, amount: float, price: float) -> None:
        await self.update_async(token, amount, price)

    def _ensure_attribution(self, pos: Position) -> None:
        entries = pos.attribution.get("entries")
        if not isinstance(entries, list):
            pos.attribution["entries"] = []
        exits = pos.attribution.get("exits")
        if not isinstance(exits, list):
            pos.attribution["exits"] = []


    def _compute_breakeven_bps(
        self, amount: float, price: float, meta: Optional[Dict[str, Any]]
    ) -> float:
        if amount <= 0 or price <= 0:
            return 0.0
        costs: Dict[str, Any] = {}
        if isinstance(meta, dict):
            raw = meta.get("exit_costs") if isinstance(meta.get("exit_costs"), dict) else meta
            if isinstance(raw, dict):
                costs = raw
        total = 0.0
        for key in ("fees_bps", "latency_bps", "impact_bps"):
            value = costs.get(key)
            if value is None:
                continue
            try:
                total += max(float(value), 0.0)
            except (TypeError, ValueError):
                continue
        if total <= 0 and isinstance(meta, dict):
            hint = meta.get("breakeven_bps")
            try:
                if hint is not None:
                    total = max(float(hint), 0.0)
            except (TypeError, ValueError):
                total = 0.0
        return total

    def _refresh_breakeven(
        self, pos: Position, meta: Optional[Dict[str, Any]]
    ) -> None:
        updated = self._compute_breakeven_bps(pos.amount, pos.entry_price, meta)
        if updated > 0:
            pos.breakeven_bps = updated

    def _mark_position(self, pos: Position, price: float) -> None:
        pos.last_mark = price
        if pos.amount == 0:
            pos.unrealized_pnl = 0.0
            pos.breakeven_price = price if price else pos.entry_price
            return
        pos.unrealized_pnl = (price - pos.entry_price) * pos.amount
        pos.breakeven_price = pos.entry_price
        if pos.amount != 0:
            pos.breakeven_price = pos.entry_price - (pos.realized_pnl / pos.amount)

    def _apply_update(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        ts: float,
        reason: str = "trade",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        pos = self.balances.get(token)
        if pos is None:
            self.closed_positions.pop(token, None)
            attribution = {
                "entries": [
                    {
                        "ts": ts,
                        "amount": amount,
                        "price": price,
                        "reason": reason,
                        "meta": meta or {},
                    }
                ],
                "exits": [],
            }
            self.balances[token] = Position(
                token=token,
                amount=amount,
                entry_price=price if amount != 0 else 0.0,
                high_price=price,
                breakeven_price=price,
                breakeven_bps=self._compute_breakeven_bps(amount, price, meta),
                lifecycle="open",
                attribution=attribution,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                last_mark=price,
            )
            self._mark_position(self.balances[token], price)
            return

        self._ensure_attribution(pos)
        new_amount = pos.amount + amount
        if amount > 0:
            total_cost = pos.amount * pos.entry_price + amount * price
            if new_amount > 0:
                pos.entry_price = total_cost / new_amount
            pos.attribution["entries"].append(
                {
                    "ts": ts,
                    "amount": amount,
                    "price": price,
                    "reason": reason,
                    "meta": meta or {},
                }
            )
            pos.lifecycle = "open" if new_amount >= 0 else "reversed"
            self._refresh_breakeven(pos, meta)
        elif amount < 0:
            exit_qty = min(-amount, pos.amount) if pos.amount > 0 else -amount
            realized = exit_qty * (price - pos.entry_price)
            pos.realized_pnl += realized
            pos.attribution["exits"].append(
                {
                    "ts": ts,
                    "amount": -exit_qty,
                    "price": price,
                    "reason": reason,
                    "meta": meta or {},
                    "pnl": realized,
                }
            )
            if new_amount > 0:
                pos.lifecycle = "reducing"
            elif new_amount == 0:
                pos.lifecycle = "closed"
            else:
                pos.lifecycle = "reversed"
            self._refresh_breakeven(pos, meta)
        else:
            if price > pos.high_price:
                pos.high_price = price
            self._mark_position(pos, price)
            return

        if new_amount <= 0:
            pos.amount = max(new_amount, 0.0)
            if price > pos.high_price:
                pos.high_price = price
            self._mark_position(pos, price)
            self.closed_positions[token] = pos
            self.balances.pop(token, None)
            return

        pos.amount = new_amount
        if price > pos.high_price:
            pos.high_price = price
        self._mark_position(pos, price)

    def update(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        ts: float | None = None,
        reason: str = "trade",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._apply_update(token, amount, price, ts=ts or time.time(), reason=reason, meta=meta)
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
        ts: float | None = None,
        reason: str = "trade",
        meta: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._apply_update(token, amount, price, ts=ts or time.time(), reason=reason, meta=meta)
        await self.save_async()
        publish(
            "portfolio_updated",
            PortfolioUpdated(
                balances={t: p.amount for t, p in self.balances.items()}
            ),
        )

    def remove(self, token: str) -> None:
        pos = self.balances.pop(token, None)
        if pos is not None:
            pos.amount = 0.0
            pos.lifecycle = "closed"
            self._mark_position(pos, pos.last_mark or pos.entry_price)
            self.closed_positions[token] = pos
            self.save()
            publish(
                "portfolio_updated",
                PortfolioUpdated(
                    balances={t: p.amount for t, p in self.balances.items()}
                ),
            )

    def get_position(self, token: str) -> Optional[Position]:
        pos = self.balances.get(token)
        if pos is not None:
            return pos
        return self.closed_positions.get(token)

    def mark_to_market(
        self, prices: Dict[str, float], *, persist: bool = False
    ) -> Dict[str, Dict[str, float]]:
        snapshots: Dict[str, Dict[str, float]] = {}
        for token, price in prices.items():
            pos = self.balances.get(token)
            if pos is None:
                continue
            self._mark_position(pos, price)
            snapshots[token] = {
                "breakeven_price": pos.breakeven_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "lifecycle": pos.lifecycle,
            }
        if persist and snapshots:
            self.save()
        return snapshots

    def apply_exit_slice(
        self,
        token: str,
        amount: float,
        price: float,
        *,
        ts: float | None = None,
        reason: str = "exit_slice",
        meta: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, float]:
        if amount >= 0:
            raise ValueError("exit slices must reduce position size")
        self.update(token, amount, price, ts=ts, reason=reason, meta=meta)
        pos = self.get_position(token)
        if pos is None:
            return {
                "realized_pnl": 0.0,
                "unrealized_pnl": 0.0,
                "breakeven_price": price,
                "breakeven_bps": 0.0,
            }
        return {
            "realized_pnl": pos.realized_pnl,
            "unrealized_pnl": pos.unrealized_pnl,
            "breakeven_price": pos.breakeven_price,
            "breakeven_bps": pos.breakeven_bps,
        }

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
            self.mark_to_market(prices)
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
