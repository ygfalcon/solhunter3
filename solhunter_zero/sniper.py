
from __future__ import annotations

import logging
import os
from typing import List, Dict, Any

# Import helpers via the main module so tests can monkeypatch them there
from . import main as main_module
from .portfolio import Portfolio, dynamic_order_size
from .agents.conviction import predict_price_movement
from .risk import RiskManager
from .simulation import SimulationResult as _SimRes

run_simulations = main_module.run_simulations
should_buy = main_module.should_buy
should_sell = main_module.should_sell
fetch_token_prices_async = main_module.fetch_token_prices_async

_limits_logged = False


async def evaluate(token: str, portfolio: Portfolio) -> List[Dict[str, Any]]:
    """Evaluate ``token`` and return buy or sell actions."""

    try:
        sims = run_simulations(token, count=100)
    except Exception:
        sims = [_SimRes(1.0, 1.0)]
    if not sims:
        return []

    prices = await fetch_token_prices_async(set(portfolio.balances.keys()) | {token})
    price = prices.get(token, 0.0)

    stop_loss = float(os.getenv("STOP_LOSS", "0") or 0)
    take_profit = float(os.getenv("TAKE_PROFIT", "0") or 0)
    trailing_stop = float(os.getenv("TRAILING_STOP", "0") or 0)

    pos = portfolio.balances.get(token)
    if pos:
        roi = portfolio.position_roi(token, price) if price else 0.0
        current_drawdown = None
        if prices:
            portfolio.update_drawdown(prices)
            current_drawdown = portfolio.current_drawdown(prices)

        trailing_val = trailing_stop or 0.0
        if trailing_val and price:
            portfolio.trailing_stop_triggered(token, price, trailing_val)

        max_drawdown_env = os.getenv("MAX_DRAWDOWN")
        try:
            max_drawdown_limit = float(max_drawdown_env) if max_drawdown_env else None
        except ValueError:
            max_drawdown_limit = None
        if max_drawdown_limit == 0:
            max_drawdown_limit = None

        if should_sell(
            sims,
            trailing_stop=trailing_stop or None,
            current_price=price if price else None,
            high_price=pos.high_price,
            realized_roi=roi,
            take_profit=take_profit or None,
            stop_loss=stop_loss or None,
            current_drawdown=current_drawdown,
            max_drawdown=max_drawdown_limit,
        ):
            return [{"token": token, "side": "sell", "amount": pos.amount, "price": price}]

    actions: List[Dict[str, Any]] = []
    if should_buy(sims):
        portfolio.update_drawdown(prices)
        drawdown = portfolio.current_drawdown(prices)
        avg_roi = sum(r.expected_roi for r in sims) / len(sims)
        volatility = getattr(sims[0], "volatility", 0.0)

        rm = RiskManager(
            risk_tolerance=float(os.getenv("RISK_TOLERANCE", "0.1")),
            max_allocation=float(os.getenv("MAX_ALLOCATION", "0.2")),
            max_risk_per_token=float(os.getenv("MAX_RISK_PER_TOKEN", "0.1")),
            max_drawdown=float(os.getenv("MAX_DRAWDOWN", "1.0")),
            volatility_factor=float(os.getenv("VOLATILITY_FACTOR", "1.0")),
            risk_multiplier=float(os.getenv("RISK_MULTIPLIER", "1.0")),
            min_portfolio_value=float(os.getenv("MIN_PORTFOLIO_VALUE", "20")),
        )

        global _limits_logged
        if not _limits_logged:
            logging.getLogger(__name__).info(
                "Risk limits: risk_tolerance=%s, max_allocation=%s, max_risk_per_token=%s, max_drawdown=%s, "
                "volatility_factor=%s, risk_multiplier=%s, min_portfolio_value=%s",
                rm.risk_tolerance,
                rm.max_allocation,
                rm.max_risk_per_token,
                rm.max_drawdown,
                rm.volatility_factor,
                rm.risk_multiplier,
                rm.min_portfolio_value,
            )
            _limits_logged = True

        balance = portfolio.total_value(prices)
        adj = rm.adjusted(
            drawdown=drawdown,
            volatility=volatility,
            volume_spike=getattr(sims[0], "volume_spike", 1.0),
            depth_change=getattr(sims[0], "depth_change", 0.0),
            whale_activity=getattr(sims[0], "whale_activity", 0.0),
            tx_rate=getattr(sims[0], "tx_rate", 1.0),
            portfolio_value=balance,
        )

        allocation = portfolio.percent_allocated(token, prices)

        try:
            pred_roi = predict_price_movement(token)
        except Exception:
            pred_roi = 0.0

        amount = dynamic_order_size(
            balance,
            avg_roi,
            pred_roi,
            volatility,
            drawdown,
            risk_tolerance=adj.risk_tolerance,
            max_allocation=adj.max_allocation,
            max_risk_per_token=adj.max_risk_per_token,
            max_drawdown=adj.max_drawdown,
            volatility_factor=adj.volatility_factor,
            current_allocation=allocation,
            min_portfolio_value=adj.min_portfolio_value,
        )
        if amount > 0:
            actions.append({"token": token, "side": "buy", "amount": amount, "price": price})

    return actions
