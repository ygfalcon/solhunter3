from __future__ import annotations

import logging
import os
from typing import AsyncGenerator, Dict, Any, Optional

import aiohttp
from .http import get_session, loads
from .util import install_uvloop

from .simulation import run_simulations
from .decision import should_buy, should_sell
from .portfolio import Portfolio, calculate_order_size
from .memory import Memory
from .prices import fetch_token_prices_async
from .exchange import place_order_async

logger = logging.getLogger(__name__)
install_uvloop()


async def subscribe_events(url: str) -> AsyncGenerator[Dict[str, Any], None]:
    """Yield parsed JSON events from a websocket ``url``."""
    session = await get_session()
    async with session.ws_connect(url) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        data = loads(msg.data)
                    except Exception:  # pragma: no cover - invalid data
                        continue
                    yield data
                elif msg.type in (
                    aiohttp.WSMsgType.CLOSE,
                    aiohttp.WSMsgType.CLOSED,
                    aiohttp.WSMsgType.ERROR,
                ):
                    break


async def listen_and_trade(
    url: str,
    memory: Memory,
    portfolio: Portfolio,
    *,
    volume_threshold: float = 0.0,
    liquidity_threshold: float = 0.0,
    testnet: bool = False,
    dry_run: bool = False,
    keypair=None,
    max_events: Optional[int] = None,
) -> None:
    """Listen to market websocket events and react with trades."""

    count = 0
    async for event in subscribe_events(url):
        token = event.get("token")
        if not token:
            continue

        volume = float(event.get("volume", 0.0))
        liquidity = float(event.get("liquidity", 0.0))
        slippage = event.get("slippage")

        if volume < volume_threshold and liquidity < liquidity_threshold:
            count += 1
            if max_events is not None and count >= max_events:
                break
            continue

        sims = run_simulations(
            token,
            count=100,
            recent_volume=volume,
            recent_slippage=slippage,
        )
        if should_buy(sims):
            price_lookup = await fetch_token_prices_async(portfolio.balances.keys())
            portfolio.update_drawdown(price_lookup)
            drawdown = portfolio.current_drawdown(price_lookup)
            avg_roi = sum(r.expected_roi for r in sims) / len(sims)
            volatility = getattr(sims[0], "volatility", 0.0) if sims else 0.0
            if price_lookup:
                balance = portfolio.total_value(price_lookup)
                alloc = portfolio.percent_allocated(token, price_lookup)
            else:
                balance = sum(p.amount for p in portfolio.balances.values()) or 1.0
                alloc = portfolio.percent_allocated(token)
            risk_tolerance = float(os.getenv("RISK_TOLERANCE", "0.1"))
            max_alloc = float(os.getenv("MAX_ALLOCATION", "0.2"))
            max_risk = float(os.getenv("MAX_RISK_PER_TOKEN", "0.1"))

            amount = calculate_order_size(
                balance,
                avg_roi,
                volatility,
                drawdown,
                risk_tolerance=risk_tolerance,
                max_allocation=max_alloc,
                max_risk_per_token=max_risk,
                current_allocation=alloc,
            )
            await place_order_async(
                token,
                side="buy",
                amount=amount,
                price=0,
                testnet=testnet,
                dry_run=dry_run,
                keypair=keypair,
            )
            if not dry_run:
                await memory.log_trade(token=token, direction="buy", amount=amount, price=0)
                await portfolio.update_async(token, amount, 0)

        if should_sell(
            sims,
            trailing_stop=None,
            current_price=None,
            high_price=(
                portfolio.balances.get(token).high_price
                if token in portfolio.balances
                else 0.0
            ),
        ):
            pos = portfolio.balances.get(token)
            if pos:
                await place_order_async(
                    token,
                    side="sell",
                    amount=pos.amount,
                    price=0,
                    testnet=testnet,
                    dry_run=dry_run,
                    keypair=keypair,
                )
                if not dry_run:
                    await memory.log_trade(
                        token=token, direction="sell", amount=pos.amount, price=0
                    )
                    await portfolio.update_async(token, -pos.amount, 0)

        count += 1
        if max_events is not None and count >= max_events:
            break
