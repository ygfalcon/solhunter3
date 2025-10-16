from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator, Dict

from .prices import fetch_token_prices_async
from .simulation import fetch_token_metrics_async

logger = logging.getLogger(__name__)

async def stream_market_data(
    token: str,
    *,
    poll_interval: float = 5.0,
) -> AsyncGenerator[Dict[str, float], None]:
    """Yield current price, volume and slippage for ``token``.

    The function polls the REST APIs for fresh metrics. It can be used to
    monitor rapid market changes when websocket data is unavailable.
    """

    while True:
        metrics, prices = await asyncio.gather(
            fetch_token_metrics_async(token),
            fetch_token_prices_async([token]),
        )
        price = prices.get(token, 0.0)
        volume = float(metrics.get("volume", 0.0))
        slippage = float(metrics.get("slippage", 0.0))
        yield {"price": price, "volume": volume, "slippage": slippage}
        await asyncio.sleep(poll_interval)
