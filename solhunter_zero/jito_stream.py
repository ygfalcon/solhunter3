from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator, Dict, Any

import aiohttp
from .http import get_session

from .event_bus import publish

logger = logging.getLogger(__name__)


async def stream_pending_transactions(
    url: str,
    *,
    auth: str | None = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Yield pending transactions from Jito's searcher websocket.

    Parameters
    ----------
    url:
        WebSocket endpoint of the Jito searcher service.
    auth:
        Optional authentication token.
    """

    headers = {"Authorization": auth} if auth else None

    backoff = 1.0
    max_backoff = 30
    while True:
        try:
            session = await get_session()
            async with session.ws_connect(url, headers=headers) as ws:
                    backoff = 1.0
                    try:  # attempt protocol init for GraphQL-style feeds
                        await ws.send_json(
                            {
                                "type": "start",
                                "payload": {
                                    "query": "subscription{pendingTransactions}"
                                },
                            }
                        )
                    except Exception:
                        pass
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except Exception:  # pragma: no cover - invalid message
                                continue
                            yield data
                        elif msg.type in (
                            aiohttp.WSMsgType.CLOSE,
                            aiohttp.WSMsgType.CLOSED,
                            aiohttp.WSMsgType.ERROR,
                        ):
                            break
        except asyncio.CancelledError:
            break
        except Exception as exc:  # pragma: no cover - network errors
            logger.error("Jito stream error: %s", exc)
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)


async def stream_pending_swaps(
    url: str,
    *,
    auth: str | None = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Yield swap events from Jito's ``pendingTransactions`` feed."""

    async for data in stream_pending_transactions(url, auth=auth):
        txs = data.get("pendingTransactions") or data.get("data")
        if not txs:
            txs = [data]
        if isinstance(txs, dict):
            txs = [txs]
        for tx in txs:
            try:
                swap = tx.get("swap") or tx
                token = swap.get("token") or swap.get("address")
                size = float(swap.get("size", swap.get("amount", 0.0)))
                slip = float(swap.get("slippage", 0.0))
                if token:
                    swap_event = {
                        "token": token,
                        "address": token,
                        "size": size,
                        "slippage": slip,
                    }
                    publish("pending_swap", swap_event)
                    yield swap_event
            except Exception as exc:  # pragma: no cover - malformed message
                logger.error("Failed to parse pending swap: %s", exc)
