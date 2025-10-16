from __future__ import annotations

import logging
import os
import re
from typing import AsyncGenerator

import aiohttp

from .jsonutil import loads
from .http import get_session
from .scanner_common import PHOENIX_WS_URL, METEORA_WS_URL

DEX_LISTING_WS_URL = os.getenv("DEX_LISTING_WS_URL", "")
PHOENIX_DEPTH_WS_URL = os.getenv("PHOENIX_DEPTH_WS_URL", PHOENIX_WS_URL)
METEORA_DEPTH_WS_URL = os.getenv("METEORA_DEPTH_WS_URL", METEORA_WS_URL)

logger = logging.getLogger(__name__)

# Base58-ish, 32–44 chars is the usual Solana pubkey/mint length range.
_ADDR_RE = re.compile(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$")


def _looks_like_address(s: object) -> bool:
    if not isinstance(s, str):
        return False
    return bool(_ADDR_RE.match(s))


async def stream_listed_tokens(
    url: str = DEX_LISTING_WS_URL,
) -> AsyncGenerator[str, None]:
    """
    Yield token addresses coming from a DEX/listing websocket.

    No name/suffix/keyword heuristics. If the payload contains an address-like
    field, it will be yielded as-is.
    """
    if not url:
        logger.debug("DEX_LISTING_WS_URL not set; stream_listed_tokens will be idle")
        return

    session = await get_session()
    async with session.ws_connect(url) as ws:
        async for msg in ws:
            if msg.type != aiohttp.WSMsgType.TEXT:
                continue
            try:
                data = loads(msg.data)
            except Exception:
                continue

            addr = None
            if isinstance(data, dict):
                addr = (
                    data.get("address")
                    or data.get("mint")
                    or data.get("id")
                    or data.get("token")
                )
            elif isinstance(data, list) and data:
                # Some feeds send single-item arrays
                item = data[0]
                if isinstance(item, dict):
                    addr = (
                        item.get("address")
                        or item.get("mint")
                        or item.get("id")
                        or item.get("token")
                    )

            if _looks_like_address(addr):
                yield addr  # type: ignore[misc]
