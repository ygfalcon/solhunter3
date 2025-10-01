"""Scan new token mints via Solana websocket logs."""

from __future__ import annotations

import asyncio
import logging
import re
from typing import AsyncGenerator, Iterable

from .http import get_session

from solders.pubkey import Pubkey
import json as _json
try:
    import websockets  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    websockets = None
try:
    # Optional solana-py client; we will prefer raw websockets for compatibility
    from solana.rpc.websocket_api import (
        RpcTransactionLogsFilterMentions,
        connect,
    )
except Exception:  # pragma: no cover - optional dependency
    RpcTransactionLogsFilterMentions = None  # type: ignore
    async def connect(*args, **kwargs):  # type: ignore
        raise RuntimeError("solana.rpc.websocket_api not available")

from .scanner_onchain import TOKEN_PROGRAM_ID
from .dex_scanner import DEX_PROGRAM_ID

from .scanner_common import (
    TOKEN_SUFFIX,
    TOKEN_KEYWORDS,
    JUPITER_WS_URL,
    token_matches,
)



logger = logging.getLogger(__name__)

NAME_RE = re.compile(r"name:\s*(\S+)", re.IGNORECASE)
MINT_RE = re.compile(r"mint:\s*(\S+)", re.IGNORECASE)

# Regex used to capture token mints from liquidity pool creation logs
POOL_TOKEN_RE = re.compile(r"token[AB]:\s*([A-Za-z0-9]{32,44})", re.IGNORECASE)

# Whether to subscribe to pool creation logs as well as mint events
include_pools = False


def _to_ws_url(url: str) -> str:
    if url.startswith("http://"):
        return "ws://" + url[len("http://"):]
    if url.startswith("https://"):
        return "wss://" + url[len("https://"):]
    return url


async def _stream_new_tokens_raw(
    rpc_url: str,
    *,
    suffix: str | None = None,
    keywords: Iterable[str] | None = None,
    include_pools: bool = True,
) -> AsyncGenerator[str, None]:
    """Raw websockets logsSubscribe compatible with stricter public RPCs."""
    if websockets is None:
        raise RuntimeError("websockets package is required for raw logsSubscribe")
    url = _to_ws_url(rpc_url)
    token_prog = str(TOKEN_PROGRAM_ID)
    dex_prog = str(DEX_PROGRAM_ID)

    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                sub_ids = []
                def _sub_msg(prog: str, use_commitment: bool) -> str:
                    params = [{"mentions": [prog]}]
                    if use_commitment:
                        params.append({"commitment": "processed"})
                    return _json.dumps({"jsonrpc": "2.0", "id": 1, "method": "logsSubscribe", "params": params})

                # Try with commitment then without
                try:
                    await ws.send(_sub_msg(token_prog, True))
                    if include_pools:
                        await ws.send(_sub_msg(dex_prog, True))
                except Exception:
                    await ws.send(_sub_msg(token_prog, False))
                    if include_pools:
                        await ws.send(_sub_msg(dex_prog, False))

                while True:
                    try:
                        msg = await ws.recv()
                    except asyncio.CancelledError:
                        return
                    except Exception as exc:
                        logger.error("Raw websocket error: %s", exc)
                        await asyncio.sleep(1)
                        break
                    try:
                        data = _json.loads(msg)
                    except Exception:
                        continue
                    if not isinstance(data, dict):
                        continue
                    params = data.get("params") or {}
                    result = params.get("result") if isinstance(params, dict) else None
                    value = result.get("value") if isinstance(result, dict) else None
                    logs = value.get("logs") if isinstance(value, dict) else None
                    if not isinstance(logs, list):
                        continue
                    tokens = set()
                    name = None
                    mint = None
                    if any("InitializeMint" in l for l in logs):
                        for line in logs:
                            if name is None:
                                m = NAME_RE.search(line)
                                if m:
                                    name = m.group(1)
                            if mint is None:
                                m = MINT_RE.search(line)
                                if m:
                                    mint = m.group(1)
                        if name and mint and token_matches(mint, name, suffix=suffix, keywords=keywords):
                            tokens.add(mint)
                    if include_pools:
                        for line in logs:
                            m = POOL_TOKEN_RE.search(line)
                            if m:
                                tok = m.group(1)
                                if token_matches(tok, None, suffix=suffix, keywords=keywords):
                                    tokens.add(tok)
                    for t in tokens:
                        yield t
        except Exception as exc:
            logger.error("Raw websocket connect failed: %s", exc)
            await asyncio.sleep(1)


async def stream_new_tokens(
    rpc_url: str,
    *,
    suffix: str | None = None,
    keywords: Iterable[str] | None = None,
    include_pools: bool = True,
) -> AsyncGenerator[str, None]:
    """Yield new token mint addresses passing configured filters.


    Parameters
    ----------
    rpc_url:
        Websocket endpoint of a Solana RPC node.
    suffix:
        Token name suffix to filter on. Case-insensitive.
    """

    if not rpc_url:
        if False:
            yield None


        return

    if suffix is None:
        suffix = TOKEN_SUFFIX
    if keywords is None:
        keywords = TOKEN_KEYWORDS
    suffix = suffix.lower() if suffix else None

    # Prefer robust raw subscription for better public RPC compatibility
    try:
        async for t in _stream_new_tokens_raw(rpc_url, suffix=suffix, keywords=keywords, include_pools=include_pools):
            yield t
    except Exception:
        # Fallback to solana-py websocket client if available
        if connect is None or RpcTransactionLogsFilterMentions is None:
            raise
        async with connect(rpc_url) as ws:
            try:
                await ws.logs_subscribe(
                    RpcTransactionLogsFilterMentions(Pubkey.from_string(str(TOKEN_PROGRAM_ID)))
                )
                if include_pools:
                    await ws.logs_subscribe(
                        RpcTransactionLogsFilterMentions(Pubkey.from_string(str(DEX_PROGRAM_ID)))
                    )
            except Exception as exc:
                logger.error("solana-py logs_subscribe failed: %s", exc)
                return
            while True:
                try:
                    msgs = await ws.recv()
                except Exception as exc:
                    logger.error("Websocket error: %s", exc)
                    await asyncio.sleep(1)
                    continue
                for msg in msgs:
                    try:
                        logs = msg.result.value.logs  # type: ignore[attr-defined]
                    except Exception:
                        try:
                            logs = msg["result"]["value"]["logs"]
                        except Exception:
                            continue
                    tokens = set()
                    name = None
                    mint = None
                    if any("InitializeMint" in l for l in logs):
                        for line in logs:
                            if name is None:
                                m = NAME_RE.search(line)
                                if m:
                                    name = m.group(1)
                            if mint is None:
                                m = MINT_RE.search(line)
                                if m:
                                    mint = m.group(1)
                        if name and mint and token_matches(mint, name, suffix=suffix, keywords=keywords):
                            tokens.add(mint)
                    if include_pools:
                        for line in logs:
                            m = POOL_TOKEN_RE.search(line)
                            if m:
                                tok = m.group(1)
                                if token_matches(tok, None, suffix=suffix, keywords=keywords):
                                    tokens.add(tok)
                    for token in tokens:
                        yield token


async def stream_jupiter_tokens(
    url: str = JUPITER_WS_URL,
    *,
    suffix: str | None = None,
    keywords: Iterable[str] | None = None,
) -> AsyncGenerator[str, None]:
    """Yield tokens from the Jupiter aggregator websocket."""

    if suffix is None:
        suffix = TOKEN_SUFFIX
    if keywords is None:
        keywords = TOKEN_KEYWORDS

    import aiohttp

    session = await get_session()
    async with session.ws_connect(url) as ws:
            async for msg in ws:
                try:
                    data = msg.json()
                except Exception:  # pragma: no cover - malformed message
                    continue
                addr = (
                    data.get("address")
                    or data.get("mint")
                    or data.get("id")
                )
                name = data.get("name") or data.get("symbol")
                vol = data.get("volume") or data.get("volume_24h")
                if addr and token_matches(addr, name, vol, suffix=suffix, keywords=keywords):
                    yield addr
