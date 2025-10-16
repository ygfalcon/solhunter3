"""Scan new token mints via Solana websocket logs."""

from __future__ import annotations

import asyncio
import inspect
import logging
import os
import re
from typing import AsyncGenerator, Iterable, Any, Dict, List

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
    connect = None  # type: ignore

HAVE_SOLANA_WS = connect is not None and RpcTransactionLogsFilterMentions is not None


def _have_solana_ws() -> bool:
    return connect is not None

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

TRAILING_STRIP = " \t\r\n'\".,;:)]}>"

SCAN_WS_PING_INTERVAL = float(os.getenv("SCAN_WS_PING_INTERVAL", "20") or 20.0)
SCAN_WS_PING_TIMEOUT = float(os.getenv("SCAN_WS_PING_TIMEOUT", "20") or 20.0)
SCAN_WS_RETRY_BASE = max(0.1, float(os.getenv("SCAN_WS_RETRY_BASE", "0.8") or 0.8))
SCAN_WS_RETRY_MAX = max(SCAN_WS_RETRY_BASE, float(os.getenv("SCAN_WS_RETRY_MAX", "8") or 8.0))

_TOKEN_MATCHES_SIG = None
try:  # pragma: no cover - defensive
    _TOKEN_MATCHES_SIG = inspect.signature(token_matches)
except Exception:  # pragma: no cover - optional dependency quirks
    _TOKEN_MATCHES_SIG = None


def _clean_capture(value: str | None) -> str | None:
    if value is None:
        return None
    return value.strip().strip(TRAILING_STRIP)


def _token_matches(
    address: str,
    name: str | None,
    *,
    suffix: str | None,
    keywords: Iterable[str] | None,
    volume: Any | None = None,
) -> bool:
    if not address:
        return False

    params = _TOKEN_MATCHES_SIG.parameters if _TOKEN_MATCHES_SIG else {}
    kwargs: Dict[str, Any] = {}
    supports_suffix = "suffix" in params
    supports_keywords = "keywords" in params
    supports_volume = "volume" in params
    supports_name = "name" in params

    if supports_suffix and suffix is not None:
        kwargs["suffix"] = suffix
    if supports_keywords and keywords is not None:
        kwargs["keywords"] = keywords
    if supports_volume and volume is not None:
        kwargs["volume"] = volume
    if supports_name and name is not None:
        kwargs["name"] = name

    match: bool | None = None
    try:
        match = bool(token_matches(address, **kwargs))
    except TypeError:
        match = None

    if match is None:
        patterns: List[str] | None = None
        if suffix or keywords:
            patterns = []
            if suffix:
                patterns.append(str(suffix))
            if keywords:
                patterns.extend(str(k) for k in keywords)
        token_info: Any
        if name is None:
            token_info = address
        else:
            token_info = {"symbol": name, "name": name}
        try:
            match = bool(token_matches(token_info, patterns))
        except Exception:  # pragma: no cover - defensive
            match = None

    if match is None:
        match = True

    return bool(match)


def _extract_logs_from_notification(message: Any) -> List[str] | None:
    if isinstance(message, dict):
        params = message.get("params")
        if isinstance(params, dict):
            result = params.get("result")
        else:
            result = None
        if isinstance(result, dict):
            value = result.get("value")
        else:
            value = None
        if isinstance(value, dict):
            logs = value.get("logs")
        else:
            logs = None
    else:
        return None
    if not isinstance(logs, list):
        return None
    clean_logs: List[str] = []
    for line in logs:
        if isinstance(line, str):
            clean_logs.append(line)
        elif isinstance(line, bytes):
            clean_logs.append(line.decode("utf-8", "ignore"))
    return clean_logs if clean_logs else None


def _extract_logs_from_client_message(message: Any) -> List[str] | None:
    result = getattr(message, "result", None)
    if result is not None:
        value = getattr(result, "value", None)
    else:
        value = None
    logs = getattr(value, "logs", None) if value is not None else None
    if logs is None and isinstance(message, dict):
        params = message.get("result") or {}
        if isinstance(params, dict):
            value = params.get("value")
            if isinstance(value, dict):
                logs = value.get("logs")
    if not isinstance(logs, list):
        return None
    clean_logs: List[str] = []
    for line in logs:
        if isinstance(line, str):
            clean_logs.append(line)
        elif isinstance(line, bytes):
            clean_logs.append(line.decode("utf-8", "ignore"))
    return clean_logs if clean_logs else None


def _tokens_from_logs(
    logs: List[str],
    *,
    include_pools: bool,
    suffix: str | None,
    keywords: Iterable[str] | None,
) -> List[str]:
    tokens: List[str] = []
    has_initialize = any("InitializeMint" in line for line in logs)
    if has_initialize:
        name: str | None = None
        mint: str | None = None
        for line in logs:
            if name is None:
                match = NAME_RE.search(line)
                if match:
                    name = _clean_capture(match.group(1))
            if mint is None:
                match = MINT_RE.search(line)
                if match:
                    mint = _clean_capture(match.group(1))
            if name and mint:
                break
        if name and mint and _token_matches(mint, name, suffix=suffix, keywords=keywords):
            tokens.append(mint)

    if include_pools:
        for line in logs:
            match = POOL_TOKEN_RE.search(line)
            if not match:
                continue
            candidate = _clean_capture(match.group(1))
            if not candidate:
                continue
            if _token_matches(candidate, None, suffix=suffix, keywords=keywords):
                tokens.append(candidate)

    if not tokens:
        return []
    seen = set()
    unique: List[str] = []
    for token in tokens:
        if token and token not in seen:
            seen.add(token)
            unique.append(token)
    return unique


def _to_ws_url(url: str) -> str:
    if not url:
        return url
    trimmed = url.strip()
    lowered = trimmed.lower()
    if lowered.startswith("ws://") or lowered.startswith("wss://"):
        return trimmed
    if lowered.startswith("http://"):
        return "ws://" + trimmed[7:]
    if lowered.startswith("https://"):
        return "wss://" + trimmed[8:]
    if "://" not in trimmed:
        return "wss://" + trimmed.lstrip("/")
    return trimmed


class _WebsocketContext:
    def __init__(self, connect_obj: Any):
        self._connect_obj = connect_obj
        self._ws = None
        self._is_cm = hasattr(connect_obj, "__aenter__") and hasattr(connect_obj, "__aexit__")

    async def __aenter__(self) -> Any:
        if self._is_cm:
            self._ws = await self._connect_obj.__aenter__()
        else:
            self._ws = await self._connect_obj
        return self._ws

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._is_cm:
            await self._connect_obj.__aexit__(exc_type, exc, tb)
            return
        ws = self._ws
        if ws is None:
            return
        close = getattr(ws, "close", None)
        if close:
            result = close()
            if inspect.isawaitable(result):
                await result


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
    backoff = SCAN_WS_RETRY_BASE
    had_success = False

    async def _subscribe(ws: Any, program: str) -> List[Any]:
        pending: List[Any] = []
        message_id = 0

        def _sub_msg(use_commitment: bool) -> str:
            nonlocal message_id
            message_id += 1
            params: List[Any] = [{"mentions": [program]}]
            if use_commitment:
                params.append({"commitment": "processed"})
            return _json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": message_id,
                    "method": "logsSubscribe",
                    "params": params,
                }
            )

        for use_commitment in (True, False):
            try:
                await ws.send(_sub_msg(use_commitment))
                try:
                    ack_raw = await asyncio.wait_for(ws.recv(), timeout=5)
                except asyncio.TimeoutError:
                    logger.debug("No subscription ack received for %s", program)
                    break
                except StopAsyncIteration:
                    logger.debug("Subscription stream ended for %s", program)
                    raise RuntimeError("subscription closed")
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive
                if use_commitment:
                    logger.debug(
                        "Subscription attempt with commitment failed for %s: %s",
                        program,
                        exc,
                    )
                    continue
                raise
            else:
                if isinstance(ack_raw, bytes):
                    ack_raw = ack_raw.decode("utf-8", "ignore")
                try:
                    ack_data = _json.loads(ack_raw)
                except Exception:
                    ack_data = None
                if isinstance(ack_data, dict) and "error" in ack_data:
                    if use_commitment:
                        logger.debug(
                            "Subscription with commitment rejected for %s: %s",
                            program,
                            ack_data.get("error"),
                        )
                        continue
                    raise RuntimeError(f"logsSubscribe rejected for {program}")
                if ack_data is not None:
                    logs = _extract_logs_from_notification(ack_data)
                    if logs:
                        pending.append(ack_data)
                break
        return pending

    while True:
        need_retry = True
        try:
            connect_obj = websockets.connect(
                url,
                ping_interval=SCAN_WS_PING_INTERVAL,
                ping_timeout=SCAN_WS_PING_TIMEOUT,
            )
            async with _WebsocketContext(connect_obj) as ws:
                backoff = SCAN_WS_RETRY_BASE
                need_retry = False
                pending_messages: List[Any] = []
                try:
                    pending_messages.extend(await _subscribe(ws, token_prog))
                    if include_pools:
                        pending_messages.extend(await _subscribe(ws, dex_prog))
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("Raw websocket subscription failed: %s", exc)
                    need_retry = True
                    if not had_success:
                        raise
                    continue

                had_success = True

                for pending in pending_messages:
                    logs = _extract_logs_from_notification(pending)
                    if not logs:
                        continue
                    for token in _tokens_from_logs(
                        logs,
                        include_pools=include_pools,
                        suffix=suffix,
                        keywords=keywords,
                    ):
                        yield token

                while True:
                    try:
                        msg = await ws.recv()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.debug("Raw websocket recv failed: %s", exc)
                        need_retry = True
                        break
                    if isinstance(msg, bytes):
                        msg = msg.decode("utf-8", "ignore")
                    try:
                        data = _json.loads(msg)
                    except Exception:
                        logger.debug("Raw websocket sent non-JSON frame")
                        continue
                    logs = _extract_logs_from_notification(data)
                    if not logs:
                        continue
                    for token in _tokens_from_logs(
                        logs,
                        include_pools=include_pools,
                        suffix=suffix,
                        keywords=keywords,
                    ):
                        yield token
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not had_success:
                raise
            logger.warning("Raw websocket connect failed: %s", exc)

        if not need_retry:
            continue
        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, SCAN_WS_RETRY_MAX)


async def _stream_new_tokens_solana(
    rpc_url: str,
    *,
    suffix: str | None = None,
    keywords: Iterable[str] | None = None,
    include_pools: bool = True,
) -> AsyncGenerator[str, None]:
    if connect is None:
        raise RuntimeError("solana.rpc.websocket_api not available")

    backoff = SCAN_WS_RETRY_BASE
    if RpcTransactionLogsFilterMentions is not None:
        token_filter = RpcTransactionLogsFilterMentions(Pubkey.from_string(str(TOKEN_PROGRAM_ID)))
        dex_filter = RpcTransactionLogsFilterMentions(Pubkey.from_string(str(DEX_PROGRAM_ID)))
    else:
        token_filter = str(TOKEN_PROGRAM_ID)
        dex_filter = str(DEX_PROGRAM_ID)

    while True:
        try:
            async with connect(rpc_url) as ws:
                backoff = SCAN_WS_RETRY_BASE
                try:
                    await ws.logs_subscribe(token_filter)
                    if include_pools:
                        await ws.logs_subscribe(dex_filter)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning("solana-py logs_subscribe failed: %s", exc)
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, SCAN_WS_RETRY_MAX)
                    continue

                while True:
                    try:
                        messages = await ws.recv()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.debug("solana-py websocket recv failed: %s", exc)
                        await asyncio.sleep(SCAN_WS_RETRY_BASE)
                        break

                    if isinstance(messages, bytes):
                        messages = messages.decode("utf-8", "ignore")
                    if isinstance(messages, str):
                        try:
                            messages = _json.loads(messages)
                        except Exception:
                            logger.debug("solana-py websocket sent non-JSON frame")
                            continue

                    if not isinstance(messages, list):
                        messages = [messages]

                    for message in messages:
                        if isinstance(message, bytes):
                            message = message.decode("utf-8", "ignore")
                        if isinstance(message, str):
                            try:
                                message = _json.loads(message)
                            except Exception:
                                logger.debug("solana-py websocket sent non-JSON frame")
                                continue
                        logs = _extract_logs_from_client_message(message)
                        if not logs:
                            continue
                        for token in _tokens_from_logs(
                            logs,
                            include_pools=include_pools,
                            suffix=suffix,
                            keywords=keywords,
                        ):
                            yield token
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("solana-py websocket connect failed: %s", exc)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, SCAN_WS_RETRY_MAX)


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
        return

    if suffix is None:
        suffix = TOKEN_SUFFIX
    if keywords is None:
        keywords = TOKEN_KEYWORDS
    suffix = suffix.lower() if suffix else None

    # Prefer robust raw subscription for better public RPC compatibility
    raw_stream = _stream_new_tokens_raw(
        rpc_url,
        suffix=suffix,
        keywords=keywords,
        include_pools=include_pools,
    )

    try:
        first_token = await raw_stream.__anext__()
    except StopAsyncIteration:
        first_token = None
    except asyncio.CancelledError:
        raise
    except Exception:
        try:
            await raw_stream.aclose()
        except Exception:
            pass
        if not _have_solana_ws():
            raise
        async for token in _stream_new_tokens_solana(
            rpc_url,
            suffix=suffix,
            keywords=keywords,
            include_pools=include_pools,
        ):
            yield token
        return
    else:
        if first_token is not None:
            yield first_token

    try:
        async for token in raw_stream:
            yield token
    except asyncio.CancelledError:
        raise


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

    session = await get_session()
    async with session.ws_connect(
        url,
        heartbeat=SCAN_WS_PING_INTERVAL,
    ) as ws:
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
            volume = data.get("volume") or data.get("volume_24h")
            if addr and _token_matches(
                addr,
                name,
                suffix=suffix,
                keywords=keywords,
                volume=volume,
            ):
                yield addr
