from __future__ import annotations

import base64
import os
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import aiohttp
from aiohttp import ClientResponseError, ClientTimeout

from ..http import get_session
from ..lru import TTLCache
from ..token_aliases import canonical_mint, validate_mint

DEFAULT_JUP_BASE = "https://lite-api.jup.ag"
DEFAULT_QUOTE_PATH = "/swap/v1/quote"
DEFAULT_SWAP_PATH = "/swap/v1/swap"
DEFAULT_TOKEN_SEARCH_PATH = "/tokens/v2/search"

DEFAULT_CONNECT_TIMEOUT = float(os.getenv("JUPITER_SWAP_CONNECT_TIMEOUT", "0.3") or 0.3)
DEFAULT_READ_TIMEOUT = float(os.getenv("JUPITER_SWAP_READ_TIMEOUT", "0.7") or 0.7)
DEFAULT_TOTAL_TIMEOUT = float(os.getenv("JUPITER_SWAP_TOTAL_TIMEOUT", "1.5") or 1.5)
TOKEN_CACHE_TTL = float(os.getenv("JUPITER_TOKEN_CACHE_TTL", "900") or 900)
TOKEN_CACHE_SIZE = max(64, int(os.getenv("JUPITER_TOKEN_CACHE_SIZE", "256") or 256))

_TOKEN_CACHE: TTLCache = TTLCache(maxsize=TOKEN_CACHE_SIZE, ttl=TOKEN_CACHE_TTL)


class JupiterSwapError(RuntimeError):
    """Raised when the Jupiter swap API cannot produce a transaction."""


@dataclass(frozen=True)
class JupiterConfig:
    quote_url: str
    swap_url: str
    token_search_url: str
    slippage_bps: int
    timeout: ClientTimeout


def load_config() -> JupiterConfig:
    base = os.getenv("JUPITER_API_BASE", DEFAULT_JUP_BASE).rstrip("/")
    quote_url = os.getenv("JUPITER_QUOTE_URL") or f"{base}{DEFAULT_QUOTE_PATH}"
    swap_url = os.getenv("JUPITER_SWAP_URL") or f"{base}{DEFAULT_SWAP_PATH}"
    token_search_url = os.getenv("JUPITER_TOKEN_SEARCH_URL") or f"{base}{DEFAULT_TOKEN_SEARCH_PATH}"
    try:
        slippage = int(os.getenv("JUPITER_SLIPPAGE_BPS", "50") or "50")
    except ValueError:
        slippage = 50
    try:
        connect = float(
            os.getenv("JUPITER_SWAP_CONNECT_TIMEOUT", str(DEFAULT_CONNECT_TIMEOUT))
            or DEFAULT_CONNECT_TIMEOUT
        )
    except ValueError:
        connect = DEFAULT_CONNECT_TIMEOUT
    try:
        read = float(
            os.getenv("JUPITER_SWAP_READ_TIMEOUT", str(DEFAULT_READ_TIMEOUT))
            or DEFAULT_READ_TIMEOUT
        )
    except ValueError:
        read = DEFAULT_READ_TIMEOUT
    try:
        total = float(
            os.getenv("JUPITER_SWAP_TOTAL_TIMEOUT", str(DEFAULT_TOTAL_TIMEOUT))
            or DEFAULT_TOTAL_TIMEOUT
        )
    except ValueError:
        total = DEFAULT_TOTAL_TIMEOUT
    timeout = ClientTimeout(total=total, sock_connect=connect, sock_read=read)
    return JupiterConfig(
        quote_url=quote_url,
        swap_url=swap_url,
        token_search_url=token_search_url,
        slippage_bps=slippage,
        timeout=timeout,
    )


async def _request_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    method: str = "GET",
    params: Dict[str, Any] | None = None,
    payload: Dict[str, Any] | None = None,
    timeout: ClientTimeout,
    expect_mapping: bool = True,
) -> Any:
    request = session.request(
        method,
        url,
        params=params,
        json=payload,
        timeout=timeout,
    )
    try:
        async with request as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
    except ClientResponseError as exc:
        raise JupiterSwapError(
            f"{method} {url} failed with HTTP {exc.status}: {exc.message}"
        ) from exc
    if expect_mapping and not isinstance(data, dict):
        raise JupiterSwapError(f"{url} returned non-JSON mapping payload")
    return data


def _cache_key(value: str) -> str:
    return (value or "").strip().lower()


async def _resolve_token_mint(
    session: aiohttp.ClientSession,
    value: str,
    *,
    token_search_url: str,
    timeout: ClientTimeout,
) -> str:
    candidate = canonical_mint(value)
    if validate_mint(candidate):
        return candidate
    key = _cache_key(value)
    cached = _TOKEN_CACHE.get(key)
    if isinstance(cached, str) and validate_mint(cached):
        return canonical_mint(cached)
    payload = await _request_json(
        session,
        token_search_url,
        params={"query": value},
        timeout=timeout,
        expect_mapping=False,
    )
    resolved: str | None = None
    if isinstance(payload, list):
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            mint = entry.get("address") or entry.get("id")
            if not isinstance(mint, str):
                continue
            canonical = canonical_mint(mint)
            if validate_mint(canonical):
                resolved = canonical
                break
    if resolved is None:
        raise JupiterSwapError(f"Unable to resolve mint '{value}' via Jupiter token search")
    _TOKEN_CACHE.set(key, resolved)
    return resolved


async def request_swap_transaction(
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    user_public_key: str,
    config: JupiterConfig | None = None,
    wrap_and_unwrap_sol: bool = False,
) -> Tuple[str, Dict[str, Any]]:
    """
    Fetch a swap transaction from Jupiter.

    Returns ``(swap_transaction_b64, quote_response)`` where the swap transaction
    must still be signed by the caller.
    """

    if amount <= 0:
        raise JupiterSwapError("Swap amount must be positive")

    cfg = config or load_config()
    session = await get_session()
    resolved_input = await _resolve_token_mint(
        session,
        input_mint,
        token_search_url=cfg.token_search_url,
        timeout=cfg.timeout,
    )
    resolved_output = await _resolve_token_mint(
        session,
        output_mint,
        token_search_url=cfg.token_search_url,
        timeout=cfg.timeout,
    )

    quote_params = {
        "inputMint": resolved_input,
        "outputMint": resolved_output,
        "amount": str(amount),
        "slippageBps": str(cfg.slippage_bps),
        "swapMode": "ExactIn",
    }

    quote = await _request_json(
        session,
        cfg.quote_url,
        params=quote_params,
        timeout=cfg.timeout,
    )

    swap_payload = {
        "quoteResponse": quote,
        "userPublicKey": user_public_key,
        "wrapAndUnwrapSol": bool(wrap_and_unwrap_sol),
    }
    swap = await _request_json(
        session,
        cfg.swap_url,
        method="POST",
        payload=swap_payload,
        timeout=cfg.timeout,
    )

    tx_b64 = swap.get("swapTransaction")
    if not isinstance(tx_b64, str):
        raise JupiterSwapError("Jupiter response missing swapTransaction")

    return tx_b64, quote


def decode_transaction(tx_b64: str) -> bytes:
    try:
        return base64.b64decode(tx_b64)
    except Exception as exc:  # pragma: no cover - defensive
        raise JupiterSwapError("Failed to decode swap transaction") from exc

