from __future__ import annotations

import asyncio
import base64
import os
from dataclasses import dataclass
from typing import Any, Dict, Tuple

import aiohttp

from ..http import get_session

DEFAULT_JUP_BASE = "https://quote-api.jup.ag"
DEFAULT_QUOTE_PATH = "/v6/quote"
DEFAULT_SWAP_PATH = "/v6/swap"


class JupiterSwapError(RuntimeError):
    """Raised when the Jupiter swap API cannot produce a transaction."""


@dataclass(frozen=True)
class JupiterConfig:
    quote_url: str
    swap_url: str
    slippage_bps: int
    timeout: float


def load_config() -> JupiterConfig:
    base = os.getenv("JUPITER_API_BASE", DEFAULT_JUP_BASE).rstrip("/")
    quote_url = os.getenv("JUPITER_QUOTE_URL") or f"{base}{DEFAULT_QUOTE_PATH}"
    swap_url = os.getenv("JUPITER_SWAP_URL") or f"{base}{DEFAULT_SWAP_PATH}"
    try:
        slippage = int(os.getenv("JUPITER_SLIPPAGE_BPS", "100") or "100")
    except ValueError:
        slippage = 100
    try:
        timeout = float(os.getenv("JUPITER_HTTP_TIMEOUT", "20") or 20)
    except ValueError:
        timeout = 20.0
    return JupiterConfig(
        quote_url=quote_url,
        swap_url=swap_url,
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
    timeout: float = 20.0,
) -> Dict[str, Any]:
    request = session.request(
        method,
        url,
        params=params,
        json=payload,
        timeout=timeout,
    )
    async with request as resp:
        resp.raise_for_status()
        data = await resp.json(content_type=None)
    if not isinstance(data, dict):
        raise JupiterSwapError(f"{url} returned non-JSON payload")
    return data


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
    quote_params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount),
        "slippageBps": str(cfg.slippage_bps),
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

