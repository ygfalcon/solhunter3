"""Connectivity checks for SolHunter Zero."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os

from .util import parse_bool_env


async def ensure_connectivity_async(*, offline: bool = False) -> None:
    """Verify Solana RPC and DEX websocket connectivity asynchronously."""
    if offline or parse_bool_env("SOLHUNTER_OFFLINE", False) or parse_bool_env(
        "SOLHUNTER_SKIP_CONNECTIVITY", False
    ):
        logging.getLogger(__name__).info(
            "Skipping connectivity checks (offline mode enabled)"
        )
        return

    from solhunter_zero.rpc_utils import ensure_rpc as _ensure_rpc
    from .dex_ws import stream_listed_tokens

    _ensure_rpc()

    url = os.getenv("DEX_LISTING_WS_URL", "")
    if not url:
        return

    raise_on_ws_fail = parse_bool_env("RAISE_ON_WS_FAIL", False)

    gen = stream_listed_tokens(url)
    try:
        await asyncio.wait_for(gen.__anext__(), timeout=1)
    except asyncio.TimeoutError:
        msg = "No data received from DEX listing websocket"
        logging.getLogger(__name__).warning(msg)
        if raise_on_ws_fail:
            raise RuntimeError(msg)
    finally:
        with contextlib.suppress(Exception):
            await gen.aclose()


def ensure_connectivity(*, offline: bool = False) -> None:
    """Synchronous wrapper for :func:`ensure_connectivity_async`."""
    asyncio.run(ensure_connectivity_async(offline=offline))


__all__ = ["ensure_connectivity_async", "ensure_connectivity"]
