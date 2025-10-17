"""Connectivity checks for SolHunter Zero."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
from typing import Iterable
from urllib.parse import urlparse

from .util import parse_bool_env

_ADDITIONAL_WS_TARGETS: tuple[tuple[str, str], ...] = (
    ("SOLANA_WS_URL", "Solana RPC WS"),
    ("HELIUS_WS_URL", "Helius RPC WS"),
    ("PHOENIX_DEPTH_WS_URL", "Phoenix depth"),
    ("METEORA_DEPTH_WS_URL", "Meteora depth"),
    ("JUPITER_WS_URL", "Jupiter quotes"),
)


async def _check_websocket_handshake(
    url: str, *, label: str, raise_on_fail: bool
) -> None:
    """Attempt to establish a websocket connection to ``url``."""

    if not url:
        return

    try:
        parsed = urlparse(url)
        if parsed.scheme not in {"ws", "wss"} or not parsed.netloc:
            raise ValueError(f"invalid ws url: {url}")

        from .http import get_session

        session = await get_session()
        async with session.ws_connect(url, timeout=8):  # type: ignore[arg-type]
            pass
    except Exception as exc:  # pragma: no cover - network failure
        logger = logging.getLogger(__name__)
        msg = f"Unable to establish websocket connection to {label} at {url}: {exc}"
        logger.warning(msg)
        if raise_on_fail:
            raise RuntimeError(msg) from exc


async def _check_additional_webs(
    urls: Iterable[str], labels: Iterable[str], *, raise_on_fail: bool
) -> None:
    """Probe additional websocket endpoints concurrently."""

    tasks = [
        _check_websocket_handshake(url, label=label, raise_on_fail=raise_on_fail)
        for url, label in zip(urls, labels)
        if url
    ]
    if tasks:
        await asyncio.gather(*tasks)


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

    raise_on_ws_fail = parse_bool_env("RAISE_ON_WS_FAIL", False)
    listing_url = os.getenv("DEX_LISTING_WS_URL", "")

    additional_urls: list[str] = []
    additional_labels: list[str] = []
    seen: set[str] = set()
    for env_key, label in _ADDITIONAL_WS_TARGETS:
        value = os.getenv(env_key, "").strip()
        if not value or value in seen:
            continue
        additional_urls.append(value)
        additional_labels.append(label)
        seen.add(value)

    tasks = []
    if listing_url:
        async def _check_listing() -> None:
            gen = None
            try:
                gen = stream_listed_tokens(listing_url)
                await asyncio.wait_for(gen.__anext__(), timeout=5)
            except asyncio.TimeoutError:
                msg = "No data received from DEX listing websocket"
                logging.getLogger(__name__).warning(msg)
                if raise_on_ws_fail:
                    raise RuntimeError(msg)
            finally:
                if gen is not None:
                    with contextlib.suppress(Exception):
                        await gen.aclose()

        tasks.append(_check_listing())

    if additional_urls:
        tasks.append(
            _check_additional_webs(
                additional_urls, additional_labels, raise_on_fail=raise_on_ws_fail
            )
        )

    if tasks:
        await asyncio.gather(*tasks)


def ensure_connectivity(*, offline: bool = False) -> None:
    """Synchronous wrapper for :func:`ensure_connectivity_async`."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        asyncio.run(ensure_connectivity_async(offline=offline))
    else:
        future = asyncio.run_coroutine_threadsafe(
            ensure_connectivity_async(offline=offline), loop
        )
        future.result()


__all__ = ["ensure_connectivity_async", "ensure_connectivity"]
