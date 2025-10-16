from __future__ import annotations

import logging
import asyncio
import importlib
from typing import List

from solders.pubkey import Pubkey

PublicKeyType = None
spec = importlib.util.find_spec("solana.publickey")
if spec is not None:  # pragma: no cover - optional dependency path
    PublicKeyType = importlib.import_module("solana.publickey").PublicKey  # type: ignore[attr-defined]
else:
    import types, sys
    from importlib.machinery import ModuleSpec

    stub = types.ModuleType("solana.publickey")
    stub.PublicKey = Pubkey
    stub.__spec__ = ModuleSpec("solana.publickey", loader=None)
    sys.modules.setdefault("solana.publickey", stub)


def _to_public_key(value: str):
    if PublicKeyType is not None:
        try:
            return PublicKeyType(value)
        except Exception:
            try:
                return PublicKeyType(bytes(Pubkey.from_string(value)))  # type: ignore[attr-defined]
            except Exception:
                pass
    return Pubkey.from_string(value)


try:
    from solana.rpc.async_api import AsyncClient  # type: ignore
except Exception:  # pragma: no cover - fallback when solana not installed
    class AsyncClient:
        """Minimal stub used when :mod:`solana` is missing.

        The class implements the async context manager protocol so that
        ``async with AsyncClient(...)`` still functions.  Methods raise
        ``RuntimeError`` so callers can detect that the real dependency is
        unavailable.  The attribute remains patchable for tests and the
        demo helper.
        """

        def __init__(self, *_a, **_k):
            pass

        async def __aenter__(self):  # pragma: no cover - simple stub
            return self

        async def __aexit__(self, exc_type, exc, tb):  # pragma: no cover - simple stub
            return False

        async def get_program_accounts(self, *_a, **_k):  # pragma: no cover - simple stub
            raise RuntimeError("AsyncClient is unavailable")

from .scanner_common import token_matches
from .rpc_helpers import extract_program_accounts

logger = logging.getLogger(__name__)

DEX_PROGRAM_ID = _to_public_key("9xQeWvG816bUx9EPB8YVJprFLaDpbZc81FNtdVUL5J7")


async def scan_new_pools(rpc_url: str) -> List[str]:
    """Return token mints from recently created pools passing filters."""
    if not rpc_url:
        raise ValueError("rpc_url is required")

    async with AsyncClient(rpc_url) as client:
        try:
            resp = await client.get_program_accounts(
                DEX_PROGRAM_ID, encoding="jsonParsed"
            )
        except Exception as exc:  # pragma: no cover - network errors
            logger.error("Pool scan failed: %s", exc)
            return []

    tokens: List[str] = []
    for acc in extract_program_accounts(resp):
        info = (
            acc.get("account", {})
            .get("data", {})
            .get("parsed", {})
            .get("info", {})
        )
        for key in ("tokenA", "tokenB"):
            mint = info.get(key, {}).get("mint")
            name = info.get(key, {}).get("name")
            if mint and token_matches(mint, name):
                tokens.append(mint)
    logger.info("Found %d tokens from pools", len(tokens))
    return tokens


def scan_new_pools_sync(rpc_url: str) -> List[str]:
    """Synchronous wrapper for :func:`scan_new_pools`."""
    return asyncio.run(scan_new_pools(rpc_url))
