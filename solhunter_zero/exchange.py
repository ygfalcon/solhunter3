import os
import base64
import logging
from typing import Optional, Dict, Any, Sequence
import asyncio
from contextlib import asynccontextmanager, suppress
import os

from .util import install_uvloop, parse_bool_env

import aiohttp
from .http import get_session, loads, dumps

IPC_SOCKET = os.getenv("DEPTH_SERVICE_SOCKET", "/tmp/depth_service.sock")
USE_RUST_EXEC = parse_bool_env("USE_RUST_EXEC", True)

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient

from .gas import get_current_fee_async
from .config import load_dex_config


class OrderPlacementError(Exception):
    """Raised when an order cannot be placed."""


logger = logging.getLogger(__name__)
install_uvloop()

# Event loop used for synchronous order placement when no running loop is
# available.  Created lazily on first use and reused across calls.
_order_loop: asyncio.AbstractEventLoop | None = None

# Persistent IPC connection pooling
_IPC_CONNECTIONS: dict[str, tuple[asyncio.StreamReader, asyncio.StreamWriter]] = {}
_IPC_LOCKS: dict[str, asyncio.Lock] = {}


@asynccontextmanager
async def _ipc_connection(socket_path: str = IPC_SOCKET):
    """Yield a reusable UNIX socket connection."""
    lock = _IPC_LOCKS.setdefault(socket_path, asyncio.Lock())
    async with lock:
        conn = _IPC_CONNECTIONS.get(socket_path)
        if conn is None or conn[1].is_closing():
            conn = await asyncio.open_unix_connection(socket_path)
            _IPC_CONNECTIONS[socket_path] = conn
        reader, writer = conn
        try:
            yield reader, writer
        except Exception:
            writer.close()
            with suppress(Exception):
                await writer.wait_closed()
            if _IPC_CONNECTIONS.get(socket_path) is conn:
                _IPC_CONNECTIONS.pop(socket_path, None)
            raise


async def close_ipc_connections() -> None:
    """Close all cached IPC connections."""
    for reader, writer in list(_IPC_CONNECTIONS.values()):
        writer.close()
        with suppress(Exception):
            await writer.wait_closed()
    _IPC_CONNECTIONS.clear()

_DEX_CFG = load_dex_config()
DEX_BASE_URL = _DEX_CFG.base_url
DEX_TESTNET_URL = _DEX_CFG.testnet_url
SWAP_URLS = {str(k): str(v) for k, v in _DEX_CFG.swap_urls.items() if str(v)}
SWAP_PATHS = {str(k): str(v) for k, v in _DEX_CFG.swap_paths.items() if str(v)}
SWAP_PRIORITIES = [p for p in _DEX_CFG.swap_priorities if p in SWAP_URLS]
if not SWAP_PRIORITIES:
    _fallback = [name for name in ("helius", "birdeye", "jupiter") if name in SWAP_URLS]
    SWAP_PRIORITIES = _fallback or list(SWAP_URLS.keys())

ORCA_DEX_URL = _DEX_CFG.venue_urls.get("orca", DEX_BASE_URL)
RAYDIUM_DEX_URL = _DEX_CFG.venue_urls.get("raydium", DEX_BASE_URL)
PHOENIX_DEX_URL = _DEX_CFG.venue_urls.get("phoenix", DEX_BASE_URL)
METEORA_DEX_URL = _DEX_CFG.venue_urls.get("meteora", DEX_BASE_URL)


def _ensure_path(path: str) -> str:
    if not path:
        return ""
    return path if path.startswith("/") else f"/{path}"


DEFAULT_SWAP_PATH = _ensure_path(SWAP_PATHS.get(SWAP_PRIORITIES[0], "/v6/swap"))


RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d")
RPC_TESTNET_URL = os.getenv("SOLANA_TESTNET_RPC_URL", "https://api.devnet.solana.com")

# Map of venue names to base API URLs used for order submission.  Callers may
# reference these keys or provide custom URLs directly.
VENUE_URLS = {
    **SWAP_URLS,
    "raydium": RAYDIUM_DEX_URL,
    "orca": ORCA_DEX_URL,
    "phoenix": PHOENIX_DEX_URL,
    "meteora": METEORA_DEX_URL,
}


def _build_swap_endpoint(name: str, base_url: str, default_path: str = DEFAULT_SWAP_PATH) -> str:
    if not base_url:
        return base_url
    path = SWAP_PATHS.get(name, default_path)
    path = _ensure_path(path)
    if not path:
        return base_url
    if "?" in base_url:
        base_part, query = base_url.split("?", 1)
        if base_part.rstrip("/").endswith(path.lstrip("/")):
            return base_url
        return f"{base_part.rstrip('/')}{path}?{query}"
    if base_url.rstrip("/").endswith(path.lstrip("/")):
        return base_url
    return f"{base_url.rstrip('/')}{path}"


def _resolve_swap_candidates(
    *,
    testnet: bool,
    base_url: str | None,
    venues: Sequence[str] | None,
) -> list[tuple[str, str]]:
    aggregator_map = dict(SWAP_URLS)
    if testnet and DEX_TESTNET_URL:
        aggregator_map["helius"] = DEX_TESTNET_URL

    base_map = {**VENUE_URLS, **aggregator_map}
    resolved: list[tuple[str, str]] = []
    seen: set[str] = set()

    def add(name: str, base: str, append_path: bool) -> None:
        if not base:
            return
        endpoint = _build_swap_endpoint(name, base) if append_path else base
        if endpoint in seen:
            return
        resolved.append((name, endpoint))
        seen.add(endpoint)

    if base_url:
        add("custom", base_url, True)

    entries = list(venues or SWAP_PRIORITIES)
    for name in entries:
        if name in aggregator_map:
            add(name, aggregator_map[name], True)
            continue
        if "://" in name:
            add(name, name, False)
            continue
        base = base_map.get(name)
        if base:
            add(name, base, False)
        else:
            add(name, name, False)

    return resolved


async def _post_swap_request(
    session: aiohttp.ClientSession,
    endpoint: str,
    payload: dict[str, Any],
    *,
    venue_name: str,
    attempt: int,
    max_attempts: int,
) -> Dict[str, Any]:
    try:
        async with session.post(endpoint, json=payload, timeout=10) as resp:
            resp.raise_for_status()
            return await resp.json()
    except aiohttp.ClientResponseError as exc:
        if exc.status == 403:
            headers = {"User-Agent": "Mozilla/5.0"}
            async with session.post(endpoint, json=payload, timeout=10, headers=headers) as resp2:
                resp2.raise_for_status()
                return await resp2.json()
        raise


def _sign_transaction(tx_b64: str, keypair: Keypair) -> VersionedTransaction:
    tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
    sig = keypair.sign_message(bytes(tx.message))
    return VersionedTransaction.populate(tx.message, [sig] + tx.signatures[1:])


async def _place_order_ipc(
    tx_b64: str,
    *,
    testnet: bool = False,
    dry_run: bool = False,
    socket_path: str = IPC_SOCKET,
    timeout: float | None = None,
    max_retries: int = 3,
    retry_delay: float = 0.5,
) -> Optional[Dict[str, Any]]:
    """Submit a pre-signed transaction through the Rust depth service."""

    if dry_run:
        logger.info("Dry run IPC order")
        return {"dry_run": True}

    for _ in range(max_retries):
        try:
            async with _ipc_connection(socket_path) as (reader, writer):
                payload = {"cmd": "submit", "tx": tx_b64, "testnet": testnet}
                data = dumps(payload)
                writer.write(data if isinstance(data, (bytes, bytearray)) else data.encode())
                await writer.drain()
                if timeout:
                    data = await asyncio.wait_for(reader.read(), timeout)
                else:
                    data = await reader.read()
                if data:
                    return loads(data)
                return None
        except asyncio.TimeoutError:
            logger.warning("IPC order timed out, retrying")
        except Exception as exc:
            logger.error("IPC order submission failed: %s", exc)
        await asyncio.sleep(retry_delay)
    return None


def place_order(
    token: str,
    side: str,
    amount: float,
    price: float,
    *,
    testnet: bool = False,
    dry_run: bool = False,
    keypair: Keypair | None = None,
    base_url: str | None = None,
) -> Optional[Dict[str, Any]]:
    """Submit an order via the configured swap partners and broadcast it."""

    payload = {
        "token": token,
        "side": side,
        "amount": amount,
        "price": price,
        "cluster": "devnet" if testnet else "mainnet-beta",
    }

    if dry_run:
        logger.info(
            "Dry run: would place %s order for %s amount %s at price %s",
            side,
            token,
            amount,
            price,
        )
        return {"dry_run": True, **payload}

    endpoints = _resolve_swap_candidates(testnet=testnet, base_url=base_url, venues=None)
    if not endpoints:
        logger.error("No swap endpoints configured")
        return None

    try:
        async def _place() -> tuple[Optional[dict[str, Any]], Optional[str], Exception | None]:
            session = await get_session()
            last_error: Exception | None = None
            for venue_name, endpoint in endpoints:
                for attempt in range(1, 4):
                    try:
                        data = await _post_swap_request(
                            session,
                            endpoint,
                            payload,
                            venue_name=venue_name,
                            attempt=attempt,
                            max_attempts=3,
                        )
                    except aiohttp.ClientError as exc:
                        last_error = exc
                        logger.warning(
                            "Swap attempt %s/%s failed via %s: %s",
                            attempt,
                            3,
                            venue_name,
                            exc,
                        )
                        if attempt < 3:
                            await asyncio.sleep(0.5)
                        continue

                    tx_b64 = data.get("swapTransaction")
                    if not tx_b64:
                        if keypair is None:
                            return data, None, None
                        logger.warning(
                            "Venue %s responded without swapTransaction; trying next venue",
                            venue_name,
                        )
                        data = None
                        break

                    return data, str(tx_b64), None

                logger.warning("Exhausted swap attempts via %s", venue_name)
            return None, None, last_error

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            global _order_loop
            if _order_loop is None:
                _order_loop = asyncio.new_event_loop()
            data, tx_b64, last_error = _order_loop.run_until_complete(_place())
        else:
            future = asyncio.run_coroutine_threadsafe(_place(), loop)
            data, tx_b64, last_error = future.result()

        if not data:
            if last_error is not None:
                raise OrderPlacementError(str(last_error))
            return None

        if not tx_b64 or keypair is None:
            return data

        tx = _sign_transaction(tx_b64, keypair)
        rpc = Client(RPC_TESTNET_URL if testnet else RPC_URL)
        result = rpc.send_raw_transaction(bytes(tx))
        data["signature"] = str(result.value)
        return data
    except aiohttp.ClientError as exc:
        data = getattr(exc.response, "text", "") if getattr(exc, "response", None) else ""
        status = exc.response.status_code if getattr(exc, "response", None) else "no-response"
        logger.error("Order failed with status %s: %s", status, data)
        raise OrderPlacementError(f"HTTP {status}: {data}") from exc
    except Exception as exc:
        logger.error("Order submission failed: %s", exc)
        return None


async def place_order_async(
    token: str,
    side: str,
    amount: float,
    price: float,
    *,
    testnet: bool = False,
    dry_run: bool = False,
    connectivity_test: bool = False,
    keypair: Keypair | None = None,
    base_url: str | None = None,
    venues: list[str] | None = None,
    max_retries: int = 3,
    timeout: float | None = None,
) -> Optional[Dict[str, Any]]:
    """Asynchronously submit an order and broadcast the transaction.

    Venues are attempted sequentially in priority order until a swap transaction
    is returned. ``max_retries`` controls the number of attempts per venue.
    When ``connectivity_test`` is ``True`` a minimal dry-run request is sent to
    verify connectivity without broadcasting a transaction.
    """

    # For pure connectivity tests, skip fee retrieval to avoid RPC dependency
    if connectivity_test:
        trade_amount = float(amount)
    else:
        fee = await get_current_fee_async(testnet=testnet)
        trade_amount = max(0.0, amount - fee)

    endpoints = _resolve_swap_candidates(testnet=testnet, base_url=base_url, venues=venues)
    if not endpoints:
        logger.error("No swap endpoints configured")
        return None

    payload_base = {
        "token": token,
        "side": side,
        "amount": trade_amount,
        "price": price,
        "cluster": "devnet" if testnet else "mainnet-beta",
    }

    if dry_run and not connectivity_test:
        logger.info(
            "Dry run: would place %s order for %s amount %s at price %s",
            side,
            token,
            amount,
            price,
        )
        return {"dry_run": True, **payload_base}

    if connectivity_test:
        keypair = None
        dry_run = True

    session = await get_session()

    for venue_name, endpoint in endpoints:
        remaining = trade_amount
        for attempt in range(1, max_retries + 1):
            payload = dict(payload_base, amount=remaining)
            try:
                data = await _post_swap_request(
                    session,
                    endpoint,
                    payload,
                    venue_name=venue_name,
                    attempt=attempt,
                    max_attempts=max_retries,
                )
            except aiohttp.ClientError as exc:
                logger.warning(
                    "Swap attempt %s/%s failed via %s (%s): %s",
                    attempt,
                    max_retries,
                    venue_name,
                    endpoint,
                    exc,
                )
                if attempt < max_retries:
                    await asyncio.sleep(0.5)
                continue

            tx_b64 = data.get("swapTransaction")
            if not tx_b64:
                if keypair is None:
                    return data
                logger.warning(
                    "Venue %s returned no swapTransaction; moving to next venue",
                    venue_name,
                )
                break

            tx = _sign_transaction(tx_b64, keypair)
            if USE_RUST_EXEC:
                res = await _place_order_ipc(
                    base64.b64encode(bytes(tx)).decode(),
                    testnet=testnet,
                    dry_run=dry_run,
                    timeout=timeout,
                    max_retries=max_retries,
                )
                if res:
                    data.update(res)
                    return data
                logger.warning("IPC execution failed via %s; retrying", venue_name)
                if attempt < max_retries:
                    await asyncio.sleep(0.5)
                continue
            else:
                try:
                    async with AsyncClient(RPC_TESTNET_URL if testnet else RPC_URL) as client:
                        result = await client.send_raw_transaction(bytes(tx))
                    data["signature"] = str(result.value)
                except Exception as exc:
                    logger.warning("RPC send failed via %s: %s", venue_name, exc)
                    if attempt < max_retries:
                        await asyncio.sleep(0.5)
                    continue

            filled = float(data.get("filled_amount", remaining))
            remaining -= filled
            if remaining <= 0:
                return data
        else:
            continue

        logger.warning("Exhausted swap attempts via %s", venue_name)

    return None
