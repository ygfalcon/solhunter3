import asyncio
import base64
import logging
import os
from contextlib import asynccontextmanager, suppress
from typing import Any, Dict, Optional, Sequence

import aiohttp

from .logging_utils import warn_once_per
from .util import install_uvloop, parse_bool_env
from .http import get_session, loads, dumps

IPC_SOCKET = os.getenv("DEPTH_SERVICE_SOCKET", "/tmp/depth_service.sock")
USE_RUST_EXEC = parse_bool_env("USE_RUST_EXEC", True)

from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.api import Client
from solana.rpc.async_api import AsyncClient

from .gas import get_current_fee_async
from .config import load_dex_config
from .onchain_metrics import fetch_dex_metrics_async
from .prices import fetch_token_prices_async
from .swap.jupiter import (
    load_config as load_jupiter_config,
    request_swap_transaction as jupiter_request_swap_transaction,
    JupiterSwapError,
)


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


SOL_MINT = "So11111111111111111111111111111111111111112"


def _parse_float_env(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return float(default)
    try:
        return float(raw)
    except ValueError:
        return float(default)


def _coerce_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value: Any) -> Optional[int]:
    try:
        if value is None:
            return None
        return int(value)
    except (TypeError, ValueError):
        return None


USE_JUPITER_FALLBACK = parse_bool_env("ENABLE_JUPITER_SWAP_FALLBACK", True)
DEFAULT_SOL_INPUT = _parse_float_env("JUPITER_DEFAULT_SOL_INPUT", 0.02)
DEFAULT_TOKEN_INPUT = _parse_float_env("JUPITER_DEFAULT_TOKEN_INPUT", 1.0)

ORCA_DEX_URL = _DEX_CFG.venue_urls.get("orca", DEX_BASE_URL)
RAYDIUM_DEX_URL = _DEX_CFG.venue_urls.get("raydium", DEX_BASE_URL)
PHOENIX_DEX_URL = _DEX_CFG.venue_urls.get("phoenix", DEX_BASE_URL)
METEORA_DEX_URL = _DEX_CFG.venue_urls.get("meteora", DEX_BASE_URL)


def _ensure_path(path: str) -> str:
    if not path:
        return ""
    return path if path.startswith("/") else f"/{path}"


DEFAULT_SWAP_PATH = _ensure_path(SWAP_PATHS.get(SWAP_PRIORITIES[0], "/v6/swap"))
# Backwards compatibility shim; agent modules historically imported ``SWAP_PATH``
# directly.  Keep the alias available while newer helpers compute endpoints
# dynamically.
SWAP_PATH = DEFAULT_SWAP_PATH


RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY")
RPC_TESTNET_URL = os.getenv(
    "SOLANA_TESTNET_RPC_URL",
    "https://devnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY",
)

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


def resolve_swap_endpoint(base_url: str, *, venue: str | None = None) -> str:
    """Return the swap endpoint for ``base_url`` respecting configured paths."""

    if not base_url:
        return base_url

    name = (venue or "").strip().lower()
    if not name or name == "custom" or "://" in name:
        name = ""

    if not name:
        base_root = base_url.split("?", 1)[0].rstrip("/")
        for candidate, candidate_url in VENUE_URLS.items():
            cand_root = str(candidate_url).split("?", 1)[0].rstrip("/")
            if cand_root == base_root:
                name = candidate
                break

    if not name:
        name = "custom"

    return _build_swap_endpoint(name, base_url)


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
            endpoint = resolve_swap_endpoint(name)
            if endpoint and endpoint not in seen:
                resolved.append((name, endpoint))
                seen.add(endpoint)
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


async def _resolve_token_decimals(token: str, metadata: Dict[str, Any]) -> int:
    hint = _coerce_int(metadata.get("decimals"))
    if hint is not None and hint >= 0:
        return hint
    meta = metadata.get("metadata")
    if isinstance(meta, dict):
        nested = _coerce_int(meta.get("decimals"))
        if nested is not None and nested >= 0:
            return nested
        for source in meta.values():
            if isinstance(source, dict):
                nested = _coerce_int(source.get("decimals"))
                if nested is not None and nested >= 0:
                    return nested
    try:
        metrics = await fetch_dex_metrics_async(token)
    except Exception:
        metrics = {}
    decimals = _coerce_int(metrics.get("decimals"))
    if decimals is None or decimals < 0:
        return 9
    return decimals


async def _jupiter_swap_fallback(
    *,
    token: str,
    side: str,
    price: float,
    context: Optional[Dict[str, Any]],
    keypair: Keypair,
    testnet: bool,
    dry_run: bool,
    max_retries: int,
    timeout: float | None,
) -> Optional[Dict[str, Any]]:
    if not USE_JUPITER_FALLBACK or testnet:
        return None

    action_ctx = context or {}
    metadata = action_ctx.get("metadata")
    if not isinstance(metadata, dict):
        metadata = {}

    if dry_run:
        return {
            "provider": "jupiter",
            "dry_run": True,
            "token": token,
            "side": side,
        }

    side_normalized = side.lower()
    if side_normalized not in {"buy", "sell"}:
        raise JupiterSwapError(f"Unsupported side for Jupiter fallback: {side}")

    amount_hint = _coerce_float(action_ctx.get("amount"))
    if amount_hint is None:
        amount_hint = _coerce_float(metadata.get("amount"))
    notional_hint = (
        _coerce_float(action_ctx.get("notional_usd"))
        or _coerce_float(metadata.get("notional_usd"))
        or _coerce_float(metadata.get("budget"))
    )

    cfg = load_jupiter_config()

    if side_normalized == "buy":
        input_mint = SOL_MINT
        output_mint = token
        sol_amount = None
        sol_price = 0.0

        if notional_hint and notional_hint > 0:
            try:
                prices = await fetch_token_prices_async([SOL_MINT])
            except Exception:
                prices = {}
            sol_price = _coerce_float(prices.get(SOL_MINT)) or 0.0
            if sol_price > 0:
                sol_amount = max(notional_hint / sol_price, 0.0)

        if amount_hint and amount_hint > 0 and price > 0:
            if sol_price <= 0:
                try:
                    prices = await fetch_token_prices_async([SOL_MINT])
                except Exception:
                    prices = {}
                sol_price = _coerce_float(prices.get(SOL_MINT)) or 0.0
            if sol_price > 0:
                est = (amount_hint * price) / sol_price
                sol_amount = max(sol_amount or 0.0, est)

        if sol_amount is None or sol_amount <= 0:
            sol_amount = DEFAULT_SOL_INPUT

        input_units = int(max(sol_amount, 0.0) * 10**9)
        if input_units <= 0:
            raise JupiterSwapError("Computed SOL amount is zero; aborting Jupiter fallback")
        wrap_sol = True
    else:  # sell
        input_mint = token
        output_mint = SOL_MINT
        token_amount = amount_hint if amount_hint and amount_hint > 0 else None
        if token_amount is None and notional_hint and price > 0:
            token_amount = notional_hint / price
        if token_amount is None or token_amount <= 0:
            token_amount = DEFAULT_TOKEN_INPUT
        decimals = await _resolve_token_decimals(token, metadata)
        input_units = int(max(token_amount, 0.0) * (10**decimals))
        if input_units <= 0:
            raise JupiterSwapError("Computed token amount is zero; aborting Jupiter fallback")
        wrap_sol = False

    tx_b64, quote = await jupiter_request_swap_transaction(
        input_mint=input_mint,
        output_mint=output_mint,
        amount=input_units,
        user_public_key=str(keypair.pubkey()),
        config=cfg,
        wrap_and_unwrap_sol=wrap_sol,
    )

    signed_tx = _sign_transaction(tx_b64, keypair)
    signed_b64 = base64.b64encode(bytes(signed_tx)).decode()

    data: Dict[str, Any] = {
        "provider": "jupiter",
        "swapTransaction": tx_b64,
        "quote": quote,
        "inputMint": input_mint,
        "outputMint": output_mint,
        "inputAmount": str(input_units),
    }

    if USE_RUST_EXEC:
        res = await _place_order_ipc(
            signed_b64,
            testnet=testnet,
            dry_run=dry_run,
            timeout=timeout,
            max_retries=max_retries,
        )
        if res:
            data.update(res)
            data.setdefault("signature", res.get("signature"))
            return data
        raise JupiterSwapError("Depth service failed to submit Jupiter swap")

    async with AsyncClient(RPC_TESTNET_URL if testnet else RPC_URL) as client:
        try:
            resp = await client.send_raw_transaction(bytes(signed_tx))
            signature = str(resp.value)
            await client.confirm_transaction(signature)
        except Exception as exc:
            raise JupiterSwapError(f"RPC broadcast failed: {exc}") from exc

    data["signature"] = signature
    return data


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
    context: Dict[str, Any] | None = None,
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

    if keypair is not None:
        try:
            fallback = await _jupiter_swap_fallback(
                token=token,
                side=side,
                price=price,
                context=context,
                keypair=keypair,
                testnet=testnet,
                dry_run=dry_run,
                max_retries=max_retries,
                timeout=timeout,
            )
        except JupiterSwapError as exc:
            warn_once_per(
                1.0,
                f"jupiter-fallback:{token}",
                "Jupiter fallback failed for %s: %s",
                token,
                exc,
                logger=logger,
            )
        except Exception as exc:
            logger.exception("Jupiter fallback unexpected error for %s: %s", token, exc)
        else:
            if fallback:
                return fallback

    return None
