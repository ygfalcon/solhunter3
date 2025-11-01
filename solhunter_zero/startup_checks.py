# solhunter_zero/startup_checks.py
from __future__ import annotations

import asyncio
import logging
import os
from typing import Any, Mapping, Optional

try:
    import aiohttp  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    aiohttp = None

logger = logging.getLogger(__name__)

# Endpoints / env
BIRDEYE_API = os.getenv("BIRDEYE_API", "https://public-api.birdeye.so/defi/tokenlist")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
SOLANA_RPC_URL = os.getenv(
    "SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=af30888b-b79f-4b12-b3fd-c5375d5bad2d"
)


# -----------------------------
# Helpers
# -----------------------------
def _short_timeout(total: float = 5.0) -> "aiohttp.ClientTimeout":
    return aiohttp.ClientTimeout(total=total, connect=max(1.0, total / 2))  # type: ignore[return-value]


async def _json_rpc_health(session: "aiohttp.ClientSession", rpc_url: str) -> tuple[bool, str]:
    """Call a simple Solana JSON-RPC method with a short timeout."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getHealth"}
    try:
        async with session.post(rpc_url, json=payload, timeout=_short_timeout(5.0)) as resp:
            text = await resp.text()
            if resp.status == 200:
                return True, "RPC reachable"
            return False, f"RPC HTTP {resp.status}: {text[:200]}"
    except Exception as exc:
        return False, f"RPC error: {exc!s}"


async def _probe_birdeye(session: "aiohttp.ClientSession") -> tuple[bool, str]:
    """Try a single BirdEye page with chain=solana; non-fatal."""
    if not BIRDEYE_API_KEY:
        return True, "BirdEye key not set (skipping)"
    headers = {"X-API-KEY": BIRDEYE_API_KEY, "Accept": "application/json"}
    params = {"offset": 0, "limit": 1, "sortBy": "v24hUSD", "chain": "solana"}
    try:
        async with session.get(BIRDEYE_API, params=params, headers=headers, timeout=_short_timeout(6.0)) as resp:
            if 200 <= resp.status < 300:
                return True, "BirdEye OK"
            text = await resp.text()
            return False, f"BirdEye HTTP {resp.status}: {text[:200]}"
    except Exception as exc:
        return False, f"BirdEye error: {exc!s}"


def _resolve_active_config(kwargs: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """Use injected helpers to load the active configuration for connectivity checks."""

    load_config = kwargs.get("load_config")
    config: Optional[Mapping[str, Any]] = None

    if callable(load_config):
        try:
            config = load_config()
        except Exception as exc:  # pragma: no cover - configuration optional for checks
            logger.warning("Unable to load configuration for startup checks: %s", exc)

    apply_env_overrides = kwargs.get("apply_env_overrides")
    if callable(apply_env_overrides) and config is not None:
        try:
            config = apply_env_overrides(config)
        except Exception as exc:  # pragma: no cover - configuration optional for checks
            logger.warning("Unable to apply environment overrides: %s", exc)

    return config


def _apply_config_to_globals(config: Optional[Mapping[str, Any]]) -> None:
    """Update module-level connectivity settings from ``config``."""

    if not config:
        return

    global SOLANA_RPC_URL, BIRDEYE_API_KEY

    rpc_url = config.get("solana_rpc_url")
    if isinstance(rpc_url, str) and rpc_url:
        SOLANA_RPC_URL = rpc_url

    api_key = config.get("birdeye_api_key")
    if api_key:
        BIRDEYE_API_KEY = str(api_key)


# -----------------------------
# Public API used by startup
# -----------------------------
def ensure_rpc(*, warn_only: bool = False) -> None:
    """Synchronous wrapper that checks RPC reachability with a small JSON-RPC call."""
    if aiohttp is None:  # pragma: no cover
        logger.warning("aiohttp not available; skipping RPC check")
        return

    async def _run() -> None:
        async with aiohttp.ClientSession() as session:
            ok, msg = await _json_rpc_health(session, SOLANA_RPC_URL)
            if ok:
                logger.info("RPC: %s", msg)
            else:
                if warn_only:
                    logger.warning("RPC: %s", msg)
                else:
                    raise SystemExit(f"RPC not reachable: {msg}")

    asyncio.run(_run())


def ensure_endpoints(_cfg: Optional[dict[str, Any]] = None) -> None:
    """
    Check HTTP endpoints briefly and non-blockingly.

    - RPC JSON-RPC: informational only here (fatality handled in ensure_rpc).
    - BirdEye tokenlist: adds `chain=solana`, warns on failure, does not block.
    """
    if aiohttp is None:  # pragma: no cover
        logger.warning("aiohttp not available; skipping endpoint checks")
        return

    async def _run() -> None:
        async with aiohttp.ClientSession() as session:
            ok, msg = await _json_rpc_health(session, SOLANA_RPC_URL)
            logger.log(logging.INFO if ok else logging.WARNING, "Endpoint RPC: %s", msg)

            ok, msg = await _probe_birdeye(session)
            logger.log(logging.INFO if ok else logging.WARNING, "Endpoint BirdEye: %s", msg)

    asyncio.run(_run())


# -----------------------------
# Compatibility shim
# -----------------------------
def perform_checks(args, rest, **kwargs) -> dict[str, Any]:
    """
    Compatibility shim so scripts.startup can still call startup_checks.perform_checks.
    This simply ensures RPC + endpoint checks run, but doesn't duplicate startup_runner.
    """
    config = _resolve_active_config(kwargs)
    _apply_config_to_globals(config)

    ensure_rpc(warn_only=True)
    ensure_endpoints()
    return {"summary_rows": [], "rest": rest, "code": 0}
