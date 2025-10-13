# solhunter_zero/scanner_common.py
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path
from typing import Any, Dict, Iterable, List
from urllib.parse import urlparse, urlunparse

import aiohttp

from .lru import TTLCache

# ---------------------------------------------------------------------
# Hard-coded RPC + WS (your Helius endpoints)
# ---------------------------------------------------------------------
DEFAULT_SOLANA_RPC = "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
DEFAULT_SOLANA_WS = "wss://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY"
DEFAULT_BIRDEYE_API_KEY = "b1e60d72780940d1bd929b9b2e9225e6"

logger = logging.getLogger(__name__)

TREND_CACHE_TTL = float(os.getenv("TREND_CACHE_TTL", "45") or 45.0)
TREND_CACHE: TTLCache = TTLCache(maxsize=1, ttl=TREND_CACHE_TTL)
_TREND_CACHE_KEY = "trending_tokens"


def _derive_ws_from_rpc(rpc_url: str | None) -> str | None:
    if not rpc_url:
        return None
    parsed = urlparse(rpc_url)
    if not parsed.scheme:
        return None
    if parsed.scheme.startswith("ws"):
        return rpc_url
    if parsed.scheme == "https":
        scheme = "wss"
    elif parsed.scheme == "http":
        scheme = "ws"
    else:
        return None
    return urlunparse(parsed._replace(scheme=scheme))


def _ensure_env(name: str, value: str) -> str:
    os.environ[name] = value
    return value


def refresh_runtime_values() -> None:
    """Synchronise RPC/WS/BirdEye globals with current environment."""
    global SOLANA_RPC_URL, SOLANA_WS_URL
    global JUPITER_WS_URL, PHOENIX_WS_URL, METEORA_WS_URL
    global BIRDEYE_API_KEY, HEADERS

    rpc = os.getenv("SOLANA_RPC_URL") or DEFAULT_SOLANA_RPC
    SOLANA_RPC_URL = _ensure_env("SOLANA_RPC_URL", rpc)

    ws = os.getenv("SOLANA_WS_URL") or _derive_ws_from_rpc(SOLANA_RPC_URL) or DEFAULT_SOLANA_WS
    SOLANA_WS_URL = _ensure_env("SOLANA_WS_URL", ws)

    JUPITER_WS_URL = os.getenv("JUPITER_WS_URL", SOLANA_WS_URL)
    PHOENIX_WS_URL = os.getenv("PHOENIX_WS_URL", SOLANA_WS_URL)
    METEORA_WS_URL = os.getenv("METEORA_WS_URL", SOLANA_WS_URL)

    api_key = (os.getenv("BIRDEYE_API_KEY") or "").strip()
    if not api_key:
        api_key = DEFAULT_BIRDEYE_API_KEY
    BIRDEYE_API_KEY = api_key

    HEADERS = {
        "accept": "application/json",
        "X-API-KEY": BIRDEYE_API_KEY,
        "x-chain": os.getenv("BIRDEYE_CHAIN", "solana"),
    }


def get_solana_ws_url() -> str:
    return SOLANA_WS_URL

# ---------------------------------------------------------------------
# BirdEye config – hardwire your key if desired
# ---------------------------------------------------------------------
BIRDEYE_API: str = "https://public-api.birdeye.so"

refresh_runtime_values()

# ---------------------------------------------------------------------
# Misc exports (safe stubs for imports)
# ---------------------------------------------------------------------

TOKEN_SUFFIX = os.getenv("TOKEN_SUFFIX", "token")
TOKEN_KEYWORDS = [k.strip() for k in os.getenv("TOKEN_KEYWORDS", "token,coin").split(",") if k.strip()]

OFFLINE_TOKENS: List[str] = [
    "So11111111111111111111111111111111111111112",  # Wrapped SOL
    "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
]

def token_matches(token_info: Dict[str, Any] | str, patterns: Iterable[str] | None = None) -> bool:
    if patterns is None:
        return True
    if isinstance(token_info, str):
        return True
    sym = str(token_info.get("symbol", "")).lower()
    name = str(token_info.get("name", "")).lower()
    for p in patterns:
        q = str(p).lower()
        if q and (q in sym or q in name):
            return True
    return False

async def fetch_trending_tokens_async(limit: int = 50) -> List[str]:
    limit = max(1, int(limit))

    async def _fetch_remote() -> List[str]:
        url = os.getenv("TREND_CACHE_URL", f"{BIRDEYE_API}/defi/trending")
        timeout = float(os.getenv("TREND_CACHE_TIMEOUT", "10") or 10.0)
        session = aiohttp.ClientSession()
        try:
            async with session.get(url, timeout=timeout) as resp:
                resp.raise_for_status()
                payload = await resp.json()
        finally:
            close = getattr(session, "close", None)
            if close:
                if asyncio.iscoroutinefunction(close):
                    await close()
                else:
                    close()
        items: List[Any] = []
        if isinstance(payload, dict):
            for key in ("trending", "items", "tokens", "data", "results"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    items = candidate
                    break
            else:
                data = payload.get("data")
                if isinstance(data, list):
                    items = data
        elif isinstance(payload, list):
            items = payload
        results: List[str] = []
        for entry in items:
            if isinstance(entry, dict):
                address = (
                    entry.get("address")
                    or entry.get("mint")
                    or entry.get("mintAddress")
                    or entry.get("tokenAddress")
                )
            else:
                address = entry
            if isinstance(address, str) and address:
                results.append(address)
            if len(results) >= limit:
                break
        return results

    try:
        tokens = await TREND_CACHE.get_or_set_async(_TREND_CACHE_KEY, _fetch_remote)
    except Exception as exc:  # pragma: no cover - defensive
        logger.debug("Trending token cache fetch failed: %s", exc)
        return []

    if not isinstance(tokens, list):
        return []

    return list(tokens)[:limit]


def scan_tokens_from_file(path: str | os.PathLike | None, *, limit: int | None = None) -> List[str]:
    if not path:
        return []
    file_path = Path(path).expanduser()
    if not file_path.exists():
        return []
    tokens: List[str] = []
    for line in file_path.read_text(encoding="utf-8").splitlines():
        mint = line.strip()
        if not mint:
            continue
        tokens.append(mint)
        if limit is not None and len(tokens) >= limit:
            return tokens[:limit]
    return tokens


def scan_tokens_from_directory(directory: str | os.PathLike, *, limit: int | None = None) -> List[str]:
    base = Path(directory).expanduser()
    if not base.exists() or not base.is_dir():
        return []
    tokens: List[str] = []
    for child in sorted(base.iterdir()):
        if not child.is_file():
            continue
        tokens.extend(scan_tokens_from_file(child, limit=limit))
        if limit is not None and len(tokens) >= limit:
            return tokens[:limit]
    return tokens
