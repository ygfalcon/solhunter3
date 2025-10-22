"""WebSocket-based fallback streamer that emits mint discovery events via Redis."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import random
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import aiohttp
import redis.asyncio as aioredis
import websockets

TOKEN_PROGRAM = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
TOKEN2022_PROGRAM = "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb"
SYSTEM_PROGRAM = "11111111111111111111111111111111"
ASSOCIATED_TOKEN_PROGRAM = "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL"
METAPLEX_METADATA_PROGRAM = "metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s"

DEFAULT_STREAM_PROGRAMS = (
    SYSTEM_PROGRAM,
    TOKEN_PROGRAM,
    TOKEN2022_PROGRAM,
    ASSOCIATED_TOKEN_PROGRAM,
    METAPLEX_METADATA_PROGRAM,
)

__all__ = ["run_rpc_mint_stream", "DEFAULT_STREAM_PROGRAMS"]

_SEEN_SIG: Dict[str, float] = {}
_SEEN_MINT: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _keep_ttl(store: Dict[str, float], ttl: float) -> None:
    """Trim TTL caches once they grow past a sane bound."""

    if ttl <= 0:
        store.clear()
        return
    if len(store) < 20000:
        return
    cutoff = _now() - ttl
    for key, recorded in list(store.items()):
        if recorded < cutoff:
            store.pop(key, None)


async def _subscribe_logs(ws: websockets.WebSocketClientProtocol, program_id: str, sub_id: int) -> None:
    """Send a Solana logsSubscribe request for the given program id."""

    payload = {
        "jsonrpc": "2.0",
        "id": sub_id,
        "method": "logsSubscribe",
        "params": [
            {"mentions": [program_id]},
            {"commitment": "confirmed"},
        ],
    }
    await ws.send(json.dumps(payload, separators=(",", ":")))


async def _fetch_tx(
    session: aiohttp.ClientSession,
    http_url: str,
    signature: str,
    *,
    timeout: float,
) -> Dict[str, object]:
    """Fetch a transaction JSON blob suitable for parsed log inspection."""

    body = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "getTransaction",
        "params": [
            signature,
            {
                "maxSupportedTransactionVersion": 0,
                "commitment": "confirmed",
            },
        ],
    }
    client_timeout = aiohttp.ClientTimeout(total=timeout)
    async with session.post(http_url, json=body, timeout=client_timeout) as resp:
        resp.raise_for_status()
        return await resp.json()


def _flatten_instructions(raw: Dict[str, object]) -> Sequence[Dict[str, object]]:
    """Return both top-level and inner parsed instruction dictionaries."""

    message = (raw.get("transaction") or {}).get("message")
    outer = (message or {}).get("instructions") or []
    meta = raw.get("meta") or {}
    inners: List[Dict[str, object]] = []
    for container in meta.get("innerInstructions") or []:
        for inner in container.get("instructions") or []:
            inners.append(inner)
    return [ix for ix in outer + inners if isinstance(ix, dict)]


def _extract_mints_from_tx(tx_json: Dict[str, object]) -> List[str]:
    """Detect InitializeMint/InitializeMint2 instructions and return mint addresses."""

    result = (tx_json or {}).get("result")
    if not isinstance(result, dict):
        return []
    meta = result.get("meta") or {}
    if isinstance(meta, dict) and meta.get("err"):
        return []
    instructions = _flatten_instructions(result)
    discovered: List[str] = []
    for ix in instructions:
        parsed = ix.get("parsed")
        if not isinstance(parsed, dict):
            continue
        ix_type = str(parsed.get("type") or "").lower()
        if not ix_type.startswith("initializemint"):
            continue
        info = parsed.get("info")
        if not isinstance(info, dict):
            continue
        mint = info.get("mint")
        if isinstance(mint, str):
            discovered.append(mint)
    # Preserve original order but drop duplicates.
    return list(dict.fromkeys(discovered))


def _should_fetch_logs(prefilter: bool, logs: Iterable[str]) -> bool:
    if not prefilter:
        return True
    for line in logs:
        if "InitializeMint" in line:
            return True
    return False


async def run_rpc_mint_stream() -> None:
    """Primary entrypoint for the RPC-side mint streamer."""

    ws_url = os.getenv("MINT_STREAM_WS_URL")
    http_url = os.getenv("MINT_STREAM_HTTP_URL")
    redis_url = os.getenv("MINT_STREAM_REDIS_URL", "redis://localhost:6379/1")
    channel = os.getenv("MINT_STREAM_BROKER_CHANNEL", "solhunter-events-v3")
    base_programs = _parse_program_list("MINT_STREAM_PROGRAM_IDS", DEFAULT_STREAM_PROGRAMS)
    pump_ids = _parse_program_list("PUMP_FUN_PROGRAM_IDS", ())
    if not pump_ids:
        pump_ids = _parse_program_list("PUMP_PROGRAM_IDS", ())
    amm_ids = _parse_program_list("AMM_PROGRAM_IDS", ())

    prefilter = os.getenv("MINT_STREAM_PREFILTER", "1") != "0"
    max_queue = _safe_int_env("MINT_STREAM_MAX_QUEUE", 512, minimum=64)
    ping_interval = _safe_int_env("MINT_STREAM_PING_INTERVAL", 20, minimum=5)
    ping_timeout = _safe_int_env("MINT_STREAM_PING_TIMEOUT", 20, minimum=5)
    backoff_start = _safe_float_env("MINT_STREAM_BACKOFF_START", 1.0, minimum=0.5)
    backoff_cap = _safe_float_env("MINT_STREAM_BACKOFF_CAP", 20.0, minimum=backoff_start)
    tx_timeout = _safe_float_env("MINT_STREAM_TX_TIMEOUT", 4.0, minimum=1.0)
    dedup_ttl = _safe_float_env("MINT_STREAM_DEDUP_TTL_SEC", 3600.0, minimum=60.0)

    if not ws_url or not http_url:
        raise RuntimeError("MINT_STREAM_WS_URL and MINT_STREAM_HTTP_URL must be configured")

    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    backoff = backoff_start
    seen_programs: set[str] = set()
    sub_ids: List[Tuple[str, int]] = []

    def _add_subscription(program: str, sub_id: int) -> None:
        if program and program not in seen_programs:
            seen_programs.add(program)
            sub_ids.append((program, sub_id))

    for idx, program_id in enumerate(base_programs, start=1000):
        _add_subscription(program_id, idx)
    for idx, program_id in enumerate(pump_ids, start=2000):
        _add_subscription(program_id, idx)
    for idx, program_id in enumerate(amm_ids, start=3000):
        _add_subscription(program_id, idx)

    while True:
        try:
            async with websockets.connect(
                ws_url,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                max_queue=max_queue,
            ) as ws, aiohttp.ClientSession() as session:
                for program_id, sub_id in sub_ids:
                    with contextlib.suppress(Exception):
                        await _subscribe_logs(ws, program_id, sub_id)

                backoff = backoff_start

                while True:
                    raw = await ws.recv()
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if message.get("method") != "logsNotification":
                        continue

                    result = (message.get("params") or {}).get("result")
                    if not isinstance(result, dict):
                        continue
                    value = result.get("value") or {}
                    signature = value.get("signature")
                    if not isinstance(signature, str):
                        continue
                    if signature in _SEEN_SIG:
                        continue
                    _SEEN_SIG[signature] = _now()

                    mentions = value.get("mentions") or []
                    if not isinstance(mentions, list):
                        mentions = []
                    logs = value.get("logs") or []
                    if not isinstance(logs, list):
                        logs = []

                    is_pump = any(pid in mentions for pid in pump_ids)
                    is_token_program = any(
                        pid in mentions for pid in (TOKEN_PROGRAM, TOKEN2022_PROGRAM)
                    )
                    is_metadata = METAPLEX_METADATA_PROGRAM in mentions
                    is_associated = ASSOCIATED_TOKEN_PROGRAM in mentions
                    is_system = SYSTEM_PROGRAM in mentions
                    is_amm = any(pid in mentions for pid in amm_ids)

                    if (
                        not is_pump
                        and not is_token_program
                        and not is_metadata
                        and not is_associated
                        and not is_system
                        and not is_amm
                    ):
                        continue
                    if (
                        prefilter
                        and not is_pump
                        and not is_metadata
                        and not is_associated
                        and not is_system
                        and not is_amm
                        and not _should_fetch_logs(True, logs)
                    ):
                        continue

                    with contextlib.suppress(Exception):
                        tx_json = await _fetch_tx(session, http_url, signature, timeout=tx_timeout)
                        mints = _extract_mints_from_tx(tx_json)
                        for mint in mints:
                            if mint in _SEEN_MINT:
                                continue
                            _SEEN_MINT[mint] = _now()
                            if is_pump:
                                source = "pump_logs"
                            elif is_amm:
                                source = "amm_logs"
                            elif is_metadata:
                                source = "metadata_logs"
                            elif is_associated:
                                source = "ata_logs"
                            elif is_system:
                                source = "system_logs"
                            elif is_token_program:
                                source = "token_logs"
                            else:
                                source = "rpc_logs"
                            event = {
                                "topic": "token_discovered",
                                "ts": _now(),
                                "source": source,
                                "mint": mint,
                                "tx": signature,
                                "tags": (
                                    ["pump"]
                                    if is_pump
                                    else (["amm"] if is_amm else [])
                                ),
                                "interface": "FungibleToken",
                                "discovery": {
                                    "method": source,
                                },
                            }
                            await redis_client.publish(channel, json.dumps(event, separators=(",", ":")))

                    _keep_ttl(_SEEN_SIG, dedup_ttl)
                    _keep_ttl(_SEEN_MINT, dedup_ttl)
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.6 + random.uniform(0, 0.5), backoff_cap)


def _parse_program_list(name: str, default: Iterable[str] | Tuple[str, ...]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return tuple(default)
    tokens = []
    for candidate in raw.split(","):
        candidate = candidate.strip()
        if candidate:
            tokens.append(candidate)
    return tuple(tokens)


def _safe_int_env(name: str, default: int, *, minimum: int) -> int:
    raw = os.getenv(name)
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except (TypeError, ValueError):
        return max(minimum, default)


def _safe_float_env(name: str, default: float, *, minimum: float) -> float:
    raw = os.getenv(name)
    if not raw:
        return max(minimum, default)
    try:
        return max(minimum, float(raw))
    except (TypeError, ValueError):
        return max(minimum, default)


if __name__ == "__main__":
    asyncio.run(run_rpc_mint_stream())
