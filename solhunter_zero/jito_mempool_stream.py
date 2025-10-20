"""Jito mempool stream fallback publisher."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import random
import time
from typing import Dict, Iterable, List, Sequence, Tuple

import redis.asyncio as aioredis
import websockets

try:  # pragma: no cover - optional dependency
    from solders.transaction import VersionedTransaction  # type: ignore
except Exception:  # pragma: no cover - solders optional
    VersionedTransaction = None  # type: ignore

from .rpc_mint_stream import (
    ASSOCIATED_TOKEN_PROGRAM,
    DEFAULT_STREAM_PROGRAMS,
    METAPLEX_METADATA_PROGRAM,
    SYSTEM_PROGRAM,
    TOKEN2022_PROGRAM,
    TOKEN_PROGRAM,
)


_SEEN_SIG: Dict[str, float] = {}
_SEEN_MINT: Dict[str, float] = {}


def _now() -> float:
    return time.time()


def _keep_ttl(store: Dict[str, float], ttl: float) -> None:
    if ttl <= 0:
        store.clear()
        return
    if len(store) < 50000:
        return
    cutoff = _now() - ttl
    for key, recorded in list(store.items()):
        if recorded < cutoff:
            store.pop(key, None)


def _parse_float_env(name: str, default: float, *, minimum: float | None = None) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = float(raw)
        except (TypeError, ValueError):
            value = default
    if minimum is not None:
        return max(minimum, value)
    return value


def _parse_int_env(name: str, default: int, *, minimum: int = 1) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        value = default
    else:
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = default
    return max(minimum, value)


def _parse_program_list(name: str, default: Iterable[str]) -> Tuple[str, ...]:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return tuple(default)
    values: List[str] = []
    for token in raw.split(","):
        token = token.strip()
        if token:
            values.append(token)
    return tuple(values)


def _looks_like_pubkey(value: str) -> bool:
    return isinstance(value, str) and 30 <= len(value) <= 44


def _normalize_account_keys(raw_keys: Sequence[object]) -> List[str]:
    keys: List[str] = []
    for entry in raw_keys:
        if isinstance(entry, str):
            keys.append(entry)
        elif isinstance(entry, dict):
            pubkey = entry.get("pubkey") or entry.get("address")
            if isinstance(pubkey, str):
                keys.append(pubkey)
    return keys


def _decode_versioned_accounts(encoded_tx: str) -> Tuple[List[str], List[Dict[str, object]]]:
    if VersionedTransaction is None:
        return [], []
    try:
        raw_bytes = base64.b64decode(encoded_tx)
        vt = VersionedTransaction.from_bytes(raw_bytes)
        message = vt.message
    except Exception:
        return [], []
    try:
        account_keys = [str(pubkey) for pubkey in message.static_account_keys()]
    except Exception:
        account_keys = []
    instructions: List[Dict[str, object]] = []
    try:
        for ix in message.instructions():
            data = getattr(ix, "data", b"")
            if not isinstance(data, (bytes, bytearray)):
                data = bytes(data)
            instructions.append(
                {
                    "programIdIndex": ix.program_id_index,
                    "accounts": list(ix.accounts),
                    "data": base64.b64encode(data).decode("ascii"),
                }
            )
    except Exception:
        return account_keys, instructions
    return account_keys, instructions


def _extract_transaction_components(result: Dict[str, object]) -> Tuple[str | None, List[str], List[Dict[str, object]]]:
    signature = None
    account_keys: List[str] = []
    instructions: List[Dict[str, object]] = []

    tx = result.get("transaction")
    if isinstance(tx, dict):
        signatures = tx.get("signatures")
        if isinstance(signatures, list) and signatures:
            sig = signatures[0]
            if isinstance(sig, str):
                signature = sig
        message = tx.get("message")
        if isinstance(message, dict):
            raw_keys = message.get("accountKeys") or []
            account_keys = _normalize_account_keys(raw_keys)
            maybe = message.get("instructions") or message.get("compiledInstructions") or []
            if isinstance(maybe, list):
                instructions = [ix for ix in maybe if isinstance(ix, dict)]
    elif isinstance(tx, list) and tx:
        encoded = tx[0]
        if isinstance(encoded, str):
            account_keys, instructions = _decode_versioned_accounts(encoded)
        meta = tx[1] if len(tx) > 1 else None
        if isinstance(meta, dict):
            sigs = meta.get("signatures")
            if isinstance(sigs, list) and sigs and isinstance(sigs[0], str):
                signature = sigs[0]
    if not signature:
        sig = result.get("signature")
        if isinstance(sig, str):
            signature = sig
    return signature, account_keys, instructions


def _resolve_program_id(ix: Dict[str, object], account_keys: Sequence[str]) -> str | None:
    program_id = ix.get("programId")
    if isinstance(program_id, str):
        return program_id
    idx = ix.get("programIdIndex") or ix.get("program_index")
    if isinstance(idx, int) and 0 <= idx < len(account_keys):
        return account_keys[idx]
    return None


def _resolve_account_keys(ix: Dict[str, object], account_keys: Sequence[str]) -> List[str]:
    resolved: List[str] = []
    accounts = ix.get("accounts") or ix.get("accountIndices") or ix.get("accountKeys")
    if not isinstance(accounts, list):
        return resolved
    for entry in accounts:
        if isinstance(entry, int):
            if 0 <= entry < len(account_keys):
                resolved.append(account_keys[entry])
        elif isinstance(entry, str):
            resolved.append(entry)
    return resolved


def _select_metadata_mint(accounts: Sequence[str]) -> str | None:
    if len(accounts) >= 2 and _looks_like_pubkey(accounts[1]):
        return accounts[1]
    for candidate in accounts:
        if _looks_like_pubkey(candidate) and candidate != METAPLEX_METADATA_PROGRAM:
            return candidate
    return None


def _select_associated_mint(accounts: Sequence[str]) -> str | None:
    if len(accounts) >= 4 and _looks_like_pubkey(accounts[3]):
        return accounts[3]
    candidates = [acct for acct in accounts if _looks_like_pubkey(acct)]
    return candidates[-1] if candidates else None


def _candidate_mints(
    program_id: str,
    accounts: Sequence[str],
    pump_ids: Sequence[str],
    amm_ids: Sequence[str],
) -> List[Tuple[str, List[str]]]:
    tags: List[str] = []
    if program_id in pump_ids:
        tags.append("pump")
    if program_id in amm_ids:
        tags.append("amm")
    if program_id in {TOKEN_PROGRAM, TOKEN2022_PROGRAM}:
        if accounts:
            return [(accounts[0], tags.copy())]
    if program_id == ASSOCIATED_TOKEN_PROGRAM:
        mint = _select_associated_mint(accounts)
        if mint:
            return [(mint, tags.copy())]
    if program_id == METAPLEX_METADATA_PROGRAM:
        mint = _select_metadata_mint(accounts)
        if mint:
            tags = tags + ["metadata"]
            return [(mint, tags)]
    if program_id in pump_ids:
        candidates = [acct for acct in accounts if _looks_like_pubkey(acct)]
        if candidates:
            return [(candidates[0], tags or ["pump"])]
    if program_id in amm_ids:
        return [(acct, tags or ["amm"]) for acct in accounts if _looks_like_pubkey(acct)]
    return []


async def run_jito_mempool_stream() -> None:
    ws_url = os.getenv("MEMPOOL_STREAM_WS_URL") or os.getenv("JITO_WS_URL")
    auth_token = os.getenv("MEMPOOL_STREAM_AUTH") or os.getenv("JITO_WS_AUTH")
    if not ws_url or not auth_token:
        raise RuntimeError("MEMPOOL_STREAM_WS_URL and MEMPOOL_STREAM_AUTH must be configured")

    redis_url = os.getenv("MEMPOOL_STREAM_REDIS_URL", "redis://localhost:6379/0")
    channel = os.getenv("MEMPOOL_STREAM_BROKER_CHANNEL", "solhunter-events-v2")
    base_programs = _parse_program_list("MEMPOOL_STREAM_PROGRAM_IDS", DEFAULT_STREAM_PROGRAMS)
    pump_ids = _parse_program_list("PUMP_FUN_PROGRAM_IDS", ())
    if not pump_ids:
        pump_ids = _parse_program_list("PUMP_PROGRAM_IDS", ())
    amm_ids = _parse_program_list("AMM_PROGRAM_IDS", ())

    prefilter = os.getenv("MEMPOOL_STREAM_PREFILTER", "1") != "0"
    max_queue = _parse_int_env("MEMPOOL_STREAM_MAX_QUEUE", 1024, minimum=128)
    ping_interval = _parse_int_env("MEMPOOL_STREAM_PING_INTERVAL", 20, minimum=5)
    ping_timeout = _parse_int_env("MEMPOOL_STREAM_PING_TIMEOUT", 20, minimum=5)
    backoff_start = _parse_float_env("MEMPOOL_STREAM_BACKOFF_START", 1.0, minimum=0.5)
    backoff_cap = _parse_float_env("MEMPOOL_STREAM_BACKOFF_CAP", 20.0, minimum=backoff_start)
    dedup_ttl = _parse_float_env("MEMPOOL_STREAM_DEDUP_TTL_SEC", 3600.0, minimum=60.0)

    redis_client = aioredis.from_url(redis_url, decode_responses=True)
    backoff = backoff_start

    seen_programs: set[str] = set(base_programs) | set(pump_ids) | set(amm_ids)

    subscribe_payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": "subscribe",
        "params": [
            "mempool",
            {
                "commitment": "processed",
                "encoding": "jsonParsed",
            },
        ],
    }

    while True:
        try:
            async with websockets.connect(
                ws_url,
                extra_headers={"Authorization": f"Bearer {auth_token}"},
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                max_queue=max_queue,
            ) as ws:
                await ws.send(json.dumps(subscribe_payload, separators=(",", ":")))
                backoff = backoff_start

                while True:
                    raw = await ws.recv()
                    try:
                        message = json.loads(raw)
                    except json.JSONDecodeError:
                        continue
                    if message.get("method") not in {"mempoolNotification", "transactionsNotification"}:
                        continue
                    result = (message.get("params") or {}).get("result")
                    if not isinstance(result, dict):
                        continue

                    signature, account_keys, instructions = _extract_transaction_components(result)
                    if not signature or not account_keys or not instructions:
                        continue
                    if signature in _SEEN_SIG:
                        continue
                    _SEEN_SIG[signature] = _now()

                    for ix in instructions:
                        program_id = _resolve_program_id(ix, account_keys)
                        if not program_id:
                            continue
                        accounts = _resolve_account_keys(ix, account_keys)
                        if not accounts:
                            continue
                        if program_id not in seen_programs:
                            if prefilter:
                                continue
                        candidates = _candidate_mints(program_id, accounts, pump_ids, amm_ids)
                        if not candidates and program_id in {TOKEN_PROGRAM, TOKEN2022_PROGRAM} and not prefilter:
                            for acct in accounts:
                                if _looks_like_pubkey(acct):
                                    candidates = [(acct, [])]
                                    break
                        for mint, tags in candidates:
                            if not _looks_like_pubkey(mint):
                                continue
                            if mint in _SEEN_MINT:
                                continue
                            _SEEN_MINT[mint] = _now()
                            event = {
                                "topic": "token_discovered",
                                "ts": _now(),
                                "source": "jito_mempool",
                                "mint": mint,
                                "tx": signature,
                                "tags": tags,
                                "interface": "FungibleToken",
                                "discovery": {
                                    "method": "jito_mempool",
                                    "program": program_id,
                                },
                            }
                            await redis_client.publish(channel, json.dumps(event, separators=(",", ":")))

                    _keep_ttl(_SEEN_SIG, dedup_ttl)
                    _keep_ttl(_SEEN_MINT, dedup_ttl)
        except Exception:
            await asyncio.sleep(backoff)
            backoff = min(backoff * 1.6 + random.uniform(0, 0.5), backoff_cap)
