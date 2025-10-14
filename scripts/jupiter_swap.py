#!/usr/bin/env python3
"""
Minimal Jupiter swap helper.

Fetches a quote from Jupiter, builds a swap transaction, signs it with the
configured wallet, and submits it through the configured Solana RPC (default:
Helius).  Mirrors the TypeScript example discussed in the playbook so the
Python runtime can perform the same flow without the rest of the pipeline.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import sys
from dataclasses import dataclass
from typing import Any, Dict

import aiohttp
import base58
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction
from solana.rpc.async_api import AsyncClient
from solana.rpc.commitment import Confirmed
from solana.rpc.types import TxOpts

DEFAULT_RPC_URL = "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"
DEFAULT_JUP_BASE = "https://quote-api.jup.ag"


def _to_units(amount: float, decimals: int) -> int:
    scale = 10 ** decimals
    return int(amount * scale)


@dataclass(slots=True)
class SwapConfig:
    rpc_url: str
    quote_url: str
    swap_url: str
    wallet: Keypair
    slippage_bps: int
    skip_preflight: bool
    max_retries: int


async def _fetch_json(
    session: aiohttp.ClientSession,
    url: str,
    *,
    method: str = "GET",
    payload: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    request_kwargs = {}
    if payload is not None:
        request_kwargs["json"] = payload
    async with session.request(method, url, **request_kwargs) as resp:
        resp.raise_for_status()
        data: Any = await resp.json(content_type=None)
        if not isinstance(data, dict):
            raise RuntimeError(f"{url} returned non-JSON payload")
        return data


async def build_and_send_swap(
    cfg: SwapConfig,
    *,
    input_mint: str,
    output_mint: str,
    amount: int,
    wrap_sol: bool,
) -> str:
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        params = (
            f"?inputMint={input_mint}"
            f"&outputMint={output_mint}"
            f"&amount={amount}"
            f"&slippageBps={cfg.slippage_bps}"
        )
        quote = await _fetch_json(session, cfg.quote_url + params)
        swap_payload = {
            "quoteResponse": quote,
            "userPublicKey": str(cfg.wallet.pubkey()),
            "wrapAndUnwrapSol": wrap_sol,
        }
        swap = await _fetch_json(
            session,
            cfg.swap_url,
            method="POST",
            payload=swap_payload,
        )

    raw = swap.get("swapTransaction")
    if not isinstance(raw, str):
        raise RuntimeError("Swap response missing transaction data")
    try:
        tx = VersionedTransaction.deserialize(base64.b64decode(raw))
    except Exception as exc:  # pragma: no cover - defensive
        raise RuntimeError("Failed to deserialize swap transaction") from exc

    tx.sign([cfg.wallet])

    async with AsyncClient(cfg.rpc_url, commitment=Confirmed) as client:
        opts = TxOpts(
            skip_preflight=cfg.skip_preflight,
            max_retries=cfg.max_retries,
        )
        resp = await client.send_raw_transaction(tx.serialize(), opts=opts)
        signature = resp.value
        await client.confirm_transaction(signature, commitment=Confirmed)
    return signature


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Execute a Jupiter swap via Helius RPC.")
    parser.add_argument("--rpc-url", default=None, help="Solana RPC endpoint (defaults to HELIUS_RPC_URL or built-in)")
    parser.add_argument("--wallet-secret", default=None, help="Base58 secret key; defaults to WALLET_SECRET env variable")
    parser.add_argument("--input-mint", required=True, help="Input mint address")
    parser.add_argument("--output-mint", required=True, help="Output mint address")
    parser.add_argument("--amount", type=float, required=True, help="Amount of input token (human units)")
    parser.add_argument("--decimals", type=int, default=9, help="Decimals for input token (default: 9)")
    parser.add_argument("--slippage-bps", type=int, default=100, help="Slippage tolerance in basis points (default: 100)")
    parser.add_argument("--wrap-sol", action="store_true", help="Wrap/unwrap SOL automatically when using native mint")
    parser.add_argument("--skip-preflight", action="store_true", help="Skip preflight checks when sending the transaction")
    parser.add_argument("--max-retries", type=int, default=3, help="Max retries for sendRawTransaction (default: 3)")
    parser.add_argument("--jup-base-url", default=None, help="Override Jupiter base URL (defaults to JUPITER_API_BASE or the canonical host)")
    return parser.parse_args()


def resolve_wallet(secret_override: str | None) -> Keypair:
    secret = secret_override or os.getenv("WALLET_SECRET")
    if not secret:
        raise SystemExit("WALLET_SECRET environment variable or --wallet-secret must be provided")
    try:
        raw = base58.b58decode(secret.strip())
    except Exception as exc:  # pragma: no cover - guard rails
        raise SystemExit(f"Failed to decode wallet secret: {exc}") from exc
    return Keypair.from_bytes(raw)


def build_config(args: argparse.Namespace, wallet: Keypair) -> SwapConfig:
    rpc_url = (
        args.rpc_url
        or os.getenv("HELIUS_RPC_URL")
        or os.getenv("SOLANA_RPC_URL")
        or DEFAULT_RPC_URL
    )

    base_url = (
        args.jup_base_url
        or os.getenv("JUPITER_API_BASE")
        or DEFAULT_JUP_BASE
    ).rstrip("/")
    quote_url = f"{base_url}/v6/quote"
    swap_url = f"{base_url}/v6/swap"

    return SwapConfig(
        rpc_url=rpc_url,
        quote_url=quote_url,
        swap_url=swap_url,
        wallet=wallet,
        slippage_bps=args.slippage_bps,
        skip_preflight=args.skip_preflight,
        max_retries=args.max_retries,
    )


async def main() -> None:
    args = parse_args()
    wallet = resolve_wallet(args.wallet_secret)
    cfg = build_config(args, wallet)

    amount = _to_units(args.amount, args.decimals)
    try:
        signature = await build_and_send_swap(
            cfg,
            input_mint=args.input_mint,
            output_mint=args.output_mint,
            amount=amount,
            wrap_sol=args.wrap_sol,
        )
    except Exception as exc:
        print(f"Swap failed: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc

    print(f"Swap signature: {signature}")


if __name__ == "__main__":
    asyncio.run(main())

