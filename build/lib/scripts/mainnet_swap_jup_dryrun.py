#!/usr/bin/env python3
"""
Mainnet dry-run: Jupiter v6 quote+swap for WSOL -> USDC (no broadcast).

Validates the full flow (quote -> swap tx -> local sign) without sending the
transaction to the network. Useful for proving integration while running agents
on devnet.

Usage:
  KEYPAIR_PATH=keypairs/default.json python scripts/mainnet_swap_jup_dryrun.py \
    --amount-sol 0.001 --slippage-bps 50
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
from typing import Any, Dict

import aiohttp
from solders.keypair import Keypair
from solders.transaction import VersionedTransaction


QUOTE_URL = os.getenv("DEX_BASE_URL", "https://swap.helius.dev") + "/v6/quote"
SWAP_URL = os.getenv("DEX_BASE_URL", "https://swap.helius.dev") + "/v6/swap"

# Mainnet mints
WSOL = "So11111111111111111111111111111111111111112"
USDC = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"


def load_keypair(path: str | None) -> Keypair:
    if not path:
        cand = os.path.join("keypairs", "default.json")
        if os.path.exists(cand):
            path = cand
    if not path or not os.path.exists(path):
        print("Set KEYPAIR_PATH to your keypair JSON path", file=sys.stderr)
        raise SystemExit(2)
    with open(path, "r") as f:
        arr = json.load(f)
    return Keypair.from_bytes(bytes(arr))


async def jup_quote_swap(
    *,
    session: aiohttp.ClientSession,
    user_pubkey: str,
    amount_in: int,
    slippage_bps: int,
    input_mint: str = WSOL,
    output_mint: str = USDC,
) -> Dict[str, Any]:
    # 1) Quote (mainnet)
    params = {
        "inputMint": input_mint,
        "outputMint": output_mint,
        "amount": str(amount_in),
        "slippageBps": str(slippage_bps),
        "swapMode": "ExactIn",
        "onlyDirectRoutes": "false",
    }
    async with session.get(QUOTE_URL, params=params, timeout=20) as r:
        if r.status >= 400:
            txt = await r.text()
            raise RuntimeError(f"quote {r.status}: {txt}")
        quote = await r.json()
    if not quote or not quote.get("routePlan"):
        raise RuntimeError("No route returned by Jupiter (mainnet)")

    # 2) Swap transaction (unsigned)
    body = {
        "quoteResponse": quote,
        "userPublicKey": user_pubkey,
        "wrapAndUnwrapSol": True,
        "useLegacyTransaction": False,
        "dynamicComputeUnitLimit": True,
        "computeUnitPriceMicroLamports": 0,
    }
    async with session.post(SWAP_URL, json=body, timeout=20) as r:
        if r.status >= 400:
            txt = await r.text()
            raise RuntimeError(f"swap {r.status}: {txt}")
        swap = await r.json()
    if not swap or not swap.get("swapTransaction"):
        raise RuntimeError(f"Swap response missing transaction: {swap}")
    return swap


def sign_tx_b64(tx_b64: str, kp: Keypair) -> tuple[bytes, str]:
    tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
    sig = kp.sign_message(bytes(tx.message))
    signed = VersionedTransaction.populate(tx.message, [sig] + tx.signatures[1:])
    return bytes(signed), base64.b64encode(bytes(sig)).decode()


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Mainnet dry-run WSOL->USDC via Jupiter v6")
    p.add_argument("--amount-sol", type=float, default=0.001, help="ExactIn amount in SOL (WSOL)")
    p.add_argument("--slippage-bps", type=int, default=50, help="Slippage tolerance in bps")
    args = p.parse_args(argv)

    kp = load_keypair(os.getenv("KEYPAIR_PATH"))
    amount_in = int(args.amount_sol * 1_000_000_000)

    headers = {"User-Agent": "Mozilla/5.0"}
    async with aiohttp.ClientSession(headers=headers) as session:
        try:
            swap = await jup_quote_swap(
                session=session,
                user_pubkey=str(kp.pubkey()),
                amount_in=amount_in,
                slippage_bps=args.slippage_bps,
            )
        except Exception as e:  # noqa: BLE001
            print({"jup_error": str(e)})
            return 1

    try:
        raw, sig_b64 = sign_tx_b64(swap["swapTransaction"], kp)
        print({
            "dry_run": True,
            "signed_locally": True,
            "tx_size": len(raw),
            "user_pubkey": str(kp.pubkey()),
            "local_signature_b64": sig_b64,
        })
    except Exception as e:  # noqa: BLE001
        print({"sign_error": str(e)})
        return 2

    # Intentionally do not broadcast.
    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        rc = asyncio.run(main())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)
