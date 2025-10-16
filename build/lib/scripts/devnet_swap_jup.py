#!/usr/bin/env python3
"""
Quote + swap on Solana devnet via Jupiter v6 for WSOL -> USDC-Dev.

Usage:
  KEYPAIR_PATH=keypairs/default.json python scripts/devnet_swap_jup.py \
      --amount-sol 0.001 --slippage-bps 50

This script will:
  - ensure the devnet wallet has enough SOL (airdrop if needed),
  - GET a Jupiter quote for WSOL -> USDC-Dev,
  - POST the swap to receive a base64 transaction,
  - sign with the provided keypair and broadcast to devnet.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import sys
import time
from typing import Any, Dict

import aiohttp
from solana.rpc.api import Client
from solana.rpc.types import TxOpts
from solders.keypair import Keypair
from solders.message import MessageV0
from solders.transaction import VersionedTransaction


DEVNET_RPC = os.getenv("SOLANA_TESTNET_RPC_URL", "https://api.devnet.solana.com")
QUOTE_URL = os.getenv("DEX_TESTNET_URL", "https://quote-api.jup.ag") + "/v6/quote"
SWAP_URL = os.getenv("DEX_TESTNET_URL", "https://quote-api.jup.ag") + "/v6/swap"

# Mints
WSOL = "So11111111111111111111111111111111111111112"
USDC_DEV = "4zMMC9srt5Ri5X14GAgXhaHii3GnPAEERYPJgZJDncDU"


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


def ensure_airdrop(client: Client, kp: Keypair, min_lamports: int = 100_000_000) -> None:
    bal = client.get_balance(kp.pubkey()).value
    print({"devnet_balance_before": bal})
    if bal >= min_lamports:
        return
    try:
        print("Requesting devnet airdrop...")
        sig = client.request_airdrop(kp.pubkey(), 1_000_000_000).value  # 1 SOL
        print({"airdrop_sig": str(sig)})
        for _ in range(25):
            time.sleep(1)
            bal = client.get_balance(kp.pubkey()).value
            if bal >= min_lamports:
                break
    except Exception as e:  # noqa: BLE001
        print({"airdrop_error": str(e)})
    print({"devnet_balance_after": bal})


async def jup_quote_swap(
    *,
    session: aiohttp.ClientSession,
    user_pubkey: str,
    amount_in: int,
    slippage_bps: int,
    input_mint: str = WSOL,
    output_mint: str = USDC_DEV,
) -> Dict[str, Any]:
    # 1) Quote
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
        raise RuntimeError("No route returned by Jupiter for devnet")

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
        r.raise_for_status()
        swap = await r.json()
    if not swap or not swap.get("swapTransaction"):
        raise RuntimeError(f"Swap response missing transaction: {swap}")
    return swap


def sign_tx_b64(tx_b64: str, kp: Keypair) -> bytes:
    tx = VersionedTransaction.from_bytes(base64.b64decode(tx_b64))
    # Sign the message with the user keypair and preserve other signatures.
    sig = kp.sign_message(bytes(tx.message))
    signed = VersionedTransaction.populate(tx.message, [sig] + tx.signatures[1:])
    return bytes(signed)


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Devnet WSOL->USDC-Dev via Jupiter v6")
    p.add_argument("--amount-sol", type=float, default=0.001, help="ExactIn amount in SOL (WSOL)")
    p.add_argument("--slippage-bps", type=int, default=50, help="Slippage tolerance in bps")
    args = p.parse_args(argv)

    kp = load_keypair(os.getenv("KEYPAIR_PATH"))
    client = Client(DEVNET_RPC)
    ensure_airdrop(client, kp)

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
        raw = sign_tx_b64(swap["swapTransaction"], kp)
        res = client.send_raw_transaction(raw, opts=TxOpts(skip_preflight=True))
        sig = str(res.value)
        print({"signature": sig})
        print(f"Explorer: https://explorer.solana.com/tx/{sig}?cluster=devnet")
    except Exception as e:  # noqa: BLE001
        print({"send_error": str(e), "swap_keys": list(swap.keys())})
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover
    try:
        rc = asyncio.run(main())
    except KeyboardInterrupt:
        rc = 130
    sys.exit(rc)
