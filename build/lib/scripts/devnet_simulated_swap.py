#!/usr/bin/env python3
"""
Simulate a swap on Solana devnet by atomically exchanging SOL for a custom SPL
token between two accounts you control (user <-> pool). This proves signing and
asset movement without relying on public DEX APIs.

Flow:
  - Ensure devnet RPC, load user keypair from KEYPAIR_PATH.
  - Load (or create) a pool keypair at keypairs/pool.json.
  - Ensure SPL token mint (via --mint or minted_mint.txt).
  - Ensure token ATAs for user and pool (via spl-token CLI).
  - Seed the pool with your SPL token if needed (via spl-token transfer).
  - Build a single transaction:
      1) user -> pool: transfer SOL (lamports)
      2) pool -> user: SPL token transfer (raw units)
    Sign with both user and pool and broadcast.

Requirements: solana + spl-token CLI available on PATH.

Usage:
  KEYPAIR_PATH=keypairs/default.json \
  python scripts/devnet_simulated_swap.py --mint $(cat minted_mint.txt) \
      --sol-lamports 100000 --token-amount 100000
"""
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
from typing import Optional

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.instruction import AccountMeta, Instruction
from solders.system_program import TransferParams, transfer
from solders.transaction import VersionedTransaction
from solders.message import MessageV0
from solana.rpc.api import Client


DEVNET_RPC = os.getenv("SOLANA_TESTNET_RPC_URL", "https://api.devnet.solana.com")
TOKEN_PROGRAM_ID = Pubkey.from_string("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")


def _require_cli(name: str) -> None:
    from shutil import which

    if which(name) is None:
        print(f"Missing CLI dependency: {name} not found on PATH", file=sys.stderr)
        raise SystemExit(2)


def _run(cmd: list[str], env: Optional[dict] = None) -> str:
    p = subprocess.run(cmd, capture_output=True, text=True, env=env or os.environ.copy())
    if p.returncode != 0:
        print("Command failed:", " ".join(cmd), file=sys.stderr)
        print(p.stderr.strip(), file=sys.stderr)
        raise SystemExit(p.returncode)
    return p.stdout.strip()


def load_or_create_pool(path: str) -> Keypair:
    if os.path.exists(path):
        with open(path, "r") as f:
            arr = json.load(f)
        return Keypair.from_bytes(bytes(arr))
    kp = Keypair()
    with open(path, "w") as f:
        json.dump(list(bytes(kp)), f)
    return kp


def ensure_ata_address(mint: str, owner: str, *, env: dict) -> str:
    # spl-token newer CLI expects --verbose; output can include extra text.
    out = _run([
        "spl-token",
        "address",
        "--owner",
        owner,
        "--token",
        mint,
        "--verbose",
    ], env=env)
    # Take the last non-empty token that looks like a base58 pubkey.
    lines = [ln.strip() for ln in out.splitlines() if ln.strip()]
    last = lines[-1]
    parts = last.split()
    return parts[-1]


def ensure_pool_seed(mint: str, pool_pubkey: str, amount: int, *, env: dict) -> None:
    # Transfer tokens from user to pool to seed liquidity if pool ATA is empty
    # spl-token balance <mint> --owner <pool_pubkey>
    try:
        bal_out = _run(["spl-token", "balance", mint, "--owner", pool_pubkey], env=env)
        bal_val = float(bal_out.split()[0]) if bal_out else 0.0
    except SystemExit:
        bal_val = 0.0
    if bal_val > 0:
        return
    # fund recipient and allow unfunded
    _run(
        [
            "spl-token",
            "transfer",
            mint,
            str(amount),
            pool_pubkey,
            "--allow-unfunded-recipient",
            "--fund-recipient",
        ],
        env=env,
    )


def build_token_transfer_ix(mint: Pubkey, src_ata: Pubkey, dst_ata: Pubkey, owner: Pubkey, amount: int) -> Instruction:
    # SPL Token Transfer instruction (index 3) with amount as u64 LE
    data = bytes([3]) + amount.to_bytes(8, "little", signed=False)
    return Instruction(
        program_id=TOKEN_PROGRAM_ID,
        accounts=[
            AccountMeta(pubkey=src_ata, is_signer=False, is_writable=True),
            AccountMeta(pubkey=dst_ata, is_signer=False, is_writable=True),
            AccountMeta(pubkey=owner, is_signer=True, is_writable=False),
        ],
        data=data,
    )


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Devnet simulated swap (SOL <-> custom SPL token)")
    p.add_argument("--mint", help="SPL token mint address (or provide minted_mint.txt)")
    p.add_argument("--sol-lamports", type=int, default=100_000, help="Lamports to send user->pool")
    p.add_argument("--token-amount", type=int, default=100_000, help="Token raw units to send pool->user")
    args = p.parse_args(argv)

    _require_cli("solana")
    _require_cli("spl-token")

    mint = args.mint
    if not mint and os.path.exists("minted_mint.txt"):
        mint = open("minted_mint.txt").read().strip()
    if not mint:
        print("Missing --mint and minted_mint.txt", file=sys.stderr)
        return 2

    keypair_path = os.getenv("KEYPAIR_PATH", "keypairs/default.json")
    if not os.path.exists(keypair_path):
        print(f"KEYPAIR_PATH not found: {keypair_path}", file=sys.stderr)
        return 2
    with open(keypair_path) as f:
        user = Keypair.from_bytes(bytes(json.load(f)))

    pool_path = os.path.join("keypairs", "pool.json")
    os.makedirs("keypairs", exist_ok=True)
    pool = load_or_create_pool(pool_path)

    # Ensure cluster
    env_user = os.environ.copy()
    env_user["SOLANA_KEYPAIR"] = keypair_path
    _run(["solana", "config", "set", "--url", DEVNET_RPC], env=env_user)
    # Airdrop small SOL for pool
    try:
        _run(["solana", "airdrop", "1", str(pool.pubkey())], env=env_user)
    except SystemExit:
        pass

    user_pub = str(user.pubkey())
    pool_pub = str(pool.pubkey())
    # Ensure ATAs (spl-token will create if missing)
    user_ata = ensure_ata_address(mint, user_pub, env=env_user)
    pool_ata = ensure_ata_address(mint, pool_pub, env=env_user)
    # Seed pool with tokens if empty
    ensure_pool_seed(mint, pool_pub, args.token_amount * 10, env=env_user)

    # Build atomic swap tx: user sends SOL, pool sends tokens
    client = Client(DEVNET_RPC)
    recent = client.get_latest_blockhash().value
    ix1 = transfer(TransferParams(from_pubkey=user.pubkey(), to_pubkey=pool.pubkey(), lamports=args.sol_lamports))
    ix2 = build_token_transfer_ix(
        mint=Pubkey.from_string(mint),
        src_ata=Pubkey.from_string(pool_ata),
        dst_ata=Pubkey.from_string(user_ata),
        owner=pool.pubkey(),
        amount=args.token_amount,
    )
    msg = MessageV0.try_compile(user.pubkey(), [ix1, ix2], [], recent.blockhash)

    # Sign by both user and pool
    sig_user = user.sign_message(bytes(msg))
    # The second signer replaces the second signature; pad if necessary
    tx = VersionedTransaction.populate(msg, [sig_user])
    # Manually append second signature: rebuild with two signatures
    tx = VersionedTransaction.populate(msg, [sig_user, pool.sign_message(bytes(msg))])
    raw = bytes(tx)
    res = client.send_raw_transaction(raw)
    print({"sim_swap_sig": str(res.value), "user": user_pub, "pool": pool_pub, "mint": mint})
    print(f"Explorer: https://explorer.solana.com/tx/{res.value}?cluster=devnet")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
