#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass
from typing import Iterable
import time

from solders.keypair import Keypair
from solders.pubkey import Pubkey
from solders.system_program import (
    CreateAccountParams as SysCreateParams,
    CreateAccountWithSeedParams as SysCreateSeedParams,
    create_account as sys_create_account,
    create_account_with_seed as sys_create_account_with_seed,
)
from solders.system_program import TransferParams as SysTransferParams, transfer as sys_transfer
from solders.transaction import VersionedTransaction
from solders.hash import Hash
from solders.message import MessageV0
from solana.rpc.api import Client

from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import (
    InitializeMintParams,
    MintToParams,
    create_idempotent_associated_token_account,
    get_associated_token_address,
    initialize_mint,
    mint_to,
    transfer as spl_transfer,
)


DEVNET_RPC = os.getenv("SOLANA_TESTNET_RPC_URL", "https://api.devnet.solana.com")


def load_or_create(path: str) -> Keypair:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if os.path.exists(path):
        return Keypair.from_bytes(bytes(json.load(open(path))))
    kp = Keypair()
    with open(path, "w") as f:
        json.dump(list(bytes(kp)), f)
    return kp


def sign_and_send(client: Client, msg: MessageV0, signers: Iterable[Keypair]) -> str:
    # Build signature list in the exact order of required signers in account_keys
    signer_map = {kp.pubkey(): kp for kp in signers}
    raw_msg = bytes(msg)
    sigs = []
    # Num required signatures is the count of leading signer keys in account_keys
    for idx, key in enumerate(msg.account_keys):
        if not msg.is_signer(idx):
            continue
        kp = signer_map.get(key)
        if kp is None:
            # If a required signer is missing, raise clearly
            raise RuntimeError(f"Missing signer for {str(key)} at index {idx}")
        sigs.append(kp.sign_message(raw_msg))
    tx = VersionedTransaction.populate(msg, sigs)
    res = client.send_raw_transaction(bytes(tx))
    return str(res.value)


def ensure_airdrop(client: Client, pubkey: Pubkey, target_sol: float = 2.0, timeout: float = 30.0) -> None:
    target = int(target_sol * 1_000_000_000)
    bal = client.get_balance(pubkey).value
    if bal >= target:
        return
    try:
        sig = client.request_airdrop(pubkey, target - bal).value
    except Exception:
        return
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(1)
        bal = client.get_balance(pubkey).value
        if bal >= target:
            break


def main(argv: list[str] | None = None) -> int:
    # 1) Load keys
    user_path = os.getenv("KEYPAIR_PATH", "keypairs/cli.json")
    user = load_or_create(user_path)
    pool = load_or_create("keypairs/pool.json")
    # Derive a mint address with seed so only the user needs to sign
    seed = "mint1"
    mint_pub = Pubkey.create_with_seed(user.pubkey(), seed, TOKEN_PROGRAM_ID)

    client = Client(DEVNET_RPC)

    # 2) Fund user for rent/fees if needed
    ensure_airdrop(client, user.pubkey(), target_sol=2.0)

    # 3) Create mint account (Tx1)
    rent = client.get_minimum_balance_for_rent_exemption(82).value
    recent = client.get_latest_blockhash().value
    bh_ca = recent.blockhash if isinstance(recent.blockhash, Hash) else Hash.from_string(recent.blockhash)
    ix_ca = sys_create_account_with_seed(
        SysCreateSeedParams(
            from_pubkey=user.pubkey(),
            to_pubkey=mint_pub,
            base=user.pubkey(),
            seed=seed,
            lamports=rent,
            space=82,
            owner=TOKEN_PROGRAM_ID,
        )
    )
    msg_ca = MessageV0.try_compile(user.pubkey(), [ix_ca], [], bh_ca)
    sig_ca = sign_and_send(client, msg_ca, [user])
    print({"create_mint_account_sig": sig_ca, "mint": str(mint_pub)})

    # 4) Initialize mint (Tx2)
    recent = client.get_latest_blockhash().value
    bh_init = recent.blockhash if isinstance(recent.blockhash, Hash) else Hash.from_string(recent.blockhash)
    ix_init = initialize_mint(
        InitializeMintParams(
            decimals=6,
            program_id=TOKEN_PROGRAM_ID,
            mint=mint_pub,
            mint_authority=user.pubkey(),
            freeze_authority=None,
        )
    )
    msg_init = MessageV0.try_compile(user.pubkey(), [ix_init], [], bh_init)
    sig_init = sign_and_send(client, msg_init, [user])
    print({"initialize_mint_sig": sig_init})

    # 5) Create ATAs idempotently (Tx3)
    user_ata = get_associated_token_address(user.pubkey(), mint_pub)
    pool_ata = get_associated_token_address(pool.pubkey(), mint_pub)
    recent = client.get_latest_blockhash().value
    bh_atas = recent.blockhash if isinstance(recent.blockhash, Hash) else Hash.from_string(recent.blockhash)
    ix_user_ata = create_idempotent_associated_token_account(payer=user.pubkey(), owner=user.pubkey(), mint=mint_pub)
    ix_pool_ata = create_idempotent_associated_token_account(payer=user.pubkey(), owner=pool.pubkey(), mint=mint_pub)
    msg_atas = MessageV0.try_compile(user.pubkey(), [ix_user_ata, ix_pool_ata], [], bh_atas)
    sig_atas = sign_and_send(client, msg_atas, [user])
    print({"create_atas_sig": sig_atas, "user_ata": str(user_ata), "pool_ata": str(pool_ata)})
    try:
        with open("minted_mint.txt", "w") as f:
            f.write(str(mint_pub))
        with open("devnet_tokens.txt", "w") as f:
            f.write(str(mint_pub) + "\n")
    except Exception:
        pass

    # 3) Mint initial liquidity to pool ATA
    recent = client.get_latest_blockhash().value
    bh_mint = recent.blockhash if isinstance(recent.blockhash, Hash) else Hash.from_string(recent.blockhash)
    ix_mint = mint_to(
        MintToParams(
            program_id=TOKEN_PROGRAM_ID,
            mint=mint_pub,
            dest=pool_ata,
            mint_authority=user.pubkey(),
            amount=1_000_000_000,  # 1000 tokens @ 6 decimals
        )
    )
    msg_mint = MessageV0.try_compile(user.pubkey(), [ix_mint], [], bh_mint)
    sig_mint = sign_and_send(client, msg_mint, [user])
    print({"mint_to_pool_sig": sig_mint})

    # 4) Atomic simulated swap: user pays SOL, pool pays tokens
    recent = client.get_latest_blockhash().value
    bh_swap = recent.blockhash if isinstance(recent.blockhash, Hash) else Hash.from_string(recent.blockhash)
    ix_sol = sys_transfer(
        SysTransferParams(
            from_pubkey=user.pubkey(), to_pubkey=pool.pubkey(), lamports=100_000
        )
    )
    ix_tok = spl_transfer(
        program_id=TOKEN_PROGRAM_ID,
        source=pool_ata,
        dest=user_ata,
        owner=pool.pubkey(),
        amount=100_000,
        signers=[],
    )
    msg_swap = MessageV0.try_compile(user.pubkey(), [ix_sol, ix_tok], [], bh_swap)
    sig_swap = sign_and_send(client, msg_swap, [user, pool])
    print({"sim_swap_sig": sig_swap, "explorer": f"https://explorer.solana.com/tx/{sig_swap}?cluster=devnet"})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
