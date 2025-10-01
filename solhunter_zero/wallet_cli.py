from argparse import ArgumentParser
import asyncio
from . import wallet
from .http import close_session
from .util import install_uvloop

install_uvloop()


def main(argv: list[str] | None = None) -> int:
    parser = ArgumentParser(description="Manage Solana keypairs")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("list", help="List available keypairs")

    save_p = subparsers.add_parser("save", help="Save a keypair under a name")
    save_p.add_argument("name")
    save_p.add_argument("path")

    derive_p = subparsers.add_parser(
        "derive", help="Derive a keypair from a mnemonic and save it"
    )
    derive_p.add_argument("name")
    derive_p.add_argument("mnemonic")
    derive_p.add_argument("--passphrase", default="")

    select_p = subparsers.add_parser("select", help="Select active keypair")
    select_p.add_argument("name")

    init_p = subparsers.add_parser(
        "generate-default", help="Generate default keypair"
    )
    init_p.add_argument(
        "--encrypt-mnemonic",
        action="store_true",
        help="Encrypt stored mnemonic using MNEMONIC_ENCRYPTION_KEY",
    )

    args = parser.parse_args(argv)

    if args.command == "list":
        for name in wallet.list_keypairs():
            print(name)
    elif args.command == "save":
        kp = wallet.load_keypair(args.path)
        wallet.save_keypair(args.name, list(kp.to_bytes()))
    elif args.command == "derive":
        kp = wallet.load_keypair_from_mnemonic(args.mnemonic, args.passphrase)
        wallet.save_keypair(args.name, list(kp.to_bytes()))
    elif args.command == "select":
        wallet.select_keypair(args.name)
    elif args.command == "generate-default":
        wallet.generate_default_keypair(encrypt=args.encrypt_mnemonic)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    finally:
        asyncio.run(close_session())
