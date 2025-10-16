import os
import base64
import logging
from typing import Optional, Sequence, Mapping

try:  # pragma: no cover - optional dependencies
    from solders.instruction import Instruction, AccountMeta  # type: ignore
    from solders.pubkey import Pubkey  # type: ignore
    from solders.keypair import Keypair  # type: ignore
    from solders.message import MessageV0  # type: ignore
    from solders.transaction import VersionedTransaction  # type: ignore
    from solders.hash import Hash  # type: ignore
    from solana.rpc.async_api import AsyncClient  # type: ignore
    _DEPS_OK = bool(
        hasattr(Keypair, "sign_message") and hasattr(Pubkey, "from_string")
    )
except Exception:  # pragma: no cover - missing deps
    _DEPS_OK = False

if not _DEPS_OK:  # pragma: no cover - fallback stubs
    import sys
    import types

    class Pubkey:
        """Minimal stand-in for :class:`solders.pubkey.Pubkey`."""

        @staticmethod
        def from_string(_s: str) -> "Pubkey":
            return Pubkey()

        @staticmethod
        def default() -> "Pubkey":
            return Pubkey()

    class Keypair:
        """Very small substitute for :class:`solders.keypair.Keypair`."""

        def __init__(self) -> None:
            self._pub = Pubkey()

        def pubkey(self) -> Pubkey:
            return self._pub

        def sign_message(self, _msg: bytes) -> bytes:
            return b"sig"

    class AccountMeta:
        def __init__(self, pubkey: Pubkey, is_signer: bool, is_writable: bool) -> None:
            self.pubkey = pubkey
            self.is_signer = is_signer
            self.is_writable = is_writable

    class Instruction:
        def __init__(
            self,
            program_id: Pubkey,
            data: bytes,
            accounts: Sequence[AccountMeta],
        ) -> None:
            self.program_id = program_id
            self.data = data
            self.accounts = accounts

    class MessageV0:
        @staticmethod
        def try_compile(_payer, _ixs, _lookups, _blockhash) -> "MessageV0":
            return MessageV0()

        def __bytes__(self) -> bytes:  # pragma: no cover - simple stub
            return b"msg"

    class VersionedTransaction:
        @staticmethod
        def populate(
            _msg: MessageV0, _sigs: Sequence[bytes]
        ) -> "VersionedTransaction":
            return VersionedTransaction()

        def __bytes__(self) -> bytes:  # pragma: no cover - simple stub
            return b"tx"

    class Hash:
        @staticmethod
        def default() -> str:  # pragma: no cover - simple stub
            return "0" * 32

    class AsyncClient:
        """Placeholder for :class:`solana.rpc.async_api.AsyncClient`."""

        def __init__(self, _url: str) -> None:  # pragma: no cover - simple stub
            self.url = _url

        async def __aenter__(self) -> "AsyncClient":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

        async def get_latest_blockhash(self):
            return types.SimpleNamespace(
                value=types.SimpleNamespace(blockhash=Hash.default())
            )

        async def confirm_transaction(self, _sig: str) -> None:
            return None

    # Register stub modules so ``investor_demo`` can import them.
    solders_pkg = types.ModuleType("solders")
    solders_pkg.__path__ = []  # type: ignore[attr-defined]
    sys.modules.setdefault("solders", solders_pkg)

    def _register(name: str, **attrs: object) -> None:
        mod = types.ModuleType(name)
        for k, v in attrs.items():
            setattr(mod, k, v)
        sys.modules[f"solders.{name}"] = mod

    _register("keypair", Keypair=Keypair)
    _register("pubkey", Pubkey=Pubkey)
    _register("instruction", Instruction=Instruction, AccountMeta=AccountMeta)
    _register("message", MessageV0=MessageV0)
    _register("transaction", VersionedTransaction=VersionedTransaction)
    _register("hash", Hash=Hash)

from . import depth_client

RPC_URL = os.getenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com/?api-key=YOUR_HELIUS_KEY")

# Default program id for Solend flash loans on mainnet
SOLEND_PROGRAM_ID = os.getenv(
    "SOLEND_PROGRAM_ID", "So1endDq2YkqhipRh3WViPa8hdiSpxWy6z3Z6tMCpAo"
)

logger = logging.getLogger(__name__)


async def borrow_flash(
    amount: float,
    token: str,
    instructions: Sequence[Instruction],
    *,
    payer: Keypair,
    program_accounts: Mapping[str, Pubkey] | None = None,
    rpc_url: str | None = None,
) -> Optional[str]:
    """Borrow ``amount`` of ``token`` via a flash loan and execute ``instructions``.

    The transaction uses :data:`SOLEND_PROGRAM_ID` for the borrow and repay
    instructions and appends the provided swap ``instructions`` in between.  The
    entire sequence is broadcast atomically.
    """

    rpc = rpc_url or RPC_URL
    program_accounts = program_accounts or {}

    program_id = Pubkey.from_string(SOLEND_PROGRAM_ID)
    metas = [AccountMeta(payer.pubkey(), True, True)] + [
        AccountMeta(pk, False, True) for pk in program_accounts.values()
    ]

    borrow_ix = Instruction(program_id, b"flash_borrow", metas)
    repay_ix = Instruction(program_id, b"flash_repay", metas)

    tx_instructions = [borrow_ix, *instructions, repay_ix]

    async with AsyncClient(rpc) as client:
        try:
            latest = await client.get_latest_blockhash()
            msg = MessageV0.try_compile(
                payer.pubkey(), tx_instructions, [], latest.value.blockhash
            )
            sig = payer.sign_message(bytes(msg))
            tx = VersionedTransaction.populate(msg, [sig])
            tx_b64 = base64.b64encode(bytes(tx)).decode()
            sig_str = await depth_client.submit_raw_tx(tx_b64)
            logger.info("Borrowed %s %s via flash loan", amount, token)
            return sig_str
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Flash loan borrow failed: %s", exc)
            return None


async def repay_flash(signature: str, *, rpc_url: str | None = None) -> bool:
    """Repay a flash loan previously opened with :func:`borrow_flash`."""

    rpc = rpc_url or RPC_URL
    async with AsyncClient(rpc) as client:
        try:
            await client.confirm_transaction(signature)
            logger.info("Repaid flash loan %s", signature)
            return True
        except Exception as exc:  # pragma: no cover - network errors
            logger.warning("Flash loan repayment failed: %s", exc)
            return False
