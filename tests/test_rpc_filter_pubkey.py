import pytest

from solders.pubkey import Pubkey

try:
    import importlib, sys
    import websockets
    if getattr(websockets, "__file__", None) is None:
        sys.modules.pop("websockets", None)
        sys.modules.pop("websockets.legacy", None)
        sys.modules.pop("websockets.legacy.client", None)
        websockets = importlib.import_module("websockets")
    from solana.rpc.websocket_api import RpcTransactionLogsFilterMentions
except Exception:  # pragma: no cover - module not available
    RpcTransactionLogsFilterMentions = None  # type: ignore

from solhunter_zero.scanner_onchain import TOKEN_PROGRAM_ID


@pytest.mark.skipif(RpcTransactionLogsFilterMentions is None, reason="websocket API unavailable")
def test_rpc_filter_accepts_pubkey() -> None:
    """Ensure RpcTransactionLogsFilterMentions accepts a solders Pubkey."""
    RpcTransactionLogsFilterMentions(
        Pubkey.from_string(str(TOKEN_PROGRAM_ID))
    )
