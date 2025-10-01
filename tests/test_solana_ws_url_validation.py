import logging
import os

from solhunter_zero.config import get_solana_ws_url


def test_derived_ws_url_no_warning(monkeypatch, caplog):
    monkeypatch.setenv("SOLANA_RPC_URL", "https://rpc.example.com")
    monkeypatch.delenv("SOLANA_WS_URL", raising=False)
    caplog.set_level(logging.WARNING)
    url = get_solana_ws_url()
    assert url == "wss://rpc.example.com"
    assert os.environ["SOLANA_WS_URL"] == "wss://rpc.example.com"
    assert caplog.text == ""
