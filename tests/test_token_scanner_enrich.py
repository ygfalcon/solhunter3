import logging

from solhunter_zero.logging_utils import reset_warn_once_cache
from solhunter_zero import token_scanner


def _capture_warning_messages(caplog) -> list[str]:
    return [record.message for record in caplog.records if record.levelno >= logging.WARNING]


def test_default_enrich_rpc_allows_plain_rpc(monkeypatch, caplog):
    reset_warn_once_cache()
    monkeypatch.delenv("HELIUS_RPC_URL", raising=False)
    monkeypatch.setenv("SOLANA_RPC_URL", "https://api.mainnet-beta.solana.com")
    caplog.set_level(logging.WARNING, logger="solhunter_zero.token_scanner")

    result = token_scanner._default_enrich_rpc()

    assert result == "https://api.mainnet-beta.solana.com"
    assert "api-key" not in result
    assert "Helius RPC URL missing api-key" not in _capture_warning_messages(caplog)


def test_default_enrich_rpc_warns_for_helius_without_key(monkeypatch, caplog):
    reset_warn_once_cache()
    monkeypatch.delenv("HELIUS_RPC_URL", raising=False)
    monkeypatch.setenv("SOLANA_RPC_URL", "https://mainnet.helius-rpc.com")
    caplog.set_level(logging.WARNING, logger="solhunter_zero.token_scanner")

    result = token_scanner._default_enrich_rpc()

    assert result == "https://mainnet.helius-rpc.com"
    messages = _capture_warning_messages(caplog)
    assert any("Helius RPC URL missing api-key" in msg for msg in messages)


def test_default_enrich_rpc_warns_when_using_public_default(monkeypatch, caplog):
    reset_warn_once_cache()
    monkeypatch.delenv("HELIUS_RPC_URL", raising=False)
    monkeypatch.delenv("SOLANA_RPC_URL", raising=False)
    caplog.set_level(logging.WARNING, logger="solhunter_zero.token_scanner")

    result = token_scanner._default_enrich_rpc()

    assert result == "https://api.mainnet-beta.solana.com"
    messages = _capture_warning_messages(caplog)
    assert any("defaulting to public Solana RPC" in msg for msg in messages)
