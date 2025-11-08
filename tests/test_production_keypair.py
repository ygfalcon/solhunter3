import json
import os
import stat

import pytest
from solders.keypair import Keypair

from solhunter_zero.production.keypair import resolve_live_keypair, verify_onchain_funds
from solhunter_zero.gas import LAMPORTS_PER_SOL


def _write_keypair(path) -> None:
    keypair = Keypair()
    data = list(keypair.to_bytes())
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(data, handle)
    os.chmod(path, 0o600)


def test_resolve_live_keypair_force(tmp_path, monkeypatch):
    keyfile = tmp_path / "id.json"
    _write_keypair(keyfile)
    monkeypatch.setenv("KEYPAIR_PATH", str(keyfile))
    monkeypatch.setenv("MODE", "paper")
    path, pubkey = resolve_live_keypair({}, announce=False, force=True)
    assert path == keyfile.resolve()
    assert os.environ["KEYPAIR_PATH"] == str(path)
    assert os.environ["SOLANA_KEYPAIR"] == str(path)
    assert os.environ["PAPER_TRADING"] == "0"
    assert os.environ["LIVE_KEYPAIR_READY"] == "1"
    assert str(pubkey) == os.environ["LIVE_KEYPAIR_PUBKEY"]
    for key in (
        "PAPER_TRADING",
        "LIVE_TRADING_DISABLED",
        "SHADOW_EXECUTOR_ONLY",
        "SOLANA_KEYPAIR",
        "LIVE_KEYPAIR_PUBKEY",
        "LIVE_KEYPAIR_READY",
        "LIVE_KEYPAIR_BALANCE_SOL",
        "LIVE_RPC_BLOCKHASH",
        "LIVE_KEYPAIR_ANNOUNCED",
    ):
        monkeypatch.delenv(key, raising=False)


def test_resolve_live_keypair_permissions(tmp_path, monkeypatch):
    keyfile = tmp_path / "id.json"
    _write_keypair(keyfile)
    os.chmod(keyfile, 0o644)
    assert stat.S_IMODE(os.stat(keyfile).st_mode) == 0o644
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("KEYPAIR_PATH", str(keyfile))
    try:
        resolve_live_keypair({}, announce=False, force=True)
    except SystemExit as exc:
        assert "permissions 0600" in str(exc)
    else:
        pytest.fail("expected SystemExit due to permissions")


def test_verify_onchain_funds_threshold(monkeypatch, tmp_path):
    keyfile = tmp_path / "id.json"
    _write_keypair(keyfile)
    monkeypatch.setenv("KEYPAIR_PATH", str(keyfile))
    monkeypatch.setenv("SOLANA_RPC_URL", "https://example.invalid")

    class DummyClient:
        def __init__(self, rpc_url: str, timeout=None) -> None:
            assert rpc_url == "https://example.invalid"
            self.timeout = timeout

        def get_balance(self, pubkey):  # pragma: no cover - interface mimics solana client
            return {"result": {"value": int(0.3 * LAMPORTS_PER_SOL)}}

        def get_latest_blockhash(self):  # pragma: no cover - interface mimics solana client
            return {"result": {"value": {"blockhash": "abc123"}}}

    monkeypatch.setattr("solhunter_zero.production.keypair.Client", DummyClient)

    with pytest.raises(SystemExit) as exc_info:
        verify_onchain_funds(min_sol=0.5)

    message = str(exc_info.value)
    assert "insufficient SOL" in message
    assert "minimum required is 0.500000000" in message
