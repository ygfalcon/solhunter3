import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from solhunter_zero._preflight import (
    derive_ws_url,
    resolve_keypair,
    validate_agent_weights,
    verify_flashloan_prereqs,
)


# ---------- derive_ws_url ----------
def test_derive_ws_url_https_with_path_and_query():
    http = "https://api.example.com/rpc/v1?auth=abc&x=1"
    ws = derive_ws_url(http)
    assert ws == "wss://api.example.com/rpc/v1?auth=abc&x=1"


def test_derive_ws_url_http_custom_port():
    http = "http://localhost:8899"
    ws = derive_ws_url(http)
    assert ws == "ws://localhost:8899"


# ---------- resolve_keypair ----------
def test_resolve_keypair_auto_select_single(tmp_path, monkeypatch):
    kpdir = tmp_path / "keypairs"
    kpdir.mkdir()
    (kpdir / "bot.json").write_text("[1,2,3,4]", encoding="utf-8")
    monkeypatch.setenv("AUTO_SELECT_KEYPAIR", "1")
    p = resolve_keypair(dir_=str(kpdir))
    assert p.name == "bot.json"


def test_resolve_keypair_auto_select_multiple_raises(tmp_path, monkeypatch):
    kpdir = tmp_path / "keypairs"
    kpdir.mkdir()
    (kpdir / "a.json").write_text("[1]", encoding="utf-8")
    (kpdir / "b.json").write_text("[2]", encoding="utf-8")
    monkeypatch.setenv("AUTO_SELECT_KEYPAIR", "1")
    with pytest.raises(RuntimeError):
        resolve_keypair(dir_=str(kpdir))


def test_resolve_keypair_explicit_env(tmp_path, monkeypatch):
    kpdir = tmp_path / "keypairs"
    kpdir.mkdir()
    target = kpdir / "chosen.json"
    target.write_text("[9]", encoding="utf-8")
    monkeypatch.delenv("AUTO_SELECT_KEYPAIR", raising=False)
    monkeypatch.setenv("KEYPAIR_PATH", str(target))
    p = resolve_keypair(dir_=str(kpdir))
    assert p.name == "chosen.json"


# ---------- agent weights ----------
def test_validate_agent_weights_fills_and_normalizes():
    cfg = {"agents": ["a", "b"], "agent_weights": {"a": 3.0}}
    out = validate_agent_weights(cfg)
    assert set(out.keys()) == {"a", "b"}
    # 3 and 1 normalized -> 0.75 and 0.25
    assert abs(out["a"] - 0.75) < 1e-9
    assert abs(out["b"] - 0.25) < 1e-9


# ---------- flash-loan prereqs ----------
def test_flashloan_prereqs_requires_portfolio_and_protocol():
    cfg = {"use_flash_loans": True, "portfolio_value": 0.0}
    with pytest.raises(RuntimeError):
        verify_flashloan_prereqs(cfg)
    cfg = {"use_flash_loans": True, "portfolio_value": 10.0}
    with pytest.raises(RuntimeError):
        verify_flashloan_prereqs(cfg)
    cfg = {
        "use_flash_loans": True,
        "portfolio_value": 10.0,
        "flash_loan_protocol": {"program_id": "X", "pool": "Y", "fee_bps": 30},
    }
    # no exception
    verify_flashloan_prereqs(cfg)


# ---------- ui_selftest smoke (logic only, mock RPC) ----------
def _mock_rpc_ok(json_payload):
    class FakeResp:
        def __init__(self):
            self.status_code = 200

        def raise_for_status(self):
            ...

        def json(self):
            return {"result": {"value": {"blockhash": "abc"}}}

    return FakeResp()


def _mock_rpc_version(json_payload):
    class FakeResp:
        def __init__(self):
            self.status_code = 200

        def raise_for_status(self):
            ...

        def json(self):
            return {"result": {"solana-core": "X.Y"}}  # shape doesn't matter

    return FakeResp()


def _requests_post_mock(url, json, timeout):
    method = json.get("method")
    if method == "getVersion":
        return _mock_rpc_version(json)
    if method == "getLatestBlockhash":
        return _mock_rpc_ok(json)
    raise AssertionError("Unexpected RPC method in test")


def _write_minimal_config(tmp: Path):
    # use TOML to avoid requiring PyYAML
    (tmp / "config_selftest.toml").write_text(
        'solana_rpc_url = "https://mainnet.helius-rpc.com/?api-key=demo-helius-key"\n'
        'dex_base_url   = "https://quote-api.jup.ag"\n'
        'agents = ["simulation"]\n',
        encoding="utf-8",
    )


@patch("requests.post", side_effect=_requests_post_mock)
def test_ui_selftest_ok_post_mocked(_rp, tmp_path, monkeypatch):
    # prepare env and keypair
    _write_minimal_config(tmp_path)
    kpdir = tmp_path / "keypairs"
    kpdir.mkdir()
    (kpdir / "bot.json").write_text("[1,2,3,4]", encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("SELFTEST_SKIP_ARTIFACTS", "1")
    monkeypatch.setenv("CI", "true")
    monkeypatch.setenv("AUTO_SELECT_KEYPAIR", "1")
    monkeypatch.setenv("SOLHUNTER_CONFIG", "config_selftest.toml")

    # import here to ensure chdir/env is set
    from solhunter_zero.ui import ui_selftest

    rc = ui_selftest()
    assert rc == 0

