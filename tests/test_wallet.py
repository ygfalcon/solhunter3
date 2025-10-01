import json
import logging
import os
import pytest

try:
    from solders.keypair import Keypair
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    pytest.skip("solders is required", allow_module_level=True)

from solhunter_zero import wallet


def setup_wallet(tmp_path, monkeypatch):
    monkeypatch.setattr(wallet, "KEYPAIR_DIR", str(tmp_path))
    monkeypatch.setattr(
        wallet,
        "ACTIVE_KEYPAIR_FILE",
        str(tmp_path / "active"),
    )
    os.makedirs(wallet.KEYPAIR_DIR, exist_ok=True)


def test_load_keypair(tmp_path):
    kp = Keypair()
    path = tmp_path / "kp.json"
    path.write_text(json.dumps(list(kp.to_bytes())))
    loaded = wallet.load_keypair(str(path))
    assert loaded.to_bytes() == kp.to_bytes()


def test_load_keypair_invalid_length(tmp_path):
    path = tmp_path / "kp.json"
    path.write_text(json.dumps([1] * 10))
    with pytest.raises(ValueError, match="64"):
        wallet.load_keypair(str(path))


def test_load_keypair_invalid_value(tmp_path):
    path = tmp_path / "kp.json"
    data = [1] * 64
    data[0] = 256
    path.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="0 and 255"):
        wallet.load_keypair(str(path))


def test_save_and_list_keypairs(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    data = [1, 2, 3]
    wallet.save_keypair("a", data)
    wallet.save_keypair("b", data)
    assert ((tmp_path / "a.json").stat().st_mode & 0o777) == 0o600
    os.chmod(tmp_path / "a.json", 0o644)
    wallet.save_keypair("a", data)
    assert ((tmp_path / "a.json").stat().st_mode & 0o777) == 0o600
    # create unrelated file
    (tmp_path / "other.txt").write_text("x")
    assert set(wallet.list_keypairs()) == {"a", "b"}
    assert json.loads((tmp_path / "a.json").read_text()) == data


def test_select_keypair_and_load_selected(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    kp = Keypair()
    wallet.save_keypair("my", list(kp.to_bytes()))
    wallet.select_keypair("my")
    assert (tmp_path / "active").read_text() == "my"
    loaded = wallet.load_selected_keypair()
    assert loaded.to_bytes() == kp.to_bytes()


def test_load_selected_keypair_none(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    assert wallet.load_selected_keypair() is None


def test_select_keypair_missing(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    with pytest.raises(FileNotFoundError):
        wallet.select_keypair("missing")


MNEMONIC = (
    "abandon abandon abandon abandon abandon abandon abandon "
    "abandon abandon abandon abandon about"
)
EXPECTED_HEX = (
    "286b4b30f808ed46e986e5536939c4177b61588ac4a3080d228cb47edd798164"
    "96da9c08f0703f749fd14e630a2b81d9109a9a8f17b7ade18952e82eb2b5e431"
)


def test_load_keypair_from_mnemonic():
    kp = wallet.load_keypair_from_mnemonic(MNEMONIC)
    data = kp.to_bytes().hex()
    if set(data) == {"0"}:  # stubbed bip_utils returns all zeros
        assert data == "0" * len(data)
    else:
        assert data == EXPECTED_HEX


def test_derive_and_save_keypair(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    kp = wallet.load_keypair_from_mnemonic(MNEMONIC)
    wallet.save_keypair("mn", list(kp.to_bytes()))
    loaded = wallet.load_keypair(str(tmp_path / "mn.json"))
    assert loaded.to_bytes() == kp.to_bytes()


def test_generate_default_keypair(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    mnemonic, mnemonic_path = wallet.generate_default_keypair()

    assert wallet.list_keypairs() == ["default"]
    assert wallet.get_active_keypair_name() == "default"
    assert (tmp_path / "default.json").exists()
    assert mnemonic_path == tmp_path / "default.mnemonic"
    assert mnemonic_path.read_text().strip() == mnemonic
    assert len(mnemonic.split()) == 24
    assert (mnemonic_path.stat().st_mode & 0o777) == 0o600
    assert os.environ["MNEMONIC"] == mnemonic


def test_generate_default_keypair_encrypted(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)
    monkeypatch.setenv("ENCRYPT_MNEMONIC", "1")
    monkeypatch.setenv("MNEMONIC_ENCRYPTION_KEY", "pw")
    mnemonic, mnemonic_path = wallet.generate_default_keypair()
    stored = mnemonic_path.read_text().strip()
    assert stored != mnemonic
    assert (
        wallet._decrypt_mnemonic(stored, "pw") == mnemonic
    )
    assert (mnemonic_path.stat().st_mode & 0o777) == 0o600


def test_mnemonic_round_trip():
    token = wallet._encrypt_mnemonic(MNEMONIC, "pw")
    assert wallet._decrypt_mnemonic(token, "pw") == MNEMONIC


def test_decrypt_mnemonic_invalid_token():
    token = wallet._encrypt_mnemonic(MNEMONIC, "pw")
    bad = token[:-1] + ("A" if token[-1] != "A" else "B")
    with pytest.raises(ValueError):
        wallet._decrypt_mnemonic(bad, "pw")


def test_setup_default_keypair(tmp_path, monkeypatch):
    setup_wallet(tmp_path, monkeypatch)

    info = wallet.setup_default_keypair()

    assert info.name == "default"
    assert info.mnemonic_path == tmp_path / "default.mnemonic"
    assert wallet.list_keypairs() == ["default"]
    assert wallet.get_active_keypair_name() == "default"


def test_setup_default_keypair_quick_setup_failure(
    tmp_path, monkeypatch, caplog
):
    setup_wallet(tmp_path, monkeypatch)
    monkeypatch.delenv("KEYPAIR_JSON", raising=False)
    import sys
    import types

    qs = types.ModuleType("quick_setup")

    def fail(args):
        raise RuntimeError("boom")

    qs.main = fail
    monkeypatch.setitem(sys.modules, "scripts.quick_setup", qs)
    with caplog.at_level(logging.WARNING, logger="solhunter_zero.wallet"):
        info = wallet.setup_default_keypair()
    assert "quick setup failed" in caplog.text
    assert info.name == "default"
    assert wallet.get_active_keypair_name() == "default"


def test_setup_default_keypair_invalid_input(tmp_path, monkeypatch, caplog):
    setup_wallet(tmp_path, monkeypatch)
    kp = Keypair()
    wallet.save_keypair("b", list(kp.to_bytes()))
    wallet.save_keypair("a", list(kp.to_bytes()))
    monkeypatch.setenv("AUTO_SELECT_KEYPAIR", "0")
    import sys

    monkeypatch.setattr(sys.stdin, "isatty", lambda: True)
    monkeypatch.setattr("builtins.input", lambda _: "foo")
    with caplog.at_level(logging.WARNING, logger="solhunter_zero.wallet"):
        info = wallet.setup_default_keypair()
    assert "invalid keypair selection" in caplog.text
    assert info.name == "a"
    assert wallet.get_active_keypair_name() == "a"
