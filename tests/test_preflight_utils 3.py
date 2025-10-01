import time
import urllib.request
from urllib.error import URLError

from solhunter_zero import preflight_utils


class DummyResponse:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def read(self):
        return b"ok"


def test_check_disk_space_success(monkeypatch):
    def fake_disk_usage(path):
        return (100, 50, 20)

    monkeypatch.setattr(preflight_utils.shutil, "disk_usage", fake_disk_usage)
    ok, msg = preflight_utils.check_disk_space(10)
    assert ok is True
    assert "Sufficient disk space" in msg


def test_check_disk_space_failure(monkeypatch):
    def fake_disk_usage(path):
        return (100, 50, 5)

    monkeypatch.setattr(preflight_utils.shutil, "disk_usage", fake_disk_usage)
    ok, msg = preflight_utils.check_disk_space(10)
    assert ok is False
    assert "Insufficient disk space" in msg


def test_check_internet_success(monkeypatch):
    def fake_urlopen(url, timeout=5):
        return DummyResponse()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    ok, msg = preflight_utils.check_internet("https://api.github.com")
    assert ok is True
    assert "Reached" in msg


def test_check_internet_failure(monkeypatch):
    def fake_urlopen(url, timeout=5):
        raise URLError("boom")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(time, "sleep", lambda s: None)
    ok, msg = preflight_utils.check_internet("https://api.github.com")
    assert ok is False
    assert "Failed to reach https://api.github.com after 3 attempts" in msg


def test_check_wallet_balance_ok(monkeypatch, tmp_path):
    import types
    import solana.rpc.api as rpc_api
    from solhunter_zero.gas import LAMPORTS_PER_SOL

    kp_path = tmp_path / "dummy.json"
    kp_path.write_text("[]")

    monkeypatch.setattr(
        preflight_utils.wallet,
        "load_keypair",
        lambda _p: types.SimpleNamespace(pubkey=lambda: "pk"),
    )

    class FakeClient:
        def __init__(self, url):
            pass

        def get_balance(self, pubkey):
            return {"result": {"value": int(2 * LAMPORTS_PER_SOL)}}

    monkeypatch.setattr(rpc_api, "Client", FakeClient)

    ok, msg = preflight_utils.check_wallet_balance(1, kp_path)
    assert ok is True
    assert "Wallet balance" in msg


def test_check_wallet_balance_insufficient(monkeypatch, tmp_path):
    import types
    import solana.rpc.api as rpc_api

    kp_path = tmp_path / "dummy.json"
    kp_path.write_text("[]")

    monkeypatch.setattr(
        preflight_utils.wallet,
        "load_keypair",
        lambda _p: types.SimpleNamespace(pubkey=lambda: "pk"),
    )

    class FakeClient:
        def __init__(self, url):
            pass

        def get_balance(self, pubkey):
            return {"result": {"value": 0}}

    monkeypatch.setattr(rpc_api, "Client", FakeClient)

    ok, msg = preflight_utils.check_wallet_balance(1, kp_path)
    assert ok is False
    assert "below required" in msg


def test_check_keypair_custom_dir(monkeypatch, tmp_path):
    key_dir = tmp_path / "a"
    other_dir = tmp_path / "b"
    key_dir.mkdir()
    other_dir.mkdir()
    (key_dir / "active").write_text("my")
    (other_dir / "my.json").write_text("[]")
    monkeypatch.setattr(preflight_utils.wallet, "KEYPAIR_DIR", str(key_dir))
    monkeypatch.setattr(
        preflight_utils.wallet, "ACTIVE_KEYPAIR_FILE", str(key_dir / "active")
    )
    ok, msg = preflight_utils.check_keypair(other_dir)
    assert ok is True
    assert "Active keypair my present" in msg
