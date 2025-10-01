import base64
import json
import os
from pathlib import Path
from types import SimpleNamespace
import urllib.request

import pytest

from solhunter_zero import jito_auth


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


def test_ensure_jito_auth_fetches_and_updates(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("")
    monkeypatch.delenv("JITO_AUTH", raising=False)
    monkeypatch.delenv("JITO_WS_AUTH", raising=False)

    class KP:
        def pubkey(self):
            return "PUB"

        def sign_message(self, msg):
            assert msg == b"challenge"
            return b"signed"

    def fake_select():
        return SimpleNamespace(name="test")

    fake_wallet = SimpleNamespace(
        KEYPAIR_DIR="keypairs",
        select_active_keypair=fake_select,
        load_keypair=lambda path: KP(),
    )
    monkeypatch.setattr(jito_auth, "wallet", fake_wallet)

    def fake_urlopen(req, timeout=5):
        payload = json.loads(req.data.decode())
        if "signature" in payload:
            assert payload["signature"] == base64.b64encode(b"signed").decode()
            return FakeResponse({"token": "TOKEN"})
        assert payload["pubkey"] == "PUB"
        return FakeResponse({"challenge": "challenge"})

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    jito_auth.ensure_jito_auth(env_file)

    assert os.environ["JITO_AUTH"] == "TOKEN"
    assert os.environ["JITO_WS_AUTH"] == "TOKEN"
    assert env_file.read_text().splitlines() == [
        "JITO_AUTH=TOKEN",
        "JITO_WS_AUTH=TOKEN",
    ]


def test_ensure_jito_auth_skips_when_present(tmp_path, monkeypatch):
    env_file = tmp_path / ".env"
    env_file.write_text("INITIAL=1\n")
    monkeypatch.setenv("JITO_AUTH", "EXIST")
    monkeypatch.setenv("JITO_WS_AUTH", "EXIST")

    called = False

    def fake_fetch():
        nonlocal called
        called = True
        return "NEW"

    monkeypatch.setattr(jito_auth, "fetch_jito_auth", fake_fetch)

    jito_auth.ensure_jito_auth(env_file)

    assert called is False
    assert env_file.read_text() == "INITIAL=1\n"
    assert os.environ["JITO_AUTH"] == "EXIST"
    assert os.environ["JITO_WS_AUTH"] == "EXIST"
