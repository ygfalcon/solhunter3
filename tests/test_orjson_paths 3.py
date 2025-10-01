import json
import sys
import types
import runpy
import asyncio
import pytest
from solders.keypair import Keypair

@pytest.mark.asyncio
async def test_wallet_load_keypair_async_uses_orjson(monkeypatch, tmp_path):
    orjson = pytest.importorskip("orjson")
    called = False
    orig = orjson.loads

    def fake(data):
        nonlocal called
        called = True
        return orig(data)
    monkeypatch.setattr(orjson, "loads", fake)
    from solhunter_zero import wallet
    path = tmp_path / "kp.json"
    kp = Keypair()
    path.write_text(json.dumps(list(kp.to_bytes())))
    loaded = await wallet.load_keypair_async(str(path))
    assert loaded.to_bytes() == kp.to_bytes()
    assert called


def test_arbitrage_parse_env_uses_orjson(monkeypatch):
    orjson = pytest.importorskip("orjson")
    called = False
    orig = orjson.loads

    def fake(data):
        nonlocal called
        called = True
        return orig(data)
    monkeypatch.setattr(orjson, "loads", fake)
    import os
    import solhunter_zero.arbitrage as arb
    monkeypatch.setenv("TEST_ENV_JSON", "{\"a\": 1}")
    assert arb._parse_mapping_env("TEST_ENV_JSON") == {"a": 1}
    assert called


def test_multi_rl_cli_uses_http_dumps(monkeypatch, tmp_path):
    pytest.importorskip("orjson")
    import solhunter_zero.http as http
    called = False
    def fake_dumps(obj):
        nonlocal called
        called = True
        return b"{}"
    monkeypatch.setattr(http, "dumps", fake_dumps)
    agent_mod = types.ModuleType("solhunter_zero.agents.memory")
    class DummyAgent:
        def __init__(self, mem):
            self.memory = types.SimpleNamespace(list_trades=lambda: [])
    agent_mod.MemoryAgent = DummyAgent
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.memory", agent_mod)
    mem_mod = types.ModuleType("solhunter_zero.memory")
    mem_mod.Memory = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.memory", mem_mod)
    monkeypatch.setattr(sys, "argv", [
        "solhunter_zero.multi_rl", "--memory", "sqlite:///:memory:",
        "--weights", str(tmp_path / "w.json")
    ])
    runpy.run_module("solhunter_zero.multi_rl", run_name="__main__")
    assert called

