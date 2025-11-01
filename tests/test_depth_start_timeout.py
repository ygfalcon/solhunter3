import pytest
import types
import sys

fake_ws_api = types.ModuleType("solana.rpc.websocket_api")
fake_ws_api.RpcTransactionLogsFilterMentions = object
fake_ws_api.connect = object

fake_rpc = types.ModuleType("solana.rpc")
fake_rpc.websocket_api = fake_ws_api
fake_rpc.api = types.ModuleType("solana.rpc.api")
fake_rpc.api.Client = object
fake_rpc.async_api = types.ModuleType("solana.rpc.async_api")
fake_rpc.async_api.AsyncClient = object

fake_solana = types.ModuleType("solana")
fake_solana.rpc = fake_rpc

sys.modules.setdefault("solana", fake_solana)
sys.modules.setdefault("solana.rpc", fake_rpc)
sys.modules.setdefault("solana.rpc.websocket_api", fake_ws_api)
sys.modules.setdefault("solana.rpc.api", fake_rpc.api)
sys.modules.setdefault("solana.rpc.async_api", fake_rpc.async_api)

import solhunter_zero.main as main
from solhunter_zero.services import DepthServiceStartupError


class DummyProc:
    def __init__(self):
        self.terminated = False

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        pass

    def kill(self):
        self.killed = True


def test_depth_service_timeout(monkeypatch):
    dummy = DummyProc()
    monkeypatch.setattr(main.subprocess, "Popen", lambda *a, **k: dummy)

    async def fake_open_unix_connection(_path):
        raise FileNotFoundError

    monkeypatch.setattr(main.asyncio, "open_unix_connection", fake_open_unix_connection)

    monkeypatch.setenv("DEPTH_START_TIMEOUT", "0.01")

    with pytest.raises(DepthServiceStartupError):
        main._start_depth_service({"depth_service": True})

    assert dummy.terminated
