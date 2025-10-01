import asyncio
import pytest
import types
import sys
import importlib.util
trans = pytest.importorskip("transformers")
if not hasattr(trans, "pipeline"):
    trans.pipeline = lambda *a, **k: lambda x: []

if importlib.util.find_spec("solana.publickey") is None:
    class _PK:
        def __init__(self, *a, **k):
            pass
    sys.modules.setdefault("solana.publickey", types.SimpleNamespace(PublicKey=_PK))
if importlib.util.find_spec("solana.rpc.websocket_api") is None:
    sys.modules.setdefault("solana.rpc.websocket_api", types.SimpleNamespace(connect=lambda *a, **k: None, RpcTransactionLogsFilterMentions=object))

import solhunter_zero.mempool_listener as listener

class DummyAgentManager:
    def __init__(self):
        self.executed = []
    async def execute(self, token, portfolio):
        self.executed.append(token)
        return []

class DummyPortfolio:
    pass

class FakeClient:
    def __init__(self, url, sigs):
        self.url = url
        self._sigs = sigs
    def get_signatures_for_address(self, addr, limit=20):
        return {"result": self._sigs}


def test_fetch_token_age(monkeypatch):
    sigs = [{"blockTime": 100}, {"blockTime": 150}]
    def fake_client(url):
        return FakeClient(url, sigs)
    from solhunter_zero import onchain_metrics as om
    monkeypatch.setattr(om, "Client", fake_client)
    monkeypatch.setattr(om, "PublicKey", lambda x: x)
    import time as _time
    monkeypatch.setattr(_time, "time", lambda: 200)
    age = om.fetch_token_age("tok", "rpc")
    assert age == pytest.approx(100.0)


def test_listen_mempool_filters(monkeypatch):
    async def fake_stream(url):
        for t in ["tok1", "tok2"]:
            yield t
    monkeypatch.setattr(listener, "stream_mempool_tokens", fake_stream)
    monkeypatch.setattr(listener, "fetch_token_age", lambda t, u: 10 if t=="tok1" else 1000)
    monkeypatch.setattr(listener, "fetch_liquidity_onchain", lambda t, u: 20)
    mgr = DummyAgentManager()
    pf = DummyPortfolio()

    async def run():
        gen = listener.listen_mempool(
            "rpc", mgr, pf, blacklist=["tok2"], age_limit=100, min_liquidity=10
        )
        token = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return token

    token = asyncio.run(run())
    assert token == "tok1"
    assert mgr.executed == ["tok1"]


def test_listen_mempool_emits_risk(monkeypatch):
    async def fake_stream(url):
        yield "tok"

    monkeypatch.setattr(listener, "stream_mempool_tokens", fake_stream)
    monkeypatch.setattr(listener, "fetch_token_age", lambda t, u: 0)
    monkeypatch.setattr(listener, "fetch_liquidity_onchain", lambda t, u: 20)
    mgr = DummyAgentManager()
    pf = DummyPortfolio()

    events = []

    async def on_risk(payload):
        events.append(payload)

    from solhunter_zero.event_bus import subscribe

    unsub = subscribe("risk_updated", on_risk)

    async def run():
        gen = listener.listen_mempool(
            "rpc",
            mgr,
            pf,
            min_liquidity=10,
            risk_base=0.1,
        )
        token = await asyncio.wait_for(anext(gen), timeout=0.1)
        await asyncio.sleep(0)
        await gen.aclose()
        return token

    token = asyncio.run(run())
    unsub()

    assert token == "tok"
    assert events and events[0]["multiplier"] == pytest.approx(0.2)
