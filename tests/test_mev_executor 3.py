import asyncio
import aiohttp

from solhunter_zero.mev_executor import MEVExecutor


async def _run_exec(mev: MEVExecutor, txs):
    return await mev.submit_bundle(txs)


def test_mev_executor_submit(monkeypatch):
    calls = []

    async def fake_submit(tx, *, priority_rpc=None, priority_fee=None):
        calls.append((tx, priority_fee, priority_rpc))

    monkeypatch.setattr(
        "solhunter_zero.mev_executor.submit_raw_tx", fake_submit
    )
    monkeypatch.setattr(
        "solhunter_zero.mev_executor.snapshot", lambda tok: ({}, 5.0)
    )
    monkeypatch.setattr(
        "solhunter_zero.mev_executor.adjust_priority_fee", lambda rate: 7
    )

    mev = MEVExecutor("TOK", priority_rpc=["u"])
    asyncio.run(_run_exec(mev, ["A", "B"]))

    assert calls == [
        ("A", 7, ["u"]),
        ("B", 7, ["u"]),
    ]


def test_mev_executor_jito(monkeypatch):
    captured = {}

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return {"result": ["s1", "s2"]}

        def raise_for_status(self):
            pass

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def post(self, url, json=None, headers=None, timeout=10):
            captured["url"] = url
            captured["json"] = json
            captured["headers"] = headers
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    monkeypatch.setattr(
        "solhunter_zero.mev_executor.snapshot", lambda tok: ({}, 5.0)
    )
    monkeypatch.setattr(
        "solhunter_zero.mev_executor.adjust_priority_fee", lambda rate: 7
    )

    mev = MEVExecutor(
        "TOK",
        jito_rpc_url="http://jito",
        jito_auth="T",
    )
    sigs = asyncio.run(_run_exec(mev, ["A", "B"]))

    assert sigs == ["s1", "s2"]
    assert captured["url"] == "http://jito"
    assert captured["json"]["params"]["transactions"] == ["A", "B"]
    assert captured["headers"]["Authorization"] == "T"


def test_jito_auth_failure(monkeypatch):
    """Missing credentials should result in None signatures."""

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return {}

        def raise_for_status(self):
            raise aiohttp.ClientError()

    class FakeSession:
        def post(self, url, json=None, headers=None, timeout=10):
            return FakeResp()

    async def fake_get_session():
        return FakeSession()

    monkeypatch.setattr(
        "solhunter_zero.mev_executor.get_session", fake_get_session
    )

    mev = MEVExecutor("TOK", jito_rpc_url="http://jito")
    sigs = asyncio.run(_run_exec(mev, ["A", "B"]))

    assert sigs == [None, None]


def test_jito_connection_failure(monkeypatch):
    """Connection errors should also return None signatures."""

    class FakeSession:
        def post(self, url, json=None, headers=None, timeout=10):
            raise aiohttp.ClientConnectionError()

    async def fake_get_session():
        return FakeSession()

    monkeypatch.setattr(
        "solhunter_zero.mev_executor.get_session", fake_get_session
    )

    mev = MEVExecutor("TOK", jito_rpc_url="http://jito", jito_auth="T")
    sigs = asyncio.run(_run_exec(mev, ["A"]))

    assert sigs == [None]
