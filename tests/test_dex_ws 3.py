import asyncio
import json
import aiohttp

from solhunter_zero import dex_ws
from solhunter_zero import token_scanner as async_scanner
from solhunter_zero import scanner_common
from solhunter_zero.event_bus import subscribe


class FakeMsg:
    def __init__(self, data):
        self.type = aiohttp.WSMsgType.TEXT
        self.data = json.dumps(data)

    def json(self):
        return json.loads(self.data)


class FakeWS:
    def __init__(self, messages):
        self.messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.messages:
            return FakeMsg(self.messages.pop(0))
        raise StopAsyncIteration


class FakeSession:
    def __init__(self, messages):
        self.messages = messages

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def ws_connect(self, url):
        self.url = url
        return FakeWS(self.messages)


def test_stream_listed_tokens(monkeypatch):
    msgs = [{"address": "tokbonk", "name": "xbonk"}]

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession(msgs))

    async def run():
        gen = dex_ws.stream_listed_tokens("ws://dex", suffix="bonk")
        token = await anext(gen)
        await gen.aclose()
        return token

    assert asyncio.run(run()) == "tokbonk"


def test_scan_tokens_async_includes_dex_ws(monkeypatch):
    data = {"data": [{"address": "abcbonk"}]}

    class FakeResp:
        status = 200

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return data

        def raise_for_status(self):
            pass

    class FakeHTTP:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def get(self, url, headers=None, timeout=10):
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeHTTP())

    async def fake_trend():
        return []

    monkeypatch.setattr(async_scanner, "fetch_trending_tokens_async", fake_trend)
    monkeypatch.setattr(async_scanner, "fetch_raydium_listings_async", fake_trend)
    monkeypatch.setattr(async_scanner, "fetch_orca_listings_async", fake_trend)

    async def fake_stream(url, *, suffix=None, keywords=None):
        yield "dexws"

    monkeypatch.setattr(dex_ws, "stream_listed_tokens", fake_stream)

    scanner_common.BIRDEYE_API_KEY = "k"
    scanner_common.HEADERS.clear()
    scanner_common.HEADERS["X-API-KEY"] = "k"
    monkeypatch.setattr(scanner_common, "DEX_LISTING_WS_URL", "ws://dex")
    import importlib
    import solhunter_zero.token_scanner as ts_mod
    importlib.reload(ts_mod)
    monkeypatch.setattr(ts_mod, "DEX_LISTING_WS_URL", "ws://dex")
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(async_scanner.scan_tokens_async())
    unsub()
    assert tokens == ["abcbonk", "dexws"]
    assert events == [tokens]
