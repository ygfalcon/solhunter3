
import asyncio
import requests
import aiohttp
from solhunter_zero import prices
from solhunter_zero import http


# reset global state before each test
def setup_function(_):
    http._session = None
    prices.PRICE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)


class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

        self.text = "resp"

    def raise_for_status(self):
        if self.status_code != 200:
            raise requests.HTTPError("bad", response=self)

    async def json(self):
        return self._data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass


def test_fetch_token_prices(monkeypatch):
    data = {"data": {"tok": {"price": 2.0}, "bad": {"price": "x"}}}
    captured = {}

    class FakeSession:
        def __init__(self):
            captured["created"] = captured.get("created", 0) + 1
        def get(self, url, timeout=10):
            captured["url"] = url
            captured["gets"] = captured.get("gets", 0) + 1
            return FakeResponse(data)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    result = prices.fetch_token_prices(["tok", "bad"])
    assert result == {"tok": 2.0}
    assert set(captured["url"].split("?ids=")[1].split(",")) == {"tok", "bad"}


def test_fetch_token_prices_async(monkeypatch):
    data = {"data": {"tok": {"price": 1.5}}}
    captured = {}

    class FakeResp:
        def __init__(self, url):
            captured["url"] = url
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        async def json(self):
            return data
        def raise_for_status(self):
            pass

    class FakeSession:
        def __init__(self):
            captured["created"] = captured.get("created", 0) + 1
        def get(self, url, timeout=10):
            captured["gets"] = captured.get("gets", 0) + 1
            return FakeResp(url)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    result = asyncio.run(prices.fetch_token_prices_async(["tok"]))
    assert result == {"tok": 1.5}
    assert "tok" in captured["url"]


def test_fetch_token_prices_async_error(monkeypatch):
    """Return empty dict when aiohttp fails."""
    warnings = {}

    class FakeSession:
        def __init__(self):
            pass
        def get(self, url, timeout=10):
            raise aiohttp.ClientError("boom")

    def fake_warning(msg, exc):
        warnings['msg'] = msg

    monkeypatch.setattr(prices.logger, "warning", fake_warning)
    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    result = asyncio.run(prices.fetch_token_prices_async(["tok"]))
    assert result == {}
    assert 'msg' in warnings


def test_price_cache_and_session_reuse(monkeypatch):
    data = {"data": {"tok": {"price": 1.0}}}
    calls = {"sessions": 0, "gets": 0}

    class FakeResp:
        def __init__(self, url):
            calls["url"] = url
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        async def json(self):
            return data
        def raise_for_status(self):
            pass

    class FakeSession:
        def __init__(self):
            calls["sessions"] += 1
        def get(self, url, timeout=10):
            calls["gets"] += 1
            return FakeResp(url)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    prices.PRICE_CACHE.ttl = 60

    result1 = prices.fetch_token_prices(["tok"])
    result2 = asyncio.run(prices.fetch_token_prices_async(["tok"]))

    assert result1 == {"tok": 1.0}
    assert result2 == {"tok": 1.0}
    assert calls["sessions"] == 1
    assert calls["gets"] == 1


def test_warm_cache(monkeypatch):
    """warm_cache populates PRICE_CACHE using fetch_token_prices_async."""
    data = {"data": {"tok": {"price": 3.0}}}

    class FakeResp:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            return data

        def raise_for_status(self):
            pass

    class FakeSession:
        def get(self, url, timeout=10):
            return FakeResp()

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    prices.warm_cache(["tok"])
    assert prices.get_cached_price("tok") == 3.0

