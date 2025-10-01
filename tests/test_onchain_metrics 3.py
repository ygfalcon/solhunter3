import pytest
import aiohttp
import asyncio
import logging
from solhunter_zero import http

from solhunter_zero import onchain_metrics


# reset global state before each test
def setup_function(_):
    http._session = None
    onchain_metrics.DEX_METRICS_CACHE = onchain_metrics.TTLCache(
        maxsize=256, ttl=onchain_metrics.DEX_METRICS_CACHE_TTL
    )
    onchain_metrics.TOKEN_VOLUME_CACHE = onchain_metrics.TTLCache(
        maxsize=1024, ttl=onchain_metrics.TOKEN_VOLUME_CACHE_TTL
    )
    onchain_metrics.TOP_VOLUME_TOKENS_CACHE = onchain_metrics.TTLCache(
        maxsize=32, ttl=onchain_metrics.TOP_VOLUME_TOKENS_CACHE_TTL
    )


class FakeAsyncClient:
    def __init__(self, url, data):
        self.url = url
        self._data = data

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def get_signatures_for_address(self, addr):
        return {"result": self._data.get(str(addr), [])}


def test_top_volume_tokens(monkeypatch):
    tokens = ["t1", "t2", "t3"]
    tx_data = {
        "t1": [{"amount": 1.0}, {"amount": 3.0}],
        "t2": [{"amount": 5.0}],
        "t3": [],
    }

    captured = {}

    def fake_scan(url):
        captured["url"] = url
        return tokens

    def fake_client(url):
        return FakeAsyncClient(url, tx_data)

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain_sync", fake_scan)
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: fake_client(url))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    result = onchain_metrics.top_volume_tokens("http://node", limit=2)

    assert captured["url"] == "http://node"
    assert result == ["t2", "t1"]


class ErrorClient:
    def __init__(self, url):
        self.url = url

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    def get_signatures_for_address(self, addr):
        raise Exception("boom")


def test_top_volume_tokens_error(monkeypatch):
    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain_sync", lambda url: ["a"])
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: ErrorClient(url))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    result = onchain_metrics.top_volume_tokens("rpc", limit=1)
    assert result == ["a"]


def test_async_top_volume_tokens(monkeypatch):
    tokens = ["t1", "t2", "t3"]
    tx_data = {"t1": [{"amount": 1.0}, {"amount": 3.0}], "t2": [{"amount": 5.0}], "t3": []}

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain", lambda url: asyncio.sleep(0, tokens))
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: FakeAsyncClient(url, tx_data))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    result = asyncio.run(onchain_metrics.async_top_volume_tokens("http://node", limit=2))
    assert result == ["t2", "t1"]


def test_async_top_volume_tokens_cache(monkeypatch):
    tokens = ["a", "b"]
    tx_data = {"a": [{"amount": 1.0}], "b": [{"amount": 2.0}]}
    calls = {"gets": 0}

    class CountingClient(FakeAsyncClient):
        async def get_signatures_for_address(self, addr):
            calls["gets"] += 1
            return await super().get_signatures_for_address(addr)

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain", lambda url: asyncio.sleep(0, tokens))
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: CountingClient(url, tx_data))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    onchain_metrics.TOKEN_VOLUME_CACHE.ttl = 60
    first = asyncio.run(onchain_metrics.async_top_volume_tokens("node"))
    second = asyncio.run(onchain_metrics.async_top_volume_tokens("node"))

    assert first == ["b", "a"]
    assert second == first
    assert calls["gets"] == 2


def test_top_volume_tokens_cache(monkeypatch):
    tokens = ["a", "b"]
    tx_data = {"a": [{"amount": 1.0}], "b": [{"amount": 2.0}]}
    calls = {"scan": 0, "gets": 0}

    class CountingClient(FakeAsyncClient):
        async def get_signatures_for_address(self, addr):
            calls["gets"] += 1
            return await super().get_signatures_for_address(addr)

    def fake_scan(url):
        calls["scan"] += 1
        return tokens

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain_sync", fake_scan)
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: CountingClient(url, tx_data))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    onchain_metrics.TOP_VOLUME_TOKENS_CACHE.ttl = 60
    onchain_metrics.TOP_VOLUME_TOKENS_CACHE.clear()

    first = onchain_metrics.top_volume_tokens("node")
    second = onchain_metrics.top_volume_tokens("node")

    assert first == ["b", "a"]
    assert second == first
    assert calls["scan"] == 1
    assert calls["gets"] == 2


def test_async_top_volume_tokens_list_cache(monkeypatch):
    tokens = ["a", "b"]
    tx_data = {"a": [{"amount": 1.0}], "b": [{"amount": 2.0}]}
    calls = {"scan": 0, "gets": 0}

    class CountingClient(FakeAsyncClient):
        async def get_signatures_for_address(self, addr):
            calls["gets"] += 1
            return await super().get_signatures_for_address(addr)

    async def fake_scan(url):
        calls["scan"] += 1
        return tokens

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain", fake_scan)
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: CountingClient(url, tx_data))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    onchain_metrics.TOP_VOLUME_TOKENS_CACHE.ttl = 60
    onchain_metrics.TOP_VOLUME_TOKENS_CACHE.clear()
    onchain_metrics.TOKEN_VOLUME_CACHE.ttl = 0
    onchain_metrics.TOKEN_VOLUME_CACHE.clear()

    first = asyncio.run(onchain_metrics.async_top_volume_tokens("node"))
    second = asyncio.run(onchain_metrics.async_top_volume_tokens("node"))

    assert first == ["b", "a"]
    assert second == first
    assert calls["scan"] == 1
    assert calls["gets"] == 2


def test_fetch_dex_metrics(monkeypatch):
    urls = []

    class FakeResp:
        def __init__(self, url):
            urls.append(url)

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            if "liquidity" in urls[-1]:
                return {"liquidity": 10.0}
            if "depth" in urls[-1]:
                return {"depth": 0.5}
            return {"volume": 20.0}

    class FakeSession:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def get(self, url, timeout=5):
            return FakeResp(url)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())

    metrics = asyncio.run(
        onchain_metrics.fetch_dex_metrics_async("tok", base_url="https://quote-api.jup.ag")
    )

    assert metrics == {"liquidity": 10.0, "depth": 0.5, "volume": 20.0}
    assert urls[0] == "https://quote-api.jup.ag/v1/liquidity?token=tok"


def test_dex_metrics_cache(monkeypatch):
    calls = {"sessions": 0, "gets": 0}

    class FakeResp:
        def __init__(self, url):
            calls["url"] = url

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            if "liquidity" in calls["url"]:
                return {"liquidity": 1.0}
            if "depth" in calls["url"]:
                return {"depth": 0.1}
            return {"volume": 2.0}

    class FakeSession:
        def __init__(self):
            calls["sessions"] += 1
            self.closed = False

        def get(self, url, timeout=5):
            calls["gets"] += 1
            return FakeResp(url)

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())
    onchain_metrics.DEX_METRICS_CACHE.ttl = 60

    metrics1 = asyncio.run(
        onchain_metrics.fetch_dex_metrics_async("tok", base_url="https://quote-api.jup.ag")
    )
    metrics2 = asyncio.run(
        onchain_metrics.fetch_dex_metrics_async("tok", base_url="https://quote-api.jup.ag")
    )

    assert metrics1 == {"liquidity": 1.0, "depth": 0.1, "volume": 2.0}
    assert metrics2 == metrics1
    assert calls["sessions"] == 1
    assert calls["gets"] == 3


def test_fetch_dex_metrics_error(monkeypatch):
    def fake_session(*args, **kwargs):
        class S:
            def get(self, url, timeout=5):
                raise aiohttp.ClientError("boom")

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                pass

        return S()

    monkeypatch.setattr("aiohttp.ClientSession", fake_session)

    metrics = asyncio.run(
        onchain_metrics.fetch_dex_metrics_async("tok", base_url="https://quote-api.jup.ag")
    )

    assert metrics == {"liquidity": 0.0, "depth": 0.0, "volume": 0.0}


class RPCClient:
    def __init__(self, url, accounts=None, sigs=None):
        self.url = url
        self._accounts = accounts or []
        self._sigs = sigs or []

    def get_token_largest_accounts(self, addr):
        return {"result": {"value": self._accounts}}

    def get_signatures_for_address(self, addr):
        return {"result": self._sigs}


def test_onchain_metric_functions(monkeypatch):
    accounts = [{"uiAmount": 5.0}, {"uiAmount": 3.0}]
    sigs = [{"amount": 2.0}, {"amount": 1.0}]

    def fake_client(url):
        return RPCClient(url, accounts, sigs)

    monkeypatch.setattr(onchain_metrics, "Client", fake_client)
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    liq = onchain_metrics.fetch_liquidity_onchain("tok", "http://node")
    vol = onchain_metrics.fetch_volume_onchain("tok", "http://node")
    slip = onchain_metrics.fetch_slippage_onchain("tok", "http://node")

    assert liq == pytest.approx(8.0)
    assert vol == pytest.approx(3.0)
    assert slip == pytest.approx((5.0 - 3.0) / 5.0)


class ErrorRPC:
    def __init__(self, url):
        self.url = url

    def get_token_largest_accounts(self, addr):
        raise Exception("boom")


def test_onchain_metric_functions_error(monkeypatch):
    monkeypatch.setattr(onchain_metrics, "Client", lambda url: ErrorRPC(url))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    liq = onchain_metrics.fetch_liquidity_onchain("tok", "http://node")
    slip = onchain_metrics.fetch_slippage_onchain("tok", "http://node")
    vol = onchain_metrics.fetch_volume_onchain("tok", "http://node")

    assert liq == 0.0
    assert slip == 0.0
    assert vol == 0.0


def test_fetch_liquidity_onchain_logs_invalid_amount(monkeypatch, caplog):
    accounts = [{"uiAmount": "bad_ui", "amount": "bad_amount"}]

    def fake_client(url):
        return RPCClient(url, accounts, [])

    monkeypatch.setattr(onchain_metrics, "Client", fake_client)
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    with caplog.at_level(logging.DEBUG):
        total = onchain_metrics.fetch_liquidity_onchain("tok", "http://node")

    assert total == 0.0
    assert "bad_amount" in caplog.text


def test_fetch_liquidity_onchain_async_logs_invalid_amount(monkeypatch, caplog):
    accounts = [
        {"uiAmount": "bad_ui", "amount": "bad_amount"},
        {"uiAmount": 1.0},
    ]

    class FakeAsyncClient:
        def __init__(self, url):
            self.url = url

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def get_token_largest_accounts(self, addr):
            return {"result": {"value": accounts}}

    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: FakeAsyncClient(url))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    with caplog.at_level(logging.DEBUG):
        total = asyncio.run(
            onchain_metrics.fetch_liquidity_onchain_async("tok", "http://node")
        )

    assert total == pytest.approx(1.0)
    assert "bad_amount" in caplog.text


def test_order_book_depth_change(monkeypatch):
    vals = [1.0, 2.0]

    async def fake_fetch(token, base_url=None):
        return {"depth": vals.pop(0)}

    monkeypatch.setattr(onchain_metrics, "fetch_dex_metrics_async", fake_fetch)

    first = onchain_metrics.order_book_depth_change("tok", base_url="https://quote-api.jup.ag")
    second = onchain_metrics.order_book_depth_change("tok", base_url="https://quote-api.jup.ag")
    assert first == 0.0
    assert second == pytest.approx(1.0)


def test_collect_onchain_insights(monkeypatch):
    monkeypatch.setattr(onchain_metrics, "order_book_depth_change", lambda t, base_url=None: 0.5)
    monkeypatch.setattr(onchain_metrics, "fetch_mempool_tx_rate", lambda t, u: 2.0)
    monkeypatch.setattr(onchain_metrics, "fetch_whale_wallet_activity", lambda t, u: 0.1)
    monkeypatch.setattr(onchain_metrics, "fetch_average_swap_size", lambda t, u: 1.5)

    data = onchain_metrics.collect_onchain_insights("tok", "http://node")
    assert data == {
        "depth_change": 0.5,
        "tx_rate": 2.0,
        "whale_activity": 0.1,
        "avg_swap_size": 1.5,
    }


def test_fetch_dex_metrics_concurrent(monkeypatch):
    calls = []

    class FakeResp:
        def __init__(self, url):
            self.url = url

        def raise_for_status(self):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def json(self):
            await asyncio.sleep(0.05)
            if "liquidity" in self.url:
                return {"liquidity": 1.0}
            if "depth" in self.url:
                return {"depth": 0.1}
            return {"volume": 2.0}

    class FakeSession:
        def __init__(self):
            self.closed = False

        def get(self, url, timeout=5):
            calls.append(url)
            return FakeResp(url)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr("aiohttp.ClientSession", lambda *a, **k: FakeSession())

    import time

    start = time.perf_counter()
    metrics = asyncio.run(
        onchain_metrics.fetch_dex_metrics_async("tok", base_url="https://quote-api.jup.ag")
    )
    elapsed = time.perf_counter() - start

    assert metrics == {"liquidity": 1.0, "depth": 0.1, "volume": 2.0}
    assert set(calls) == {
        "https://quote-api.jup.ag/v1/liquidity?token=tok",
        "https://quote-api.jup.ag/v1/depth?token=tok",
        "https://quote-api.jup.ag/v1/volume?token=tok",
    }
    assert elapsed < 0.12


def test_top_volume_tokens_concurrent(monkeypatch):
    tokens = ["a", "b", "c"]
    calls = []

    class SlowClient(FakeAsyncClient):
        async def get_signatures_for_address(self, addr):
            calls.append(str(addr))
            await asyncio.sleep(0.05)
            return {"result": [{"amount": 1.0}]}

    monkeypatch.setattr(onchain_metrics, "scan_tokens_onchain_sync", lambda url: tokens)
    monkeypatch.setattr(onchain_metrics, "AsyncClient", lambda url: SlowClient(url, {}))
    monkeypatch.setattr(onchain_metrics, "PublicKey", lambda x: x)

    import time

    start = time.perf_counter()
    result = onchain_metrics.top_volume_tokens("node")
    elapsed = time.perf_counter() - start

    assert set(calls) == set(tokens)
    assert result == tokens  # volumes equal so order preserved
    assert elapsed < 0.12


def test_tx_volume_str_values():
    entries = [
        {"amount": "1.5"},
        {"amount": "2"},
        {"amount": "bad"},
        {},
    ]

    assert onchain_metrics._tx_volume(entries) == pytest.approx(3.5)
