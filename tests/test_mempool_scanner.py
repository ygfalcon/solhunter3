import asyncio
import logging
import pytest

import types
import sys

def _decorator(*args, **kwargs):
    def wrap(func):
        return func
    return wrap

class _BaseModel:
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

    def dict(self):
        return dict(self.__dict__)

    def model_dump(self, *args, **kwargs):  # pragma: no cover - pydantic v2 compat
        return dict(self.__dict__)

pydantic_stub = types.SimpleNamespace(
    BaseModel=_BaseModel,
    AnyUrl=str,
    ValidationError=Exception,
    field_validator=_decorator,
    model_validator=_decorator,
    validator=_decorator,
    root_validator=_decorator,
)

sys.modules.setdefault("pydantic", pydantic_stub)

import solhunter_zero.mempool_scanner as mp_scanner
from solhunter_zero import dynamic_limit
from solhunter_zero import scanner_common, event_bus, resource_monitor as rm

scanner_common.TOKEN_SUFFIX = ""
mp_scanner.RpcTransactionLogsFilterMentions = lambda *a, **k: None


class FakeWS:
    def __init__(self, messages):
        self.messages = list(messages)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def logs_subscribe(self, *args, **kwargs):
        pass

    async def recv(self):
        if self.messages:
            return [self.messages.pop(0)]
        raise asyncio.CancelledError


class FakeConnect:
    def __init__(self, url, messages):
        self.url = url
        self.ws = FakeWS(messages)

    async def __aenter__(self):
        return await self.ws.__aenter__()

    async def __aexit__(self, exc_type, exc, tb):
        await self.ws.__aexit__(exc_type, exc, tb)


def test_connect_with_https_url(monkeypatch):
    urls = []

    def fake_connect(url):
        urls.append(url)
        return FakeConnect(url, [])

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    class _PK(str):
        @property
        def _key(self):
            return self

    monkeypatch.setattr(mp_scanner, "PublicKey", _PK)
    monkeypatch.setattr(mp_scanner, "RpcTransactionLogsFilterMentions", lambda *a, **k: None)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("https://node")
        try:
            await anext(gen)
        except StopAsyncIteration:
            pass
        await gen.aclose()

    asyncio.run(run())
    assert urls == ["wss://node"]


def test_connect_with_wss_url(monkeypatch):
    urls = []

    def fake_connect(url):
        urls.append(url)
        return FakeConnect(url, [])

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    class _PK(str):
        @property
        def _key(self):
            return self

    monkeypatch.setattr(mp_scanner, "PublicKey", _PK)
    monkeypatch.setattr(mp_scanner, "RpcTransactionLogsFilterMentions", lambda *a, **k: None)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("wss://node")
        try:
            await anext(gen)
        except StopAsyncIteration:
            pass
        await gen.aclose()

    asyncio.run(run())
    assert urls == ["wss://node"]


def test_connect_with_host_only_url(monkeypatch):
    urls = []

    def fake_connect(url):
        urls.append(url)
        return FakeConnect(url, [])

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    class _PK(str):
        @property
        def _key(self):
            return self

    monkeypatch.setattr(mp_scanner, "PublicKey", _PK)
    monkeypatch.setattr(mp_scanner, "RpcTransactionLogsFilterMentions", lambda *a, **k: None)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("rpc.solana.com")
        try:
            await anext(gen)
        except StopAsyncIteration:
            pass
        await gen.aclose()

    asyncio.run(run())
    assert urls == ["wss://rpc.solana.com"]


def test_filter_creation_uses_public_api(monkeypatch):
    calls = []

    class DummyPK:
        def __str__(self):
            return "dummy"

        def to_bytes(self):  # pragma: no cover - optional
            return b"dummy"

    monkeypatch.setattr(mp_scanner, "TOKEN_PROGRAM_ID", DummyPK())

    def fake_filter(arg):
        calls.append(arg)
        return None

    monkeypatch.setattr(mp_scanner, "RpcTransactionLogsFilterMentions", fake_filter)

    def fake_connect(url):
        return FakeConnect(url, [])

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("ws://node", include_pools=False)
        try:
            await anext(gen)
        except StopAsyncIteration:
            pass
        await gen.aclose()

    asyncio.run(run())
    assert calls[0] in {"dummy", b"dummy"}


def test_stream_mempool_tokens_subscribes_pubkey(monkeypatch):
    def fake_connect(url):
        return FakeConnect(url, [])

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    def fake_filter(arg, *args, **kwargs):
        if isinstance(arg, str):
            raise TypeError("expected Pubkey")
        return None

    monkeypatch.setattr(
        mp_scanner, "RpcTransactionLogsFilterMentions", fake_filter
    )

    async def run():
        gen = mp_scanner.stream_mempool_tokens("ws://node", include_pools=False)
        try:
            await anext(gen)
        except StopAsyncIteration:
            pass
        await gen.aclose()

    asyncio.run(run())


def test_stream_mempool_tokens(monkeypatch):
    msgs = [
        {
            "result": {
                "value": {"logs": ["InitializeMint", "name: coolbonk", "mint: tok1"]}
            }
        },
        {"result": {"value": {"logs": ["something else"]}}},
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("ws://node", suffix="bonk")
        token = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return token

    token = asyncio.run(run())
    assert token == "tok1"


def test_stream_mempool_tokens_pool(monkeypatch):
    token = "A" * 36 + "BONK"
    msgs = [
        {"result": {"value": {"logs": [f"tokenA: {token}", "tokenB: x"]}}},
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("ws://node", include_pools=True)
        token_out = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return token_out

    token_out = asyncio.run(run())
    assert token_out == token


def test_offline_or_onchain_async_mempool(monkeypatch):
    async def fake_stream(url, *, suffix=None, keywords=None, include_pools=True):
        yield "tokmp"

    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)

    scanner_common.BIRDEYE_API_KEY = None
    scanner_common.SOLANA_RPC_URL = "ws://node"

    tokens = asyncio.run(
        scanner_common.offline_or_onchain_async(False, method="mempool")
    )
    assert tokens == ["tokmp"]


def test_stream_mempool_tokens_with_metrics(monkeypatch):
    msgs = [
        {
            "result": {
                "value": {"logs": ["InitializeMint", "name: coolbonk", "mint: tok1"]}
            }
        },
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    import solhunter_zero.onchain_metrics as om

    monkeypatch.setattr(om, "fetch_volume_onchain", lambda t, u: 1.0)
    monkeypatch.setattr(om, "fetch_liquidity_onchain", lambda t, u: 2.0)

    async def run():
        gen = mp_scanner.stream_mempool_tokens("ws://node", return_metrics=True)
        data = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return data

    data = asyncio.run(run())
    assert data == {"address": "tok1", "volume": 1.0, "liquidity": 2.0}


def test_stream_ranked_mempool_tokens(monkeypatch):
    msgs = [
        {"result": {"value": {"logs": ["InitializeMint", "name: tok1", "mint: tok1"]}}},
        {"result": {"value": {"logs": ["InitializeMint", "name: tok2", "mint: tok2"]}}},
    ]

    def fake_connect(url):
        return FakeConnect(url, msgs)

    monkeypatch.setattr(mp_scanner, "connect", fake_connect)

    import solhunter_zero.onchain_metrics as om

    monkeypatch.setattr(
        om,
        "fetch_volume_onchain_async",
        lambda t, u: asyncio.sleep(0, 10.0 if t == "tok1" else 1.0),
    )
    monkeypatch.setattr(
        om,
        "fetch_liquidity_onchain_async",
        lambda t, u: asyncio.sleep(0, 5.0 if t == "tok1" else 0.5),
    )
    monkeypatch.setattr(
        om,
        "collect_onchain_insights_async",
        lambda t, u: asyncio.sleep(
            0,
            {
                "tx_rate": 2.0 if t == "tok1" else 0.1,
                "whale_activity": 0.0,
                "avg_swap_size": 1.0,
            },
        ),
    )

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens(
            "ws://node", suffix="", threshold=10.0
        )
        data = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return data

    data = asyncio.run(run())
    assert data["address"] == "tok1"
    assert data["score"] >= 10.0
    expected = data["momentum"] * (1.0 - data["whale_activity"])
    assert data["combined_score"] == expected


def test_stream_ranked_returns_sorted_limit(monkeypatch):
    scores = {
        "tok1": 1.5,
        "tok2": 9.0,
        "tok3": 4.2,
        "tok4": 7.1,
        "tok5": 0.5,
    }

    async def fake_stream(_url, **__):
        for tok in ["tok3", "tok1", "tok5", "tok4", "tok2"]:
            yield tok

    async def fake_rank(tok, _rpc):
        await asyncio.sleep(0)
        score = scores[tok]
        return score, {
            "volume": 0.0,
            "liquidity": 0.0,
            "tx_rate": 0.0,
            "whale_activity": 0.0,
            "wallet_concentration": 1.0,
            "avg_swap_size": 0.0,
            "momentum": score,
            "anomaly": 0.0,
            "score": score,
        }

    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)
    monkeypatch.setattr(mp_scanner, "rank_token", fake_rank)

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens("rpc", limit=3)
        results = []
        async for evt in gen:
            results.append(evt)
        return results

    events = asyncio.run(run())
    assert [evt["address"] for evt in events] == ["tok2", "tok4", "tok3"]
    assert [evt["combined_score"] for evt in events] == [9.0, 7.1, 4.2]


def test_rank_token_momentum(monkeypatch):
    mp_scanner._ROLLING_STATS.clear()
    import solhunter_zero.onchain_metrics as om

    monkeypatch.setattr(
        om, "fetch_volume_onchain_async", lambda t, u: asyncio.sleep(0, 1.0)
    )
    monkeypatch.setattr(
        om, "fetch_liquidity_onchain_async", lambda t, u: asyncio.sleep(0, 1.0)
    )
    rates = [1.0, 3.0]

    def fake_insights(t, u):
        return {"tx_rate": rates.pop(0), "whale_activity": 0.0, "avg_swap_size": 1.0}

    async def fake_insights_async(t, u):
        return fake_insights(t, u)

    monkeypatch.setattr(om, "collect_onchain_insights_async", fake_insights_async)

    async def run():
        first = await mp_scanner.rank_token("tok", "rpc")
        second = await mp_scanner.rank_token("tok", "rpc")
        return first, second

    first, second = asyncio.run(run())
    assert second[1]["momentum"] != 0.0


def test_stream_ranked_with_depth(monkeypatch):
    async def fake_gen(url, **_):
        yield {
            "address": "tok1",
            "combined_score": 1.0,
            "momentum": 1.0,
            "whale_activity": 0.0,
        }
        yield {
            "address": "tok2",
            "combined_score": 1.0,
            "momentum": 1.0,
            "whale_activity": 0.0,
        }

    monkeypatch.setattr(mp_scanner, "stream_ranked_mempool_tokens", fake_gen)

    import solhunter_zero.order_book_ws as obws

    monkeypatch.setattr(
        obws, "snapshot", lambda t: (5.0 if t == "tok1" else 10.0, 0.0, 0.0)
    )

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens_with_depth("rpc")
        r1 = await asyncio.wait_for(anext(gen), timeout=0.1)
        r2 = await asyncio.wait_for(anext(gen), timeout=0.1)
        await gen.aclose()
        return r1, r2

    first, second = asyncio.run(run())
    assert second["combined_score"] > first["combined_score"]


def test_stream_ranked_with_depth_timeout(monkeypatch):
    closed = False

    async def fake_gen(url, **_):
        nonlocal closed
        try:
            yield {
                "address": "tok1",
                "combined_score": 1.0,
                "momentum": 1.0,
                "whale_activity": 0.0,
            }
            await asyncio.Event().wait()
        finally:
            closed = True

    monkeypatch.setattr(mp_scanner, "stream_ranked_mempool_tokens", fake_gen)
    monkeypatch.setattr(mp_scanner, "MEMPOOL_DEPTH_STREAM_TIMEOUT", 0.05)

    import solhunter_zero.order_book_ws as obws

    monkeypatch.setattr(obws, "snapshot", lambda _t: (5.0, 0.0, 0.0))

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens_with_depth("rpc")
        first = await asyncio.wait_for(anext(gen), timeout=0.1)
        with pytest.raises(StopAsyncIteration):
            await asyncio.wait_for(anext(gen), timeout=0.2)
        return first

    first = asyncio.run(run())
    assert first["depth"] == 5.0
    assert closed is True


def test_default_concurrency(monkeypatch):
    monkeypatch.setattr(mp_scanner.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: 0.0)

    async def fake_stream(_url, **__):
        for t in ("a", "b", "c", "d"):
            yield t

    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)

    running = 0
    max_running = 0

    async def fake_rank(_t, _u):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1
        return 0.0, {"whale_activity": 0.0, "momentum": 0.0}

    monkeypatch.setattr(mp_scanner, "rank_token", fake_rank)

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens("rpc")
        async for _ in gen:
            pass

    asyncio.run(run())
    assert max_running <= 4


def test_cpu_threshold_reduces_concurrency(monkeypatch):
    monkeypatch.setattr(mp_scanner.os, "cpu_count", lambda: 4)
    cpu_val = 90.0
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: cpu_val)

    orig_sleep = asyncio.sleep

    async def fake_sleep(delay):
        nonlocal cpu_val
        if delay == 0.05:
            cpu_val = 10.0
        await orig_sleep(0)

    monkeypatch.setattr(mp_scanner.asyncio, "sleep", fake_sleep)

    async def fake_stream(_url, **__):
        for t in ("a", "b"):
            yield t

    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)

    running = 0
    max_running = 0

    async def fake_rank(_t, _u):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1
        return 0.0, {"whale_activity": 0.0, "momentum": 0.0}

    monkeypatch.setattr(mp_scanner, "rank_token", fake_rank)

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens("rpc", cpu_usage_threshold=80.0)
        async for _ in gen:
            pass

    asyncio.run(run())
    assert max_running <= 2


def test_cpu_fallback(monkeypatch):
    called = False

    def fake_cpu(*_a, **_k):
        nonlocal called
        called = True
        return 33.0

    monkeypatch.setattr(rm.psutil, "cpu_percent", fake_cpu)
    rm._CPU_PERCENT = 0.0
    rm._CPU_LAST = 0.0
    cpu = rm.get_cpu_usage()
    assert cpu == 33.0
    assert called


def test_dynamic_concurrency(monkeypatch):
    monkeypatch.setattr(mp_scanner.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: 90.0)
    mp_scanner._DYN_INTERVAL = 0.0

    async def fake_stream(_url, **__):
        await asyncio.sleep(0)
        for t in ("a", "b", "c", "d"):
            yield t

    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)

    running = 0
    max_running = 0

    async def fake_rank(_t, _u):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1
        return 0.0, {"whale_activity": 0.0, "momentum": 0.0}

    monkeypatch.setattr(mp_scanner, "rank_token", fake_rank)

    async def run():
        gen = mp_scanner.stream_ranked_mempool_tokens("rpc", dynamic_concurrency=True)
        async for _ in gen:
            pass

    asyncio.run(run())
    assert max_running <= 2


def test_mempool_concurrency_functions(monkeypatch):
    import types

    monkeypatch.setattr(
        dynamic_limit.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=0.0),
    )
    monkeypatch.setenv("CONCURRENCY_SMOOTHING", "0.5")
    monkeypatch.setenv("CONCURRENCY_KI", "0")
    dynamic_limit.refresh_params()
    dynamic_limit._CPU_EMA = 0.0
    dynamic_limit._ERR_INT = 0.0

    tgt = dynamic_limit._target_concurrency(90.0, 4, 40.0, 80.0)
    cur = dynamic_limit._step_limit(4, tgt, 4)
    assert tgt == 1
    assert cur == 2

    for _ in range(5):
        tgt = dynamic_limit._target_concurrency(10.0, 4, 40.0, 80.0)
        cur = dynamic_limit._step_limit(cur, tgt, 4)

    assert cur >= 3

    monkeypatch.setattr(
        dynamic_limit.psutil,
        "virtual_memory",
        lambda: types.SimpleNamespace(percent=95.0),
    )
    tgt = dynamic_limit._target_concurrency(10.0, 4, 40.0, 80.0)
    assert tgt == 1


def test_metrics_subscription_exit_logs_warning(monkeypatch, caplog):
    """Ensure a warning is logged when metrics subscription cleanup fails."""

    class BadSub:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            raise RuntimeError("boom")

    async def fake_stream(*args, **kwargs):
        if False:  # pragma: no cover - keeps async generator type
            yield None

    monkeypatch.setattr(mp_scanner, "subscription", lambda *a, **k: BadSub())
    monkeypatch.setattr(mp_scanner, "stream_mempool_tokens", fake_stream)

    async def run():
        async for _ in mp_scanner.stream_ranked_mempool_tokens("rpc"):
            pass

    with caplog.at_level(logging.WARNING):
        asyncio.run(run())

    assert "metrics subscription exit failed" in caplog.text.lower()
    assert "boom" in caplog.text.lower()
