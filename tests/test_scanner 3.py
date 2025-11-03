import asyncio
from solhunter_zero import token_scanner as scanner
from solhunter_zero import dynamic_limit
from solhunter_zero import scanner_common
from solhunter_zero import resource_monitor as rm
from solhunter_zero.event_bus import subscribe
from solhunter_zero import event_bus

data = {"data": [{"address": "abcbonk"}, {"address": "otherbonk"}]}

class FakeResponse:
    def __init__(self, data, status_code=200):
        self._data = data
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code != 200:
            raise Exception('bad status')

    def json(self):
        return self._data

def test_scan_tokens_websocket(monkeypatch):
    async def fake_stream(url, *, suffix="bonk", include_pools=True):
        yield "webbonk"


    captured = {}

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

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, headers=None, timeout=10):
            captured['headers'] = headers
            return FakeResp()

    async def fake_session():
        return FakeSession()
    monkeypatch.setattr(scanner, "get_session", fake_session)
    async def fake_trend():
        return ['trend']
    monkeypatch.setattr(scanner, 'fetch_trending_tokens_async', fake_trend)
    async def fr():
        return ['ray']
    async def fo():
        return ['orca']
    monkeypatch.setattr(scanner, 'fetch_raydium_listings_async', fr)
    monkeypatch.setattr(scanner, 'fetch_orca_listings_async', fo)
    scanner_common.BIRDEYE_API_KEY = "test"
    scanner_common.HEADERS.clear()
    scanner_common.HEADERS["X-API-KEY"] = "test"
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens())
    unsub()
    assert tokens == ['abcbonk', 'xyzBONK', 'trend', 'ray', 'orca']
    assert captured['headers'] == scanner.HEADERS
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)




# codex/add-offline-option-to-solhunter_zero.main
def test_scan_tokens_offline(monkeypatch):
    called = {}

    def fake_session(*args, **kwargs):
        called['called'] = True
        raise AssertionError('network used')

    monkeypatch.setattr("solhunter_zero.websocket_scanner.stream_new_tokens", lambda *a, **k: (_ for _ in ()).throw(AssertionError('ws')))
    async def fake_session_async():
        return fake_session()
    monkeypatch.setattr(scanner, "get_session", fake_session_async)
    async def fail():
        raise AssertionError('trending')
    monkeypatch.setattr(scanner, 'fetch_trending_tokens_async', fail)
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens(offline=True))
    unsub()
    assert tokens == scanner.OFFLINE_TOKENS
    assert 'called' not in called
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_onchain(monkeypatch):
    captured = {}

    async def fake_onchain(url):
        captured['url'] = url
        return ['tok']

    def fake_session(*args, **kwargs):
        raise AssertionError('should not call BirdEye')

    monkeypatch.setattr(scanner_common, "scan_tokens_onchain", fake_onchain)
    async def fake_session_async2():
        return fake_session()
    monkeypatch.setattr(scanner, "get_session", fake_session_async2)
    async def ft():
        return ['t2']
    async def fr():
        return []
    monkeypatch.setattr(scanner, 'fetch_trending_tokens_async', ft)
    monkeypatch.setattr(scanner, 'fetch_raydium_listings_async', fr)
    monkeypatch.setattr(scanner, 'fetch_orca_listings_async', fr)

    scanner_common.SOLANA_RPC_URL = 'http://node'
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens(method="onchain"))
    unsub()
    assert tokens == ['tok', 't2']
    assert captured['url'] == 'http://node'
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


from solhunter_zero.token_scanner import scan_tokens_async as async_scan


def test_scan_tokens_async(monkeypatch):
    data = {"data": [{"address": "abcbonk"}, {"address": "otherbonk"}]}
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

    class FakeSession:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            pass
        def get(self, url, headers=None, timeout=10):
            return FakeResp()

    async def fake_session3():
        return FakeSession()
    monkeypatch.setattr(scanner, "get_session", fake_session3)
    async def fake_trend():
        return ['trend']
    import solhunter_zero.token_scanner as async_scanner_mod
    monkeypatch.setattr(async_scanner_mod, 'fetch_trending_tokens_async', fake_trend)
    scanner_async_module = __import__('solhunter_zero.token_scanner', fromlist=[''])
    scanner_common.BIRDEYE_API_KEY = 'key'
    scanner_common.HEADERS.clear()
    scanner_common.HEADERS["X-API-KEY"] = "key"
    async def fr_func():
        return ['ray']
    async def fo_func():
        return ['orca']
    monkeypatch.setattr(async_scanner_mod, 'fetch_raydium_listings_async', fr_func)
    monkeypatch.setattr(async_scanner_mod, 'fetch_orca_listings_async', fo_func)
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(async_scan())
    unsub()
    assert tokens == ["abcbonk", "otherbonk", "trend", "ray", "orca"]
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_from_file(monkeypatch, tmp_path):
    path = tmp_path / "tokens.txt"
    path.write_text("tok1\n tok2\n\n#comment\n")

    def fake_session(*a, **k):
        raise AssertionError("should not call network")

    async def fake_session4():
        return fake_session()
    monkeypatch.setattr(scanner, "get_session", fake_session4)
    monkeypatch.setattr(scanner_common, "scan_tokens_onchain", lambda _: asyncio.sleep(0, ["x"]))  # should not be called
    monkeypatch.setattr(
        scanner,
        "fetch_trending_tokens_async",
        lambda: (_ for _ in ()).throw(AssertionError("trending")),
    )

    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens(token_file=str(path)))
    unsub()
    assert tokens == ["tok1", "tok2"]
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_async_from_file(monkeypatch, tmp_path):
    path = tmp_path / "tokens.txt"
    path.write_text("a\nb\n")

    def fake_session():
        raise AssertionError("network should not be used")

    async def fake_session5():
        return fake_session()
    monkeypatch.setattr(scanner, "get_session", fake_session5)
    async def fail():
        raise AssertionError("trending")
    import solhunter_zero.token_scanner as async_scanner_mod
    monkeypatch.setattr(async_scanner_mod, "fetch_trending_tokens_async", fail)

    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(async_scan(token_file=str(path)))
    unsub()
    assert tokens == ["a", "b"]
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_mempool(monkeypatch):
    async def fake_stream(url, *, suffix="bonk", include_pools=True):
        yield "memtok"

    monkeypatch.setattr(
        "solhunter_zero.mempool_scanner.stream_mempool_tokens", fake_stream
    )
    async def fr():
        return []
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fr)
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fr)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fr)
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    tokens = asyncio.run(scanner.scan_tokens(method="mempool"))
    unsub()
    assert tokens == ["memtok"]
    assert [evt.tokens for evt in events] == [tokens]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_async_mempool(monkeypatch):
    async def fake_stream(url, *, suffix=None, keywords=None, include_pools=True):
        yield "memtok"

    monkeypatch.setattr(
        "solhunter_zero.mempool_scanner.stream_mempool_tokens", fake_stream
    )
    async def fr():
        return []

    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fr)
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fr)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fr)
    events = []
    unsub = subscribe("token_discovered", lambda p: events.append(p))
    result = asyncio.run(async_scan(method="mempool"))
    unsub()
    assert result == ["memtok"]
    assert [evt.tokens for evt in events] == [result]
    assert all(not evt.metadata_refresh for evt in events)


def test_scan_tokens_async_concurrency(monkeypatch):
    running = 0
    max_running = 0

    async def fake_task(*_a, **_k):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1
        return ["x"]

    monkeypatch.setattr(scanner.TokenScanner, "scan", lambda *a, **k: fake_task())
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fake_task)
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fake_task)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fake_task)
    monkeypatch.setattr(scanner, "_fetch_dex_ws_tokens", fake_task)
    monkeypatch.setattr(scanner, "DEX_LISTING_WS_URL", "ws://dex")
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: 0.0)

    asyncio.run(scanner.scan_tokens_async(max_concurrency=2))
    assert max_running <= 2


def test_scan_tokens_async_dynamic(monkeypatch):
    running = 0
    max_running = 0

    async def fake_task(*_a, **_k):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0)
        running -= 1
        return ["x"]

    monkeypatch.setattr(scanner.TokenScanner, "scan", lambda *a, **k: fake_task())
    monkeypatch.setattr(scanner, "fetch_trending_tokens_async", fake_task)
    monkeypatch.setattr(scanner, "fetch_raydium_listings_async", fake_task)
    monkeypatch.setattr(scanner, "fetch_orca_listings_async", fake_task)
    monkeypatch.setattr(scanner, "_fetch_dex_ws_tokens", fake_task)
    monkeypatch.setattr(scanner, "DEX_LISTING_WS_URL", "ws://dex")
    monkeypatch.setattr(scanner.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(rm, "get_cpu_usage", lambda: 90.0)
    scanner._DYN_INTERVAL = 0.0

    asyncio.run(scanner.scan_tokens_async(dynamic_concurrency=True))
    assert max_running <= 2


def test_cpu_fallback(monkeypatch):
    called = False

    def fake_cpu(*_a, **_k):
        nonlocal called
        called = True
        return 55.0

    monkeypatch.setattr(rm.psutil, "cpu_percent", fake_cpu)
    rm._CPU_PERCENT = 0.0
    rm._CPU_LAST = 0.0
    cpu = rm.get_cpu_usage()
    assert cpu == 55.0
    assert called


def test_scanner_concurrency_controller(monkeypatch):
    import types

    monkeypatch.setattr(dynamic_limit.psutil, "virtual_memory", lambda: types.SimpleNamespace(percent=0.0))
    monkeypatch.setattr(dynamic_limit, "_KP", 0.5)
    dynamic_limit._CPU_EMA = 0.0

    tgt = dynamic_limit._target_concurrency(90.0, 4, 40.0, 80.0)
    cur = dynamic_limit._step_limit(4, tgt, 4)
    assert tgt == 1
    assert cur == 2

    for _ in range(5):
        tgt = dynamic_limit._target_concurrency(0.0, 4, 40.0, 80.0)
        cur = dynamic_limit._step_limit(cur, tgt, 4)

    assert cur >= 3


def test_concurrency_memory_pressure(monkeypatch):
    import types

    monkeypatch.setattr(dynamic_limit.psutil, "virtual_memory", lambda: types.SimpleNamespace(percent=90.0))
    dynamic_limit._CPU_EMA = 0.0
    tgt = dynamic_limit._target_concurrency(10.0, 4, 40.0, 80.0)
    assert tgt == 1

