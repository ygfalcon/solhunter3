import asyncio
import time
import contextlib
import gc
import warnings

import pytest
pytest.importorskip("solders")
from solders.keypair import Keypair
from solhunter_zero import arbitrage as arb
from solhunter_zero.arbitrage import detect_and_execute_arbitrage
from solhunter_zero import prices


# reset global state before each test
def setup_function(_):
    arb._SESSION = None
    arb.PRICE_CACHE.clear()
    prices.PRICE_CACHE = prices.TTLCache(maxsize=256, ttl=prices.PRICE_CACHE_TTL)


@pytest.fixture(autouse=True)
def _disable_jup(monkeypatch):
    monkeypatch.setattr(arb, "JUPITER_WS_URL", "")


def test_arbitrage_executes_orders(monkeypatch):
    async def feed1(token):
        return 1.0

    async def feed2(token):
        return 1.2

    called = []

    async def fake_place_order(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        called.append((side, price))
        return {"ok": True}

    monkeypatch.setattr("solhunter_zero.arbitrage.place_order_async", fake_place_order)

    result = asyncio.run(
        detect_and_execute_arbitrage("tok", [feed1, feed2], threshold=0.1, amount=5)
    )

    assert result == (0, 1)
    assert ("buy", 1.0) in called
    assert ("sell", 1.2) in called


def test_flash_loan_arbitrage(monkeypatch):
    async def feed1(token):
        return 1.0

    async def feed2(token):
        return 1.2

    kp = Keypair()
    sent = {}

    class FakeClient:
        def __init__(self, url):
            self.url = url

        async def send_raw_transaction(self, data, opts=None):
            sent["tx"] = data

            class Resp:
                value = "sig"

            return Resp()

        async def confirm_transaction(self, sig):
            sent["confirm"] = sig

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr("solhunter_zero.flash_loans.AsyncClient", FakeClient)

    result = asyncio.run(
        detect_and_execute_arbitrage(
            "tok",
            [feed1, feed2],
            threshold=0.1,
            amount=5,
            use_flash_loans=True,
            keypair=kp,
            max_flash_amount=10,
        )
    )

    assert result == (0, 1)
    assert sent["confirm"] == "sig"
    parts = sent["tx"][2:].split(b"|")
    assert parts[0] == b"borrow"
    assert parts[-1] == b"repay"
    assert b"swap" in parts[1]


def test_flashloan_multi_hop(monkeypatch):
    async def feed1(token):
        return 1.0

    async def feed2(token):
        return 1.2

    async def feed3(token):
        return 1.5

    kp = Keypair()
    sent = {}

    class FakeClient:
        def __init__(self, url):
            self.url = url

        async def send_raw_transaction(self, data, opts=None):
            sent["tx"] = data

            class Resp:
                value = "sig"

            return Resp()

        async def confirm_transaction(self, sig):
            sent["confirm"] = sig

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

    monkeypatch.setattr("solhunter_zero.flash_loans.AsyncClient", FakeClient)
    monkeypatch.setattr(
        arb.depth_client,
        "snapshot",
        lambda t: (
            {
                "feed1": {"bids": 10, "asks": 10},
                "feed2": {"bids": 10, "asks": 10},
                "feed3": {"bids": 10, "asks": 10},
            },
            5.0,
        ),
    )

    result = asyncio.run(
        detect_and_execute_arbitrage(
            "tok",
            [feed1, feed2, feed3],
            threshold=0.0,
            amount=5,
            use_flash_loans=True,
            keypair=kp,
            max_flash_amount=10,
            max_hops=3,
            path_algorithm="dijkstra",
            fees={"feed1": 0.0, "feed2": 0.0, "feed3": 0.0},
            gas={"feed1": 0.0, "feed2": 0.0, "feed3": 0.0},
            latencies={"feed1": 0.0, "feed2": 0.0, "feed3": 0.0},
        )
    )

    assert result == (0, 2)
    assert sent["confirm"] == "sig"


def test_no_arbitrage(monkeypatch):
    async def feed1(token):
        return 1.0

    async def feed2(token):
        return 1.05

    called = {}

    async def fake_place_order(*a, **k):
        called["called"] = True
        return {}

    monkeypatch.setattr("solhunter_zero.arbitrage.place_order_async", fake_place_order)

    result = asyncio.run(
        detect_and_execute_arbitrage("tok", [feed1, feed2], threshold=0.1, amount=5)
    )

    assert result is None
    assert "called" not in called


def test_default_price_feeds(monkeypatch):
    """Uses built-in Orca and Raydium feeds when none are provided."""

    async def fake_orca(token):
        return 1.5

    async def fake_raydium(token):
        return 1.8

    orders = []

    async def fake_place_order(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        orders.append((side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "fetch_orca_price_async", fake_orca)
    monkeypatch.setattr(arb, "fetch_raydium_price_async", fake_raydium)
    monkeypatch.setattr(arb, "place_order_async", fake_place_order)

    result = asyncio.run(
        detect_and_execute_arbitrage("tok", None, threshold=0.1, amount=2)
    )

    assert result == (0, 1)
    assert ("buy", 1.5) in orders
    assert ("sell", 1.8) in orders


async def _stream_once(value):
    yield value


def test_arbitrage_websocket(monkeypatch):
    calls = []

    async def fake_place(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        calls.append((side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "place_order_async", fake_place)

    stream1 = _stream_once(1.0)
    stream2 = _stream_once(1.3)

    result = asyncio.run(
        detect_and_execute_arbitrage(
            "tok",
            streams=[stream1, stream2],
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result == (0, 1)
    assert ("buy", 1.0) in calls
    assert ("sell", 1.3) in calls


def test_arbitrage_websocket_no_op(monkeypatch):
    calls = {}

    async def fake_place(*a, **k):
        calls["called"] = True
        return {}

    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        detect_and_execute_arbitrage(
            "tok",
            streams=[_stream_once(1.0), _stream_once(1.05)],
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result is None
    assert "called" not in calls


def test_arbitrage_multiple_tokens(monkeypatch):
    async def feed_low(token):
        prices = {"tok1": 1.0, "tok2": 2.0}
        return prices[token]

    async def feed_high(token):
        prices = {"tok1": 1.2, "tok2": 2.5}
        return prices[token]

    placed = []

    async def fake_place(token, side, amount, price, **_):
        placed.append((token, side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "place_order_async", fake_place)

    results = asyncio.run(
        arb.detect_and_execute_arbitrage(
            [
                "tok1",
                "tok2",
            ],
            [feed_low, feed_high],
            threshold=0.1,
            amount=3,
        )
    )

    assert results == [(0, 1), (0, 1)]
    assert ("tok1", "buy", 1.0) in placed
    assert ("tok2", "sell", 2.5) in placed


def test_arbitrage_env_streams(monkeypatch):
    """Automatically open websocket streams when URLs are set."""

    async def fake_orca(token, url="ws://orca"):
        yield 1.0

    async def fake_ray(token, url="ws://ray"):
        yield 1.5

    calls = []

    async def fake_place(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        calls.append((side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "ORCA_WS_URL", "ws://orca")
    monkeypatch.setattr(arb, "RAYDIUM_WS_URL", "ws://ray")
    monkeypatch.setattr(arb, "stream_orca_prices", fake_orca)
    monkeypatch.setattr(arb, "stream_raydium_prices", fake_ray)
    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        arb.detect_and_execute_arbitrage(
            "tok",
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result == (0, 1)
    assert ("buy", 1.0) in calls
    assert ("sell", 1.5) in calls


def test_arbitrage_env_streams_no_op(monkeypatch):
    async def fake_orca(token, url="ws://orca"):
        yield 1.0

    async def fake_ray(token, url="ws://ray"):
        yield 1.05

    called = {}

    async def fake_place(*a, **k):
        called["called"] = True
        return {}

    monkeypatch.setattr(arb, "ORCA_WS_URL", "ws://orca")
    monkeypatch.setattr(arb, "RAYDIUM_WS_URL", "ws://ray")
    monkeypatch.setattr(arb, "stream_orca_prices", fake_orca)
    monkeypatch.setattr(arb, "stream_raydium_prices", fake_ray)
    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        arb.detect_and_execute_arbitrage(
            "tok",
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result is None
    assert "called" not in called


def test_arbitrage_env_streams_three(monkeypatch):
    async def fake_orca(token, url="ws://o"):
        yield 1.0

    async def fake_ray(token, url="ws://r"):
        yield 1.3

    async def fake_jup(token, url="ws://j"):
        yield 1.5

    calls = []

    async def fake_place(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        calls.append((side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "ORCA_WS_URL", "ws://o")
    monkeypatch.setattr(arb, "RAYDIUM_WS_URL", "ws://r")
    monkeypatch.setattr(arb, "JUPITER_WS_URL", "ws://j")
    monkeypatch.setattr(arb, "stream_orca_prices", fake_orca)
    monkeypatch.setattr(arb, "stream_raydium_prices", fake_ray)
    monkeypatch.setattr(arb, "stream_jupiter_prices", fake_jup)
    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        arb.detect_and_execute_arbitrage(
            "tok",
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result == (0, 2)
    assert ("buy", 1.0) in calls
    assert ("sell", 1.5) in calls


def test_arbitrage_env_streams_three_no_op(monkeypatch):
    async def fake_orca(token, url="ws://o"):
        yield 1.0

    async def fake_ray(token, url="ws://r"):
        yield 1.05

    async def fake_jup(token, url="ws://j"):
        yield 1.08

    called = {}

    async def fake_place(*a, **k):
        called["called"] = True
        return {}

    monkeypatch.setattr(arb, "ORCA_WS_URL", "ws://o")
    monkeypatch.setattr(arb, "RAYDIUM_WS_URL", "ws://r")
    monkeypatch.setattr(arb, "JUPITER_WS_URL", "ws://j")
    monkeypatch.setattr(arb, "stream_orca_prices", fake_orca)
    monkeypatch.setattr(arb, "stream_raydium_prices", fake_ray)
    monkeypatch.setattr(arb, "stream_jupiter_prices", fake_jup)
    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        arb.detect_and_execute_arbitrage(
            "tok",
            threshold=0.2,
            amount=5,
            max_updates=1,
        )
    )

    assert result is None
    assert "called" not in called


def test_depth_aware_routing(monkeypatch):
    monkeypatch.setattr(arb, "USE_DEPTH_STREAM", False)
    monkeypatch.setattr(arb, "USE_SERVICE_EXEC", False)

    async def dex_a(token):
        return 1.0

    async def dex_b(token):
        return 1.2

    async def dex_c(token):
        return 1.2

    monkeypatch.setattr(
        "solhunter_zero.depth_client.snapshot",
        lambda t: (
            {
                "dex_a": {"bids": 50, "asks": 100},
                "dex_b": {"bids": 5, "asks": 100},
                "dex_c": {"bids": 100, "asks": 100},
            },
            0.0,
        ),
    )

    called = []

    async def fake_place(
        token, side, amount, price, testnet=False, dry_run=False, keypair=None
    ):
        called.append((side, price))
        return {"ok": True}

    monkeypatch.setattr(arb, "place_order_async", fake_place)

    result = asyncio.run(
        detect_and_execute_arbitrage(
            "tok",
            [dex_a, dex_b, dex_c],
            threshold=0.0,
            amount=5,
            fees={"dex_a": 0.0, "dex_b": 0.0, "dex_c": 0.0},
            gas={"dex_a": 0.0, "dex_b": 0.0, "dex_c": 0.0},
            latencies={"dex_a": 0.0, "dex_b": 0.0, "dex_c": 0.0},
        )
    )

    assert result == (0, 2)
    assert ("sell", 1.2) in called


def test_price_cache(monkeypatch):
    data = {"price": 1.1}
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

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession())
    arb.PRICE_CACHE.ttl = 60
    prices.PRICE_CACHE.ttl = 60

    result1 = asyncio.run(arb.fetch_orca_price_async("tok"))
    result2 = asyncio.run(arb.fetch_orca_price_async("tok"))

    assert prices.get_cached_price("tok") == 1.1

    assert result1 == 1.1
    assert result2 == 1.1
    assert calls["sessions"] == 1
    assert calls["gets"] == 1


def test_latency_measurement_parallel(monkeypatch):
    async def fake_get_session():
        return object()

    async def fake_ping(session, url, attempts=1):
        await asyncio.sleep(0.05)
        return 0.05

    monkeypatch.setattr(arb, "get_session", fake_get_session)
    monkeypatch.setattr(arb, "_ping_url", fake_ping)

    start = time.perf_counter()
    asyncio.run(arb.measure_dex_latency_async({"d1": "u1"}, attempts=1))
    single = time.perf_counter() - start

    start = time.perf_counter()
    asyncio.run(
        arb.measure_dex_latency_async(
            {"d1": "u1", "d2": "u2", "d3": "u3"}, attempts=1
        )
    )
    multi = time.perf_counter() - start

    assert multi <= single * 1.5


@pytest.mark.asyncio
async def test_latency_refresh_loop_start_stop_no_warning(monkeypatch):
    if arb._LATENCY_TASK is not None:
        arb.stop_latency_refresh()

    async def fake_measure(urls, dynamic_concurrency=True):
        await asyncio.sleep(0)
        return {}

    monkeypatch.setattr(arb, "measure_dex_latency_async", fake_measure)
    monkeypatch.setattr(arb, "publish", lambda *a, **kw: None)

    with warnings.catch_warnings():
        warnings.simplefilter("error", RuntimeWarning)
        task = arb.start_latency_refresh(interval=0.01)
        await asyncio.sleep(0.02)
        arb.stop_latency_refresh()
        with contextlib.suppress(asyncio.CancelledError):
            await task
        gc.collect()
        assert task.cancelled() or task.done()
