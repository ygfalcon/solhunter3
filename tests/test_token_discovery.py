import asyncio
import threading

from solhunter_zero import token_discovery as td


def test_discover_candidates_prioritises_scores(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    class FakeResp:
        status = 200

        def __init__(self, payload):
            self._payload = payload

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def json(self):
            return self._payload

        def raise_for_status(self):
            return None

    class FakeSession:
        def __init__(self):
            self.calls = []

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def get(self, url, **kwargs):
            self.calls.append(url)
            payload = {
                "data": {
                    "tokens": [
                        {
                            "address": "bird1",
                            "name": "Bird One",
                            "symbol": "B1",
                            "v24hUSD": 200000,
                            "liquidity": 300000,
                            "price": 1.2,
                            "v24hChangePercent": 5.0,
                        },
                        {
                            "address": "bird2",
                            "name": "Bird Two",
                            "symbol": "B2",
                            "v24hUSD": 80000,
                            "liquidity": 90000,
                            "price": 0.8,
                            "v24hChangePercent": -2.0,
                        },
                    ],
                    "total": 2,
                }
            }
            return FakeResp(payload)

    fake_session = FakeSession()

    class FakeClientSession:
        def __init__(self, *args, **kwargs):
            self._session = fake_session

        async def __aenter__(self):
            return fake_session

        async def __aexit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(td.aiohttp, "ClientSession", FakeClientSession)

    async def fake_mempool(_rpc_url, threshold):
        _ = threshold
        yield {
            "address": "memToken",
            "score": 2.0,
            "volume": 15000,
            "liquidity": 40000,
            "momentum": 0.2,
        }

    async def mempool_gen(rpc_url, threshold):
        agen = fake_mempool(rpc_url, threshold)
        async for item in agen:
            yield item

    monkeypatch.setattr(td, "stream_ranked_mempool_tokens_with_depth", mempool_gen)

    async def run():
        return await td.discover_candidates("https://rpc", limit=3, mempool_threshold=0.0)

    results = asyncio.run(run())
    addresses = [r["address"] for r in results]

    assert len(results) <= 3
    assert addresses[0] == "memToken"
    assert set(addresses) >= {"bird1", "bird2", "memToken"}
    assert results[0]["sources"] == ["mempool"]
    assert any("birdeye" in r["sources"] for r in results)
    assert len(fake_session.calls) == 1


def test_collect_mempool_signals_times_out(monkeypatch):
    async def silent_gen(*_args, **_kwargs):
        while True:
            await asyncio.sleep(3600)
            yield {"address": "never"}

    monkeypatch.setattr(
        td,
        "stream_ranked_mempool_tokens_with_depth",
        lambda *_a, **_k: silent_gen(),
    )
    monkeypatch.setattr(td, "_MEMPOOL_SIGNAL_TIMEOUT", 0.05)

    messages: list[str] = []

    class DummyLogger:
        def debug(self, msg, *args, **kwargs):
            if args:
                try:
                    msg = msg % args
                except Exception:
                    msg = msg.format(*args)
            messages.append(msg)

        def __getattr__(self, name):  # pragma: no cover - defensive for unused methods
            def _noop(*_args, **_kwargs):
                return None

            return _noop

    monkeypatch.setattr(td, "logger", DummyLogger())

    async def run():
        return await td._collect_mempool_signals("https://rpc", threshold=0.0)

    scores = asyncio.run(run())

    assert scores == {}
    assert any("timed out" in message for message in messages), messages


def test_warm_cache_safe_with_concurrent_fetch(monkeypatch):
    td._BIRDEYE_CACHE.clear()

    async def empty_mempool(*_args, **_kwargs):
        if False:  # pragma: no cover - generator stub
            yield None

    monkeypatch.setattr(
        td,
        "stream_ranked_mempool_tokens_with_depth",
        lambda *_a, **_k: empty_mempool(),
    )

    call_threads: list[str] = []
    counter_lock = threading.Lock()
    first_call_started = threading.Event()
    second_call_started = threading.Event()
    original_thread = threading.Thread
    worker_finished = threading.Event()
    worker_errors: list[BaseException] = []

    class RecordingThread(original_thread):
        def run(self) -> None:
            try:
                super().run()
            except BaseException as exc:  # pragma: no cover - defensive
                worker_errors.append(exc)
                raise
            finally:
                worker_finished.set()

    monkeypatch.setattr(td.threading, "Thread", RecordingThread)

    async def fake_fetch() -> list[dict[str, float]]:
        with counter_lock:
            idx = len(call_threads) + 1
            call_threads.append(threading.current_thread().name)
        if idx == 1:
            first_call_started.set()
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, second_call_started.wait)
        else:
            second_call_started.set()
            await asyncio.sleep(0)

        cached = td._BIRDEYE_CACHE.get("tokens") or []
        data = [
            {
                "address": f"token-{idx}",
                "symbol": f"T{idx}",
                "name": f"Token {idx}",
                "liquidity": 100000.0 + idx,
                "volume": 200000.0 + idx,
                "price": 1.0,
                "price_change": 0.0,
                "sources": {"birdeye"},
                "cached_before": [entry.get("address") for entry in cached],
            }
        ]
        td._BIRDEYE_CACHE.set("tokens", data)
        return data

    monkeypatch.setattr(td, "_fetch_birdeye_tokens", fake_fetch)

    td.warm_cache("https://rpc", limit=1)

    assert first_call_started.wait(timeout=1.0)

    asyncio.run(td._fetch_birdeye_tokens())

    assert worker_finished.wait(timeout=1.0)
    assert not worker_errors

    final = td._BIRDEYE_CACHE.get("tokens")

    assert final is not None
    assert all(entry.get("address") for entry in final)
    assert len(call_threads) == 2
    assert {"discovery-warm", "MainThread"} <= set(call_threads)
