import asyncio

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
