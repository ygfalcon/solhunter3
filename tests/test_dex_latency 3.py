import importlib
import asyncio
import time
import pytest
import solhunter_zero.arbitrage as arb


@pytest.fixture
def ensure_ffi(monkeypatch):
    from pathlib import Path
    import subprocess
    import importlib

    lib_path = Path(__file__).resolve().parents[1] / "route_ffi/target/release/libroute_ffi.so"
    if not lib_path.exists():
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(Path(__file__).resolve().parents[1] / "route_ffi/Cargo.toml"),
                "--release",
            ],
            check=True,
        )
    monkeypatch.setenv("ROUTE_FFI_LIB", str(lib_path))
    importlib.reload(arb._routeffi)
    importlib.reload(arb)
    yield


def test_refresh_costs_updates_latency(monkeypatch):
    monkeypatch.setenv("MEASURE_DEX_LATENCY", "0")
    mod = importlib.reload(arb)

    latencies = {"jupiter": 0.12}
    monkeypatch.setattr(mod, "measure_dex_latency", lambda urls=None, attempts=3: latencies)
    mod.MEASURE_DEX_LATENCY = True

    _, _, lat = mod.refresh_costs()
    assert lat["jupiter"] == pytest.approx(0.12)
    assert mod.DEX_LATENCY["jupiter"] == pytest.approx(0.12)


@pytest.mark.asyncio
async def test_ping_url_parallel():
    import solhunter_zero.arbitrage as mod

    class DummyResp:
        async def __aenter__(self):
            await asyncio.sleep(0.05)
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        async def read(self):
            pass

    class DummySession:
        def get(self, *_, **__):
            return DummyResp()

        def ws_connect(self, *_, **__):
            return DummyResp()

    session = DummySession()
    start = time.perf_counter()
    val = await mod._ping_url(session, "http://x", attempts=2)
    duration = time.perf_counter() - start
    assert val == pytest.approx(0.05, rel=0.4)
    assert duration < 0.1


@pytest.mark.asyncio
async def test_measure_latency_parallel(monkeypatch):
    import solhunter_zero.arbitrage as mod

    async def fake_get_session():
        return object()

    calls = []

    async def fake_ping(session, url, attempts=1):
        calls.append(url)
        await asyncio.sleep(0.05)
        return 0.05

    monkeypatch.setattr(mod, "get_session", fake_get_session)
    monkeypatch.setattr(mod, "_ping_url", fake_ping)

    start = time.perf_counter()
    res = await mod.measure_dex_latency_async({"d1": "u1", "d2": "u2"}, attempts=3)
    duration = time.perf_counter() - start
    assert res["d1"] == pytest.approx(0.05)
    assert len(calls) == 2
    assert duration < 0.1


@pytest.mark.asyncio
async def test_measure_latency_concurrency_limit(monkeypatch):
    import solhunter_zero.arbitrage as mod

    async def fake_get_session():
        return object()

    running = 0
    max_running = 0

    async def fake_ping(session, url, attempts=1):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0.05)
        running -= 1
        return 0.05

    monkeypatch.setattr(mod, "get_session", fake_get_session)
    monkeypatch.setattr(mod, "_ping_url", fake_ping)

    await mod.measure_dex_latency_async(
        {"d1": "u1", "d2": "u2", "d3": "u3"}, attempts=1, max_concurrency=2
    )
    assert max_running <= 2


@pytest.mark.asyncio
async def test_measure_latency_dynamic_limit(monkeypatch):
    import solhunter_zero.arbitrage as mod

    monkeypatch.setattr(mod.os, "cpu_count", lambda: 4)
    monkeypatch.setattr(mod.resource_monitor, "get_cpu_usage", lambda: 90.0)
    mod._DYN_INTERVAL = 0.0

    async def fake_get_session():
        return object()

    running = 0
    max_running = 0

    async def fake_ping(session, url, attempts=1):
        nonlocal running, max_running
        running += 1
        max_running = max(max_running, running)
        await asyncio.sleep(0.01)
        running -= 1
        return 0.05

    monkeypatch.setattr(mod, "get_session", fake_get_session)
    monkeypatch.setattr(mod, "_ping_url", fake_ping)

    await mod.measure_dex_latency_async(
        {"d1": "u1", "d2": "u2", "d3": "u3", "d4": "u4"},
        attempts=1,
        dynamic_concurrency=True,
    )
    assert max_running <= 2


@pytest.mark.asyncio
async def test_measure_latency_cache(monkeypatch):
    import solhunter_zero.arbitrage as mod

    async def fake_get_session():
        return object()

    calls = {"pings": 0}

    async def fake_ping(session, url, attempts=1):
        calls["pings"] += 1
        return 0.1

    monkeypatch.setattr(mod, "get_session", fake_get_session)
    monkeypatch.setattr(mod, "_ping_url", fake_ping)

    mod.DEX_LATENCY_CACHE.ttl = 60
    mod.DEX_LATENCY_CACHE.clear()

    urls = {"d": "u"}
    res1 = await mod.measure_dex_latency_async(urls, attempts=1)
    res2 = await mod.measure_dex_latency_async(urls, attempts=1)

    assert res1 == {"d": 0.1}
    assert res2 == res1
    assert calls["pings"] == 1


@pytest.mark.asyncio
async def test_measure_latency_ffi(monkeypatch, ensure_ffi):
    import solhunter_zero.arbitrage as mod

    def fake_measure(urls, attempts=1):
        return {k: 0.01 for k in urls}

    monkeypatch.setattr(mod._routeffi, "measure_latency", fake_measure)
    monkeypatch.setattr(mod, "_ping_url", lambda *a, **k: (_ for _ in ()).throw(AssertionError("called")))

    res = await mod.measure_dex_latency_async({"d": "u"}, attempts=1)
    assert res == {"d": 0.01}
