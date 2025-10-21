import asyncio
import inspect
import json
import time

import pytest

import sys
import types

from tests import stubs as _test_stubs

_test_stubs.install_stubs()

sa_mod = sys.modules.get("sqlalchemy")
if sa_mod is None:
    sa_mod = types.ModuleType("sqlalchemy")
    sys.modules["sqlalchemy"] = sa_mod
if not hasattr(sa_mod, "Index"):
    sa_mod.Index = lambda *args, **kwargs: None  # type: ignore[attr-defined]
if not hasattr(sa_mod, "event"):
    sa_mod.event = types.SimpleNamespace(  # type: ignore[attr-defined]
        listens_for=lambda *a, **k: (lambda func: func)
    )

if "base58" not in sys.modules:
    base58_stub = types.ModuleType("base58")
    base58_stub.b58decode = lambda *args, **kwargs: b""
    base58_stub.b58encode = lambda *args, **kwargs: b""
    sys.modules["base58"] = base58_stub

import solhunter_zero.ui as ui

pytest_plugins = ["tests.golden_pipeline.synth_seed"]

CHAOS_PAIRS = 3200
QUEUE_LIMIT = 256
MAX_LAG_MS = 150.0


@pytest.fixture
def chaos_websockets(monkeypatch):
    ws_mod = pytest.importorskip("websockets")
    serve_params = inspect.signature(ws_mod.serve).parameters
    if "ping_interval" not in serve_params or "ping_timeout" not in serve_params:
        pytest.skip("websockets stub does not support ping configuration")
    monkeypatch.setenv("UI_RL_WS_PORT", "0")
    monkeypatch.setenv("UI_EVENT_WS_PORT", "0")
    monkeypatch.setenv("UI_LOG_WS_PORT", "0")
    monkeypatch.setenv("UI_WS_QUEUE_SIZE", str(QUEUE_LIMIT))
    threads = ui.start_websockets()
    try:
        yield {"module": ws_mod, "queue_limit": QUEUE_LIMIT, "threads": threads}
    finally:
        ui.stop_websockets()
        for thread in threads.values():
            if thread is not None:
                thread.join(timeout=1)


def test_ui_runtime_handles_chaos(runtime, bus, chaos_replay, chaos_websockets):
    ws_mod = chaos_websockets["module"]
    queue_limit = chaos_websockets["queue_limit"]

    urls = ui.get_ws_urls()

    async def _probe(url: str) -> None:
        async with ws_mod.connect(url) as conn:
            hello = await asyncio.wait_for(conn.recv(), timeout=5)
            payload = json.loads(hello)
            assert payload.get("event") == "hello"

    asyncio.run(_probe(urls["events"]))

    result = chaos_replay(pairs=CHAOS_PAIRS, queue_push_interval=20)

    time.sleep(0.25)

    ws_metrics = ui.get_ws_metrics()
    for channel, stats in ws_metrics.items():
        assert stats.get("queue_limit") == queue_limit
        assert stats.get("max_queue_depth", 0) <= queue_limit
        assert stats.get("queue_drops", 0) == 0
        assert stats.get("send_failures", 0) == 0
        assert 1006 not in stats.get("close_codes", [])
        if result["push_events"]:
            assert stats.get("total_enqueued", 0) >= result["push_events"]

    bus_metrics = runtime.ui_state.snapshot_bus_metrics()
    streams = bus_metrics.get("streams", {})
    assert streams
    for key in ("golden", "depth"):
        entry = streams.get(key)
        assert entry is not None
        assert entry.get("events", 0) >= CHAOS_PAIRS
        lag = entry.get("max_lag_ms")
        assert lag is not None and lag < MAX_LAG_MS
        assert entry.get("over_500_ms", 0) == 0
    assert bus_metrics.get("total_events", 0) >= CHAOS_PAIRS * 2
    overall_lag = bus_metrics.get("max_lag_ms")
    if overall_lag is not None:
        assert overall_lag < MAX_LAG_MS

    assert result["push_events"] > 0
