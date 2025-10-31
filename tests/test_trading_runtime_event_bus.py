import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_retries_and_succeeds(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    start_calls = []
    sleep_calls = []

    async def fake_start_ws_server(host, port):
        start_calls.append((host, port))
        if len(start_calls) == 1:
            raise RuntimeError("boom")
        return object()

    verify_calls = []

    async def fake_verify_broker_connection(*, timeout):
        verify_calls.append(timeout)
        return len(verify_calls) >= 2

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    subscribe_called = False

    def fake_subscribe():
        nonlocal subscribe_called
        subscribe_called = True

    monkeypatch.setattr(runtime, "_subscribe_to_events", fake_subscribe)

    await runtime._start_event_bus()

    assert start_calls == [("127.0.0.1", 8779), ("127.0.0.1", 8779)]
    assert verify_calls == [2.0, 2.0]
    assert sleep_calls[0] == pytest.approx(0.25)
    assert sleep_calls[1] == pytest.approx(0.5)
    assert runtime.bus_started is True
    assert runtime.status.event_bus is True
    assert subscribe_called is True

    activity = runtime.activity.snapshot()
    assert any(entry["stage"] == "event_bus" and entry["ok"] for entry in activity)
    assert any(entry["stage"] == "broker" and entry["ok"] for entry in activity)


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_records_failure_and_raises(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    async def failing_start_ws_server(host, port):
        raise RuntimeError("port in use")

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    monkeypatch.setattr(trading_runtime, "start_ws_server", failing_start_ws_server)
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)

    async def should_not_verify(**kwargs):  # pragma: no cover - ensured by assertion
        pytest.fail("verify_broker_connection should not be called when start fails")

    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", should_not_verify
    )
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    with pytest.raises(RuntimeError) as exc:
        await runtime._start_event_bus()

    assert "failed to start event bus websocket" in str(exc.value)
    assert sleep_calls == [0.25, 0.5]
    assert runtime.bus_started is False
    assert runtime.status.event_bus is False

    activity = runtime.activity.snapshot()
    assert activity[-1]["stage"] == "event_bus"
    assert activity[-1]["ok"] is False
    assert "failed to start event bus websocket" in activity[-1]["detail"]


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_raises_on_broker_failure(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    async def fake_start_ws_server(host, port):
        return object()

    async def fake_verify_broker_connection(*, timeout):
        return False

    sleep_calls = []

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    subscribe_called = False

    def fake_subscribe():
        nonlocal subscribe_called
        subscribe_called = True

    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(runtime, "_subscribe_to_events", fake_subscribe)
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    with pytest.raises(RuntimeError) as exc:
        await runtime._start_event_bus()

    assert "broker verification failed" in str(exc.value)
    assert sleep_calls == [0.5, 1.0]
    assert subscribe_called is False
    assert runtime.bus_started is True
    assert runtime.status.event_bus is False

    activity = runtime.activity.snapshot()
    assert any(entry["stage"] == "event_bus" and entry["ok"] for entry in activity)
    assert activity[-1]["stage"] == "broker"
    assert activity[-1]["ok"] is False
    assert "broker verification failed" in activity[-1]["detail"]
