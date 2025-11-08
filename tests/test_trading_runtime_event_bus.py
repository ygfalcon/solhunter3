import os

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

    start_calls = []

    async def failing_start_ws_server(host, port):
        start_calls.append((host, port))
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
    assert start_calls == [
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 0),
    ]
    assert sleep_calls == [0.25, 0.5, 1.0]
    assert runtime.bus_started is False
    assert runtime.status.event_bus is False

    activity = runtime.activity.snapshot()
    assert activity[-1]["stage"] == "event_bus"
    assert activity[-1]["ok"] is False
    assert "failed to start event bus websocket" in activity[-1]["detail"]


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_fails_when_redis_bootstrap_fails(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    def failing_ensure(urls):
        raise RuntimeError("redis-server missing")

    async def should_not_start_ws(*args, **kwargs):  # pragma: no cover - ensured by assertion
        pytest.fail("start_ws_server should not be called when redis bootstrap fails")

    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", failing_ensure
    )
    monkeypatch.setattr(trading_runtime, "start_ws_server", should_not_start_ws)

    with pytest.raises(RuntimeError) as exc:
        await runtime._start_event_bus()

    assert "redis-server missing" in str(exc.value)
    assert runtime.status.event_bus is False

    activity = runtime.activity.snapshot()
    assert activity[-1]["stage"] == "broker"
    assert activity[-1]["ok"] is False
    assert "redis" in activity[-1]["detail"]


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_falls_back_to_ephemeral_port(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)
    monkeypatch.delenv("EVENT_BUS_WS_PORT_RANGE", raising=False)

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    bound_port = 9901
    start_calls = []
    sleep_calls = []

    class DummySocket:
        def __init__(self, port: int) -> None:
            self._port = port

        def getsockname(self):
            return ("127.0.0.1", self._port)

    class DummyServer:
        def __init__(self, port: int) -> None:
            self.sockets = [DummySocket(port)]

    async def fake_start_ws_server(host, port):
        start_calls.append((host, port))
        if len(start_calls) <= 3:
            raise RuntimeError("busy")
        return DummyServer(bound_port)

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    async def fake_verify_broker_connection(*, timeout):
        return True

    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    subscribe_called = False

    def fake_subscribe():
        nonlocal subscribe_called
        subscribe_called = True

    monkeypatch.setattr(runtime, "_subscribe_to_events", fake_subscribe)

    await runtime._start_event_bus()

    assert start_calls == [
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 0),
    ]
    assert sleep_calls == [0.25, 0.5, 1.0]
    assert os.environ["EVENT_BUS_URL"] == f"ws://127.0.0.1:{bound_port}"
    assert os.environ["BROKER_WS_URLS"] == f"ws://127.0.0.1:{bound_port}"
    assert runtime.bus_started is True
    assert runtime.status.event_bus is True
    assert subscribe_called is True

    event_entries = [
        entry for entry in runtime.activity.snapshot() if entry["stage"] == "event_bus"
    ]
    assert event_entries
    assert any(f"ws://127.0.0.1:{bound_port}" in entry["detail"] for entry in event_entries)
    assert any("(fallback)" in entry["detail"] for entry in event_entries)


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_uses_configured_port_range(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)
    monkeypatch.setenv("EVENT_BUS_WS_PORT_RANGE", "8800-8802")

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    start_calls = []
    sleep_calls = []
    bound_port = 8801

    class DummySocket:
        def __init__(self, port: int) -> None:
            self._port = port

        def getsockname(self):
            return ("127.0.0.1", self._port)

    class DummyServer:
        def __init__(self, port: int) -> None:
            self.sockets = [DummySocket(port)]

    async def fake_start_ws_server(host, port):
        start_calls.append((host, port))
        if port in {8779, 8800}:
            raise RuntimeError("busy")
        if port == bound_port:
            return DummyServer(bound_port)
        raise RuntimeError("unexpected port request")

    async def fake_sleep(delay):
        sleep_calls.append(delay)

    async def fake_verify_broker_connection(*, timeout):
        return True

    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(trading_runtime.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    subscribe_called = False

    def fake_subscribe():
        nonlocal subscribe_called
        subscribe_called = True

    monkeypatch.setattr(runtime, "_subscribe_to_events", fake_subscribe)

    await runtime._start_event_bus()

    assert start_calls == [
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8779),
        ("127.0.0.1", 8800),
        ("127.0.0.1", bound_port),
    ]
    assert sleep_calls == [0.25, 0.5, 1.0, 2.0]
    assert os.environ["EVENT_BUS_URL"] == f"ws://127.0.0.1:{bound_port}"
    assert os.environ["BROKER_WS_URLS"] == f"ws://127.0.0.1:{bound_port}"
    assert runtime.bus_started is True
    assert runtime.status.event_bus is True
    assert subscribe_called is True

    event_entries = [
        entry for entry in runtime.activity.snapshot() if entry["stage"] == "event_bus"
    ]
    assert event_entries
    assert any("(fallback)" in entry["detail"] for entry in event_entries)
    assert any(f"ws://127.0.0.1:{bound_port}" in entry["detail"] for entry in event_entries)


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


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_updates_env_for_dynamic_port(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_DISABLE_LOCAL", raising=False)
    monkeypatch.setenv("EVENT_BUS_WS_PORT", "0")
    monkeypatch.setenv("EVENT_BUS_URL", "ws://remote:9999")
    monkeypatch.setenv("BROKER_WS_URLS", "ws://remote:9999")

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {}

    bound_port = 9543
    start_calls = []

    class DummySocket:
        def __init__(self, port: int) -> None:
            self._port = port

        def getsockname(self):
            return ("127.0.0.1", self._port)

    class DummyServer:
        def __init__(self, port: int) -> None:
            self.sockets = [DummySocket(port)]

    async def fake_start_ws_server(host, port):
        start_calls.append((host, port))
        return DummyServer(bound_port)

    async def fake_verify_broker_connection(*, timeout):
        return True

    monkeypatch.setattr(trading_runtime, "start_ws_server", fake_start_ws_server)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )
    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    subscribe_called = False

    def fake_subscribe():
        nonlocal subscribe_called
        subscribe_called = True

    monkeypatch.setattr(runtime, "_subscribe_to_events", fake_subscribe)

    await runtime._start_event_bus()

    assert start_calls == [("127.0.0.1", 0)]
    assert os.environ["EVENT_BUS_URL"] == f"ws://127.0.0.1:{bound_port}"
    assert os.environ["BROKER_WS_URLS"] == f"ws://127.0.0.1:{bound_port}"
    assert runtime.bus_started is True
    assert subscribe_called is True


@pytest.mark.anyio("asyncio")
async def test_start_event_bus_uses_remote_when_local_disabled(monkeypatch):
    monkeypatch.setenv("EVENT_BUS_DISABLE_LOCAL", "1")
    monkeypatch.setenv("EVENT_BUS_URL", "ws://127.0.0.1:8779")
    monkeypatch.setenv("BROKER_WS_URLS", "ws://127.0.0.1:8779")

    runtime = trading_runtime.TradingRuntime()
    runtime.cfg = {
        "event_bus_url": "wss://primary.example/ws",
        "event_bus_peers": ["wss://backup.example/ws"],
    }

    monkeypatch.setattr(
        trading_runtime, "ensure_local_redis_if_needed", lambda urls: None
    )

    async def should_not_start_ws(*args, **kwargs):  # pragma: no cover - ensured by assertion
        pytest.fail("start_ws_server should not be called when local bus disabled")

    async def fake_verify_broker_connection(*, timeout):
        return True

    monkeypatch.setattr(trading_runtime, "start_ws_server", should_not_start_ws)
    monkeypatch.setattr(
        trading_runtime, "verify_broker_connection", fake_verify_broker_connection
    )

    await runtime._start_event_bus()

    assert runtime.bus_started is False
    assert runtime.status.event_bus is True
    assert os.environ["EVENT_BUS_URL"] == "wss://primary.example/ws"
    assert (
        os.environ["BROKER_WS_URLS"]
        == "wss://primary.example/ws,wss://backup.example/ws"
    )

    event_entries = [
        entry for entry in runtime.activity.snapshot() if entry["stage"] == "event_bus"
    ]
    assert event_entries
    assert any("disabled" in entry["detail"] for entry in event_entries)
    assert any("primary.example" in entry["detail"] for entry in event_entries)
