import asyncio
import importlib
import os

import pytest
import sys
import types
import importlib.util
import importlib.machinery


if importlib.util.find_spec("pydantic") is None:
    pyd = types.ModuleType("pydantic")
    pyd.__spec__ = importlib.machinery.ModuleSpec("pydantic", None)

    class BaseModel:
        def __init__(self, **data):
            pass

        def dict(self):
            return {}

        def model_dump(self, *a, **k):
            return {}

    class ValidationError(Exception):
        pass

    def _identity(*a, **k):
        return lambda func: func

    pyd.BaseModel = BaseModel
    pyd.ValidationError = ValidationError
    pyd.AnyUrl = str
    pyd.field_validator = _identity
    pyd.model_validator = _identity
    pyd.validator = _identity
    pyd.root_validator = _identity
    sys.modules.setdefault("pydantic", pyd)


@pytest.mark.asyncio
@pytest.mark.chaos
async def test_shutdown_event_bus_cancels_tasks(monkeypatch):
    monkeypatch.setenv("BROKER_WS_URLS", "ws://bus")
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)

    async def fake_reachable(urls, timeout=1.0):
        await asyncio.sleep(0.1)
        return set(urls)

    async def fake_connect(url):
        await asyncio.sleep(0)

    monkeypatch.setattr(ev, "_reachable_ws_urls", fake_reachable)
    monkeypatch.setattr(ev, "connect_ws", fake_connect)

    ev._reload_bus(None)
    assert ev._reconnect_task is not None
    ev.shutdown_event_bus()
    await asyncio.sleep(0.1)
    assert ev._reconnect_task is None or ev._reconnect_task.cancelled()
    importlib.reload(ev)


@pytest.mark.chaos
def test_reconnect_ws_shutdown_no_pending_tasks(monkeypatch, capsys):
    import solhunter_zero.event_bus as ev
    ev = importlib.reload(ev)

    class DummyWS:
        def __init__(self):
            self.closed = asyncio.Event()

        def __aiter__(self):
            return self

        async def __anext__(self):
            await self.closed.wait()
            raise StopAsyncIteration

        async def close(self):
            self.closed.set()

        async def wait_closed(self):
            await self.closed.wait()

    async def fake_connect(url, *a, **k):
        return DummyWS()

    monkeypatch.setattr(ev, "websockets", types.SimpleNamespace(connect=fake_connect))

    async def main():
        await ev.reconnect_ws("ws://bus")
        assert ev._ws_tasks
        ev.shutdown_event_bus()
        await asyncio.sleep(0.05)
        assert not ev._ws_tasks

    asyncio.run(main())
    assert "Task was destroyed but it is pending" not in capsys.readouterr().err
    importlib.reload(ev)
