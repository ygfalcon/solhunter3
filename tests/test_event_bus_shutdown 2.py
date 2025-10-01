import asyncio
import importlib
import os

import pytest


@pytest.mark.asyncio
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
