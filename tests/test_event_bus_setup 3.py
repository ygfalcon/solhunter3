import asyncio
import importlib

import pytest
import pytest_asyncio


@pytest_asyncio.fixture
async def no_pending_tasks():
    yield
    loop = asyncio.get_running_loop()
    pending = [t for t in asyncio.all_tasks(loop) if not t.done()]
    if pending:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        pytest.fail(f"Pending tasks left: {pending}")


@pytest.mark.asyncio
async def test_reload_bus_invalid_url(monkeypatch, no_pending_tasks):
    import solhunter_zero.event_bus as ev

    importlib.reload(ev)

    def fake_resolve(_cfg):
        return ev._validate_ws_urls(["http://invalid"])

    monkeypatch.setattr(ev, "_resolve_ws_urls", fake_resolve)

    with pytest.raises(RuntimeError):
        ev._reload_bus(None)

    loop = asyncio.get_running_loop()
    current = asyncio.current_task()
    pending = [t for t in asyncio.all_tasks(loop) if t is not current and not t.done()]
    assert not pending
