import asyncio
import contextlib
import logging

import pytest

import solhunter_zero.main as main


class DummyProc:
    """Simple process stub with a predefined poll sequence."""
    def __init__(self, polls):
        self.polls = list(polls)
        self._last = self.polls[-1] if self.polls else None

    def poll(self):
        if self.polls:
            self._last = self.polls.pop(0)
        return self._last


@pytest.mark.asyncio
async def test_watchdog_restarts_multiple_times(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("DEPTH_MAX_RESTARTS", "2")

    async def fake_sleep(_):
        await asyncio.sleep(0)

    async def fake_to_thread(func, *a, **k):
        return func(*a, **k)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(main.asyncio, "to_thread", fake_to_thread)

    procs = [DummyProc([1]), DummyProc([1]), DummyProc([None])]
    proc_iter = iter(procs)
    proc_ref = [next(proc_iter)]
    monkeypatch.setattr(main, "_start_depth_service", lambda cfg: next(proc_iter))

    task = asyncio.create_task(main._depth_service_watchdog({}, proc_ref))
    await asyncio.sleep(0)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task

    assert proc_ref[0] is procs[-1]
    attempts = [r for r in caplog.records if "attempting restart" in r.message]
    assert len(attempts) == 2


@pytest.mark.asyncio
async def test_watchdog_aborts_after_limit(monkeypatch, caplog):
    caplog.set_level(logging.WARNING)
    monkeypatch.setenv("DEPTH_MAX_RESTARTS", "2")

    async def fake_sleep(_):
        await asyncio.sleep(0)

    async def fake_to_thread(func, *a, **k):
        return func(*a, **k)

    monkeypatch.setattr(main.asyncio, "sleep", fake_sleep)
    monkeypatch.setattr(main.asyncio, "to_thread", fake_to_thread)

    procs = [DummyProc([1]), DummyProc([1]), DummyProc([1])]
    proc_iter = iter(procs)
    proc_ref = [next(proc_iter)]
    monkeypatch.setattr(main, "_start_depth_service", lambda cfg: next(proc_iter))

    with pytest.raises(RuntimeError):
        await main._depth_service_watchdog({}, proc_ref)

    attempts = [r for r in caplog.records if "attempting restart" in r.message]
    assert len(attempts) == 2
