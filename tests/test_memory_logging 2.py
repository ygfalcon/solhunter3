import asyncio
import logging
from contextlib import suppress

from solhunter_zero.memory import Memory


def failing_publish(*args, **kwargs):
    raise RuntimeError("boom")


def test_log_trade_publish_warning(monkeypatch, caplog):
    mem = Memory('sqlite:///:memory:')
    caplog.set_level(logging.WARNING)
    monkeypatch.setattr('solhunter_zero.memory.publish', failing_publish)

    asyncio.run(
        mem.log_trade(token='ABC', direction='buy', amount=1.0, price=1.0)
    )
    assert any('trade_logged publish failed' in r.message for r in caplog.records)


def test_log_trade_publish_warning_with_queue(monkeypatch, caplog):
    async def run_test():
        mem = Memory('sqlite:///:memory:')
        mem.start_writer()
        await mem.wait_ready()
        caplog.set_level(logging.WARNING)
        monkeypatch.setattr('solhunter_zero.memory.publish', failing_publish)
        await mem.log_trade(token='XYZ', direction='sell', amount=2.0, price=3.0)
        assert any('trade_logged publish failed' in r.message for r in caplog.records)
        mem._writer_task.cancel()
        with suppress(asyncio.CancelledError):
            await mem._writer_task

    asyncio.run(run_test())
