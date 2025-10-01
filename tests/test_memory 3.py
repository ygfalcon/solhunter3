import pytest
from solhunter_zero.memory import Memory


@pytest.mark.asyncio
async def test_log_and_list_trades():
    mem = Memory('sqlite:///:memory:')
    await mem.log_trade(token='TEST', direction='buy', amount=1.5, price=2.0)
    await mem.log_trade(token='TEST2', direction='sell', amount=0.5, price=1.0)

    trades = await mem.list_trades()
    assert len(trades) == 2
    assert trades[0].token == 'TEST'
    assert trades[0].direction == 'buy'
    assert trades[1].token == 'TEST2'
    assert trades[1].direction == 'sell'


@pytest.mark.asyncio
async def test_list_trades_filters():
    mem = Memory('sqlite:///:memory:')
    a = await mem.log_trade(token='A', direction='buy', amount=1.0, price=1.0)
    b = await mem.log_trade(token='B', direction='sell', amount=1.0, price=1.0)
    c = await mem.log_trade(token='A', direction='sell', amount=2.0, price=1.0)

    assert len(await mem.list_trades(token='A')) == 2
    assert len(await mem.list_trades(limit=2)) == 2
    ids = [t.id for t in await mem.list_trades(since_id=b)]
    assert ids == [c]


@pytest.mark.asyncio
async def test_log_and_list_vars():
    mem = Memory('sqlite:///:memory:')
    await mem.log_var(0.1)
    await mem.log_var(0.2)
    vals = await mem.list_vars()
    assert [v.value for v in vals] == [0.1, 0.2]


def test_trade_replication_event(tmp_path):
    db = tmp_path / "rep.db"
    idx = tmp_path / "rep.index"
    from solhunter_zero.advanced_memory import AdvancedMemory
    from solhunter_zero.event_bus import publish
    from solhunter_zero.schemas import TradeLogged

    mem = AdvancedMemory(url=f"sqlite:///{db}", index_path=str(idx), replicate=True)
    publish(
        "trade_logged",
        TradeLogged(token="TOK", direction="buy", amount=1.0, price=1.0),
    )
    trades = mem.list_trades()
    assert trades and trades[0].token == "TOK"


@pytest.mark.asyncio
async def test_async_queue_commit(tmp_path, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession
    import asyncio

    commits = 0
    orig_commit = AsyncSession.commit

    async def counting_commit(self, *a, **k):
        nonlocal commits
        commits += 1
        return await orig_commit(self, *a, **k)

    monkeypatch.setattr(AsyncSession, "commit", counting_commit)

    mem = Memory(f"sqlite:///{tmp_path/'direct.db'}")
    for _ in range(5):
        await mem.log_trade(token="TOK", direction="buy", amount=1.0, price=1.0)
    assert commits == 5

    commits = 0
    mem_q = Memory(f"sqlite:///{tmp_path/'queued.db'}")
    mem_q.start_writer(batch_size=5, interval=0.01)
    for _ in range(5):
        await mem_q.log_trade(token="TOK", direction="buy", amount=1.0, price=1.0)
    await asyncio.sleep(0.05)
    await mem_q.close()

    assert commits <= 1


@pytest.mark.asyncio
async def test_queue_flush_interval(tmp_path, monkeypatch):
    from sqlalchemy.ext.asyncio import AsyncSession
    import asyncio

    commits = 0
    orig_commit = AsyncSession.commit

    async def counting_commit(self, *a, **k):
        nonlocal commits
        commits += 1
        return await orig_commit(self, *a, **k)

    monkeypatch.setattr(AsyncSession, "commit", counting_commit)

    mem = Memory(f"sqlite:///{tmp_path/'int.db'}")
    mem.start_writer(batch_size=10, interval=0.01)
    for _ in range(5):
        await mem.log_trade(token="TOK", direction="buy", amount=1.0, price=1.0)
    await asyncio.sleep(0.05)
    await mem.close()

    assert commits == 1


@pytest.mark.asyncio
async def test_start_writer_env(monkeypatch):
    monkeypatch.setenv("MEMORY_BATCH_SIZE", "3")
    monkeypatch.setenv("MEMORY_FLUSH_INTERVAL", "0.5")
    mem = Memory("sqlite:///:memory:")
    mem.start_writer()
    assert mem._batch_size == 3
    assert mem._interval == 0.5


@pytest.mark.asyncio
async def test_persistent_trades(tmp_path, monkeypatch):
    """Trades logged to disk should be visible to a new Memory instance."""
    monkeypatch.chdir(tmp_path)

    mem1 = Memory("sqlite:///tmpfile.db")
    await mem1.log_trade(token="TOK", direction="buy", amount=1.0, price=2.0)
    await mem1.close()

    mem2 = Memory("sqlite:///tmpfile.db")
    trades = await mem2.list_trades()
    await mem2.close()

    assert len(trades) == 1
    assert trades[0].token == "TOK"
