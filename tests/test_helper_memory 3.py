import pytest
from solhunter_zero.memory import Memory

# create memory instances outside any running event loop
mem_sync = Memory('sqlite:///:memory:')
mem_async = Memory('sqlite:///:memory:')

@pytest.mark.asyncio
async def test_log_and_list_vars_async():
    """log_var/list_vars should work when an event loop is already running."""
    await mem_async.log_var(0.1)
    await mem_async.log_var(0.2)
    vals = await mem_async.list_vars()
    assert [v.value for v in vals] == [0.1, 0.2]


def test_log_and_list_vars_sync():
    """log_var/list_vars should work without a running loop."""
    mem_sync.log_var(0.3)
    mem_sync.log_var(0.4)
    vals = mem_sync.list_vars()
    assert [v.value for v in vals][-2:] == [0.3, 0.4]
