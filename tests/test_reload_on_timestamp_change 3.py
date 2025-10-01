import asyncio
import inspect
import subprocess

import pytest

from tests.test_full_system_integration import (
    DQNAgent,
    Memory,
    MemoryAgent,
    OfflineData,
    RLDaemon,
    torch,
)


@pytest.mark.asyncio
async def test_reload_on_timestamp_change(tmp_path, monkeypatch):
    if not inspect.isclass(getattr(torch.optim, "Adam", None)):
        pytest.skip("real torch required")
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    model_path = tmp_path / 'model.pt'

    reloaded = asyncio.Event()

    def fake_reload():
        reloaded.set()

    agent = DQNAgent(memory_agent=MemoryAgent(mem), epsilon=0.0, model_path=model_path)
    monkeypatch.setattr(agent, 'reload_weights', fake_reload)

    class DummyProc:
        def __init__(self, *a, **k):
            pass

        def poll(self):
            return None

    monkeypatch.setattr(subprocess, 'Popen', DummyProc)

    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=model_path,
        algo='dqn',
        agents=[agent],
    )
    daemon.start(0.05, auto_train=True, tune_interval=0.05)

    await asyncio.sleep(0.1)
    from solhunter_zero.rl_daemon import _DQN
    torch.save(_DQN().state_dict(), model_path)

    await asyncio.wait_for(reloaded.wait(), timeout=1.0)

