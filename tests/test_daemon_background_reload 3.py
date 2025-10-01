import asyncio
import inspect
from pathlib import Path

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
async def test_daemon_background_reload(tmp_path, monkeypatch):
    if not inspect.isclass(getattr(torch.optim, "Adam", None)):
        pytest.skip("real torch required")
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)
    await mem.log_trade(token='tok', direction='sell', amount=1, price=2)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, total_depth=1.5, imbalance=0.0)
    await data.log_snapshot('tok', 1.1, 1.0, total_depth=1.6, imbalance=0.0)

    model_path = tmp_path / 'model.pt'

    monkeypatch.setattr(torch.cuda, 'is_available', lambda: False)

    reloaded = asyncio.Event()

    def fake_reload():
        reloaded.set()

    agent = DQNAgent(memory_agent=MemoryAgent(mem), epsilon=0.0, model_path=model_path)
    monkeypatch.setattr(agent, 'reload_weights', fake_reload)

    def fake_train(self):
        Path(self.model_path).write_text('x')
        for ag in self.agents:
            ag.reload_weights()

    monkeypatch.setattr(RLDaemon, 'train', fake_train)

    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=model_path,
        algo='dqn',
    )
    daemon.register_agent(agent)
    daemon.start(0.01)

    await asyncio.wait_for(reloaded.wait(), timeout=1.0)

