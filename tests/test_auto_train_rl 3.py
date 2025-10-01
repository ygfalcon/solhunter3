import asyncio
from pathlib import Path
import subprocess

import pytest
pytest.importorskip("torch.nn.utils.rnn")

from solhunter_zero.rl_daemon import RLDaemon
from solhunter_zero.agents.ppo_agent import PPOAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.memory import Memory
from solhunter_zero.offline_data import OfflineData


@pytest.mark.asyncio
async def test_auto_train_reload(tmp_path, monkeypatch):
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    data_db = f"sqlite:///{data_path}"

    mem = Memory(mem_db)
    await mem.log_trade(token='tok', direction='buy', amount=1, price=1)
    await mem.log_trade(token='tok', direction='sell', amount=1, price=2)

    data = OfflineData(data_db)
    await data.log_snapshot('tok', 1.0, 1.0, imbalance=0.0, total_depth=1.0)

    model_path = tmp_path / 'model.pt'
    agent = PPOAgent(memory_agent=MemoryAgent(mem), data_url=data_db, model_path=model_path)

    reloaded = asyncio.Event()

    def fake_reload(self):
        reloaded.set()

    monkeypatch.setattr(agent, 'reload_weights', fake_reload)

    class DummyProc:
        def __init__(self, *a, **k):
            Path(model_path).write_text('x')
        def poll(self):
            return None
    monkeypatch.setattr(subprocess, 'Popen', DummyProc)

    daemon = RLDaemon(memory_path=mem_db, data_path=str(data_path), model_path=model_path, algo='ppo', agents=[agent])
    daemon.start(0.01, auto_train=True, tune_interval=0.01)

    await asyncio.wait_for(reloaded.wait(), timeout=1.0)


def test_jit_inference_speed():
    import torch
    import time
    from solhunter_zero.rl_daemon import _PPO

    model = _PPO()
    data = torch.randn(1024, 8)

    start = time.time()
    for _ in range(200):
        model(data)
    regular = time.time() - start

    scripted = torch.jit.script(model)
    start = time.time()
    for _ in range(200):
        scripted(data)
    scripted_time = time.time() - start

    assert scripted_time < regular
