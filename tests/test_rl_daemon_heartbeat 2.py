import asyncio
import inspect

import pytest

from tests.test_full_system_integration import (
    Memory,
    OfflineData,
    RLDaemon,
    torch,
)

from solhunter_zero.event_bus import publish, subscribe


@pytest.mark.asyncio
async def test_rl_daemon_heartbeat(tmp_path, monkeypatch):
    if not inspect.isclass(getattr(torch.optim, "Adam", None)):
        pytest.skip("real torch required")
    mem_db = f"sqlite:///{tmp_path/'mem.db'}"
    data_path = tmp_path / 'data.db'
    OfflineData(f"sqlite:///{data_path}")
    Memory(mem_db)

    events = []
    unsub = subscribe("heartbeat", lambda p: events.append(p))

    async def fake_send(
        service: str,
        interval: float = 30.0,
        metrics_interval: float | None = None,
    ):
        publish("heartbeat", {"service": service})
        await asyncio.sleep(0)

    monkeypatch.setattr("solhunter_zero.event_bus.send_heartbeat", fake_send)
    monkeypatch.setattr(RLDaemon, "train", lambda self: None)

    daemon = RLDaemon(
        memory_path=mem_db,
        data_path=str(data_path),
        model_path=tmp_path/'model.pt',
    )
    daemon.start(0.01)
    await asyncio.sleep(0.05)
    unsub()

    assert any(e.get("service") == "rl_daemon" for e in events)

