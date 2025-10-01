import asyncio
import types

import pytest

import solhunter_zero.resource_monitor as rm
from solhunter_zero import event_bus


@pytest.mark.asyncio
async def test_resource_monitor_publish(monkeypatch):
    stub = types.SimpleNamespace(
        cpu_percent=lambda interval=None: 5.0,
        Process=lambda: types.SimpleNamespace(cpu_percent=lambda interval=None: 1.0),
        virtual_memory=lambda: types.SimpleNamespace(percent=42.0),
    )
    monkeypatch.setattr(rm, "psutil", stub)

    received = []
    unsub1 = event_bus.subscribe('system_metrics', lambda p: received.append(('local', p)))
    unsub2 = event_bus.subscribe('remote_system_metrics', lambda p: received.append(('remote', p)))

    rm.start_monitor(0.01)
    await asyncio.sleep(0.05)
    rm.stop_monitor()
    unsub1()
    unsub2()

    assert received
    kinds = {k for k, _ in received}
    assert 'local' in kinds
    for kind, payload in received:
        if kind == 'local':
            assert payload.cpu == 5.0
            assert payload.memory == 42.0
            assert payload.proc_cpu == 1.0
        else:
            assert payload['cpu'] == 5.0
            assert payload['proc_cpu'] == 1.0
            assert payload['memory'] == 42.0


def test_get_cpu_usage_fallback(monkeypatch):
    called = False

    def fake_cpu(*_a, **_k):
        nonlocal called
        called = True
        return 12.0

    monkeypatch.setattr(rm.psutil, 'cpu_percent', fake_cpu)
    rm._CPU_PERCENT = 0.0
    rm._CPU_LAST = 0.0
    cpu = rm.get_cpu_usage()
    assert cpu == 12.0
    assert called
