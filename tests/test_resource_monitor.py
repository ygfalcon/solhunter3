import asyncio
import types

import solhunter_zero.resource_monitor as rm
from solhunter_zero import event_bus


def test_resource_monitor_publish(monkeypatch):
    stub = types.SimpleNamespace(
        cpu_percent=lambda interval=None: 5.0,
        Process=lambda: types.SimpleNamespace(cpu_percent=lambda interval=None: 1.0),
        virtual_memory=lambda: types.SimpleNamespace(percent=42.0),
    )
    monkeypatch.setattr(rm, "psutil", stub)

    async def _run() -> None:
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

    asyncio.run(_run())


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


def test_resource_budget_alert(monkeypatch):
    rm.stop_monitor()
    monkeypatch.setenv("RESOURCE_CPU_CEILING", "5")
    monkeypatch.setenv("RESOURCE_CPU_ACTION", "exit")
    monkeypatch.setenv("RESOURCE_BUDGET_GRACE", "2")

    stub = types.SimpleNamespace(
        cpu_percent=lambda interval=None: 50.0,
        Process=lambda: types.SimpleNamespace(cpu_percent=lambda interval=None: 5.0),
        virtual_memory=lambda: types.SimpleNamespace(percent=10.0),
    )
    monkeypatch.setattr(rm, "psutil", stub)

    published: list[tuple[str, object]] = []

    def _capture(topic: str, payload: object) -> None:
        published.append((topic, payload))

    monkeypatch.setattr(rm, "publish", _capture)

    async def _run() -> None:
        task = rm.start_monitor(0.01)
        assert task is not None
        await asyncio.sleep(0.05)
        rm.stop_monitor()
        alerts = [payload for topic, payload in published if topic == "resource_alert"]
        assert alerts, "expected resource_alert events"
        budgets = rm.get_budget_status()
        assert budgets["cpu"]["active"] is True
        exit_budgets = rm.active_budget("exit")
        assert exit_budgets and exit_budgets[0]["resource"] == "cpu"

    try:
        asyncio.run(_run())
    finally:
        monkeypatch.delenv("RESOURCE_CPU_CEILING", raising=False)
        monkeypatch.delenv("RESOURCE_CPU_ACTION", raising=False)
        monkeypatch.delenv("RESOURCE_BUDGET_GRACE", raising=False)
        rm._BUDGETS.clear()
        rm._BUDGET_STATUS.clear()
