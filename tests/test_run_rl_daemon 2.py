import asyncio
import importlib
import os
import sys
import types


def test_run_rl_daemon_sets_event_bus(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_URL", raising=False)

    rl_mod = types.ModuleType("solhunter_zero.rl_daemon")

    class DummyDaemon:
        def __init__(self, *a, **k):
            from solhunter_zero.config import get_event_bus_url
            DummyDaemon.url = get_event_bus_url()
        def start(self, *a, **k):
            pass

    rl_mod.RLDaemon = DummyDaemon
    monkeypatch.setitem(sys.modules, "solhunter_zero.rl_daemon", rl_mod)

    agents_pkg = types.ModuleType("solhunter_zero.agents")
    agents_pkg.__path__ = []
    dqn_mod = types.ModuleType("solhunter_zero.agents.dqn")
    dqn_mod.DQNAgent = lambda *a, **k: None
    ppo_mod = types.ModuleType("solhunter_zero.agents.ppo_agent")
    ppo_mod.PPOAgent = lambda *a, **k: None
    mem_mod = types.ModuleType("solhunter_zero.agents.memory")
    mem_mod.MemoryAgent = lambda *a, **k: None
    agents_pkg.dqn = dqn_mod
    agents_pkg.ppo_agent = ppo_mod
    agents_pkg.memory = mem_mod
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents", agents_pkg)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.dqn", dqn_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.ppo_agent", ppo_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.memory", mem_mod)

    mem_module = types.ModuleType("solhunter_zero.memory")
    mem_module.Memory = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.memory", mem_module)

    run_mod = importlib.import_module("scripts.run_rl_daemon")

    class DummyEvent:
        async def wait(self):
            pass

    monkeypatch.setattr(run_mod.asyncio, "Event", DummyEvent)
    monkeypatch.setattr(sys, "argv", ["run_rl_daemon.py", "--event-bus", "ws://bus"])

    asyncio.run(run_mod.main())

    from solhunter_zero.config import get_event_bus_url

    assert DummyDaemon.url == "ws://bus"
    assert get_event_bus_url() == "ws://bus"


def test_run_rl_daemon_distributed_flag(monkeypatch):
    rl_mod = types.ModuleType("solhunter_zero.rl_daemon")

    class DummyDaemon:
        def __init__(self, *a, **k):
            DummyDaemon.dist = k.get("distributed_rl")
        def start(self, *a, **k):
            pass

    rl_mod.RLDaemon = DummyDaemon
    monkeypatch.setitem(sys.modules, "solhunter_zero.rl_daemon", rl_mod)

    agents_pkg = types.ModuleType("solhunter_zero.agents")
    agents_pkg.__path__ = []
    dqn_mod = types.ModuleType("solhunter_zero.agents.dqn")
    dqn_mod.DQNAgent = lambda *a, **k: None
    ppo_mod = types.ModuleType("solhunter_zero.agents.ppo_agent")
    ppo_mod.PPOAgent = lambda *a, **k: None
    mem_mod = types.ModuleType("solhunter_zero.agents.memory")
    mem_mod.MemoryAgent = lambda *a, **k: None
    agents_pkg.dqn = dqn_mod
    agents_pkg.ppo_agent = ppo_mod
    agents_pkg.memory = mem_mod
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents", agents_pkg)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.dqn", dqn_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.ppo_agent", ppo_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.memory", mem_mod)

    mem_module = types.ModuleType("solhunter_zero.memory")
    mem_module.Memory = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.memory", mem_module)

    run_mod = importlib.import_module("scripts.run_rl_daemon")

    class DummyEvent:
        async def wait(self):
            pass

    monkeypatch.setattr(run_mod.asyncio, "Event", DummyEvent)
    monkeypatch.setattr(sys, "argv", ["run_rl_daemon.py", "--distributed-rl"])

    asyncio.run(run_mod.main())

    assert DummyDaemon.dist is True

