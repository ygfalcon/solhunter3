import asyncio
import importlib
import os
import sys
import types


def _setup_dummy_daemon(monkeypatch):
    """Register a dummy RLDaemon implementation used in tests."""

    rl_mod = types.ModuleType("solhunter_zero.rl_daemon")

    class DummyDaemon:
        intervals: list[float] = []
        tasks: list[object] = []
        dist: bool | None = None
        url: str | None = None

        def __init__(self, *a, **k):
            DummyDaemon.url = os.environ.get("EVENT_BUS_URL")
            DummyDaemon.dist = k.get("distributed_rl")

        def start(self, interval: float, *a, **k):
            DummyDaemon.intervals.append(interval)
            task = types.SimpleNamespace(done=lambda: False)
            DummyDaemon.tasks.append(task)
            return task

        # ``train`` is kept for compatibility with older implementations
        def train(self):  # pragma: no cover - not used in new logic
            DummyDaemon.intervals.append(-1)
            return None

    rl_mod.RLDaemon = DummyDaemon
    monkeypatch.setitem(sys.modules, "solhunter_zero.rl_daemon", rl_mod)

    # stub out agent modules referenced by the script
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

    return DummyDaemon


def test_run_rl_daemon_sets_event_bus(monkeypatch):
    monkeypatch.delenv("EVENT_BUS_URL", raising=False)
    DummyDaemon = _setup_dummy_daemon(monkeypatch)

    run_mod = importlib.import_module("scripts.run_rl_daemon")
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_rl_daemon.py", "--event-bus", "ws://bus", "--interval", "5"],
    )

    asyncio.run(run_mod.main())

    assert DummyDaemon.url == "ws://bus"
    assert os.environ["EVENT_BUS_URL"] == "ws://bus"
    # two daemons should be started with the provided interval
    assert DummyDaemon.intervals == [5.0, 5.0]
    assert all(not t.done() for t in DummyDaemon.tasks)


def test_run_rl_daemon_distributed_flag(monkeypatch):
    DummyDaemon = _setup_dummy_daemon(monkeypatch)

    run_mod = importlib.import_module("scripts.run_rl_daemon")
    monkeypatch.setattr(
        sys,
        "argv",
        ["run_rl_daemon.py", "--distributed-rl", "--interval", "7"],
    )

    asyncio.run(run_mod.main())

    assert DummyDaemon.dist is True
    assert DummyDaemon.intervals == [7.0, 7.0]
    assert len(DummyDaemon.tasks) == 2

