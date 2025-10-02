import asyncio
import importlib
import sys
import types

import pytest


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_rl_daemon_emits_heartbeat_when_enabled(monkeypatch):
    events: list[tuple[str, object]] = []

    def _publish(topic: str, payload: object) -> None:
        events.append((topic, payload))

    event_mod = types.ModuleType("solhunter_zero.event_bus")
    event_mod.publish = _publish  # type: ignore[attr-defined]

    class _ExecAgent:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def execute(self, action):  # pragma: no cover - unused in test
            return action

    exec_mod = types.ModuleType("solhunter_zero.agents.execution")
    exec_mod.ExecutionAgent = _ExecAgent  # type: ignore[attr-defined]

    class _EvalContext:
        def __init__(self, *args, **kwargs) -> None:
            pass

    agent_mgr_mod = types.ModuleType("solhunter_zero.agent_manager")
    agent_mgr_mod.EvaluationContext = _EvalContext  # type: ignore[attr-defined]

    class _StrategyManager:
        def __init__(self, *args, **kwargs) -> None:
            pass

    strat_mod = types.ModuleType("solhunter_zero.strategy_manager")
    strat_mod.StrategyManager = _StrategyManager  # type: ignore[attr-defined]

    class _SwarmPipeline:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def run(self):  # pragma: no cover - unused in test
            return {}

    swarm_mod = types.ModuleType("solhunter_zero.swarm_pipeline")
    swarm_mod.SwarmPipeline = _SwarmPipeline  # type: ignore[attr-defined]

    for name in [
        "solhunter_zero.loop",
        "solhunter_zero.agent_manager",
        "solhunter_zero.strategy_manager",
        "solhunter_zero.agents.execution",
        "solhunter_zero.swarm_pipeline",
        "solhunter_zero.event_bus",
    ]:
        sys.modules.pop(name, None)

    monkeypatch.setitem(sys.modules, "solhunter_zero.event_bus", event_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agents.execution", exec_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", agent_mgr_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.strategy_manager", strat_mod)
    monkeypatch.setitem(sys.modules, "solhunter_zero.swarm_pipeline", swarm_mod)

    loop_mod = importlib.import_module("solhunter_zero.loop")

    real_sleep = asyncio.sleep

    async def _fast_sleep(_delay: float) -> None:
        await real_sleep(0)

    monkeypatch.setattr(loop_mod.asyncio, "sleep", _fast_sleep)

    monkeypatch.delenv("RL_DAEMON", raising=False)

    task = await loop_mod._init_rl_training({"rl_auto_train": True}, rl_daemon=False, rl_interval=0)
    assert task is not None

    await real_sleep(0)
    for _ in range(10):
        if any(topic == "heartbeat" for topic, _ in events):
            break
        await real_sleep(0)
    assert any(topic == "heartbeat" for topic, _ in events)
    assert any(
        topic == "runtime.log" and getattr(payload, "detail", "") == "daemon-start"
        for topic, payload in events
    )

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert any(
        topic == "runtime.log" and getattr(payload, "detail", "") == "daemon-stop"
        for topic, payload in events
    )

    events.clear()
    monkeypatch.setenv("RL_DAEMON", "true")
    task_env = await loop_mod._init_rl_training({}, rl_daemon=False, rl_interval=0)
    assert task_env is not None

    await real_sleep(0)
    for _ in range(10):
        if any(topic == "heartbeat" for topic, _ in events):
            break
        await real_sleep(0)
    assert any(topic == "heartbeat" for topic, _ in events)

    task_env.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task_env

    events.clear()
    monkeypatch.delenv("RL_DAEMON", raising=False)
    assert await loop_mod._init_rl_training({}, rl_daemon=False, rl_interval=0) is None
