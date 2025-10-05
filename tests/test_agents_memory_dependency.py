from __future__ import annotations

import builtins
import sys

def test_agents_warn_and_skip_memory_when_sqlalchemy_missing(monkeypatch, caplog):
    import solhunter_zero.agents as agents

    original_registry = agents.BUILT_IN_AGENTS.copy()
    agents.BUILT_IN_AGENTS.clear()

    for module_name in [
        "solhunter_zero.agents.memory",
        "solhunter_zero.agents.reinforcement",
        "solhunter_zero.agents.opportunity_cost",
        "solhunter_zero.agents.dqn",
        "solhunter_zero.agents.ppo_agent",
        "solhunter_zero.agents.sac_agent",
        "solhunter_zero.agents.rl_weight_agent",
        "solhunter_zero.agents.fractal_agent",
        "solhunter_zero.memory",
        "solhunter_zero.advanced_memory",
    ]:
        monkeypatch.delitem(sys.modules, module_name, raising=False)

    for module_name in list(sys.modules):
        if module_name.startswith("sqlalchemy"):
            monkeypatch.delitem(sys.modules, module_name, raising=False)

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name.startswith("sqlalchemy"):
            raise ModuleNotFoundError("No module named 'sqlalchemy'")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    try:
        with caplog.at_level("WARNING", logger="solhunter_zero.agents"):
            agents._ensure_agents_loaded()

        assert "arbitrage" in agents.BUILT_IN_AGENTS
        for name in [
            "memory",
            "reinforcement",
            "opportunity_cost",
            "dqn",
            "ppo",
            "sac",
            "rl_weight",
            "inferna",
        ]:
            assert name not in agents.BUILT_IN_AGENTS
        assert "sqlalchemy" in caplog.text
        assert "Skipping memory-dependent agents" in caplog.text
    finally:
        agents.BUILT_IN_AGENTS.clear()
        agents.BUILT_IN_AGENTS.update(original_registry)

