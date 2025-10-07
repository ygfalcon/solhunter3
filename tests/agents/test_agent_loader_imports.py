import importlib


def test_import_agent_module_accepts_absolute_path(monkeypatch):
    import solhunter_zero.agents as agents

    module = agents._import_agent_module("solhunter_zero.flash_loans")

    assert module.__name__ == "solhunter_zero.flash_loans"
    # The extended search path should still provide the agents shim.
    shim = importlib.import_module("solhunter_zero.agents.flash_loans")
    assert shim is module or shim.__file__ == module.__file__


def test_import_agent_module_accepts_agents_qualified_path(monkeypatch):
    import solhunter_zero.agents as agents

    module = agents._import_agent_module("solhunter_zero.agents.flash_loans")

    assert module.__name__ == "solhunter_zero.agents.flash_loans"
