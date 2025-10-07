from __future__ import annotations

import importlib
import logging

import pytest


@pytest.fixture(autouse=True)
def _reset_agents_registry(monkeypatch):
    import solhunter_zero.agents as agents

    importlib.reload(agents)
    monkeypatch.setitem(agents.__dict__, "BUILT_IN_AGENTS", {})
    yield


def test_missing_optional_dependency_logs_warning(monkeypatch, caplog):
    import solhunter_zero.agents as agents

    original_import_module = importlib.import_module

    def fake_import(name, package=None):
        if name == ".memory" and package == "solhunter_zero.agents":
            raise ModuleNotFoundError(
                "No module named 'sqlalchemy'", name="sqlalchemy"
            )
        return original_import_module(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    caplog.set_level(logging.WARNING, logger="solhunter_zero.agents")

    agents._ensure_agents_loaded()

    assert "memory" not in agents.BUILT_IN_AGENTS
    assert "simulation" in agents.BUILT_IN_AGENTS

    warning_messages = [record.message for record in caplog.records if record.levelno >= logging.WARNING]
    assert any("sqlalchemy" in message for message in warning_messages)
    assert any("memory" in message for message in warning_messages)
