from __future__ import annotations

import importlib
import sys
import types

import pytest


def _get_launch_module():
    if "yaml" not in sys.modules:
        sys.modules["yaml"] = types.ModuleType("yaml")
    module = importlib.import_module("solhunter_zero.runtime.launch")
    return module


def test_launch_cli_starts_and_stops_websockets(monkeypatch):
    module = _get_launch_module()
    calls: list[str] = []

    def fake_start_websockets():
        calls.append("start")
        return {"events": object()}

    def fake_stop_websockets():
        calls.append("stop")

    class DummyRuntime:
        def __init__(self, *args, **kwargs):
            calls.append("init")

        def run_forever(self):
            calls.append("run")

    monkeypatch.setattr(module.ui, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module.ui, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module, "TradingRuntime", DummyRuntime)

    result = module.main([])

    assert result == 0
    assert calls == ["start", "init", "run", "stop"]


def test_launch_cli_stops_websockets_on_failure(monkeypatch):
    module = _get_launch_module()
    calls: list[str] = []

    def fake_start_websockets():
        calls.append("start")
        return {"events": object()}

    def fake_stop_websockets():
        calls.append("stop")

    class FailingRuntime:
        def __init__(self, *args, **kwargs):
            calls.append("init")

        def run_forever(self):
            calls.append("run")
            raise RuntimeError("boom")

    monkeypatch.setattr(module.ui, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module.ui, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module, "TradingRuntime", FailingRuntime)

    with pytest.raises(RuntimeError, match="boom"):
        module.main([])

    assert calls == ["start", "init", "run", "stop"]
