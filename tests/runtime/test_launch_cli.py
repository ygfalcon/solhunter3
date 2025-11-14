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


def test_launch_cli_starts_and_stops_websockets(monkeypatch, capsys):
    module = _get_launch_module()
    calls: list[str] = []

    def fake_start_websockets():
        calls.append("start")
        return {"events": object(), "rl": object(), "logs": object()}

    def fake_stop_websockets():
        calls.append("stop")

    def fake_get_ws_urls():
        return {
            "events": "ws://localhost:9100/ws/events",
            "rl": "ws://localhost:9101/ws/rl",
            "logs": "ws://localhost:9102/ws/logs",
        }

    class DummyRuntime:
        def __init__(self, *args, **kwargs):
            calls.append("init")

        def run_forever(self):
            calls.append("run")

    monkeypatch.setattr(module.ui, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module.ui, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module.ui, "get_ws_urls", fake_get_ws_urls)
    monkeypatch.setattr(module, "TradingRuntime", DummyRuntime)

    result = module.main([])

    assert result == 0
    assert calls == ["start", "init", "run", "stop"]
    captured = capsys.readouterr()
    assert (
        "UI websocket channels: events=ws://localhost:9100/ws/events, "
        "rl=ws://localhost:9101/ws/rl, logs=ws://localhost:9102/ws/logs"
        in captured.out
    )


def test_launch_cli_stops_websockets_on_failure(monkeypatch, capsys):
    module = _get_launch_module()
    calls: list[str] = []

    def fake_start_websockets():
        calls.append("start")
        return {"events": object(), "rl": object(), "logs": object()}

    def fake_stop_websockets():
        calls.append("stop")

    def fake_get_ws_urls():
        return {
            "events": "ws://localhost:9100/ws/events",
            "rl": "ws://localhost:9101/ws/rl",
            "logs": "ws://localhost:9102/ws/logs",
        }

    class FailingRuntime:
        def __init__(self, *args, **kwargs):
            calls.append("init")

        def run_forever(self):
            calls.append("run")
            raise RuntimeError("boom")

    monkeypatch.setattr(module.ui, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module.ui, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module.ui, "get_ws_urls", fake_get_ws_urls)
    monkeypatch.setattr(module, "TradingRuntime", FailingRuntime)

    with pytest.raises(RuntimeError, match="boom"):
        module.main([])

    assert calls == ["start", "init", "run", "stop"]
    captured = capsys.readouterr()
    assert (
        "UI websocket channels: events=ws://localhost:9100/ws/events, "
        "rl=ws://localhost:9101/ws/rl, logs=ws://localhost:9102/ws/logs"
        in captured.out
    )
