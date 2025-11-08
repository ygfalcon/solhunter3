from __future__ import annotations

import importlib
import logging
import os


def _reload_start_all():
    module = importlib.import_module("scripts.start_all")
    return importlib.reload(module)


def test_rl_health_gate_skips_when_config_disables_rl(monkeypatch, caplog):
    start_module = _reload_start_all()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("RL_WEIGHTS_DISABLED", "0")
    start_module._PIPELINE_CONFIG = {"use_rl_weights": False}

    called = {"resolve": False}

    def fake_resolve(require_health_file: bool = False):  # pragma: no cover - defensive
        called["resolve"] = True
        return "http://rl:7070/health"

    monkeypatch.setattr(start_module, "resolve_rl_health_url", fake_resolve)

    caplog.set_level(logging.INFO)

    result = start_module.rl_health_gate()

    assert result is None
    assert not called["resolve"]
    assert any("Skipping RL health gate" in record.message for record in caplog.records)


def test_rl_health_gate_waits_for_health_when_enabled(monkeypatch):
    start_module = _reload_start_all()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("RL_WEIGHTS_DISABLED", "0")
    start_module._PIPELINE_CONFIG = {"use_rl_weights": True}

    url = "http://rl:7070/health"

    def fake_resolve(*, require_health_file: bool = True):
        assert require_health_file
        return url

    wait_calls: list[tuple[int, float]] = []

    def fake_wait_for(func, *, retries: int, sleep: float):
        wait_calls.append((retries, sleep))
        return True, "ok"

    def fake_http_ok(target: str):
        assert target == url
        return True, "healthy"

    monkeypatch.setattr(start_module, "resolve_rl_health_url", fake_resolve)
    monkeypatch.setattr(start_module, "wait_for", fake_wait_for)
    monkeypatch.setattr(start_module, "http_ok", fake_http_ok)

    result = start_module.rl_health_gate()

    assert result == url
    assert os.environ.get("RL_HEALTH_URL") == url
    assert wait_calls and wait_calls[0][0] == start_module._parse_int_env("RL_HEALTH_RETRIES", 30)
    assert wait_calls[0][1] == start_module._parse_float_env("RL_HEALTH_INTERVAL", 1.0)
