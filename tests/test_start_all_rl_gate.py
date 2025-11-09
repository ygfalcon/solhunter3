from __future__ import annotations

import importlib
import logging
import os


def _reload_rl_gate():
    module = importlib.import_module("solhunter_zero.rl_gate")
    return importlib.reload(module)


def test_rl_health_gate_skips_when_config_disables_rl(monkeypatch, caplog):
    gate_module = _reload_rl_gate()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("RL_WEIGHTS_DISABLED", "0")

    called = {"resolve": False}

    def fake_resolve(*, require_health_file: bool = True):  # pragma: no cover - defensive
        called["resolve"] = True
        return "http://rl:7070/health"

    monkeypatch.setattr(gate_module, "resolve_rl_health_url", fake_resolve)

    caplog.set_level(logging.INFO)

    result = gate_module.rl_health_gate(config={"use_rl_weights": False})

    assert result.skipped
    assert result.skip_reason == "config.use_rl_weights disabled"
    assert not called["resolve"]
    assert any("Skipping RL health gate" in record.message for record in caplog.records)


def test_rl_health_gate_waits_for_health_when_enabled(monkeypatch):
    gate_module = _reload_rl_gate()

    monkeypatch.setenv("MODE", "live")
    monkeypatch.setenv("RL_WEIGHTS_DISABLED", "0")

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

    monkeypatch.setattr(gate_module, "resolve_rl_health_url", fake_resolve)
    monkeypatch.setattr(gate_module, "wait_for", fake_wait_for)
    monkeypatch.setattr(gate_module, "http_ok", fake_http_ok)

    result = gate_module.rl_health_gate(config={"use_rl_weights": True})

    assert not result.skipped
    assert result.url == url
    assert os.environ.get("RL_HEALTH_URL") == url
    retries = gate_module._parse_positive_int_env("RL_HEALTH_RETRIES", 30)
    interval = gate_module._parse_positive_float_env("RL_HEALTH_INTERVAL", 1.0)
    assert wait_calls and wait_calls[0][0] == retries
    assert wait_calls[0][1] == interval


def test_rl_health_gate_bypass(monkeypatch, caplog):
    gate_module = _reload_rl_gate()

    monkeypatch.setenv("RL_HEALTH_BYPASS", "1")

    called = {"resolve": False}

    def fake_resolve(*, require_health_file: bool = True):  # pragma: no cover - defensive
        called["resolve"] = True
        return "http://rl:7070/health"

    monkeypatch.setattr(gate_module, "resolve_rl_health_url", fake_resolve)

    caplog.set_level(logging.INFO)

    result = gate_module.rl_health_gate(config={"use_rl_weights": True})

    assert result.skipped
    assert result.skip_reason == "RL_HEALTH_BYPASS=1"
    assert not called["resolve"]
    assert any("Bypassing RL health gate" in record.message for record in caplog.records)
