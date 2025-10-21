import solhunter_zero.health_runtime as hr


def _stub_health_payload(**overrides):
    base = {
        "ui": {"ws_clients": {"events": 2, "logs": 1, "rl": 0}},
        "heartbeat": {"age": 3.0, "threshold": 10.0},
        "queues": {"execution_queue": 5},
    }
    for key, value in overrides.items():
        base[key] = value
    return base


def test_check_ui_websockets_ok(monkeypatch):
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: _stub_health_payload(),
    )
    ok, msg = hr.check_ui_websockets("http://example.com", min_clients=1)
    assert ok is True
    assert "events=2" in msg


def test_check_ui_websockets_missing_channel(monkeypatch):
    payload = _stub_health_payload()
    payload["ui"]["ws_clients"].pop("rl")
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: payload,
    )
    ok, msg = hr.check_ui_websockets("http://example.com")
    assert ok is False
    assert "missing channels" in msg


def test_check_agent_loop_ok(monkeypatch):
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: _stub_health_payload(),
    )
    ok, msg = hr.check_agent_loop("http://example.com", max_age=5.0)
    assert ok is True
    assert "age=" in msg


def test_check_agent_loop_stale(monkeypatch):
    payload = _stub_health_payload()
    payload["heartbeat"]["age"] = 120.0
    payload["heartbeat"]["threshold"] = 30.0
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: payload,
    )
    ok, msg = hr.check_agent_loop("http://example.com", max_age=10.0)
    assert ok is False
    assert "age=120.00s" in msg


def test_check_execution_queue_ok(monkeypatch):
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: _stub_health_payload(),
    )
    ok, msg = hr.check_execution_queue("http://example.com", max_depth=10)
    assert ok is True
    assert msg == "depth=5"


def test_check_execution_queue_exceeds(monkeypatch):
    payload = _stub_health_payload()
    payload["queues"]["execution_queue"] = 250
    monkeypatch.setattr(
        hr,
        "_fetch_runtime_health",
        lambda base_url, timeout=1.0: payload,
    )
    ok, msg = hr.check_execution_queue("http://example.com", max_depth=200)
    assert ok is False
    assert "depth=250" in msg
