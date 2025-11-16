import subprocess

import pytest

import solhunter_zero.ui as ui


def test_golden_demo_disabled_by_default(monkeypatch):
    monkeypatch.delenv("GOLDEN_DEMO_ENABLED", raising=False)
    monkeypatch.delenv("GOLDEN_DEMO_SECRET", raising=False)

    called = False

    def _fake_run(*_args, **_kwargs):
        nonlocal called
        called = True
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    app = ui.create_app()
    client = app.test_client()
    ui.request.headers = {}
    response = client.post("/ops/golden-demo")

    assert response.status_code == 403
    assert response.get_json()["ok"] is False
    assert called is False


def test_golden_demo_requires_secret(monkeypatch):
    monkeypatch.setenv("GOLDEN_DEMO_ENABLED", "true")
    monkeypatch.setenv("GOLDEN_DEMO_SECRET", "expected-secret")

    called = False

    def _fake_run(*_args, **_kwargs):
        nonlocal called
        called = True
        return subprocess.CompletedProcess([], 0, "", "")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    app = ui.create_app()
    client = app.test_client()
    ui.request.headers = {}
    response = client.post("/ops/golden-demo")

    assert response.status_code == 401
    assert response.headers.get("WWW-Authenticate") == "Bearer"
    assert response.get_json()["ok"] is False
    assert called is False


def test_golden_demo_accepts_authorized_secret(monkeypatch):
    secret = "expected-secret"
    monkeypatch.setenv("GOLDEN_DEMO_ENABLED", "true")
    monkeypatch.setenv("GOLDEN_DEMO_SECRET", secret)

    recorded = {}

    def _fake_run(cmd, check, capture_output, text, timeout):  # type: ignore[override]
        recorded["cmd"] = cmd
        recorded["kwargs"] = {
            "check": check,
            "capture_output": capture_output,
            "text": text,
            "timeout": timeout,
        }
        return subprocess.CompletedProcess(cmd, 0, "ok", "")

    monkeypatch.setattr(subprocess, "run", _fake_run)

    app = ui.create_app()
    client = app.test_client()
    ui.request.headers = {"Authorization": f"Bearer {secret}"}
    response = client.post("/ops/golden-demo")

    data = response.get_json()
    assert response.status_code == 200
    assert data["ok"] is True
    assert recorded["cmd"][0] == "bash"
    assert recorded["kwargs"]["check"] is True
    assert recorded["kwargs"]["capture_output"] is True
