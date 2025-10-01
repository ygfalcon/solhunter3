import pytest
from scripts import preflight


def test_preflight_logs_write_errors(monkeypatch):
    monkeypatch.setattr(preflight, "CHECKS", [("ok", lambda: (True, "ok"))])
    logged = []

    def fake_log_startup(msg, *args):
        logged.append(msg % args)

    monkeypatch.setattr(preflight, "log_startup", fake_log_startup)

    def fake_open(*args, **kwargs):
        raise OSError("boom")

    monkeypatch.setattr("builtins.open", fake_open)

    preflight.run_preflight()

    assert any("preflight.json write failed" in m for m in logged)
    assert any("preflight.log write failed" in m for m in logged)
