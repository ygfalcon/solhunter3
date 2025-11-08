import importlib

import pytest


@pytest.fixture
def start_all_module():
    module = importlib.import_module("scripts.start_all")
    return module


def test_kill_lingering_processes_runs_when_pkill_available(monkeypatch, caplog, start_all_module):
    caplog.set_level("INFO")
    expected_path = "/usr/bin/pkill"
    monkeypatch.setattr(start_all_module.shutil, "which", lambda name: expected_path if name == "pkill" else None)

    calls = []

    def fake_run(cmd, **kwargs):
        calls.append(cmd)
        return None

    monkeypatch.setattr(start_all_module.subprocess, "run", fake_run)
    monkeypatch.setattr(start_all_module.time, "sleep", lambda *_: None)

    start_all_module.kill_lingering_processes()

    patterns = [
        "solhunter_zero.primary_entry_point",
        "solhunter_zero.runtime.launch",
        "depth_service",
        "run_rl_daemon.py",
    ]
    assert calls == [[expected_path, "-f", pat] for pat in patterns]


def test_kill_lingering_processes_skips_when_pkill_missing(monkeypatch, caplog, start_all_module):
    caplog.set_level("INFO")
    monkeypatch.setattr(start_all_module.shutil, "which", lambda name: None)

    def fail_run(*args, **kwargs):  # pragma: no cover - should not be called
        raise AssertionError("subprocess.run should not be invoked when pkill is unavailable")

    monkeypatch.setattr(start_all_module.subprocess, "run", fail_run)
    monkeypatch.setattr(start_all_module.time, "sleep", lambda *_: None)

    start_all_module.kill_lingering_processes()

    assert any(
        record.levelname == "WARNING" and "pkill" in record.getMessage()
        for record in caplog.records
    )
