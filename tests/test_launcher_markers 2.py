import importlib
import sys

import pytest


def test_launcher_writes_ok_markers(monkeypatch, tmp_path):
    monkeypatch.setenv("SOLHUNTER_TESTING", "1")
    monkeypatch.setenv("SOLHUNTER_PYTHON", sys.executable)

    tools_marker = tmp_path / "cache" / "tools_ok"
    venv_marker = tmp_path / "cache" / "venv_ok"

    import solhunter_zero.cache_paths as cp
    monkeypatch.setattr(cp, "TOOLS_OK_MARKER", tools_marker)
    monkeypatch.setattr(cp, "VENV_OK_MARKER", venv_marker)

    launcher = importlib.import_module("solhunter_zero.launcher")

    import solhunter_zero.logging_utils as lu
    monkeypatch.setattr(lu, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(lu, "log_startup", lambda *a, **k: None)
    import solhunter_zero.macos_setup as ms
    monkeypatch.setattr(ms, "ensure_tools", lambda **k: None)
    import solhunter_zero.bootstrap_utils as bu
    monkeypatch.setattr(bu, "ensure_venv", lambda *a, **k: None)
    import solhunter_zero.device as device
    monkeypatch.setattr(device, "initialize_gpu", lambda: None)
    import solhunter_zero.system as system
    monkeypatch.setattr(system, "set_rayon_threads", lambda: None)
    import types
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.env_config",
        types.SimpleNamespace(configure_startup_env=lambda root: None),
    )

    def stop(*args, **kwargs):
        raise RuntimeError

    monkeypatch.setattr(launcher.os, "execvp", stop)

    with pytest.raises(RuntimeError):
        launcher.main(["--skip-preflight"], False)

    assert tools_marker.exists() and tools_marker.read_text() == "ok"
    assert venv_marker.exists() and venv_marker.read_text() == "ok"
