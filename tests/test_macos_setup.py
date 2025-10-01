import platform
import types

import solhunter_zero.macos_setup as ms


def test_ensure_tools_skips_when_marker_exists(monkeypatch, tmp_path):
    marker = tmp_path / ".cache" / "macos_tools_ok"
    marker.parent.mkdir(parents=True)
    marker.write_text("ok")
    monkeypatch.setattr(ms, "TOOLS_OK_MARKER", marker)
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    monkeypatch.setattr(ms.shutil, "which", lambda cmd: "/usr/bin/fake")

    class DummyCompleted:
        def __init__(self, returncode=0):
            self.returncode = returncode

    monkeypatch.setattr(ms.subprocess, "run", lambda *a, **k: DummyCompleted(0))

    called = {"value": False}

    def fake_prepare(**kwargs):
        called["value"] = True
        return {"steps": {}, "success": True}

    monkeypatch.setattr(ms, "prepare_macos_env", fake_prepare)
    monkeypatch.setattr(ms, "mac_setup_completed", lambda: False)

    report = ms.ensure_tools()
    assert report == {"steps": {}, "success": True, "missing": []}
    assert not called["value"]


def test_ensure_tools_runs_setup_and_marks(monkeypatch, tmp_path):
    marker = tmp_path / ".cache" / "macos_tools_ok"
    monkeypatch.setattr(ms, "TOOLS_OK_MARKER", marker)
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    monkeypatch.setattr(ms.shutil, "which", lambda cmd: "/usr/bin/fake")

    calls = {"count": 0}

    def fake_run(*args, **kwargs):
        calls["count"] += 1
        rc = 1 if calls["count"] == 1 else 0
        return types.SimpleNamespace(returncode=rc)

    monkeypatch.setattr(ms.subprocess, "run", fake_run)

    def fake_prepare(**kwargs):
        return {"steps": {}, "success": True}

    monkeypatch.setattr(ms, "prepare_macos_env", fake_prepare)
    monkeypatch.setattr(ms, "mac_setup_completed", lambda: False)

    report = ms.ensure_tools()
    assert calls["count"] >= 2
    assert report["success"] is True
    assert report["missing"] == []
    assert marker.exists()


def test_prepare_macos_env_auto_fix(monkeypatch, tmp_path):
    marker = tmp_path / "marker"
    monkeypatch.setattr(ms, "MAC_SETUP_MARKER", marker)
    monkeypatch.setattr(ms, "REPORT_PATH", tmp_path / "report.json")
    monkeypatch.setattr(ms, "ensure_network", lambda: None)
    monkeypatch.setattr(ms, "ensure_xcode", lambda non_interactive: None)
    monkeypatch.setattr(ms, "install_brew_packages", lambda: None)
    monkeypatch.setattr(ms, "ensure_rustup", lambda: None)
    monkeypatch.setattr(ms, "upgrade_pip_and_torch", lambda: None)
    monkeypatch.setattr(ms, "verify_tools", lambda: None)
    monkeypatch.setattr(ms, "install_deps", lambda: None)
    monkeypatch.setattr(ms, "ensure_profile", lambda: None)

    calls = {"func": 0, "fix": 0}

    def fail_then_pass():
        calls["func"] += 1
        if calls["func"] == 1:
            raise RuntimeError("fail")

    def fix_homebrew():
        calls["fix"] += 1

    monkeypatch.setattr(ms, "ensure_homebrew", fail_then_pass)
    monkeypatch.setitem(ms.AUTO_FIXES, "homebrew", fix_homebrew)

    report = ms.prepare_macos_env()
    assert report["steps"]["homebrew"]["status"] == "ok"
    assert report["steps"]["homebrew"].get("fixed")
    assert calls == {"func": 2, "fix": 1}


def test_prepare_macos_env_fix_failure(monkeypatch, tmp_path):
    marker = tmp_path / "marker"
    monkeypatch.setattr(ms, "MAC_SETUP_MARKER", marker)
    monkeypatch.setattr(ms, "REPORT_PATH", tmp_path / "report.json")
    monkeypatch.setattr(ms, "ensure_network", lambda: None)
    monkeypatch.setattr(ms, "ensure_xcode", lambda non_interactive: None)
    monkeypatch.setattr(ms, "install_brew_packages", lambda: None)
    monkeypatch.setattr(ms, "ensure_rustup", lambda: None)
    monkeypatch.setattr(ms, "upgrade_pip_and_torch", lambda: None)
    monkeypatch.setattr(ms, "verify_tools", lambda: None)
    monkeypatch.setattr(ms, "install_deps", lambda: None)
    monkeypatch.setattr(ms, "ensure_profile", lambda: None)

    def fail():
        raise RuntimeError("boom")

    monkeypatch.setattr(ms, "ensure_homebrew", fail)
    monkeypatch.setitem(ms.AUTO_FIXES, "homebrew", fail)

    report = ms.prepare_macos_env()
    assert report["success"] is False
    assert report["steps"]["homebrew"]["status"] == "error"
    assert report["steps"]["brew_packages"]["status"] == "skipped"
