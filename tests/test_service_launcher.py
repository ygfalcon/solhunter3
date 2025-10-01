from __future__ import annotations

import subprocess

from solhunter_zero import service_launcher as sl


def test_start_depth_service_installs_rustup(monkeypatch, tmp_path):
    marker = tmp_path / "cargo-installed"
    monkeypatch.setattr(sl, "CARGO_MARKER", marker)
    cargo_installed = {"ok": False}

    def fake_which(cmd: str):
        if cmd == "cargo" and not cargo_installed["ok"]:
            return None
        return f"/usr/bin/{cmd}"

    run_calls: list[list[str]] = []

    def fake_run(cmd, check=False, **kwargs):
        run_calls.append(cmd)
        if cmd[0] == "rustup-init":
            cargo_installed["ok"] = True
        if cmd[0] == "cargo" and "build" in cmd:
            depth_bin = tmp_path / "target" / "release" / "depth_service"
            depth_bin.parent.mkdir(parents=True, exist_ok=True)
            depth_bin.write_text("bin")
        return subprocess.CompletedProcess(cmd, 0)

    depth_bin = tmp_path / "target" / "release" / "depth_service"
    monkeypatch.setattr(sl.shutil, "which", fake_which)
    monkeypatch.setattr(sl.subprocess, "run", fake_run)
    monkeypatch.setattr(sl, "ROOT", tmp_path)
    monkeypatch.setattr(sl.os, "access", lambda p, m: depth_bin.exists())

    class DummyPopen:
        def __init__(self, *a, **k):
            self.args = a
            self.stderr = None
        def poll(self):
            return None
    monkeypatch.setattr(sl.subprocess, "Popen", DummyPopen)

    sl.start_depth_service()
    assert ["rustup-init", "-y"] in run_calls
    assert marker.exists()

    run_calls.clear()
    sl.start_depth_service()
    assert ["rustup-init", "-y"] not in run_calls

