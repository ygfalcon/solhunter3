import builtins
import shutil
import subprocess
import sys
import types

import pytest

from scripts import diagnostics, startup
from solhunter_zero import bootstrap, diagnostics as core_diagnostics


def test_collect_no_torch(monkeypatch, tmp_path):
    orig_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "torch":
            raise ImportError
        return orig_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    sys.modules.pop("torch", None)
    monkeypatch.setattr("solhunter_zero.device.get_gpu_backend", lambda: None)
    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        diagnostics,
        "run_preflight",
        lambda: {"successes": [{"name": "ok", "message": "ok"}], "failures": []},
    )

    info = diagnostics.collect()
    assert info["torch"] == "not installed"
    assert info["config"] == "missing"
    assert "python" in info
    assert "gpu_backend" in info


def test_collect_with_torch_and_keypair(monkeypatch, tmp_path):
    cfg = tmp_path / "config.toml"
    cfg.write_text("")
    monkeypatch.chdir(tmp_path)

    dummy_torch = types.SimpleNamespace(__version__="1.0")
    orig_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "torch":
            return dummy_torch
        return orig_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)
    monkeypatch.setattr(
        "solhunter_zero.device.get_gpu_backend", lambda: "torch"
    )
    monkeypatch.setattr(
        "solhunter_zero.device.get_default_device", lambda: "cuda"
    )
    monkeypatch.delenv("SOLHUNTER_GPU_DEVICE", raising=False)
    dummy_wallet = types.SimpleNamespace(
        list_keypairs=lambda: ["a"], get_active_keypair_name=lambda: "a"
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.wallet", dummy_wallet)
    monkeypatch.setattr(sys.modules["solhunter_zero"], "wallet", dummy_wallet, raising=False)

    monkeypatch.setattr(
        diagnostics,
        "run_preflight",
        lambda: {"successes": [{"name": "ok", "message": "ok"}], "failures": []},
    )

    def fake_check_output(cmd, text=True):
        if cmd[0] == "rustc":
            return "rustc 1.70.0"
        if cmd[0] == "cargo":
            return "cargo 1.70.0"
        raise ValueError

    monkeypatch.setattr(subprocess, "check_output", fake_check_output)
    monkeypatch.setattr("shutil.which", lambda cmd: cmd)

    info = diagnostics.collect()
    assert info["torch"] == "1.0"
    assert info["gpu_backend"] == "torch"
    assert info["gpu_device"] == "cuda"
    assert info["config"] == "present"
    assert info["preflight"]["successes"][0]["name"] == "ok"
    assert info["keypair"] == "a"
    assert info["rustc"].startswith("rustc")


def test_startup_diagnostics_flag(capsys):
    code = startup.run(["--diagnostics"])
    out = capsys.readouterr().out.lower()
    assert code == 0
    assert "python" in out


def test_startup_runs_diagnostics_on_failure(monkeypatch, capsys):
    monkeypatch.setattr(startup, "main", lambda args: (_ for _ in ()).throw(SystemExit(2)))
    monkeypatch.setattr("scripts.diagnostics.main", lambda: print("python"))
    code = startup.run(["--one-click"])
    out = capsys.readouterr().out.lower()
    assert code == 2
    assert "python" in out


def _prep_startup(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    monkeypatch.setattr(startup, "ensure_target", lambda name: None)
    monkeypatch.setattr(bootstrap.device, "initialize_gpu", lambda: None)
    monkeypatch.setattr(startup.device, "initialize_gpu", lambda: {"SOLHUNTER_GPU_DEVICE": "cpu"})
    monkeypatch.setattr(startup.device, "detect_gpu", lambda: False)
    monkeypatch.setattr(bootstrap.wallet, "ensure_default_keypair", lambda: None)
    monkeypatch.setattr(bootstrap, "ensure_cargo", lambda: None)
    import scripts.healthcheck as healthcheck
    monkeypatch.setattr(healthcheck, "main", lambda *a, **k: 0)
    dummy_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", dummy_torch)
    monkeypatch.setattr(startup, "torch", dummy_torch, raising=False)


def test_startup_collects_diagnostics(monkeypatch, tmp_path, capsys):
    _prep_startup(monkeypatch, tmp_path)

    called = {}

    def fake_run(cmd):
        called["run"] = True
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(startup.subprocess, "run", fake_run)

    from pathlib import Path

    def fake_write(status):
        called["write"] = status
        Path("diagnostics.json").write_text("{}")

    monkeypatch.setattr(bootstrap, "write_diagnostics", fake_write)

    args = [
        "--skip-deps",
        "--skip-setup",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-preflight",
    ]
    code = startup.run(args)
    out = capsys.readouterr().out
    assert code == 0
    assert called.get("run")
    assert called.get("write") is not None
    assert (tmp_path / "diagnostics.json").is_file()


def test_startup_no_diagnostics_flag(monkeypatch, tmp_path, capsys):
    _prep_startup(monkeypatch, tmp_path)

    called = {}

    def fake_run(cmd):
        return types.SimpleNamespace(returncode=0)

    monkeypatch.setattr(startup.subprocess, "run", fake_run)

    def fake_write(status):
        called["write"] = status

    monkeypatch.setattr(bootstrap, "write_diagnostics", fake_write)

    args = [
        "--skip-deps",
        "--skip-setup",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-preflight",
        "--no-diagnostics",
    ]
    code = startup.run(args)
    out = capsys.readouterr().out
    assert code == 0
    assert not (tmp_path / "diagnostics.json").exists()
    assert "write" not in called


def test_run_preflight(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    from scripts import preflight

    def ok():
        return True, "ok"

    def fail():
        return False, "bad"

    monkeypatch.setattr(preflight, "CHECKS", [("ok", ok), ("fail", fail)])

    result = diagnostics.run_preflight()
    assert result["successes"] == [{"name": "ok", "message": "ok"}]
    assert result["failures"] == [{"name": "fail", "message": "bad"}]


def test_write_diagnostics_tuple_warning(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(core_diagnostics, "ROOT", tmp_path)
    monkeypatch.setattr(core_diagnostics, "log_startup", lambda msg: None)
    status = {"preflight_warnings": [("foo", True, "bar"), ("baz", "qux")]}
    core_diagnostics.write_diagnostics(status)
    import json

    data = json.loads((tmp_path / "diagnostics.json").read_text())
    assert data["preflight_warnings"] == [
        {"name": "foo", "message": "bar"},
        {"name": "baz", "message": "qux"},
    ]
