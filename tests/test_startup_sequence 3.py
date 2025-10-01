import logging
import os
import sys
import types

import pytest


def test_startup_sequence(monkeypatch, caplog):
    caplog.set_level(logging.INFO)

    dummy_pydantic = types.SimpleNamespace(
        BaseModel=object,
        AnyUrl=str,
        ValidationError=Exception,
        root_validator=lambda *a, **k: (lambda f: f),
        validator=lambda *a, **k: (lambda f: f),
        field_validator=lambda *a, **k: (lambda f: f),
        model_validator=lambda *a, **k: (lambda f: f),
    )
    monkeypatch.setitem(sys.modules, "pydantic", dummy_pydantic)

    class _Console:
        def print(self, *a, **k):
            pass

    panel_mod = types.ModuleType("rich.panel")
    panel_mod.Panel = type("Panel", (), {"fit": staticmethod(lambda x: x)})
    console_mod = types.ModuleType("rich.console")
    console_mod.Console = _Console
    progress_mod = types.ModuleType("rich.progress")
    progress_mod.Progress = type("Progress", (), {})
    table_mod = types.ModuleType("rich.table")
    table_mod.Table = type("Table", (), {"add_column": lambda *a, **k: None, "add_row": lambda *a, **k: None})
    monkeypatch.setitem(sys.modules, "rich.panel", panel_mod)
    monkeypatch.setitem(sys.modules, "rich.console", console_mod)
    monkeypatch.setitem(sys.modules, "rich.progress", progress_mod)
    monkeypatch.setitem(sys.modules, "rich.table", table_mod)
    monkeypatch.setitem(
        sys.modules,
        "rich",
        types.SimpleNamespace(panel=panel_mod, console=console_mod, progress=progress_mod, table=table_mod),
    )

    import solhunter_zero.python_env as python_env
    monkeypatch.setattr(python_env, "find_python", lambda repair=False: sys.executable)

    import solhunter_zero.config as config_mod
    monkeypatch.setattr(config_mod, "load_config", lambda *a, **k: {})

    import solhunter_zero.logging_utils as logging_utils
    monkeypatch.setattr(logging_utils, "setup_logging", lambda *a, **k: None)
    monkeypatch.setattr(logging_utils, "log_startup", lambda msg: logging.getLogger().info(msg))

    startup_checks = types.ModuleType("solhunter_zero.startup_checks")
    def fake_perform_checks(args, rest, **kwargs):
        return {"code": 0, "rest": rest}
    startup_checks.perform_checks = fake_perform_checks
    startup_checks.ensure_target = lambda *a, **k: None
    startup_checks.ensure_wallet_cli = lambda *a, **k: None
    startup_checks.run_quick_setup = lambda *a, **k: None
    startup_checks.ensure_cargo = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.startup_checks", startup_checks)

    preflight_stub = types.ModuleType("scripts.preflight")
    preflight_stub.CHECKS = []
    monkeypatch.setitem(sys.modules, "scripts.preflight", preflight_stub)

    import solhunter_zero.macos_setup as macos_setup
    import solhunter_zero.bootstrap_utils as bootstrap_utils
    import solhunter_zero.env_config as env_config
    import solhunter_zero.device as device
    import solhunter_zero.system as system
    import solhunter_zero.startup_runner as startup_runner
    import solhunter_zero.startup_cli as startup_cli
    import solhunter_zero.launcher as launcher

    monkeypatch.setattr(launcher, "write_ok_marker", lambda path: None)

    def log(msg: str) -> None:
        logging.getLogger().info(msg)

    monkeypatch.setattr(macos_setup, "ensure_tools", lambda *a, **k: log("tools check"))
    monkeypatch.setattr(bootstrap_utils, "ensure_venv", lambda arg: log("venv setup"))
    monkeypatch.setattr(env_config, "configure_startup_env", lambda root: log("env config"))
    monkeypatch.setattr(launcher, "set_rayon_threads", lambda: log("Rayon threads"))
    monkeypatch.setattr(device, "initialize_gpu", lambda: log("GPU init"))

    monkeypatch.setattr(startup_cli.console, "print", lambda *a, **k: None)

    def stub_run(args, ctx, log_startup=None):
        cmd = [sys.executable, "scripts/start_all.py"]
        if getattr(args, "one_click", False):
            cmd.append("--one-click")
        if getattr(args, "full_deps", False):
            cmd.append("--full-deps")
        cmd.extend(ctx.get("rest", []))
        stub_run.command = cmd
        log("first trade")
        return 0

    monkeypatch.setattr(startup_runner, "run", stub_run)

    captured = {}

    def fake_execvp(prog, argv):
        captured["argv"] = argv
        log("startup launch")
        from scripts import startup as startup_mod
        code = startup_mod.run(argv[3:])
        raise SystemExit(code)

    monkeypatch.setattr(os, "execvp", fake_execvp)

    with pytest.raises(SystemExit) as exc:
        launcher.main([], False)
    assert exc.value.code == 0

    messages = [rec.getMessage() for rec in caplog.records]
    expected = [
        "tools check",
        "venv setup",
        "env config",
        "Rayon threads",
        "GPU init",
        "startup launch",
        "first trade",
    ]
    for msg in expected:
        assert msg in messages
    positions = [messages.index(msg) for msg in expected]
    assert positions == sorted(positions)

    cmd = stub_run.command
    assert cmd[1] == "scripts/start_all.py"
    assert "--one-click" in cmd
    assert "--full-deps" in cmd
