import asyncio
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

os.environ.setdefault("TORCH_METAL_VERSION", "2.8.0")
os.environ.setdefault("TORCHVISION_METAL_VERSION", "0.23.0")

from solhunter_zero.device import METAL_EXTRA_INDEX, load_torch_metal_versions

TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION = load_torch_metal_versions()


class DummyTask:
    def __init__(self, description: str):
        self.description = description


class DummyProgress:
    def __init__(self, *a, **k):
        self._tasks = {}
        self._next = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def add_task(self, description, total=1):
        tid = self._next
        self._next += 1
        self._tasks[tid] = DummyTask(description)
        return tid

    def advance(self, task_id, advance=1):
        pass

    def update(self, task_id, description=None, advance=0):
        if description:
            self._tasks[task_id].description = description

    @property
    def tasks(self):
        return self._tasks


@pytest.fixture
def startup_stubs(monkeypatch, tmp_path):
    import types
    import sys
    import os

    monkeypatch.setitem(
        sys.modules,
        "scripts.preflight",
        types.SimpleNamespace(run_preflight=lambda: []),
    )
    monkeypatch.setitem(
        sys.modules,
        "scripts.deps",
        types.SimpleNamespace(check_deps=lambda: ([], [])),
    )
    monkeypatch.setitem(
        sys.modules,
        "scripts.quick_setup",
        types.SimpleNamespace(_is_placeholder=lambda v: False),
    )

    monkeypatch.setenv("KEYPAIR_DIR", str(tmp_path))
    from solhunter_zero import wallet as wallet_mod

    os.makedirs(wallet_mod.KEYPAIR_DIR, exist_ok=True)
    monkeypatch.setattr(
        wallet_mod,
        "ACTIVE_KEYPAIR_FILE",
        str(tmp_path / "active"),
        raising=False,
    )


def test_startup_passes_extra_rest_to_start_all(monkeypatch):
    import types

    from scripts import startup as startup_mod
    from solhunter_zero import startup_checks
    import solhunter_zero.startup_runner as startup_runner
    import scripts.start_all as start_all_module
    import solhunter_zero.logging_utils as logging_utils

    monkeypatch.setattr(startup_checks, "ensure_rpc", lambda warn_only=True: None)
    monkeypatch.setattr(startup_checks, "ensure_endpoints", lambda: None)

    monkeypatch.setattr(logging_utils, "log_startup", lambda *a, **k: None)
    monkeypatch.setattr(logging_utils, "rotate_preflight_log", lambda: None)
    monkeypatch.setattr(startup_mod.startup_cli, "render_banner", lambda: None)

    extra_rest = ["--ui-port", "6000"]

    def fake_parse_args(argv):
        args = types.SimpleNamespace(
            quiet=False,
            non_interactive=False,
            skip_deps=False,
            skip_setup=False,
            skip_rpc_check=False,
            skip_endpoint_check=False,
            offline=False,
            skip_preflight=False,
        )
        return args, list(extra_rest)

    monkeypatch.setattr(startup_mod.startup_cli, "parse_args", fake_parse_args)

    captured_ctx: dict[str, dict] = {}

    def fake_run(args, ctx, log_startup=None):
        captured_ctx["ctx"] = ctx
        rest_args = list(ctx.get("rest", []))
        return start_all_module.main([*rest_args, "--foreground"])

    monkeypatch.setattr(startup_runner, "run", fake_run)

    captured_parse: dict[str, list[str]] = {}

    def fake_start_all_parse_args(argv):
        captured_parse["parse_args"] = list(argv)
        return types.SimpleNamespace()

    def fake_start_all_main(argv):
        captured_parse["main_args"] = list(argv)
        fake_start_all_parse_args(argv)
        return 0

    monkeypatch.setattr(start_all_module, "parse_args", fake_start_all_parse_args)
    monkeypatch.setattr(start_all_module, "main", fake_start_all_main)

    exit_code = startup_mod.main(["--ui-port", "6000"])

    assert exit_code == 0
    assert captured_parse["parse_args"] == ["--ui-port", "6000", "--foreground"]
    assert captured_parse["main_args"] == ["--ui-port", "6000", "--foreground"]

    ctx = captured_ctx["ctx"]
    assert isinstance(ctx.get("rest"), list)
    assert ctx["rest"] == extra_rest
    assert ctx.get("summary_rows") == []


def test_startup_help():
    result = subprocess.run([sys.executable, 'scripts/startup.py', '--help'], capture_output=True, text=True)
    assert result.returncode == 0
    out = result.stdout.lower() + result.stderr.lower()
    assert 'usage' in out


def test_check_disk_space(startup_stubs, monkeypatch):
    import types
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "Progress", DummyProgress)
    monkeypatch.setattr(sc, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(sc.preflight_utils, "check_disk_space", lambda req: (True, "ok"))
    assert sc.check_disk_space(1, lambda m: None) == "passed"
    monkeypatch.setattr(sc.preflight_utils, "check_disk_space", lambda req: (False, "bad"))
    with pytest.raises(SystemExit):
        sc.check_disk_space(1, lambda m: None)


def test_check_network(startup_stubs, monkeypatch):
    import types
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(sc.preflight_utils, "check_internet", lambda: (True, "ok"))
    args = SimpleNamespace(offline=False, skip_rpc_check=False)
    assert sc.check_network(args, lambda m: None) == "passed"
    args = SimpleNamespace(offline=True, skip_rpc_check=False)
    assert sc.check_network(args, lambda m: None) == "skipped"
    args = SimpleNamespace(offline=False, skip_rpc_check=False)
    monkeypatch.setattr(sc.preflight_utils, "check_internet", lambda: (False, "bad"))
    with pytest.raises(SystemExit):
        sc.check_network(args, lambda m: None)


def test_disk_and_network_checks(startup_stubs, monkeypatch):
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "_disk_space_required_bytes", lambda a, b: 5)
    monkeypatch.setattr(sc, "check_disk_space", lambda req, log: "disk")
    monkeypatch.setattr(sc, "check_network", lambda args, log: "net")

    args = SimpleNamespace()
    result = sc.disk_and_network_checks(args, lambda m: None, lambda c: c, lambda: {})
    assert result == (5, "disk", "net")


def test_ensure_configuration_and_wallet(startup_stubs, monkeypatch, tmp_path):
    import types
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(sc, "Progress", DummyProgress)
    cfg_path = tmp_path / "cfg.json"

    def fake_ensure_config(path=None):
        return cfg_path, {}

    def fake_select_active_keypair(auto=False):
        return SimpleNamespace(name="kp", mnemonic_path=tmp_path / "mn")

    dummy_wallet = SimpleNamespace(KEYPAIR_DIR=str(tmp_path))
    monkeypatch.setattr("solhunter_zero.config_bootstrap.ensure_config", fake_ensure_config)
    monkeypatch.setattr("solhunter_zero.config_utils.select_active_keypair", fake_select_active_keypair)
    monkeypatch.setitem(sys.modules, "solhunter_zero.wallet", dummy_wallet)

    args = SimpleNamespace(skip_setup=False, one_click=False)
    result = sc.ensure_configuration_and_wallet(args, lambda: None, lambda: str(cfg_path))
    assert result[5] == str(cfg_path)
    assert result[6] == "kp"

    args = SimpleNamespace(skip_setup=True, one_click=False)
    result = sc.ensure_configuration_and_wallet(args, lambda: None, lambda: None)
    assert result[5] == "skipped"
    assert result[6] == "skipped"


def test_setup_configuration(startup_stubs, monkeypatch, tmp_path):
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    cfg_path = tmp_path / "cfg.json"
    monkeypatch.setattr(
        sc,
        "ensure_configuration_and_wallet",
        lambda *a: (cfg_path, {}, Path("kp"), Path("mn"), "ak", "cfg", "wallet"),
    )
    monkeypatch.setattr(sc, "check_endpoints", lambda *a: "ep")

    result = sc.setup_configuration(SimpleNamespace(), lambda: None, lambda: None, lambda cfg: None)
    assert result[-1] == "ep"
    assert result[0] == cfg_path


def test_check_endpoints(startup_stubs, monkeypatch):
    import types
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(sc, "Progress", DummyProgress)
    args = SimpleNamespace(offline=True, skip_endpoint_check=False, skip_setup=False)
    assert sc.check_endpoints(args, {}, lambda cfg: None) == "offline"
    args = SimpleNamespace(offline=False, skip_endpoint_check=True, skip_setup=False)
    assert sc.check_endpoints(args, {}, lambda cfg: None) == "skipped"
    args = SimpleNamespace(offline=False, skip_endpoint_check=False, skip_setup=False)
    assert sc.check_endpoints(args, {}, lambda cfg: None) == "reachable"


def test_install_dependencies(startup_stubs, monkeypatch):
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc, "Progress", DummyProgress)
    calls = []

    def fake_ensure_deps(install_optional=False):
        calls.append("deps")

    def fake_ensure_target(name):
        calls.append(name)

    sc.install_dependencies(SimpleNamespace(skip_deps=False, full_deps=False), fake_ensure_deps, fake_ensure_target)
    assert set(calls) == {"deps", "protos", "route_ffi", "depth_service"}
    calls.clear()
    sc.install_dependencies(SimpleNamespace(skip_deps=True, full_deps=False), fake_ensure_deps, fake_ensure_target)
    assert calls == []


def test_perform_preflight(startup_stubs, monkeypatch):
    from types import SimpleNamespace
    from solhunter_zero import startup_checks as sc

    monkeypatch.setattr(sc.preflight, "run_preflight", lambda: [("a", True, "ok")])
    sc.perform_preflight(SimpleNamespace(skip_preflight=False), lambda m: None)
    monkeypatch.setattr(sc.preflight, "run_preflight", lambda: [("a", False, "bad")])
    with pytest.raises(SystemExit):
        sc.perform_preflight(SimpleNamespace(skip_preflight=False), lambda m: None)
    called = {"called": False}

    def fake_run():
        called["called"] = True
        return []

    monkeypatch.setattr(sc.preflight, "run_preflight", fake_run)
    sc.perform_preflight(SimpleNamespace(skip_preflight=True), lambda m: None)
    assert called["called"] is False


def test_perform_bootstrap(monkeypatch):
    from types import SimpleNamespace, ModuleType
    from solhunter_zero import startup_checks as sc

    monkeypatch.setenv("SOLHUNTER_GPU_DEVICE", "gpu")
    monkeypatch.setenv("SOLANA_RPC_URL", "url")

    calls = {}

    def fake_rpc(warn_only=False):
        calls["rpc"] = warn_only

    def fake_bootstrap(one_click=False):
        calls["bootstrap"] = one_click

    def fake_cargo():
        calls["cargo"] = True

    dummy_bootstrap_mod = ModuleType("solhunter_zero.bootstrap")
    dummy_bootstrap_mod.bootstrap = fake_bootstrap
    monkeypatch.setitem(sys.modules, "solhunter_zero.bootstrap", dummy_bootstrap_mod)

    args = SimpleNamespace(offline=False, skip_rpc_check=False, one_click=True)
    gpu, status, url = sc.perform_bootstrap(args, fake_rpc, fake_cargo, lambda m: None)
    assert calls == {"rpc": True, "bootstrap": True, "cargo": True}
    assert status == "reachable"
    assert gpu == "gpu"
    assert url == "url"


def test_build_summary():
    from solhunter_zero import startup_checks as sc

    rows = sc.build_summary("d", "i", "c", "w", "e")
    assert rows[0] == ("Disk space", "d")


def test_startup_task_failure(monkeypatch, capsys):
    import types, sys
    from pathlib import Path

    dummy_wallet = types.SimpleNamespace(
        KEYPAIR_DIR=str(Path(".")),
        setup_default_keypair=lambda: types.SimpleNamespace(name="test", mnemonic_path=Path("mn")),
        KeypairInfo=types.SimpleNamespace,
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.wallet", dummy_wallet)
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

    from scripts import startup
    from solhunter_zero import preflight_utils

    monkeypatch.setattr(preflight_utils, "check_disk_space", lambda *a, **k: (True, "ok"))
    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_protos", lambda: None)
    monkeypatch.setattr(startup, "ensure_depth_service", lambda: None)

    def boom():
        raise RuntimeError("boom")

    monkeypatch.setattr(startup, "ensure_route_ffi", boom)

    code = startup.main([
        "--skip-setup",
        "--skip-preflight",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--no-diagnostics",
    ])

    out = capsys.readouterr().out
    assert code == 1
    assert "Building route FFI" in out
    assert "failed" in out.lower()


def test_startup_passthrough_args(monkeypatch, tmp_path):
    import types
    import sys

    from scripts import startup
    import solhunter_zero.startup_runner as startup_runner

    captured: dict[str, list[str]] = {}

    start_all_mod = types.ModuleType("scripts.start_all")

    def fake_start_all_main(args):
        captured["args"] = list(args)
        return 0

    start_all_mod.main = fake_start_all_main
    healthcheck_mod = types.ModuleType("scripts.healthcheck")
    healthcheck_mod.main = lambda *a, **k: 0

    monkeypatch.setitem(sys.modules, "scripts.start_all", start_all_mod)
    monkeypatch.setitem(sys.modules, "scripts.healthcheck", healthcheck_mod)
    monkeypatch.setattr(startup_runner, "preflight", types.SimpleNamespace(CHECKS=[]))

    class DummyAgentManager:
        @classmethod
        def from_config(cls, config):
            return cls()

    agent_manager_mod = types.ModuleType("solhunter_zero.agent_manager")
    agent_manager_mod.AgentManager = DummyAgentManager
    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", agent_manager_mod)
    monkeypatch.setattr(startup_runner, "log_startup_info", lambda **kwargs: None)
    monkeypatch.setattr(startup_runner, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(startup_runner, "log_startup", lambda msg: None)
    monkeypatch.setattr(startup_runner, "STARTUP_LOG", tmp_path / "startup.log")
    monkeypatch.setattr("solhunter_zero.config.find_config_file", lambda: str(tmp_path / "config.toml"))
    monkeypatch.setattr("solhunter_zero.config.load_config", lambda path: {})
    monkeypatch.setattr("solhunter_zero.logging_utils.log_startup", lambda msg: None)
    monkeypatch.setattr("solhunter_zero.logging_utils.rotate_preflight_log", lambda: None)

    code = startup.main(
        [
            "--skip-preflight",
            "--skip-rpc-check",
            "--skip-endpoint-check",
            "--skip-setup",
            "--skip-deps",
            "--offline",
            "--no-diagnostics",
            "--user-flag",
        ]
    )

    assert code == 0
    assert captured.get("args") == ["--user-flag", "--foreground"]


def test_startup_repair_clears_markers(monkeypatch, capsys):
    import platform
    import types, sys
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

    dummy_rich = types.ModuleType("rich")
    dummy_console_mod = types.ModuleType("rich.console")
    dummy_console_mod.Console = lambda *a, **k: types.SimpleNamespace(print=lambda *a, **k: None)
    dummy_rich.console = dummy_console_mod
    monkeypatch.setitem(sys.modules, "rich", dummy_rich)
    monkeypatch.setitem(sys.modules, "rich.console", dummy_console_mod)
    from scripts import startup

    monkeypatch.setattr(startup.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(startup.platform, "machine", lambda: "arm64")

    cargo_marker = startup.ROOT / ".cache" / "cargo-installed"
    cargo_marker.parent.mkdir(parents=True, exist_ok=True)
    cargo_marker.write_text("ok")

    deps_marker = startup.ROOT / ".cache" / "deps-installed"
    deps_marker.parent.mkdir(parents=True, exist_ok=True)
    deps_marker.write_text("ok")

    from solhunter_zero import device

    device.MPS_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
    device.MPS_SENTINEL.write_text("ok")

    called = {}

    def fake_prepare(non_interactive=True):
        called["called"] = True
        return {
            "success": False,
            "steps": {"xcode": {"status": "error", "message": "boom"}}
        }
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", fake_prepare
    )
    monkeypatch.setattr("solhunter_zero.bootstrap.bootstrap", lambda one_click: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    def fake_gpu_env():
        os.environ["SOLHUNTER_GPU_AVAILABLE"] = "0"
        os.environ["SOLHUNTER_GPU_DEVICE"] = "cpu"
        os.environ["TORCH_DEVICE"] = "cpu"
        return {
            "SOLHUNTER_GPU_AVAILABLE": "0",
            "SOLHUNTER_GPU_DEVICE": "cpu",
            "TORCH_DEVICE": "cpu",
        }

    monkeypatch.setattr(startup.device, "initialize_gpu", fake_gpu_env)
    monkeypatch.setattr(startup.device, "get_default_device", lambda: "cpu")
    from scripts import preflight as preflight_mod
    monkeypatch.setattr(preflight_mod, "check_internet", lambda: (True, "ok"))
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0))

    code = startup.main([
        "--repair",
        "--skip-preflight",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-setup",
        "--skip-deps",
        "--no-diagnostics",
    ])
    assert code == 0
    out = capsys.readouterr().out
    assert "Manual fix for xcode" in out
    assert called["called"]
    assert not cargo_marker.exists()
    assert not deps_marker.exists()
    assert not device.MPS_SENTINEL.exists()


def test_mac_startup_prereqs(monkeypatch):
    """Mac-specific startup helpers run without errors."""
    import platform
    import types, sys
    from scripts import startup
    from solhunter_zero import bootstrap
    from solhunter_zero.bootstrap_utils import ensure_venv

    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("PYTORCH_ENABLE_MPS_FALLBACK", raising=False)

    # ensure_venv is a no-op when argv is provided
    ensure_venv([])

    monkeypatch.setattr(startup.deps, "check_deps", lambda: ([], []))
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.ensure_tools", lambda: {"success": True}
    )
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", lambda non_interactive=True: {"success": True}
    )
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)
    startup.ensure_deps(ensure_wallet_cli=False)

    dummy_torch = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: False),
    )
    monkeypatch.setattr(bootstrap.device, "torch", dummy_torch)
    monkeypatch.setitem(sys.modules, "torch", dummy_torch)

    env = bootstrap.device.ensure_gpu_env()
    assert env.get("TORCH_DEVICE") == "mps"
    assert env.get("PYTORCH_ENABLE_MPS_FALLBACK") == "1"


def test_launcher_sets_rayon_threads_on_darwin(tmp_path):
    repo_root = Path(__file__).resolve().parent.parent
    bindir = tmp_path / "bin"
    bindir.mkdir()

    (bindir / "uname").write_text("#!/bin/bash\necho Darwin\n")
    os.chmod(bindir / "uname", 0o755)

    (bindir / "arch").write_text("#!/bin/bash\nshift\n\"$@\"\n")
    os.chmod(bindir / "arch", 0o755)

    venv = repo_root / ".venv"
    bin_dir = venv / "bin"
    bin_dir.mkdir(parents=True, exist_ok=True)
    try:
        stub = bin_dir / "python3"
        stub.write_text(
            "#!/bin/bash\n"
            "if [ \"$1\" = '-c' ]; then\n"
            f"  echo {sys.version_info.major}.{sys.version_info.minor}\n"
            "  exit 0\n"
            "fi\n"
            "echo RAYON_NUM_THREADS=$RAYON_NUM_THREADS\n"
        )
        os.chmod(stub, 0o755)

        env = {
            **os.environ,
            "PATH": f"{bindir}{os.pathsep}{os.environ['PATH']}",
            "SOLHUNTER_PYTHON": str(stub),
            "SOLHUNTER_TESTING": "1",
            "PYTHONPATH": str(repo_root),
        }
        env.pop("RAYON_NUM_THREADS", None)

        result = subprocess.run(
            [sys.executable, "scripts/launcher.py", "--skip-preflight"],
            cwd=repo_root,
            env=env,
            capture_output=True,
            text=True,
        )
    finally:
        shutil.rmtree(venv)

    assert result.stdout.startswith("RAYON_NUM_THREADS=")


def test_launcher_injects_one_click_once(monkeypatch):
    import os, sys, importlib

    monkeypatch.setenv("SOLHUNTER_TESTING", "1")
    monkeypatch.setenv("SOLHUNTER_PYTHON", sys.executable)
    monkeypatch.setattr(os, "execv", lambda *a, **k: None)
    monkeypatch.setattr("solhunter_zero.bootstrap_utils.ensure_venv", lambda argv: None)

    launcher = importlib.import_module("solhunter_zero.launcher")

    monkeypatch.setattr(launcher.device, "initialize_gpu", lambda: None)
    monkeypatch.setattr(launcher.system, "set_rayon_threads", lambda: None)

    captured = {}

    def fake_execvp(file, argv):
        captured["cmd"] = [file, *argv]
        raise RuntimeError

    monkeypatch.setattr(launcher.os, "execvp", fake_execvp)

    with pytest.raises(RuntimeError):
        launcher.main(["--skip-preflight"], False)

    cmd = captured.get("cmd", [])
    assert cmd.count("--one-click") == 1


def test_cluster_setup_assemble(tmp_path):
    cfg = tmp_path / 'cluster.toml'
    cfg.write_text(
        """
event_bus_url = "ws://bus"
broker_url = "redis://host"

[[nodes]]
solana_rpc_url = "url1"
solana_keypair = "kp1"

[[nodes]]
solana_rpc_url = "url2"
solana_keypair = "kp2"
"""
    )

    import importlib
    mod = importlib.import_module('scripts.cluster_setup')
    config = mod.load_cluster_config(str(cfg))
    cmds = mod.assemble_commands(config)

    assert len(cmds) == 2

    cmd1, env1 = cmds[0]
    assert cmd1[1].endswith('start_all.py')
    assert env1['EVENT_BUS_URL'] == 'ws://bus'
    assert env1['BROKER_URL'] == 'redis://host'
    assert env1['SOLANA_RPC_URL'] == 'url1'
    assert env1['SOLANA_KEYPAIR'] == 'kp1'


@pytest.mark.parametrize("preset", [False, True])
def test_apply_production_defaults_broker_matrix(monkeypatch, preset):
    import importlib

    start_all = importlib.reload(importlib.import_module("scripts.start_all"))

    for name in [
        "BROKER_WS_URLS",
        "BROKER_URLS",
        "BROKER_URL",
        "EVENT_BUS_URL",
        "MODE",
        "SOLHUNTER_MODE",
    ]:
        monkeypatch.delenv(name, raising=False)

    cfg: dict[str, object] = {}
    if preset:
        monkeypatch.setenv("BROKER_WS_URLS", "ws://broker.example:1234")

    applied = start_all.apply_production_defaults(cfg)

    if preset:
        assert "BROKER_WS_URLS" not in applied
        assert os.environ["BROKER_WS_URLS"] == "ws://broker.example:1234"
    else:
        assert applied["BROKER_WS_URLS"] == "ws://127.0.0.1:8769"
        assert os.environ["BROKER_WS_URLS"] == "ws://127.0.0.1:8769"

    assert os.environ["MODE"] == os.environ["SOLHUNTER_MODE"] == "live"


def test_apply_production_defaults_respects_config(monkeypatch):
    import importlib

    start_all = importlib.reload(importlib.import_module("scripts.start_all"))

    for name in [
        "BROKER_WS_URLS",
        "BROKER_URLS",
        "BROKER_URL",
        "EVENT_BUS_URL",
        "MODE",
        "SOLHUNTER_MODE",
    ]:
        monkeypatch.delenv(name, raising=False)

    cfg = {
        "mode": "paper",
        "broker_urls": ["redis://redis.example:6379"],
        "event_bus_url": "wss://bus.example/ws",
    }

    applied = start_all.apply_production_defaults(cfg)

    assert "BROKER_WS_URLS" not in applied
    assert "EVENT_BUS_URL" not in applied
    assert "BROKER_WS_URLS" not in os.environ
    assert "EVENT_BUS_URL" not in os.environ
    assert os.environ["MODE"] == os.environ["SOLHUNTER_MODE"] == "paper"


def test_ensure_keypair_generates_default(tmp_path, monkeypatch):
    monkeypatch.setenv("KEYPAIR_DIR", str(tmp_path))
    import sys
    import solhunter_zero
    sys.modules.pop("solhunter_zero.wallet", None)
    if hasattr(solhunter_zero, "wallet"):
        delattr(solhunter_zero, "wallet")

    from solhunter_zero.bootstrap import ensure_keypair

    from solhunter_zero import wallet

    calls = {"count": 0}
    real = wallet.generate_default_keypair

    def wrapped() -> tuple[str, Path]:
        calls["count"] += 1
        return real()

    monkeypatch.setattr(wallet, "generate_default_keypair", wrapped)

    ensure_keypair()

    assert calls["count"] == 1
    assert wallet.list_keypairs() == ["default"]
    assert wallet.get_active_keypair_name() == "default"
    assert (tmp_path / "default.json").exists()
    mn = tmp_path / "default.mnemonic"
    assert mn.exists()
    assert mn.read_text().strip()
    assert (mn.stat().st_mode & 0o777) == 0o600


def test_ensure_keypair_from_json(tmp_path, monkeypatch):
    monkeypatch.setenv("KEYPAIR_DIR", str(tmp_path))
    import sys
    import solhunter_zero
    sys.modules.pop("solhunter_zero.wallet", None)
    if hasattr(solhunter_zero, "wallet"):
        delattr(solhunter_zero, "wallet")

    monkeypatch.delenv("MNEMONIC", raising=False)

    import json
    monkeypatch.setenv("KEYPAIR_JSON", json.dumps([0] * 64))

    from solhunter_zero.bootstrap import ensure_keypair

    from solhunter_zero import wallet

    def boom() -> tuple[str, Path]:
        raise AssertionError("should not be called")

    monkeypatch.setattr(wallet, "generate_default_keypair", boom)

    ensure_keypair()

    assert wallet.list_keypairs() == ["default"]
    assert wallet.get_active_keypair_name() == "default"
    assert (Path(wallet.KEYPAIR_DIR) / "default.json").exists()
    # Mnemonic file should not be created when KEYPAIR_JSON provided
    assert not (Path(wallet.KEYPAIR_DIR) / "default.mnemonic").exists()


def test_perform_startup_live_bootstraps_keypair(tmp_path, monkeypatch):
    import types

    base58_mod = types.ModuleType("base58")
    base58_mod.b58decode = lambda *a, **k: b""
    base58_mod.b58encode = lambda *a, **k: b""
    monkeypatch.setitem(sys.modules, "base58", base58_mod)

    from solhunter_zero import main as main_module

    monkeypatch.setenv("MODE", "live")
    monkeypatch.delenv("KEYPAIR_PATH", raising=False)
    monkeypatch.delenv("SOLANA_KEYPAIR", raising=False)
    monkeypatch.setenv("DEPTH_SERVICE", "0")

    key_dir = tmp_path / "keys"
    key_dir.mkdir()
    key_path = key_dir / "default.json"

    # Ensure wallet globals point to the temporary directory
    monkeypatch.setattr(main_module.wallet, "KEYPAIR_DIR", str(key_dir))
    monkeypatch.setattr(
        main_module.wallet,
        "ACTIVE_KEYPAIR_FILE",
        os.path.join(str(key_dir), "active"),
    )

    load_calls: list[Path] = []
    state = {"exists": False}

    def fake_load_keypair(path: str):
        load_calls.append(Path(path))
        if not state["exists"]:
            raise FileNotFoundError(path)
        return object()

    def fake_setup_default_keypair():
        state["exists"] = True
        key_path.write_text("[]", encoding="utf-8")
        return main_module.wallet.KeypairInfo("default", None)

    monkeypatch.setattr(main_module.wallet, "load_keypair", fake_load_keypair)
    monkeypatch.setattr(main_module.wallet, "setup_default_keypair", fake_setup_default_keypair)
    monkeypatch.setattr(main_module.metrics_aggregator, "publish", lambda *a, **k: None)
    monkeypatch.setattr(main_module, "initialize_event_bus", lambda: None)

    async def fake_connectivity(*_a, **_k):
        return None

    monkeypatch.setattr(main_module, "ensure_connectivity_async", fake_connectivity)
    monkeypatch.setattr(main_module, "_start_depth_service", lambda _cfg: None)
    monkeypatch.setattr(
        main_module.prices,
        "validate_pyth_overrides_on_boot",
        lambda *a, **k: None,
    )
    monkeypatch.setattr(
        main_module,
        "load_config",
        lambda _path: {
            "solana_rpc_url": "https://rpc.example",
            "dex_base_url": "https://dex.example",
            "agents": ["dummy"],
            "agent_weights": {"dummy": 1.0},
            "solana_keypair": str(key_path),
        },
    )
    monkeypatch.setattr(
        main_module.Config,
        "from_env",
        classmethod(lambda cls, _cfg: _cfg),
    )

    config_path = tmp_path / "live.toml"
    config_path.write_text(
        "solana_rpc_url = \"https://rpc.example\"\n"
        "dex_base_url = \"https://dex.example\"\n"
        f"solana_keypair = \"{key_path}\"\n",
        encoding="utf-8",
    )

    asyncio.run(
        main_module.perform_startup_async(
            str(config_path), offline=True, dry_run=True
        )
    )

    assert state["exists"] is True
    assert load_calls
    assert load_calls[-1] == key_path
    assert os.environ["KEYPAIR_PATH"] == str(key_path)
    assert os.environ["SOLANA_KEYPAIR"] == str(key_path)

def test_ensure_deps_installs_optional(monkeypatch):
    from scripts import startup
    from solhunter_zero import bootstrap

    calls: list[list[str]] = []

    def fake_pip_install(*args):
        calls.append([sys.executable, "-m", "pip", "install", *args])

    results = [
        (
            [],
            [
                "faiss",
                "sentence_transformers",
                "torch",
                "orjson",
                "lz4",
                "zstandard",
                "msgpack",
            ],
        )
    ]
    monkeypatch.setattr(startup.deps, "check_deps", lambda: results.pop(0))
    monkeypatch.setattr(startup.bootstrap_utils, "_pip_install", fake_pip_install)
    monkeypatch.setattr(
        startup.bootstrap_utils, "_package_missing", lambda pkg: True
    )
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)

    startup.ensure_deps(install_optional=True, ensure_wallet_cli=False)

    assert calls[0] == [
        sys.executable,
        "-m",
        "pip",
        "install",
        ".[fastjson,fastcompress,msgpack]",
    ]
    assert set(c[-1] for c in calls[1:]) == {
        "faiss-cpu",
        "sentence-transformers",
        "torch",
    }
    assert not results  # ensure check_deps called once


def test_ensure_deps_warns_on_missing_optional(monkeypatch, capsys):
    from scripts import startup
    from solhunter_zero import bootstrap

    results = [([], ["orjson", "faiss"])]

    monkeypatch.setattr(startup.deps, "check_deps", lambda: results.pop(0))
    monkeypatch.setattr(startup.bootstrap_utils, "_pip_install", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)

    startup.ensure_deps(ensure_wallet_cli=False)
    out = capsys.readouterr().out

    assert "Optional modules missing: orjson, faiss (features disabled)." in out
    assert not results


def test_ensure_deps_installs_torch_metal(monkeypatch):
    from scripts import startup
    from solhunter_zero import bootstrap

    calls: list[list[str]] = []

    def fake_install():
        calls.append(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                f"torch=={TORCH_METAL_VERSION}",
                f"torchvision=={TORCHVISION_METAL_VERSION}",
                "--extra-index-url",
                "https://download.pytorch.org/whl/metal",
            ]
        )
        return {}

    results = [([], ["torch"])]

    monkeypatch.setattr(startup.deps, "check_deps", lambda: results.pop(0))
    monkeypatch.setattr(startup.device, "initialize_gpu", fake_install)
    monkeypatch.setattr(startup.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(startup.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.ensure_tools", lambda: {"success": True}
    )
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", lambda non_interactive=True: {"success": True}
    )
    monkeypatch.setattr(startup.bootstrap_utils, "_package_missing", lambda pkg: True)
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", lambda non_interactive=True: {"success": True}
    )
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)
    startup.ensure_deps(install_optional=True, ensure_wallet_cli=False)

    assert calls == [
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            f"torch=={TORCH_METAL_VERSION}",
            f"torchvision=={TORCHVISION_METAL_VERSION}",
            *METAL_EXTRA_INDEX,
        ]
    ]
    assert not results


def test_ensure_deps_requires_mps(monkeypatch):
    from scripts import startup

    calls: list[list[str]] = []

    def fake_pip_install(*args):
        calls.append([sys.executable, "-m", "pip", "install", *args])

    def fake_install():
        calls.append(
            [
                sys.executable,
                "-m",
                "pip",
                "install",
                "--force-reinstall",
                f"torch=={TORCH_METAL_VERSION}",
                f"torchvision=={TORCHVISION_METAL_VERSION}",
                "--extra-index-url",
                "https://download.pytorch.org/whl/metal",
            ]
        )
        raise RuntimeError("install the Metal wheel manually")

    results = [(["req"], [])]

    monkeypatch.setattr(startup.deps, "check_deps", lambda: results.pop(0))
    monkeypatch.setattr(startup.bootstrap_utils, "_pip_install", fake_pip_install)
    monkeypatch.setattr(startup.device, "initialize_gpu", fake_install)
    monkeypatch.setattr(startup.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(startup.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.ensure_tools", lambda: {"success": True}
    )
    monkeypatch.setattr(startup.bootstrap_utils, "_package_missing", lambda pkg: True)
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.prepare_macos_env", lambda non_interactive=True: {"success": True}
    )
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "ensure_target", lambda name: None)
    import importlib
    orig_find_spec = importlib.util.find_spec

    def fake_find_spec(name):
        if name == "req":
            return object()
        return orig_find_spec(name)

    monkeypatch.setattr(importlib.util, "find_spec", fake_find_spec)
    with pytest.raises(SystemExit) as excinfo:
        startup.ensure_deps(install_optional=True, ensure_wallet_cli=False)

    assert calls[-1] == [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--force-reinstall",
        f"torch=={TORCH_METAL_VERSION}",
        f"torchvision=={TORCHVISION_METAL_VERSION}",
        *METAL_EXTRA_INDEX,
    ]
    assert "install the Metal wheel manually" in str(excinfo.value)


def test_ensure_endpoints_success(monkeypatch):
    from solhunter_zero.bootstrap_utils import ensure_endpoints
    import urllib.request
    import websockets

    calls: list[tuple[str, str]] = []
    ws_calls: list[tuple[str, dict | None]] = []

    class DummyHTTP:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    def fake_urlopen(req, timeout=5):
        calls.append((req.full_url, req.get_method()))
        return DummyHTTP()

    class DummyWS:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(url, **kwargs):
        ws_calls.append((url, kwargs.get("extra_headers")))
        return DummyWS()

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)
    monkeypatch.setattr(websockets, "connect", fake_connect)

    cfg = {
        "dex_base_url": "https://quote-api.jup.ag",
        "birdeye_api_key": "k",
        "jito_ws_url": "wss://mainnet.block-engine.jito.wtf/api/v1/ws",
        "jito_ws_auth": "T",
    }
    ensure_endpoints(cfg)

    urls = {u for u, _ in calls}
    assert urls == {
        "https://quote-api.jup.ag",
        "https://api.birdeye.so/defi/tokenlist",
    }
    assert all(m == "HEAD" for _, m in calls)
    assert ws_calls == [
        (
            "wss://mainnet.block-engine.jito.wtf/api/v1/ws",
            {"Authorization": "T"},
        )
    ]


def test_ensure_endpoints_failure(monkeypatch, capsys):
    from solhunter_zero.bootstrap_utils import ensure_endpoints
    import urllib.request, urllib.error

    def fake_urlopen(req, timeout=5):
        raise urllib.error.URLError("boom")

    monkeypatch.setattr(urllib.request, "urlopen", fake_urlopen)

    cfg = {"dex_base_url": "https://quote-api.jup.ag"}

    with pytest.raises(SystemExit):
        ensure_endpoints(cfg)

    out = capsys.readouterr().out.lower()
    assert "dex_base_url" in out


def test_ensure_endpoints_ws_failure(monkeypatch, capsys):
    from solhunter_zero.bootstrap_utils import ensure_endpoints
    import asyncio
    import websockets

    class Dummy:
        async def __aenter__(self):
            raise OSError("boom")

        async def __aexit__(self, exc_type, exc, tb):
            return False

    def fake_connect(url, **kwargs):
        return Dummy()

    async def fake_sleep(_):
        pass

    monkeypatch.setattr(websockets, "connect", fake_connect)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    cfg = {"jito_ws_url": "wss://mainnet.block-engine.jito.wtf/api/v1/ws"}

    with pytest.raises(SystemExit):
        ensure_endpoints(cfg)

    out = capsys.readouterr().out.lower()
    assert "jito_ws_url" in out


def test_ensure_cargo_requires_curl(monkeypatch, capsys, tmp_path):
    from scripts import startup

    def fake_which(cmd):
        return None if cmd in {"cargo", "curl", "brew"} else "/usr/bin/" + cmd

    monkeypatch.setattr(startup.shutil, "which", fake_which)
    monkeypatch.setattr(startup.platform, "system", lambda: "Linux")
    monkeypatch.setattr(startup, "ROOT", tmp_path)

    with pytest.raises(SystemExit):
        startup.ensure_cargo()

    out = capsys.readouterr().out.lower()
    assert "curl is required" in out


def test_ensure_cargo_requires_pkg_config_and_cmake(monkeypatch, capsys, tmp_path):
    from scripts import startup

    def fake_which(cmd):
        return None if cmd in {"pkg-config", "cmake"} else "/usr/bin/" + cmd

    monkeypatch.setattr(startup.shutil, "which", fake_which)
    monkeypatch.setattr(startup.platform, "system", lambda: "Linux")
    monkeypatch.setattr(startup.subprocess, "check_call", lambda *a, **k: None)
    monkeypatch.setattr(startup, "ROOT", tmp_path)

    with pytest.raises(SystemExit):
        startup.ensure_cargo()

    out = capsys.readouterr().out.lower()
    assert "pkg-config" in out and "cmake" in out


def test_ensure_cargo_installs_pkg_config_and_cmake_with_brew(monkeypatch, tmp_path):
    from scripts import startup

    installed = {
        "cargo": "/usr/bin/cargo",
        "pkg-config": None,
        "cmake": None,
        "brew": "/usr/local/bin/brew",
    }

    def fake_which(cmd: str):
        return installed.get(cmd, f"/usr/bin/{cmd}")

    calls: list[list[str]] = []

    def fake_check_call(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["brew", "install"] and "rustup" not in cmd:
            for tool in ("pkg-config", "cmake"):
                installed[tool] = f"/usr/local/bin/{tool}"

    monkeypatch.setattr(startup.shutil, "which", fake_which)
    monkeypatch.setattr(startup.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(startup.platform, "machine", lambda: "x86_64")
    monkeypatch.setattr(startup.subprocess, "check_call", fake_check_call)
    monkeypatch.setattr(startup, "ROOT", tmp_path)

    startup.ensure_cargo()

    assert ["brew", "install", "pkg-config", "cmake"] in calls


def test_ensure_cargo_installs_rustup_with_brew(monkeypatch, tmp_path):
    from scripts import startup

    installed = {"cargo": None, "brew": "/usr/local/bin/brew"}

    def fake_which(cmd: str):
        return installed.get(cmd, f"/usr/bin/{cmd}")

    calls: list[list[str] | str] = []

    def fake_check_call(cmd, **kwargs):
        calls.append(cmd)
        if cmd[:2] == ["brew", "install"] and "rustup" in cmd:
            installed["cargo"] = "/usr/bin/cargo"
        if cmd == ["cargo", "--version"]:
            return

    monkeypatch.setattr(startup.shutil, "which", fake_which)
    monkeypatch.setattr(startup.platform, "system", lambda: "Linux")
    monkeypatch.setattr(startup.subprocess, "check_call", fake_check_call)
    monkeypatch.setattr(startup, "ROOT", tmp_path)

    startup.ensure_cargo()

    assert ["brew", "install", "rustup"] in calls
    assert ["rustup-init", "-y"] in calls
    assert (tmp_path / ".cache" / "cargo-installed").exists()


def test_ensure_cargo_skips_install_when_cached(monkeypatch, tmp_path, capsys):
    from scripts import startup

    def fake_which(cmd: str):
        return None if cmd == "cargo" else f"/usr/bin/{cmd}"

    marker = tmp_path / ".cache" / "cargo-installed"
    marker.parent.mkdir()
    marker.write_text("ok")

    monkeypatch.setattr(startup.shutil, "which", fake_which)
    monkeypatch.setattr(startup.platform, "system", lambda: "Linux")
    monkeypatch.setattr(startup, "ROOT", tmp_path)

    with pytest.raises(SystemExit):
        startup.ensure_cargo()

    out = capsys.readouterr().out.lower()
    assert "previously installed" in out


def test_main_calls_ensure_endpoints(monkeypatch, capsys):
    from scripts import startup
    import types, sys
    from solhunter_zero import preflight_utils
    monkeypatch.setattr(preflight_utils, "check_disk_space", lambda *a, **k: (True, "ok"))

    called: dict[str, object] = {}

    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_wallet_cli", lambda: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "bootstrap", lambda one_click=False: None)

    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "ensure_target", lambda name: None)
    dummy_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setattr(bootstrap_mod.device, "torch", dummy_torch)
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.ensure_tools", lambda: {"success": True}
    )
    monkeypatch.setattr("scripts.preflight.main", lambda: 0)

    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "ensure_target", lambda name: None)
    monkeypatch.setattr(
        "solhunter_zero.macos_setup.ensure_tools", lambda: {"success": True}
    )
    monkeypatch.setattr("scripts.preflight.main", lambda: 0)
    monkeypatch.setattr(startup, "ensure_target", lambda name: None)
    monkeypatch.setattr(startup, "ensure_endpoints", lambda cfg: called.setdefault("endpoints", cfg))
    stub_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", stub_torch)
    monkeypatch.setattr(
        startup,
        "device",
        types.SimpleNamespace(
            initialize_gpu=lambda: {},
            get_default_device=lambda: "cpu",
            detect_gpu=lambda: False,
            ensure_gpu_env=lambda: {},
        ),
    )
    monkeypatch.setattr(startup.os, "execv", lambda *a, **k: (_ for _ in ()).throw(SystemExit(0)))
    monkeypatch.setattr(
        startup.subprocess, "run", lambda *a, **k: types.SimpleNamespace(returncode=0)
    )
    conf = types.SimpleNamespace(
        load_config=lambda path=None: {"dex_base_url": "https://quote-api.jup.ag"},
        validate_config=lambda cfg: cfg,
        apply_env_overrides=lambda cfg: cfg,
        find_config_file=lambda: "config.toml",
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", conf)
    dummy_manager = types.SimpleNamespace(
        AgentManager=types.SimpleNamespace(from_config=lambda cfg: object())
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", dummy_manager)

    ret = startup.main(["--skip-deps", "--skip-rpc-check", "--skip-preflight"])
    out = capsys.readouterr().out
    assert "endpoints" in called
    assert re.search(r"HTTP endpoints\s+reachable", out)
    assert ret == 0


def test_main_skips_endpoint_check(monkeypatch, capsys):
    from scripts import startup

    called: dict[str, object] = {}

    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_wallet_cli", lambda: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "bootstrap", lambda one_click=False: None)
    monkeypatch.setattr(startup, "ensure_endpoints", lambda cfg: called.setdefault("endpoints", cfg))
    from solhunter_zero import preflight_utils
    monkeypatch.setattr(preflight_utils, "check_disk_space", lambda *a, **k: (True, "ok"))
    import types, sys
    stub_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", stub_torch)
    monkeypatch.setattr(
        startup,
        "device",
        types.SimpleNamespace(
            initialize_gpu=lambda: {},
            get_default_device=lambda: "cpu",
            detect_gpu=lambda: False,
            ensure_gpu_env=lambda: {},
        ),
    )
    monkeypatch.setattr(startup.os, "execv", lambda *a, **k: (_ for _ in ()).throw(SystemExit(0)))
    monkeypatch.setattr(
        startup.subprocess, "run", lambda *a, **k: types.SimpleNamespace(returncode=0)
    )
    conf = types.SimpleNamespace(
        load_config=lambda path=None: {"dex_base_url": "https://quote-api.jup.ag"},
        validate_config=lambda cfg: cfg,
        apply_env_overrides=lambda cfg: cfg,
        find_config_file=lambda: "config.toml",
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", conf)
    dummy_manager = types.SimpleNamespace(
        AgentManager=types.SimpleNamespace(from_config=lambda cfg: object())
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", dummy_manager)

    ret = startup.main([
        "--skip-deps",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-preflight",
    ])

    out = capsys.readouterr().out
    assert "endpoints" not in called
    assert "HTTP endpoints" in out and "skipped" in out
    assert ret == 0


def test_main_preflight_success(monkeypatch):
    from scripts import startup
    import types, sys

    called = {}

    def fake_preflight():
        called["preflight"] = True
        raise SystemExit(0)

    monkeypatch.setattr("scripts.preflight.main", fake_preflight)
    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_wallet_cli", lambda: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "bootstrap", lambda one_click=False: None)
    import types as _types, sys
    stub_torch = _types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", stub_torch)
    monkeypatch.setattr(
        startup,
        "device",
        _types.SimpleNamespace(
            initialize_gpu=lambda: {},
            get_default_device=lambda: "cpu",
            detect_gpu=lambda: False,
        ),
    )
    monkeypatch.setattr(startup.os, "execv", lambda *a, **k: (_ for _ in ()).throw(SystemExit(0)))

    with pytest.raises(SystemExit) as exc:
        startup.main([
            "--one-click",
            "--skip-setup",
            "--skip-deps",
        ])

    assert called.get("preflight") is True
    assert exc.value.code == 0


def test_main_preflight_failure(monkeypatch, capsys):
    from scripts import startup
    from pathlib import Path

    def fake_preflight():
        print("out")
        print("err", file=sys.stderr)
        raise SystemExit(2)

    monkeypatch.setattr("scripts.preflight.main", fake_preflight)

    import types
    stub_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", stub_torch)
    monkeypatch.setattr(
        startup,
        "device",
        types.SimpleNamespace(
            initialize_gpu=lambda: {},
            get_default_device=lambda: "cpu",
            detect_gpu=lambda: False,
            ensure_gpu_env=lambda: {},
        ),
    )
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "bootstrap", lambda one_click=False: None)

    log_file = Path(__file__).resolve().parent.parent / "preflight.log"
    if log_file.exists():
        log_file.unlink()

    ret = startup.main([
        "--one-click",
        "--skip-deps",
        "--skip-setup",
    ])

    assert ret == 2
    captured = capsys.readouterr()
    assert "out" in captured.out
    assert "err" in captured.err
    assert log_file.exists()
    log_contents = log_file.read_text()
    assert "out" in log_contents
    assert "err" in log_contents


def test_preflight_log_rotation(tmp_path):
    from scripts import startup
    log_path = tmp_path / "preflight.log"
    log_path.write_text("x" * 20)
    rotated = tmp_path / "preflight.log.1"

    startup.rotate_preflight_log(log_path, max_bytes=10)

    assert rotated.exists()
    assert not log_path.exists()


def test_startup_log_rotation(tmp_path):
    from solhunter_zero import logging_utils

    log_path = tmp_path / "startup.log"
    log_path.write_text("x" * (logging_utils.MAX_STARTUP_LOG_SIZE + 1))
    rotated = tmp_path / "startup.log.1"

    logging_utils.rotate_startup_log(log_path)

    assert rotated.exists()
    assert not log_path.exists()


def test_startup_sets_mps_device(monkeypatch):
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("PYTORCH_ENABLE_MPS_FALLBACK", raising=False)

    import platform
    import types, sys
    from solhunter_zero import bootstrap

    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")

    dummy_torch = types.SimpleNamespace()
    dummy_torch.backends = types.SimpleNamespace()
    dummy_torch.backends.mps = types.SimpleNamespace()
    dummy_torch.backends.mps.is_available = lambda: True
    dummy_torch.cuda = types.SimpleNamespace(is_available=lambda: False)
    dummy_torch.set_default_device = lambda dev: None
    monkeypatch.setitem(sys.modules, "torch", dummy_torch)

    monkeypatch.setattr(bootstrap, "ensure_venv", lambda *a, **k: None)
    monkeypatch.setattr(bootstrap, "ensure_deps", lambda *a, **k: None)
    monkeypatch.setattr(bootstrap, "ensure_keypair", lambda: None)
    monkeypatch.setattr(bootstrap, "ensure_config", lambda: (Path("config.toml"), {}))
    monkeypatch.setattr(bootstrap, "ensure_cargo", lambda: None)
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)
    monkeypatch.setattr(bootstrap.device, "torch", dummy_torch)

    bootstrap.bootstrap(one_click=True)

    assert os.environ.get("TORCH_DEVICE") == "mps"
    assert os.environ.get("PYTORCH_ENABLE_MPS_FALLBACK") == "1"


def test_wallet_cli_failure_propagates(monkeypatch):
    from scripts import startup

    monkeypatch.setattr(startup, "ensure_deps", lambda: None)
    monkeypatch.setattr(startup, "ensure_endpoints", lambda cfg: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    from solhunter_zero import bootstrap as bootstrap_mod
    monkeypatch.setattr(bootstrap_mod, "bootstrap", lambda one_click=False: None)
    import types, sys
    stub_torch = types.SimpleNamespace(set_default_device=lambda dev: None)
    monkeypatch.setitem(sys.modules, "torch", stub_torch)
    monkeypatch.setattr(
        startup,
        "device",
        types.SimpleNamespace(
            initialize_gpu=lambda: {},
            get_default_device=lambda: "cpu",
            detect_gpu=lambda: False,
            ensure_gpu_env=lambda: {},
        ),
    )
    conf = types.SimpleNamespace(
        load_config=lambda path=None: {"dex_base_url": "https://quote-api.jup.ag"},
        validate_config=lambda cfg: cfg,
        apply_env_overrides=lambda cfg: cfg,
        find_config_file=lambda: "config.toml",
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", conf)

    def fail_wallet():
        raise SystemExit(5)

    monkeypatch.setattr(startup, "ensure_wallet_cli", fail_wallet)

    ret = startup.main(["--skip-deps", "--skip-rpc-check", "--skip-preflight"])
    assert ret == 5


def test_ensure_wallet_cli_attempts_install(monkeypatch, capsys):
    import types, shutil, sys
    import solhunter_zero.bootstrap_utils as bootstrap_utils

    dummy_fernet = types.ModuleType("fernet")
    dummy_fernet.Fernet = object
    dummy_fernet.InvalidToken = Exception
    dummy_crypto = types.ModuleType("cryptography")
    dummy_crypto.fernet = dummy_fernet
    dummy_crypto.__path__ = []
    monkeypatch.setitem(sys.modules, "cryptography", dummy_crypto)
    monkeypatch.setitem(sys.modules, "cryptography.fernet", dummy_fernet)

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

    dummy_rich = types.ModuleType("rich")
    dummy_rich.__path__ = []
    dummy_console_mod = types.ModuleType("rich.console")
    dummy_console_mod.Console = lambda *a, **k: types.SimpleNamespace(print=lambda *a, **k: None)
    dummy_rich.console = dummy_console_mod
    dummy_progress_mod = types.ModuleType("rich.progress")
    dummy_progress_mod.Progress = lambda *a, **k: types.SimpleNamespace(__enter__=lambda self: self, __exit__=lambda self, exc_type, exc, tb: None)
    dummy_panel_mod = types.ModuleType("rich.panel")
    dummy_panel_mod.Panel = object
    dummy_table_mod = types.ModuleType("rich.table")
    dummy_table_mod.Table = object
    monkeypatch.setitem(sys.modules, "rich.progress", dummy_progress_mod)
    monkeypatch.setitem(sys.modules, "rich.panel", dummy_panel_mod)
    monkeypatch.setitem(sys.modules, "rich.table", dummy_table_mod)
    monkeypatch.setitem(sys.modules, "rich", dummy_rich)
    monkeypatch.setitem(sys.modules, "rich.console", dummy_console_mod)

    dummy_preflight = types.SimpleNamespace(check_internet=lambda: (True, "ok"))
    monkeypatch.setitem(sys.modules, "scripts.preflight", dummy_preflight)

    from scripts import startup
    monkeypatch.setattr(sys.modules["scripts"], "preflight", dummy_preflight, raising=False)

    bootstrap_utils.DEPS_MARKER.unlink(missing_ok=True)
    monkeypatch.setattr(shutil, "which", lambda cmd: None)
    monkeypatch.setattr(bootstrap_utils.shutil, "which", lambda cmd: None)
    monkeypatch.setattr(bootstrap_utils.deps, "check_deps", lambda: ([], []))

    calls: list[tuple[str, ...]] = []

    def fake_pip_install(*args, **kwargs):
        calls.append(args)
        if args and args[0] == "solhunter-wallet":
            print("Failed to install. To retry manually, run: pip install solhunter-wallet")
            raise SystemExit(1)

    monkeypatch.setattr(bootstrap_utils, "_pip_install", fake_pip_install)

    with pytest.raises(SystemExit):
        startup.ensure_wallet_cli()

    assert any(args[0] == "solhunter-wallet" for args in calls)
    assert "pip install solhunter-wallet" in capsys.readouterr().out


def test_main_runs_quick_setup_when_config_missing(monkeypatch, tmp_path, capsys):
    from scripts import startup
    from solhunter_zero.wallet import KeypairInfo
    import subprocess

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("")
    calls = {}

    def missing_config():
        raise FileNotFoundError("missing")

    import solhunter_zero.config_utils as cu
    monkeypatch.setattr(cu, "ensure_default_config", missing_config)

    def fake_quick_setup():
        calls["quick_setup"] = True
        return str(cfg_path)

    monkeypatch.setattr(startup, "run_quick_setup", fake_quick_setup)

    import types, sys
    config_mod = types.SimpleNamespace(
        load_config=lambda path: {}, validate_config=lambda cfg: cfg
    )
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", config_mod)
    monkeypatch.setattr(startup, "ensure_wallet_cli", lambda: None)

    def fake_select(auto=False):
        calls["auto"] = auto
        return KeypairInfo("kp1", None)

    monkeypatch.setattr(cu, "select_active_keypair", fake_select)
    import solhunter_zero.wallet as wallet_mod
    monkeypatch.setattr(wallet_mod, "KEYPAIR_DIR", tmp_path)
    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    monkeypatch.setattr(
        "solhunter_zero.preflight_utils.check_disk_space", lambda min_bytes: (True, "ok")
    )
    monkeypatch.setattr(startup, "ensure_endpoints", lambda cfg: None)
    monkeypatch.setattr(startup, "log_startup", lambda msg: None)
    monkeypatch.setattr(startup.device, "initialize_gpu", lambda: {"SOLHUNTER_GPU_DEVICE": "cpu"})
    monkeypatch.setattr(startup.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0))
    import scripts.healthcheck as healthcheck
    monkeypatch.setattr(healthcheck, "main", lambda *a, **k: 0)

    ret = startup.main([
        "--one-click",
        "--skip-deps",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-preflight",
        "--no-diagnostics",
    ])

    assert ret == 0
    assert calls.get("quick_setup") is True
    assert calls.get("auto") is True
    out = capsys.readouterr().out
    assert str(cfg_path) in out
    assert "Active keypair: kp1" in out


def test_main_runs_quick_setup_on_invalid_config(monkeypatch, tmp_path, capsys):
    from scripts import startup
    from solhunter_zero.wallet import KeypairInfo
    import subprocess

    cfg_path = tmp_path / "config.toml"
    cfg_path.write_text("invalid")
    calls: dict[str, int | bool] = {}

    import solhunter_zero.config_utils as cu
    monkeypatch.setattr(cu, "ensure_default_config", lambda: str(cfg_path))

    def fake_quick_setup():
        calls["quick_setup"] = calls.get("quick_setup", 0) + 1
        return str(cfg_path)

    monkeypatch.setattr(startup, "run_quick_setup", fake_quick_setup)

    def fake_select(auto=False):
        calls["auto"] = auto
        return KeypairInfo("kp1", None)

    monkeypatch.setattr(cu, "select_active_keypair", fake_select)
    import solhunter_zero.wallet as wallet_mod
    monkeypatch.setattr(wallet_mod, "KEYPAIR_DIR", tmp_path)

    def fake_validate(cfg):
        if calls.get("validated"):
            return cfg
        calls["validated"] = True
        raise ValueError("bad config")

    import types, sys
    config_mod = types.SimpleNamespace(load_config=lambda path: {}, validate_config=fake_validate)
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", config_mod)

    monkeypatch.setattr(startup, "ensure_wallet_cli", lambda: None)
    monkeypatch.setattr(startup, "ensure_deps", lambda install_optional=False: None)
    monkeypatch.setattr(startup, "ensure_rpc", lambda warn_only=False: None)
    monkeypatch.setattr(startup, "ensure_cargo", lambda: None)
    monkeypatch.setattr(
        "solhunter_zero.preflight_utils.check_disk_space", lambda min_bytes: (True, "ok")
    )
    monkeypatch.setattr(startup, "ensure_endpoints", lambda cfg: None)
    monkeypatch.setattr(startup, "log_startup", lambda msg: None)
    monkeypatch.setattr(startup.device, "initialize_gpu", lambda: {"SOLHUNTER_GPU_DEVICE": "cpu"})
    monkeypatch.setattr(startup.subprocess, "run", lambda *a, **k: subprocess.CompletedProcess(a, 0))
    import scripts.healthcheck as healthcheck
    monkeypatch.setattr(healthcheck, "main", lambda *a, **k: 0)

    ret = startup.main([
        "--one-click",
        "--skip-deps",
        "--skip-rpc-check",
        "--skip-endpoint-check",
        "--skip-preflight",
        "--no-diagnostics",
    ])

    assert ret == 0
    assert calls.get("quick_setup") == 1
    assert calls.get("auto") is True

def test_bootstrap_aborts_on_low_balance(monkeypatch, tmp_path):
    from solhunter_zero import bootstrap, preflight_utils
    from solhunter_zero.wallet import KeypairInfo
    import types
    import solana.rpc.api as rpc_api
    import pytest

    monkeypatch.setenv("SOLHUNTER_SKIP_VENV", "1")
    monkeypatch.setenv("SOLHUNTER_SKIP_DEPS", "1")
    monkeypatch.setenv("MIN_STARTING_BALANCE", "1")

    monkeypatch.setattr(bootstrap, "ensure_venv", lambda *_: None)
    monkeypatch.setattr(bootstrap, "ensure_deps", lambda *_: None)
    monkeypatch.setattr(bootstrap, "ensure_cargo", lambda: None)
    monkeypatch.setattr(bootstrap, "ensure_target", lambda name: None)
    monkeypatch.setattr(bootstrap.device, "initialize_gpu", lambda: None)
    monkeypatch.setattr(bootstrap, "ensure_config", lambda: (tmp_path / "c.toml", {}))

    kp_path = tmp_path / "dummy.json"
    kp_path.write_text("[]")
    monkeypatch.setattr(
        bootstrap,
        "ensure_keypair",
        lambda: (KeypairInfo("dummy", None), kp_path),
    )
    monkeypatch.setattr(bootstrap.wallet, "ensure_default_keypair", lambda: None)
    monkeypatch.setattr(
        preflight_utils.wallet,
        "load_keypair",
        lambda _p: types.SimpleNamespace(pubkey=lambda: "pk"),
    )

    class FakeClient:
        def __init__(self, url):
            pass

        def get_balance(self, pubkey):
            return {"result": {"value": 0}}

    monkeypatch.setattr(rpc_api, "Client", FakeClient)

    with pytest.raises(SystemExit):
        bootstrap.bootstrap(one_click=True)


def test_startup_non_interactive_offline_sets_env(monkeypatch):
    import types
    from scripts import startup as startup_mod

    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)
    monkeypatch.setattr(startup_mod.startup_cli, "render_banner", lambda: None)
    monkeypatch.setattr("solhunter_zero.logging_utils.log_startup", lambda *a, **k: None)
    monkeypatch.setattr("solhunter_zero.logging_utils.rotate_preflight_log", lambda *a, **k: None)

    def fake_parse_args(_):
        return types.SimpleNamespace(offline=True, non_interactive=True, quiet=False), []

    captured = {}

    def fake_launch_only(rest, *, offline=None, subprocess_module=None):
        captured["rest"] = list(rest)
        captured["offline"] = offline
        captured["env"] = os.environ.get("SOLHUNTER_OFFLINE")
        return 0

    monkeypatch.setattr(startup_mod.startup_cli, "parse_args", fake_parse_args)
    monkeypatch.setattr("solhunter_zero.startup_runner.launch_only", fake_launch_only)

    code = startup_mod._main_impl(["--offline", "--non-interactive"])

    assert code == 0
    assert captured["offline"] is True
    assert captured["env"] == "1"


def test_startup_interactive_offline_propagates_to_start_all(monkeypatch):
    import types
    from scripts import startup as startup_mod

    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)
    monkeypatch.setattr(startup_mod.startup_cli, "render_banner", lambda: None)
    monkeypatch.setattr("solhunter_zero.logging_utils.log_startup", lambda *a, **k: None)
    monkeypatch.setattr("solhunter_zero.logging_utils.rotate_preflight_log", lambda *a, **k: None)

    def fake_parse_args(_):
        return (
            types.SimpleNamespace(
                offline=True,
                non_interactive=False,
                quiet=False,
                skip_deps=False,
                skip_setup=False,
                skip_rpc_check=False,
                skip_endpoint_check=False,
                skip_preflight=False,
            ),
            [],
        )

    fake_checks = types.SimpleNamespace(
        perform_checks=lambda *a, **k: {
            "rest": [],
            "config_path": Path("cfg.toml"),
            "config": {},
            "summary_rows": [],
        }
    )

    monkeypatch.setattr(startup_mod, "_startup_checks", lambda: fake_checks)
    monkeypatch.setattr(startup_mod.startup_cli, "parse_args", fake_parse_args)

    captured = {}

    def fake_start_all_main(rest):
        captured["rest"] = list(rest)
        captured["env"] = os.environ.get("SOLHUNTER_OFFLINE")
        return 0

    monkeypatch.setattr(
        "solhunter_zero.agent_manager.AgentManager.from_config",
        classmethod(lambda cls, cfg: object()),
    )
    monkeypatch.setattr("scripts.start_all.main", fake_start_all_main)
    monkeypatch.setattr("scripts.healthcheck.main", lambda *a, **k: 0)
    monkeypatch.setattr("scripts.preflight.CHECKS", [])

    code = startup_mod._main_impl(["--offline"])

    assert code == 0
    assert captured["env"] == "1"
    assert captured["rest"] and captured["rest"][-1] == "--foreground"


def _interactive_args():
    import types

    return types.SimpleNamespace(
        offline=False,
        non_interactive=False,
        quiet=False,
        skip_deps=False,
        skip_setup=False,
        skip_rpc_check=False,
        skip_endpoint_check=False,
        skip_preflight=False,
    )


def _stub_rich(monkeypatch):
    import sys
    import types

    class DummyConsole:
        def __init__(self, *_, **__):
            pass

        def print(self, *args, **kwargs):  # pragma: no cover - diagnostic stub
            return None

    class DummyTable:
        def __init__(self, *_, **__):
            pass

        def add_column(self, *args, **kwargs):
            return None

        def add_row(self, *args, **kwargs):
            return None

    monkeypatch.setitem(sys.modules, "rich", types.SimpleNamespace())
    monkeypatch.setitem(sys.modules, "rich.console", types.SimpleNamespace(Console=DummyConsole))
    monkeypatch.setitem(sys.modules, "rich.table", types.SimpleNamespace(Table=DummyTable))


def _stub_agent_manager(monkeypatch):
    import sys
    import types

    class DummyAgentManager:
        @classmethod
        def from_config(cls, cfg):
            return object()

    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", types.SimpleNamespace(AgentManager=DummyAgentManager))


def _stub_scripts(monkeypatch):
    import sys
    import types

    scripts_pkg = sys.modules.get("scripts", types.ModuleType("scripts"))
    preflight_mod = types.SimpleNamespace(CHECKS=[])
    start_all_mod = types.SimpleNamespace(main=lambda rest: 0)
    healthcheck_mod = types.SimpleNamespace(main=lambda *a, **k: 0)

    scripts_pkg.preflight = preflight_mod
    scripts_pkg.start_all = start_all_mod
    scripts_pkg.healthcheck = healthcheck_mod

    monkeypatch.setitem(sys.modules, "scripts", scripts_pkg)
    monkeypatch.setitem(sys.modules, "scripts.preflight", preflight_mod)
    monkeypatch.setitem(sys.modules, "scripts.start_all", start_all_mod)
    monkeypatch.setitem(sys.modules, "scripts.healthcheck", healthcheck_mod)


@pytest.mark.parametrize(
    "config_path_factory, exc_factory, create_file, expected_fragment",
    [
        (
            lambda tmp_path: tmp_path / "missing.toml",
            lambda path: FileNotFoundError(path),
            False,
            "not found; run config bootstrap or pass --config",
        ),
        (
            lambda tmp_path: tmp_path / "bad.toml",
            lambda path: ValueError("bad config"),
            True,
            "Invalid config",
        ),
    ],
)
def test_run_handles_config_load_errors(
    monkeypatch, tmp_path, config_path_factory, exc_factory, create_file, expected_fragment
):
    import types

    _stub_rich(monkeypatch)
    _stub_scripts(monkeypatch)

    import solhunter_zero.startup_runner as startup_runner

    logs: list[str] = []
    printed: list[str] = []

    def fake_log(msg: str) -> None:
        logs.append(msg)

    monkeypatch.setattr(
        startup_runner,
        "console",
        types.SimpleNamespace(print=lambda *args, **kwargs: printed.append(" ".join(str(a) for a in args))),
    )
    monkeypatch.setattr(startup_runner, "STARTUP_LOG", Path("startup.log"))

    cfg_path = config_path_factory(tmp_path)
    if create_file:
        cfg_path.write_text("invalid")

    monkeypatch.setattr("solhunter_zero.config.find_config_file", lambda: str(cfg_path))
    monkeypatch.setattr("solhunter_zero.config.load_config", lambda path: (_ for _ in ()).throw(exc_factory(path)))

    args = _interactive_args()
    ctx = {"rest": [], "summary_rows": []}

    code = startup_runner.run(args, ctx, log_startup=fake_log)

    assert code == startup_runner.CONFIG_LOAD_EXIT_CODE
    messages = logs + printed
    assert any("run config bootstrap" in msg for msg in messages)
    assert any(expected_fragment in msg for msg in messages)
    assert str(cfg_path) in "".join(messages)


def test_run_logs_ui_status_before_ready(monkeypatch):
    import types

    _stub_rich(monkeypatch)
    _stub_agent_manager(monkeypatch)
    _stub_scripts(monkeypatch)

    import solhunter_zero.startup_runner as startup_runner

    logs: list[str] = []

    def fake_log(msg: str) -> None:
        logs.append(msg)

    monkeypatch.setattr(startup_runner, "log_startup", fake_log)
    monkeypatch.setattr(startup_runner, "log_startup_info", lambda **_: None)
    monkeypatch.setattr(startup_runner, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(startup_runner, "STARTUP_LOG", Path("startup.log"))
    monkeypatch.setattr(startup_runner, "_poll_ui_readiness", lambda: (True, "ui-ok", {}, []))

    args = _interactive_args()
    ctx = {"rest": [], "summary_rows": [], "config_path": Path("cfg"), "config": {}}

    code = startup_runner.run(args, ctx, log_startup=fake_log)

    assert code == 0
    ui_index = next(i for i, msg in enumerate(logs) if msg.startswith("UI readiness"))
    ready_index = logs.index("SolHunter launch complete – system ready.")
    assert ui_index < ready_index


def test_run_logs_ui_failure(monkeypatch):
    import types

    _stub_rich(monkeypatch)
    _stub_agent_manager(monkeypatch)
    _stub_scripts(monkeypatch)

    import solhunter_zero.startup_runner as startup_runner

    logs: list[str] = []

    def fake_log(msg: str) -> None:
        logs.append(msg)

    monkeypatch.setattr(startup_runner, "log_startup", fake_log)
    monkeypatch.setattr(startup_runner, "log_startup_info", lambda **_: None)
    monkeypatch.setattr(startup_runner, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(startup_runner, "STARTUP_LOG", Path("startup.log"))

    def fake_poll():
        return False, "ui-http: FAIL (timeout)", {}, []

    monkeypatch.setattr(startup_runner, "_poll_ui_readiness", fake_poll)

    args = _interactive_args()
    ctx = {"rest": [], "summary_rows": [], "config_path": Path("cfg"), "config": {}}

    code = startup_runner.run(args, ctx, log_startup=fake_log)

    assert code == 0
    assert any(msg.startswith("UI readiness unhealthy") for msg in logs)


def test_run_summary_rows_include_ui(monkeypatch):
    import types

    _stub_rich(monkeypatch)
    _stub_agent_manager(monkeypatch)
    _stub_scripts(monkeypatch)

    import solhunter_zero.startup_runner as startup_runner

    logs: list[str] = []

    def fake_log(msg: str) -> None:
        logs.append(msg)

    monkeypatch.setattr(startup_runner, "log_startup", fake_log)
    monkeypatch.setattr(startup_runner, "log_startup_info", lambda **_: None)
    monkeypatch.setattr(startup_runner, "console", types.SimpleNamespace(print=lambda *a, **k: None))
    monkeypatch.setattr(startup_runner, "STARTUP_LOG", Path("startup.log"))

    def fake_poll():
        targets = {
            "ui-http": "http://localhost:5001/",
            "ui-ws": "ws://localhost:7001",
        }
        http_result = types.SimpleNamespace(
            name="ui-http",
            ok=False,
            error="service unavailable",
            status=None,
            status_code=503,
        )
        ws_result = types.SimpleNamespace(name="ui-ws", ok=True, error=None, status=None, status_code=None)
        return (
            False,
            "ui-http: FAIL (503 service unavailable); ui-ws: OK",
            targets,
            [http_result, ws_result],
        )

    monkeypatch.setattr(startup_runner, "_poll_ui_readiness", fake_poll)

    args = _interactive_args()
    ctx = {
        "rest": [],
        "summary_rows": [("Config", "loaded")],
        "config_path": Path("cfg"),
        "config": {},
    }

    code = startup_runner.run(args, ctx, log_startup=fake_log)

    assert code == 0
    assert any("UI HTTP" in msg and "503 service unavailable" in msg for msg in logs)
    assert any("UI HTTP" in msg and "port 5001" in msg for msg in logs)
    assert any("UI WS" in msg and "port 7001" in msg for msg in logs)
    assert any(msg.startswith("UI readiness unhealthy") for msg in logs)


def test_ui_summary_rows_include_status_codes(monkeypatch):
    import types

    _stub_rich(monkeypatch)

    import solhunter_zero.startup_runner as startup_runner

    targets = {"ui-http": "http://example.com:8080"}
    results = [
        types.SimpleNamespace(
            name="ui-http", ok=False, error="service unavailable", status_code=503, status=None
        )
    ]

    rows = startup_runner._ui_summary_rows(
        targets=targets, results=results, readiness_message="", readiness_ok=False
    )

    assert ("UI HTTP", "FAIL (503 service unavailable) – http://example.com:8080; port 8080") in rows


def test_poll_ui_readiness_retries_until_healthy(monkeypatch):
    import types

    _stub_rich(monkeypatch)

    from solhunter_zero import startup_runner

    attempts: list[int] = []

    class DummyChecker:
        def __init__(self):
            self.targets = [
                {"name": "ui-http", "url": "http://localhost:5001", "type": "http"}
            ]

        async def check_all(self):
            attempts.append(1)
            ready = len(attempts) >= 2
            return [
                types.SimpleNamespace(
                    name="ui-http", ok=ready, error=None if ready else "booting", status=None, status_code=None
                )
            ]

    monkeypatch.setattr(
        "solhunter_zero.production.connectivity.ConnectivityChecker", DummyChecker
    )
    monkeypatch.setattr(startup_runner.time, "sleep", lambda *_: None)

    ready, summary, targets, results = startup_runner._poll_ui_readiness(
        timeout=1.0, interval=0.01, target_timeout=0.5
    )

    assert ready is True
    assert len(attempts) == 2
    assert "ui-http: OK" in summary
    assert targets["ui-http"] == "http://localhost:5001"
    assert results and getattr(results[-1], "ok", False)


def test_poll_ui_readiness_enforces_target_timeout(monkeypatch):
    import asyncio
    import types

    _stub_rich(monkeypatch)

    from solhunter_zero import startup_runner

    class SlowChecker:
        def __init__(self):
            self.targets = [
                {"name": "ui-http", "url": "http://localhost:5001", "type": "http"}
            ]

        async def check_all(self):
            await asyncio.sleep(0.05)
            return [
                types.SimpleNamespace(
                    name="ui-http", ok=False, error="slow", status=None, status_code=None
                )
            ]

    monkeypatch.setattr(
        "solhunter_zero.production.connectivity.ConnectivityChecker", SlowChecker
    )
    monkeypatch.setattr(startup_runner.time, "sleep", lambda *_: None)

    ready, summary, targets, results = startup_runner._poll_ui_readiness(
        timeout=0.05, interval=0.01, target_timeout=0.01
    )

    assert ready is False
    assert "timed out" in summary
    assert targets["ui-http"] == "http://localhost:5001"
    assert results == []


def test_launch_only_sets_env_when_offline(monkeypatch):
    from types import SimpleNamespace
    from solhunter_zero import startup_runner

    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)

    captured = {}

    class DummySubprocess:
        def run(self, cmd, env):
            captured["cmd"] = cmd
            captured["env"] = env
            return SimpleNamespace(returncode=0)

    code = startup_runner.launch_only(
        ["--foo"], offline=True, subprocess_module=DummySubprocess()
    )

    assert code == 0
    assert captured["env"].get("SOLHUNTER_OFFLINE") == "1"


def test_agent_manager_failure_logs_traceback(monkeypatch, tmp_path):
    import types
    from solhunter_zero import logging_utils

    _stub_rich(monkeypatch)
    from solhunter_zero import startup_runner

    log_path = tmp_path / "startup.log"

    monkeypatch.setattr(startup_runner, "STARTUP_LOG", log_path)
    monkeypatch.setattr(
        startup_runner,
        "log_startup",
        lambda msg: logging_utils.log_startup(msg, path=log_path),
    )
    monkeypatch.setattr(startup_runner, "log_startup_info", lambda **kwargs: None)
    monkeypatch.setattr(startup_runner, "_extract_ui_targets", lambda: ({}, None))
    monkeypatch.setattr(
        startup_runner, "_poll_ui_readiness", lambda **kwargs: (False, "skipped", {}, [])
    )
    monkeypatch.setattr(
        startup_runner, "console", types.SimpleNamespace(print=lambda *a, **k: None)
    )

    healthcheck_mod = types.ModuleType("scripts.healthcheck")
    healthcheck_mod.main = lambda *a, **k: 0
    monkeypatch.setitem(sys.modules, "scripts.healthcheck", healthcheck_mod)

    agent_manager_mod = types.ModuleType("solhunter_zero.agent_manager")

    class DummyAgentManager:
        @classmethod
        def from_config(cls, _cfg):
            raise RuntimeError("boom")

    agent_manager_mod.AgentManager = DummyAgentManager
    monkeypatch.setitem(sys.modules, "solhunter_zero.agent_manager", agent_manager_mod)

    args = types.SimpleNamespace(
        offline=False,
        non_interactive=False,
        quiet=False,
        skip_deps=False,
        skip_setup=False,
        skip_rpc_check=False,
        skip_endpoint_check=False,
        skip_preflight=False,
    )
    ctx = {"config_path": tmp_path / "cfg.toml", "config": {}, "summary_rows": []}
    monkeypatch.setattr(startup_runner.preflight, "CHECKS", [])

    result = startup_runner.run(args, ctx, log_startup=startup_runner.log_startup)

    log_text = log_path.read_text()
    assert result == 1
    assert "AgentManager initialization failed: boom" in log_text
    assert "Verify [agent_manager] and [agents] config sections" in log_text
    assert "Traceback (most recent call last)" in log_text
    assert "RuntimeError: boom" in log_text


def test_disk_space_threshold_uses_config(monkeypatch):
    from scripts import startup

    cfg = {"offline_data_limit_gb": 2}
    monkeypatch.setattr(startup, "load_config", lambda path=None: cfg)
    monkeypatch.setattr(startup, "apply_env_overrides", lambda c: c)

    one_gb = 1024 ** 3

    def fake_disk_usage(path):
        return (0, 0, one_gb)

    monkeypatch.setattr(startup.preflight_utils.shutil, "disk_usage", fake_disk_usage)
    monkeypatch.setattr(startup, "log_startup", lambda msg: None)

    with pytest.raises(SystemExit):
        startup.main([])
