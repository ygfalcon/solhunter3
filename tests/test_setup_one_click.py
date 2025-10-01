import os
import platform
import runpy
import sys
import types
import subprocess
import shutil
import site
from pathlib import Path
import importlib

import pytest

# Stub modules to avoid heavy side effects during import
macos_setup_mod = types.ModuleType("solhunter_zero.macos_setup")
macos_setup_mod.ensure_tools = lambda non_interactive=True: None
macos_setup_mod._resolve_metal_versions = lambda: ("torch", "vision")
macos_setup_mod._write_versions_to_config = lambda *a, **k: None
sys.modules["solhunter_zero.macos_setup"] = macos_setup_mod

bootstrap_utils_mod = types.ModuleType("solhunter_zero.bootstrap_utils")
bootstrap_utils_mod.ensure_venv = lambda arg: None
bootstrap_utils_mod.ensure_deps = lambda install_optional=False: None
bootstrap_utils_mod.ensure_endpoints = lambda cfg: None
bootstrap_utils_mod.METAL_EXTRA_INDEX = []
sys.modules["solhunter_zero.bootstrap_utils"] = bootstrap_utils_mod

logging_utils_mod = types.ModuleType("solhunter_zero.logging_utils")
logging_utils_mod.log_startup = lambda msg: None
logging_utils_mod.setup_logging = lambda name: None
sys.modules["solhunter_zero.logging_utils"] = logging_utils_mod

env_config_mod = types.ModuleType("solhunter_zero.env_config")
env_config_mod.configure_environment = lambda root: {}
env_config_mod.configure_startup_env = lambda root: {}
sys.modules["solhunter_zero.env_config"] = env_config_mod

sys.modules.setdefault("tomli_w", types.ModuleType("tomli_w"))

system_mod = types.ModuleType("solhunter_zero.system")
system_mod.set_rayon_threads = lambda: None
sys.modules["solhunter_zero.system"] = system_mod

# Provide dummy implementations that emit messages when invoked so the test can
# assert that the script attempted each action.

def _fake_gpu_env():
    env = {
        "SOLHUNTER_GPU_AVAILABLE": "0",
        "SOLHUNTER_GPU_DEVICE": "cpu",
        "TORCH_DEVICE": "cpu",
    }
    for k, v in env.items():
        print(f"{k}={v}")
        os.environ[k] = v
    return env

# Dummy device module

dummy_device = types.ModuleType("solhunter_zero.device")
dummy_device.detect_gpu = lambda: True
dummy_device.get_default_device = lambda: "cpu"
dummy_device.ensure_gpu_env = _fake_gpu_env
dummy_device.initialize_gpu = _fake_gpu_env
dummy_device.METAL_EXTRA_INDEX = []
sys.modules["solhunter_zero.device"] = dummy_device
if "solhunter_zero" in sys.modules:
    setattr(sys.modules["solhunter_zero"], "device", dummy_device)

# Dummy config utils with automatic keypair selection

def _fake_select_keypair(auto=True):
    print("Selected keypair: default")
    return types.SimpleNamespace(name="default", mnemonic_path=None)

config_utils_mod = types.ModuleType("solhunter_zero.config_utils")
config_utils_mod.select_active_keypair = _fake_select_keypair
config_utils_mod.ensure_default_config = lambda *a, **k: None
sys.modules["solhunter_zero.config_utils"] = config_utils_mod

# Dummy bootstrap module to simulate service launches
bootstrap_mod = types.ModuleType("solhunter_zero.bootstrap")
bootstrap_mod.ensure_target = (
    lambda name: print("Launching route-ffi" if name == "route_ffi" else "Launching depth-service")
)
bootstrap_mod.bootstrap = lambda one_click=False: None
sys.modules["solhunter_zero.bootstrap"] = bootstrap_mod

quick_setup_mod = types.ModuleType("scripts.quick_setup")
quick_setup_mod.main = lambda argv: _fake_select_keypair()
sys.modules["scripts.quick_setup"] = quick_setup_mod

# Stub wallet module to avoid heavy crypto imports
wallet_mod = types.ModuleType("solhunter_zero.wallet")
wallet_mod.setup_default_keypair = lambda: _fake_select_keypair()
sys.modules["solhunter_zero.wallet"] = wallet_mod


def test_setup_one_click_dry_run(monkeypatch, capsys):
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(shutil, "which", lambda name: None)

    script = Path("scripts/setup_one_click.py")
    monkeypatch.setattr(sys, "argv", [str(script), "--dry-run"])

    monkeypatch.setattr(os, "execvp", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)

    Path(".env").write_text("")

    runpy.run_path(str(script), run_name="__main__")

    out = capsys.readouterr().out
    assert "Selected keypair: default" in out


def test_regenerates_proto_when_stale(monkeypatch):
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(shutil, "which", lambda name: None)

    script = Path("scripts/setup_one_click.py")
    monkeypatch.setattr(sys, "argv", [str(script), "--dry-run"])

    monkeypatch.setattr(os, "execvp", lambda *a, **k: None)

    event_pb2 = Path("solhunter_zero/event_pb2.py")
    event_proto = Path("proto/event.proto")
    orig_times = (event_pb2.stat().st_atime, event_pb2.stat().st_mtime)
    os.utime(event_pb2, (event_proto.stat().st_atime, event_proto.stat().st_mtime - 10))

    called = {}

    def fake_check_call(cmd, *a, **k):
        called["cmd"] = cmd
        return 0

    monkeypatch.setattr(subprocess, "check_call", fake_check_call)

    Path(".env").write_text("")

    runpy.run_path(str(script), run_name="__main__")

    assert "gen_proto.py" in " ".join(called["cmd"])
    os.utime(event_pb2, orig_times)


def test_single_trading_loop(monkeypatch):
    """setup_one_click should only start one trading loop."""

    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(shutil, "which", lambda name: None)
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    monkeypatch.delenv("AUTO_START", raising=False)
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)

    calls: list[list[str]] = []

    class DummyPM:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def start(self, cmd, *, stream_stderr=False):
            calls.append(cmd)
            return types.SimpleNamespace(poll=lambda: 0)

        def monitor_processes(self):
            pass

    def launch_services(pm):
        pm.start([sys.executable, "-m", "solhunter_zero.main"])

    def launch_ui(pm):
        if os.getenv("AUTO_START") == "1":
            pm.start([sys.executable, "-m", "solhunter_zero.main"])

    def main() -> None:
        with DummyPM() as pm:
            launch_services(pm)
            launch_ui(pm)
            pm.monitor_processes()

    start_all_stub = types.ModuleType("scripts.start_all")
    start_all_stub.ProcessManager = DummyPM
    start_all_stub.launch_services = launch_services
    start_all_stub.launch_ui = launch_ui
    start_all_stub.main = main
    sys.modules["scripts.start_all"] = start_all_stub

    monkeypatch.setattr(os, "execvp", lambda *a, **k: start_all_stub.main())

    script = Path("scripts/setup_one_click.py")
    Path(".env").write_text("")
    runpy.run_path(str(script), run_name="__main__")

    main_calls = [
        cmd for cmd in calls if cmd == [sys.executable, "-m", "solhunter_zero.main"]
    ]
    assert len(main_calls) == 1


def test_purges_corrupt_dist_info(monkeypatch, tmp_path):
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(shutil, "which", lambda name: None)

    script = Path("scripts/setup_one_click.py")
    monkeypatch.setattr(sys, "argv", [str(script), "--dry-run"])
    monkeypatch.setattr(os, "execvp", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)
    monkeypatch.delenv("PYTEST_CURRENT_TEST", raising=False)

    site_dir = tmp_path / "site-packages"
    site_dir.mkdir()
    pkg_dir = site_dir / "solhunter_zero"
    pkg_dir.mkdir()
    dist_dir = site_dir / "solhunter_zero-0.1-invalid.dist-info"
    dist_dir.mkdir()

    monkeypatch.setattr(site, "getsitepackages", lambda: [str(site_dir)])
    monkeypatch.setattr(site, "getusersitepackages", lambda: str(site_dir))

    def fake_run(cmd, *a, **k):
        if cmd[:3] == [sys.executable, "-m", "pip"] and cmd[3] == "show":
            out = f"Name: solhunter-zero\nVersion: invalid\nLocation: {site_dir}\n"
            return types.SimpleNamespace(returncode=0, stdout=out)
        return types.SimpleNamespace(returncode=0, stdout="")

    monkeypatch.setattr(subprocess, "run", fake_run)

    Path(".env").write_text("")
    runpy.run_path(str(script), run_name="__main__")

    assert not pkg_dir.exists()
    assert not dist_dir.exists()


def test_writes_bus_urls(monkeypatch, tmp_path):
    monkeypatch.setenv("PYTEST_CURRENT_TEST", "1")
    monkeypatch.setattr(platform, "system", lambda: "Darwin")
    monkeypatch.setattr(platform, "machine", lambda: "arm64")
    monkeypatch.setattr(shutil, "which", lambda name: None)
    monkeypatch.delenv("EVENT_BUS_URL", raising=False)
    monkeypatch.delenv("BROKER_WS_URLS", raising=False)

    monkeypatch.setattr("solhunter_zero.paths.ROOT", tmp_path)
    script = importlib.reload(importlib.import_module("scripts.setup_one_click"))
    monkeypatch.setattr(script, "ROOT", tmp_path)

    monkeypatch.setattr(os, "execvp", lambda *a, **k: None)
    monkeypatch.setattr(subprocess, "check_call", lambda *a, **k: 0)
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: types.SimpleNamespace(returncode=0))

    script.main([])

    env_text = (tmp_path / ".env").read_text()
    assert "EVENT_BUS_URL=ws://127.0.0.1:8769" in env_text
    assert "BROKER_WS_URLS=ws://127.0.0.1:8769" in env_text

    dummy_cfg = types.ModuleType("solhunter_zero.config")
    dummy_cfg.get_event_bus_peers = lambda cfg=None: []
    dummy_cfg.get_event_bus_url = lambda cfg=None: os.getenv("EVENT_BUS_URL", "")
    sys.modules["solhunter_zero.config"] = dummy_cfg
    import solhunter_zero.event_bus as event_bus
    urls = event_bus._resolve_ws_urls({})
    assert urls == {"ws://127.0.0.1:8769"}


def test_respects_active_venv(monkeypatch, tmp_path):
    import scripts.setup_env as setup_env

    venv_dir = setup_env.ROOT / ".venv"
    if venv_dir.exists():
        shutil.rmtree(venv_dir)

    monkeypatch.setattr(sys, "prefix", sys.base_prefix)
    monkeypatch.setenv("VIRTUAL_ENV", str(tmp_path / "custom"))

    created = {}

    def fake_create(*a, **k):
        created["called"] = True

    monkeypatch.setattr(setup_env.venv, "create", fake_create)
    monkeypatch.setattr(setup_env.os, "execv", lambda *a, **k: pytest.fail("should not execv"))

    setup_env.ensure_venv()

    assert "called" not in created
    assert not venv_dir.exists()
