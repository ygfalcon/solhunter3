import importlib
import runpy
import sys
import os
from pathlib import Path
import types

import pytest


def test_start_all_imports(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    scripts_dir = root / "scripts"

    bu = types.ModuleType("solhunter_zero.bootstrap_utils")
    sys.modules.setdefault("aiofiles", types.ModuleType("aiofiles"))
    solders = types.ModuleType("solders")
    keypair = types.ModuleType("keypair")
    keypair.Keypair = type("Keypair", (), {})
    solders.keypair = keypair  # type: ignore[attr-defined]
    sys.modules["solders"] = solders
    sys.modules["solders.keypair"] = keypair
    bip_utils = types.ModuleType("bip_utils")
    bip_utils.Bip39SeedGenerator = bip_utils.Bip44 = bip_utils.Bip44Coins = bip_utils.Bip44Changes = object
    sys.modules["bip_utils"] = bip_utils
    autopilot = types.ModuleType("solhunter_zero.autopilot")
    autopilot._maybe_start_event_bus = lambda cfg: None
    autopilot.shutdown_event_bus = lambda: None
    sys.modules["solhunter_zero.autopilot"] = autopilot
    ui = types.ModuleType("solhunter_zero.ui")
    ui.rl_ws_loop = ui.event_ws_loop = ui.log_ws_loop = None
    ui.create_app = lambda *a, **k: None
    ui.start_websockets = lambda: {}
    sys.modules["solhunter_zero.ui"] = ui
    bootstrap_module = types.ModuleType("solhunter_zero.bootstrap")
    bootstrap_module.bootstrap = lambda one_click=True: None
    bootstrap_module.ensure_keypair = lambda: None
    sys.modules["solhunter_zero.bootstrap"] = bootstrap_module

    def ensure_venv(argv=None):
        return None

    def ensure_cargo(*a, **k):
        return None

    def prepend_repo_root() -> None:
        if str(root) not in sys.path:
            sys.path.insert(0, str(root))

    bu.ensure_venv = ensure_venv
    bu.ensure_cargo = ensure_cargo
    bu.prepend_repo_root = prepend_repo_root
    sys.modules["solhunter_zero.bootstrap_utils"] = bu

    monkeypatch.setattr(sys, "prefix", str(root / ".venv"))
    monkeypatch.setenv("SOLHUNTER_TESTING", "1")

    sys.modules.pop("solhunter_zero.device", None)
    original_path = list(sys.path)
    sys.path[:] = [str(scripts_dir)] + [p for p in original_path if p not in (str(root), str(scripts_dir))]
    monkeypatch.chdir(scripts_dir)

    runpy.run_path("start_all.py", run_name="not_main")

    assert "solhunter_zero.device" in sys.modules


def test_ensure_venv_skips_when_active(monkeypatch, tmp_path):
    root = Path(__file__).resolve().parents[1]

    device = types.ModuleType("solhunter_zero.device")
    device.METAL_EXTRA_INDEX = []
    device.initialize_gpu = lambda: None
    sys.modules["solhunter_zero.device"] = device

    logging_utils = types.ModuleType("solhunter_zero.logging_utils")
    logging_utils.log_startup = lambda msg: None
    sys.modules["solhunter_zero.logging_utils"] = logging_utils

    paths = types.ModuleType("solhunter_zero.paths")
    paths.ROOT = root
    sys.modules["solhunter_zero.paths"] = paths

    preflight = types.ModuleType("solhunter_zero.preflight_utils")
    preflight.check_internet = lambda: (True, "")
    sys.modules["solhunter_zero.preflight_utils"] = preflight

    import solhunter_zero.bootstrap_utils as bu

    # Ensure repository root isn't already on sys.path
    path = [p for p in sys.path if p != str(bu.ROOT)]
    monkeypatch.setattr(sys, "path", path)

    called: list[str] = []

    def fake_needs_recreation():
        called.append("needs")
        return None, ""

    monkeypatch.setattr(bu, "_venv_needs_recreation", fake_needs_recreation)
    execv_called: list[tuple] = []
    monkeypatch.setattr(os, "execv", lambda *a, **k: execv_called.append(a))
    monkeypatch.setattr(bu, "VENV_DIR", tmp_path / ".venv")
    monkeypatch.setattr(sys, "prefix", "/existing")
    monkeypatch.setattr(sys, "base_prefix", "/usr")

    bu.ensure_venv(None)

    assert not called
    assert not execv_called
    assert not (tmp_path / ".venv").exists()
    assert str(bu.ROOT) in sys.path


def test_parse_args_rejects_non_numeric_port(monkeypatch):
    monkeypatch.delenv("UI_PORT", raising=False)
    start_all = importlib.reload(importlib.import_module("scripts.start_all"))
    with pytest.raises(SystemExit):
        start_all.parse_args(["--ui-port", "invalid"])
