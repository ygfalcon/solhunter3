import importlib
import sys
import types
import threading
from pathlib import Path

import pytest


def stub_module(monkeypatch, name: str, **attrs) -> None:
    mod = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(mod, attr, value)
    monkeypatch.setitem(sys.modules, name, mod)


def test_wait_for_rl_daemon_waits_for_heartbeat(monkeypatch):
    root = Path(__file__).resolve().parents[1]

    stub_module(
        monkeypatch,
        "solhunter_zero.bootstrap_utils",
        ensure_venv=lambda argv=None: None,
        prepend_repo_root=lambda: None,
        ensure_cargo=lambda *a, **k: None,
    )
    stub_module(monkeypatch, "solhunter_zero.logging_utils", log_startup=lambda msg: None)
    stub_module(monkeypatch, "solhunter_zero.paths", ROOT=root)
    stub_module(monkeypatch, "solhunter_zero.device", ensure_gpu_env=lambda: None)
    stub_module(monkeypatch, "solhunter_zero.system", set_rayon_threads=lambda: None)
    stub_module(
        monkeypatch,
        "solhunter_zero.data_sync",
        start_scheduler=lambda *a, **k: None,
        stop_scheduler=lambda: None,
    )
    stub_module(
        monkeypatch,
        "solhunter_zero.service_launcher",
        start_depth_service=lambda *a, **k: None,
        start_rl_daemon=lambda: None,
        wait_for_depth_ws=lambda *a, **k: None,
    )
    stub_module(
        monkeypatch,
        "solhunter_zero.autopilot",
        _maybe_start_event_bus=lambda cfg: None,
        shutdown_event_bus=lambda: None,
    )
    stub_module(
        monkeypatch,
        "solhunter_zero.bootstrap",
        bootstrap=lambda one_click=True: None,
        ensure_keypair=lambda: None,
    )
    stub_module(
        monkeypatch,
        "solhunter_zero.config",
        REQUIRED_ENV_VARS=[],
        set_env_from_config=lambda *a, **k: None,
        ensure_config_file=lambda *a, **k: None,
        validate_env=lambda *a, **k: {},
        initialize_event_bus=lambda: None,
        reload_active_config=lambda: None,
    )
    stub_module(
        monkeypatch,
        "solhunter_zero.ui",
        rl_ws_loop=None,
        event_ws_loop=None,
        log_ws_loop=None,
        start_websockets=lambda: {},
        create_app=lambda *a, **k: None,
    )

    start_all = importlib.import_module("scripts.start_all")

    from solhunter_zero.event_bus import publish, reset

    reset()

    class DummyProc:
        def poll(self):
            return None
        stderr = None

    # Without heartbeat, should timeout quickly
    with pytest.raises(TimeoutError):
        start_all._wait_for_rl_daemon(DummyProc(), timeout=0.1)

    reset()
    proc = DummyProc()
    threading.Timer(0.05, lambda: publish("heartbeat", {"service": "rl_daemon"})).start()
    start_all._wait_for_rl_daemon(proc, timeout=1.0)

    monkeypatch.delitem(sys.modules, "scripts.start_all", raising=False)
