import importlib
import sys
import types
import threading
from pathlib import Path

import pytest


def stub_module(name: str, **attrs) -> None:
    mod = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(mod, attr, value)
    sys.modules[name] = mod


class DummyProc:
    def poll(self):
        return None

    def terminate(self):
        pass

    def wait(self, timeout=None):
        return 0

    def kill(self):
        pass


class DummyProcessManager:
    def __init__(self):
        self.procs = []
        self.ws_threads = {}
        self.redis_proc = None
        self.stop_called = False

    def start(self, cmd, stream_stderr=False):
        proc = DummyProc()
        self.procs.append(proc)
        return proc

    def stop_all(self, *_args, **_kwargs):
        self.stop_called = True


def test_rl_daemon_starts_before_ui(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    stub_module(
        "solhunter_zero.bootstrap_utils",
        ensure_venv=lambda argv=None: None,
        prepend_repo_root=lambda: None,
        ensure_cargo=lambda: None,
    )
    stub_module("solhunter_zero.logging_utils", log_startup=lambda msg: None)
    stub_module("solhunter_zero.paths", ROOT=root)
    stub_module("solhunter_zero.device", ensure_gpu_env=lambda: None)
    stub_module("solhunter_zero.system", set_rayon_threads=lambda: None)
    stub_module(
        "solhunter_zero.config",
        REQUIRED_ENV_VARS=[],
        set_env_from_config=lambda *a, **k: None,
        ensure_config_file=lambda *a, **k: "cfg.toml",
        validate_env=lambda *a, **k: {},
        initialize_event_bus=lambda: None,
        reload_active_config=lambda: None,
        get_solana_ws_url=lambda: "ws://",
    )
    stub_module(
        "solhunter_zero.data_sync",
        start_scheduler=lambda *a, **k: None,
        stop_scheduler=lambda: None,
    )

    call_order: list[str] = []
    ui_run = threading.Event()

    def fake_start_rl_daemon():
        call_order.append("rl")
        return DummyProc()

    def fake_create_app(*_args, **_kwargs):
        call_order.append("ui")

        class App:
            def run(self):
                ui_run.set()

        return App()

    stub_module(
        "solhunter_zero.autopilot",
        _maybe_start_event_bus=lambda cfg: None,
        shutdown_event_bus=lambda: None,
    )
    stub_module(
        "solhunter_zero.bootstrap",
        bootstrap=lambda one_click=True: None,
        ensure_keypair=lambda: None,
    )
    stub_module(
        "solhunter_zero.service_launcher",
        start_depth_service=lambda cfg, stream_stderr=True: DummyProc(),
        start_rl_daemon=fake_start_rl_daemon,
        wait_for_depth_ws=lambda *a, **k: None,
    )
    stub_module(
        "solhunter_zero.ui",
        rl_ws_loop=None,
        event_ws_loop=None,
        log_ws_loop=None,
        start_websockets=lambda: {},
        create_app=fake_create_app,
    )

    start_all = importlib.import_module("scripts.start_all")
    monkeypatch.setattr(start_all, "_wait_for_rl_daemon", lambda *a, **k: None)
    monkeypatch.setattr(start_all, "_is_port_open", lambda h, p: True)
    monkeypatch.setattr(start_all.webbrowser, "open", lambda *a, **k: None)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    monkeypatch.setenv("BROKER_URL", "redis://localhost:6379")

    pm = DummyProcessManager()
    start_all.launch_services(pm)
    start_all.launch_ui(pm)

    assert ui_run.wait(1.0)
    assert call_order == ["rl", "ui"]


def test_launch_ui_aborts_on_ws_error(monkeypatch):
    root = Path(__file__).resolve().parents[1]
    messages: list[str] = []
    stub_module(
        "solhunter_zero.bootstrap_utils",
        ensure_venv=lambda argv=None: None,
        prepend_repo_root=lambda: None,
        ensure_cargo=lambda: None,
    )
    stub_module("solhunter_zero.logging_utils", log_startup=lambda msg: messages.append(msg))
    stub_module("solhunter_zero.paths", ROOT=root)
    stub_module("solhunter_zero.device", ensure_gpu_env=lambda: None)
    stub_module("solhunter_zero.system", set_rayon_threads=lambda: None)
    stub_module(
        "solhunter_zero.config",
        REQUIRED_ENV_VARS=[],
        set_env_from_config=lambda *a, **k: None,
        ensure_config_file=lambda *a, **k: "cfg.toml",
        validate_env=lambda *a, **k: {},
        initialize_event_bus=lambda: None,
    )
    stub_module(
        "solhunter_zero.data_sync",
        start_scheduler=lambda *a, **k: None,
        stop_scheduler=lambda: None,
    )

    ui_run = threading.Event()

    def fake_create_app(*_args, **_kwargs):
        class App:
            def run(self):
                ui_run.set()

        return App()

    def fake_start_websockets():
        raise RuntimeError("fail")

    stub_module(
        "solhunter_zero.autopilot",
        _maybe_start_event_bus=lambda cfg: None,
        shutdown_event_bus=lambda: None,
    )
    stub_module("solhunter_zero.bootstrap", bootstrap=lambda one_click=True: None)
    stub_module(
        "solhunter_zero.service_launcher",
        start_depth_service=lambda cfg, stream_stderr=True: DummyProc(),
        start_rl_daemon=lambda: DummyProc(),
        wait_for_depth_ws=lambda *a, **k: None,
    )
    stub_module(
        "solhunter_zero.ui",
        start_websockets=fake_start_websockets,
        create_app=fake_create_app,
    )

    sys.modules.pop("scripts.start_all", None)
    start_all = importlib.import_module("scripts.start_all")
    monkeypatch.setattr(start_all.webbrowser, "open", lambda *a, **k: None)

    pm = DummyProcessManager()
    start_all.launch_ui(pm)
    pm.ui_thread.join(timeout=1)

    assert not ui_run.is_set()
    assert pm.stop_called
    assert any("Websocket initialization failed" in m for m in messages)
