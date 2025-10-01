import importlib
import sys
import types
from pathlib import Path
import socket
import threading
import time
import os
import pytest


def stub_module(name: str, **attrs) -> None:
    mod = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(mod, attr, value)
    sys.modules[name] = mod


class DummyProc:
    def __init__(self) -> None:
        self._terminated = False

    def poll(self) -> int | None:
        return 0 if self._terminated else None

    def terminate(self) -> None:
        self._terminated = True

    def wait(self, timeout: float | None = None) -> int:
        self._terminated = True
        return 0

    def kill(self) -> None:
        self._terminated = True


def fake_popen(cmd, *_, **__):
    if cmd[0] == "redis-server":
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", 6379))
        srv.listen()
        stop = threading.Event()

        def run():
            while not stop.is_set():
                try:
                    conn, _ = srv.accept()
                    conn.close()
                except OSError:
                    break

        threading.Thread(target=run, daemon=True).start()

        class RedisProc(DummyProc):
            def terminate(self):
                stop.set()
                try:
                    srv.close()
                finally:
                    super().terminate()

            def kill(self):
                stop.set()
                try:
                    srv.close()
                finally:
                    super().kill()

        return RedisProc()
    return DummyProc()


def test_launch_services_starts_and_stops_redis(monkeypatch):
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
        ensure_config_file=lambda *a, **k: None,
        validate_env=lambda *a, **k: {},
        initialize_event_bus=lambda: None,
        reload_active_config=lambda: None,
    )
    stub_module(
        "solhunter_zero.data_sync",
        start_scheduler=lambda *a, **k: None,
        stop_scheduler=lambda: None,
    )
    called = {}
    stub_module(
        "solhunter_zero.autopilot",
        _maybe_start_event_bus=lambda cfg: called.setdefault("maybe", True),
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
        start_rl_daemon=lambda: DummyProc(),
        wait_for_depth_ws=lambda *a, **k: None,
    )
    stub_module(
        "solhunter_zero.ui",
        rl_ws_loop=None,
        event_ws_loop=None,
        log_ws_loop=None,
        start_websockets=lambda: {},
        create_app=lambda *a, **k: None,
    )

    start_all = importlib.import_module("scripts.start_all")
    monkeypatch.setattr(start_all, "_wait_for_rl_daemon", lambda *a, **k: None)
    monkeypatch.setattr(start_all.subprocess, "Popen", fake_popen)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    monkeypatch.setenv("BROKER_URL", "redis://localhost:6379")

    with pytest.raises(SystemExit):
        with start_all.ProcessManager() as pm:
            start_all.launch_services(pm)
            redis_proc = pm.redis_proc
            assert redis_proc is not None
            assert called["maybe"]
    assert redis_proc._terminated
