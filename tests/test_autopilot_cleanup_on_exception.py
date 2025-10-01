import sys
import types

import pytest


def test_run_auto_exception_triggers_cleanup(monkeypatch, tmp_path):
    wallet_stub = types.ModuleType("wallet")
    wallet_stub.KEYPAIR_DIR = str(tmp_path)
    wallet_stub.setup_default_keypair = lambda: types.SimpleNamespace(name="kp")
    data_sync_stub = types.ModuleType("data_sync")
    data_sync_stub.start_scheduler = lambda *a, **k: None
    data_sync_stub.stop_scheduler = lambda: None
    main_stub = types.ModuleType("main")
    main_stub.run_auto = lambda: None
    config_stub = types.ModuleType("config")
    config_stub.CONFIG_DIR = ""
    config_stub.get_active_config_name = lambda: None
    config_stub.load_config = lambda p: {}
    config_stub.apply_env_overrides = lambda c: c
    config_stub.initialize_event_bus = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.wallet", wallet_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.data_sync", data_sync_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.main", main_stub)
    monkeypatch.setitem(sys.modules, "solhunter_zero.config", config_stub)

    from solhunter_zero import autopilot

    # Avoid modifying global signal handlers and logging configuration
    monkeypatch.setattr(autopilot.signal, "signal", lambda *a, **k: None)
    monkeypatch.setattr(autopilot.logging, "basicConfig", lambda **k: None)

    # Stub device module imported inside main
    device_stub = types.SimpleNamespace(initialize_gpu=lambda: None)
    monkeypatch.setitem(sys.modules, "solhunter_zero.device", device_stub)

    # Skip keypair and configuration setup
    monkeypatch.setattr(autopilot, "_ensure_keypair", lambda: None)
    monkeypatch.setattr(autopilot, "_get_config", lambda: (None, {}))
    monkeypatch.setattr(autopilot, "ROOT", tmp_path)
    monkeypatch.setattr(autopilot, "_maybe_start_event_bus", lambda cfg: None)

    # Track scheduler shutdown
    stop_called = {}
    monkeypatch.setattr(autopilot.data_sync, "start_scheduler", lambda *a, **k: None)
    monkeypatch.setattr(
        autopilot.data_sync,
        "stop_scheduler",
        lambda: stop_called.setdefault("called", True),
    )

    class DummyProc:
        def __init__(self):
            self.terminated = False

        def poll(self):
            return None

        def terminate(self):
            self.terminated = True

        def wait(self, timeout=None):  # pragma: no cover - simple stub
            pass

        def kill(self):  # pragma: no cover - simple stub
            pass

    proc1 = DummyProc()
    proc2 = DummyProc()
    monkeypatch.setattr(autopilot, "start_depth_service", lambda cfg: proc1)
    monkeypatch.setattr(autopilot, "start_rl_daemon", lambda: proc2)
    monkeypatch.setattr(autopilot, "wait_for_depth_ws", lambda *a, **k: None)
    monkeypatch.setattr(autopilot, "PROCS", [])

    def fail():
        raise RuntimeError("boom")

    monkeypatch.setattr(autopilot.main_module, "run_auto", fail)

    with pytest.raises(SystemExit) as exc:
        autopilot.main()

    assert exc.value.code != 0
    assert stop_called.get("called") is True
    assert proc1.terminated is True
    assert proc2.terminated is True

