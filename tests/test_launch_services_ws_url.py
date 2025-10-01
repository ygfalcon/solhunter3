import importlib
import os
import sys
import types


def test_launch_services_derives_ws_url(monkeypatch, tmp_path):
    bu = types.ModuleType("solhunter_zero.bootstrap_utils")
    bu.ensure_venv = lambda argv=None: None
    bu.prepend_repo_root = lambda: None
    bu.ensure_cargo = lambda *a, **k: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.bootstrap_utils", bu)

    autopilot = types.ModuleType("solhunter_zero.autopilot")
    autopilot._maybe_start_event_bus = lambda cfg: None
    autopilot.shutdown_event_bus = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.autopilot", autopilot)

    ui = types.ModuleType("solhunter_zero.ui")
    ui.rl_ws_loop = ui.event_ws_loop = ui.log_ws_loop = None
    ui.create_app = lambda *a, **k: None
    ui.start_websockets = lambda: {}
    monkeypatch.setitem(sys.modules, "solhunter_zero.ui", ui)

    bootstrap = types.ModuleType("solhunter_zero.bootstrap")
    bootstrap.bootstrap = lambda one_click=True: None
    bootstrap.ensure_keypair = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.bootstrap", bootstrap)

    solders = types.ModuleType("solders")
    keypair = types.ModuleType("keypair")
    keypair.Keypair = type("Keypair", (), {})
    solders.keypair = keypair
    monkeypatch.setitem(sys.modules, "solders", solders)
    monkeypatch.setitem(sys.modules, "solders.keypair", keypair)

    bip_utils = types.ModuleType("bip_utils")
    bip_utils.Bip39SeedGenerator = (
        bip_utils.Bip44
    ) = bip_utils.Bip44Coins = bip_utils.Bip44Changes = object
    monkeypatch.setitem(sys.modules, "bip_utils", bip_utils)

    monkeypatch.setitem(sys.modules, "aiofiles", types.ModuleType("aiofiles"))

    data_sync = types.ModuleType("solhunter_zero.data_sync")
    data_sync.start_scheduler = lambda *a, **k: None
    data_sync.stop_scheduler = lambda: None
    monkeypatch.setitem(sys.modules, "solhunter_zero.data_sync", data_sync)

    service_launcher = types.ModuleType("solhunter_zero.service_launcher")
    service_launcher.start_depth_service = lambda *a, **k: None
    service_launcher.start_rl_daemon = lambda *a, **k: None
    service_launcher.wait_for_depth_ws = lambda *a, **k: None
    monkeypatch.setitem(
        sys.modules, "solhunter_zero.service_launcher", service_launcher
    )

    monkeypatch.setenv("SOLHUNTER_TESTING", "1")

    start_all = importlib.import_module("scripts.start_all")

    import inspect

    src = inspect.getsource(start_all.launch_services)
    src = src.replace(
        "    import solhunter_zero.config as config  # noqa: E402\n", ""
    )
    namespace: dict[str, object] = {}
    exec(src, start_all.__dict__, namespace)
    start_all.launch_services = namespace["launch_services"]  # type: ignore[assignment]

    monkeypatch.setenv("SOLANA_RPC_URL", "https://rpc.example.com")
    monkeypatch.delenv("SOLANA_WS_URL", raising=False)

    monkeypatch.setattr(start_all.config, "REQUIRED_ENV_VARS", ())
    monkeypatch.setattr(start_all, "ensure_config_file", lambda: None)
    monkeypatch.setattr(
        start_all, "validate_env", lambda req, cfg: {"rl_db_path": tmp_path / "db"}
    )
    monkeypatch.setattr(start_all, "set_env_from_config", lambda cfg: None)
    monkeypatch.setattr(start_all.config, "reload_active_config", lambda: None)
    monkeypatch.setattr(start_all, "_maybe_start_event_bus", lambda cfg: None)
    monkeypatch.setattr(start_all, "initialize_event_bus", lambda: None)
    monkeypatch.setattr(start_all, "ensure_cargo", lambda: None)
    monkeypatch.setattr(start_all, "wait_for_depth_ws", lambda *a, **k: None)
    monkeypatch.setattr(start_all, "_wait_for_rl_daemon", lambda *a, **k: None)

    class DummyProc:
        def poll(self):  # pragma: no cover - simple stub
            return None

        stderr = None

    monkeypatch.setattr(start_all, "start_rl_daemon", lambda: DummyProc())

    captured: dict[str, str | None] = {}

    def fake_start_depth_service(cfg, stream_stderr=False):
        captured["ws"] = os.environ.get("SOLANA_WS_URL")
        return DummyProc()

    monkeypatch.setattr(start_all, "start_depth_service", fake_start_depth_service)

    pm = start_all.ProcessManager()
    monkeypatch.setattr(pm, "start", lambda cmd, stream_stderr=False: None)
    monkeypatch.setattr(start_all, "ROOT", tmp_path)

    start_all.launch_services(pm)

    assert captured["ws"] == "wss://rpc.example.com"

