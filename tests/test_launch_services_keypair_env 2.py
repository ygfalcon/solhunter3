import importlib
import os
import sys
import types
from pathlib import Path


def stub_module(name: str, **attrs) -> None:
    mod = types.ModuleType(name)
    for attr, value in attrs.items():
        setattr(mod, attr, value)
    sys.modules[name] = mod


def test_launch_services_sets_keypair(monkeypatch, tmp_path):
    root = Path(__file__).resolve().parents[1]

    # Minimal stubs for modules imported at startup
    stub_module(
        "solhunter_zero.bootstrap_utils",
        DepsConfig=type("DepsConfig", (), {}),
        ensure_deps=lambda *a, **k: None,
        ensure_venv=lambda argv=None: None,
        prepend_repo_root=lambda: None,
        ensure_cargo=lambda *a, **k: None,
    )
    stub_module("solhunter_zero.logging_utils", log_startup=lambda msg: None)
    stub_module("solhunter_zero.paths", ROOT=root)
    stub_module("solhunter_zero.device", ensure_gpu_env=lambda: None)
    stub_module("solhunter_zero.system", set_rayon_threads=lambda: None)
    stub_module(
        "solhunter_zero.data_sync",
        start_scheduler=lambda *a, **k: None,
        stop_scheduler=lambda: None,
    )

    class DummyProc:
        def poll(self):
            return None

        stderr = None

    def dummy_proc():
        return DummyProc()

    stub_module(
        "solhunter_zero.service_launcher",
        start_depth_service=lambda *a, **k: dummy_proc(),
        start_rl_daemon=lambda: dummy_proc(),
        wait_for_depth_ws=lambda *a, **k: None,
    )
    stub_module(
        "solhunter_zero.autopilot",
        _maybe_start_event_bus=lambda cfg: None,
        shutdown_event_bus=lambda: None,
    )
    stub_module(
        "solhunter_zero.ui",
        rl_ws_loop=None,
        event_ws_loop=None,
        log_ws_loop=None,
        start_websockets=lambda: {},
        create_app=lambda: None,
    )
    stub_module(
        "solhunter_zero.config",
        REQUIRED_ENV_VARS=[],
        set_env_from_config=lambda *a, **k: None,
        ensure_config_file=lambda *a, **k: None,
        validate_env=lambda req, cfg: {"rl_db_path": tmp_path / "db"},
        initialize_event_bus=lambda: None,
        reload_active_config=lambda: None,
    )

    # Dependencies of wallet.ensure_keypair
    keypair_mod = types.ModuleType("keypair")
    keypair_mod.Keypair = type("Keypair", (), {})
    solders_mod = types.ModuleType("solders")
    solders_mod.keypair = keypair_mod
    sys.modules["solders"] = solders_mod
    sys.modules["solders.keypair"] = keypair_mod
    stub_module(
        "bip_utils",
        Bip39SeedGenerator=object,
        Bip44=object,
        Bip44Coins=object,
        Bip44Changes=object,
    )
    stub_module("aiofiles")

    fernet_mod = types.ModuleType("fernet")
    fernet_mod.Fernet = object
    fernet_mod.InvalidToken = Exception
    cryptography_mod = types.ModuleType("cryptography")
    cryptography_mod.fernet = fernet_mod
    sys.modules["cryptography"] = cryptography_mod
    sys.modules["cryptography.fernet"] = fernet_mod

    console_mod = types.ModuleType("console")
    console_mod.Console = object
    rich_mod = types.ModuleType("rich")
    rich_mod.console = console_mod
    sys.modules["rich"] = rich_mod
    sys.modules["rich.console"] = console_mod

    # Import real ensure_keypair and patch wallet behaviour
    import solhunter_zero.bootstrap as real_bootstrap
    import solhunter_zero.wallet as wallet

    wallet.KEYPAIR_DIR = str(tmp_path)

    def fake_setup_default_keypair():
        kp_json_path = Path(wallet.KEYPAIR_DIR) / "default.json"
        kp_json_path.write_text("[]")
        mn_path = Path(wallet.KEYPAIR_DIR) / "default.mnemonic"
        mn_path.write_text("mnemonic")
        return wallet.KeypairInfo("default", mn_path)

    monkeypatch.setattr(wallet, "setup_default_keypair", fake_setup_default_keypair)

    monkeypatch.delenv("SOLANA_KEYPAIR", raising=False)
    monkeypatch.delenv("BROKER_URL", raising=False)
    monkeypatch.delenv("BROKER_URLS", raising=False)
    monkeypatch.setenv("SOLHUNTER_TESTING", "1")

    start_all = importlib.import_module("scripts.start_all")
    monkeypatch.setattr(start_all, "_check_redis_connection", lambda: None)
    start_all.bootstrap.bootstrap = lambda one_click=True: None
    start_all.bootstrap.ensure_keypair = real_bootstrap.ensure_keypair

    captured: dict[str, str | None] = {}

    def fake_start_depth_service(cfg, stream_stderr=False):
        captured["keypair"] = os.environ.get("SOLANA_KEYPAIR")
        return DummyProc()

    start_all.start_depth_service = fake_start_depth_service
    start_all.start_rl_daemon = lambda: DummyProc()
    start_all._wait_for_rl_daemon = lambda proc: None
    start_all.wait_for_depth_ws = lambda *a, **k: None

    pm = start_all.ProcessManager()
    monkeypatch.setattr(pm, "start", lambda cmd, stream_stderr=False: None)

    start_all.launch_services(pm)

    expected = str(Path(wallet.KEYPAIR_DIR) / "default.json")
    assert captured["keypair"] == expected
    assert os.environ["SOLANA_KEYPAIR"] == expected

