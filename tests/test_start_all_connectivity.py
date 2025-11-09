import contextlib
import importlib
import os
import types


def test_main_passes_cli_ui_overrides_to_connectivity(monkeypatch, tmp_path):
    start_all = importlib.import_module("scripts.start_all")

    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("UI_HOST", "0.0.0.0")
    monkeypatch.setenv("UI_PORT", "6000")
    monkeypatch.setenv("CONNECTIVITY_SOAK_DURATION", "1")

    monkeypatch.setattr(start_all, "configure_runtime_logging", lambda force=True: None)

    @contextlib.contextmanager
    def dummy_lock(_path):
        yield

    monkeypatch.setattr(start_all, "_acquire_runtime_lock", dummy_lock)
    monkeypatch.setattr(start_all, "kill_lingering_processes", lambda: None)
    monkeypatch.setattr(
        start_all,
        "ensure_environment",
        lambda cfg: {"config_path": str(tmp_path / "cfg.toml"), "config": {}},
    )
    monkeypatch.setattr(start_all, "ensure_live_keypair", lambda cfg: {})
    monkeypatch.setattr(start_all, "_load_production_environment", lambda: {})
    monkeypatch.setattr(start_all, "apply_production_defaults", lambda cfg: {})
    monkeypatch.setattr(start_all, "_validate_keys", lambda: "ok")
    monkeypatch.setattr(start_all, "verify_live_account", lambda: {"skipped": True})
    monkeypatch.setattr(start_all, "rl_health_gate", lambda: None)

    manifest_calls: list[dict[str, str]] = []

    def fake_write_manifest(_loaded_env):
        host = os.environ.get("UI_HOST")
        port = os.environ.get("UI_PORT")
        manifest_calls.append({"host": host, "port": port})
        path = tmp_path / "manifest.json"
        path.write_text("{}")
        return path

    monkeypatch.setattr(start_all, "_write_manifest", fake_write_manifest)

    connectivity_envs: list[dict[str, str]] = []
    connectivity_targets: list[str] = []

    class DummyConnectivityChecker:
        def __init__(self, *_, **__):
            connectivity_envs.append(
                {"host": os.environ.get("UI_HOST"), "port": os.environ.get("UI_PORT")}
            )

        async def check_all(self):
            host = os.environ.get("UI_HOST")
            port = os.environ.get("UI_PORT")
            target = f"http://{host}:{port}/health"
            connectivity_targets.append(target)
            return [
                types.SimpleNamespace(
                    name="ui-http",
                    target=target,
                    ok=True,
                    latency_ms=1.0,
                    status="OK",
                    status_code=200,
                    error=None,
                )
            ]

        async def run_soak(self, duration, output_path):
            return types.SimpleNamespace(duration=duration, metrics={}, reconnect_count=0)

    monkeypatch.setattr(start_all, "ConnectivityChecker", DummyConnectivityChecker)

    def fake_launch_foreground(args, cfg_path):
        assert cfg_path.endswith("cfg.toml")
        return 0

    monkeypatch.setattr(start_all, "launch_foreground", fake_launch_foreground)

    def fail_launch_detached(*_args, **_kwargs):  # pragma: no cover - sanity guard
        raise AssertionError("detached launch not expected")

    monkeypatch.setattr(start_all, "launch_detached", fail_launch_detached)

    exit_code = start_all.main(
        [
            "--config",
            str(tmp_path / "cfg.toml"),
            "--ui-host",
            "8.8.8.8",
            "--ui-port",
            "8888",
            "--foreground",
            "--skip-clean",
        ]
    )

    assert exit_code == 0
    assert connectivity_targets == ["http://8.8.8.8:8888/health"]
    assert manifest_calls == [{"host": "8.8.8.8", "port": "8888"}]
    assert connectivity_envs == [
        {"host": "8.8.8.8", "port": "8888"},
        {"host": "8.8.8.8", "port": "8888"},
    ]
