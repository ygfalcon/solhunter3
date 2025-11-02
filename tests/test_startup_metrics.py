import importlib

import solhunter_zero.config as config


def _reload_main(monkeypatch):
    monkeypatch.setattr(config, "load_config", lambda path=None: {})
    monkeypatch.setattr(config, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(config, "set_env_from_config", lambda cfg: None)
    monkeypatch.setattr(config, "initialize_event_bus", lambda: None)
    return importlib.reload(importlib.import_module("solhunter_zero.main"))


def test_startup_metrics_emitted(monkeypatch):
    main = _reload_main(monkeypatch)

    calls = {}

    def fake_publish(name, value):
        calls[name] = value

    async def fake_connectivity_async(*, offline=False):
        return None

    monkeypatch.setattr(main.metrics_aggregator, "publish", fake_publish)
    monkeypatch.setattr(main, "ensure_connectivity_async", fake_connectivity_async)
    monkeypatch.setattr(main, "_start_depth_service", lambda cfg: None)

    main.perform_startup(None, offline=True, dry_run=False)

    assert "startup_config_load_duration" in calls
    assert "startup_connectivity_check_duration" in calls
    assert "startup_depth_service_start_duration" in calls


def test_startup_dry_run_skips_depth_service(monkeypatch):
    main = _reload_main(monkeypatch)

    async def fake_connectivity_async(*, offline=False):
        return None

    called = False

    def fake_start(cfg):
        nonlocal called
        called = True
        return object()

    monkeypatch.setattr(main, "ensure_connectivity_async", fake_connectivity_async)
    monkeypatch.setattr(main.metrics_aggregator, "publish", lambda *a, **k: None)
    monkeypatch.setattr(main, "_start_depth_service", fake_start)

    _, _, proc = main.perform_startup(None, offline=True, dry_run=True)

    assert proc is None
    assert called is False

