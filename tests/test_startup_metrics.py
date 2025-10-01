def test_startup_metrics_emitted(monkeypatch):
    import importlib
    import solhunter_zero.config as config

    monkeypatch.setattr(config, "load_config", lambda path=None: {})
    monkeypatch.setattr(config, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(config, "set_env_from_config", lambda cfg: None)
    monkeypatch.setattr(config, "initialize_event_bus", lambda: None)

    main = importlib.reload(importlib.import_module("solhunter_zero.main"))

    calls = {}

    def fake_publish(name, value):
        calls[name] = value

    monkeypatch.setattr(main.metrics_aggregator, "publish", fake_publish)
    monkeypatch.setattr(main, "ensure_connectivity", lambda offline=False: None)
    monkeypatch.setattr(main, "_start_depth_service", lambda cfg: None)

    main.perform_startup(None, offline=True, dry_run=False)

    assert "startup_config_load_duration" in calls
    assert "startup_connectivity_check_duration" in calls
    assert "startup_depth_service_start_duration" in calls

