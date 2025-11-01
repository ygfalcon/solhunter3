from __future__ import annotations


def test_startup_forwards_cli_rest(monkeypatch):
    from scripts import startup as startup_mod
    from solhunter_zero import (
        startup_checks,
        startup_runner,
        config as config_module,
        logging_utils,
        agent_manager,
    )
    from scripts import start_all as start_all_module
    from scripts import healthcheck as healthcheck_module

    recorded: dict[str, object] = {}

    def fake_perform_checks(args, rest, **kwargs):
        recorded["rest_arg"] = rest
        return {"code": 0}

    monkeypatch.setattr(startup_checks, "perform_checks", fake_perform_checks)

    monkeypatch.setattr(
        agent_manager.AgentManager,
        "from_config",
        classmethod(lambda cls, cfg: object()),
    )
    monkeypatch.setattr(startup_runner.preflight, "CHECKS", [])
    monkeypatch.setattr(healthcheck_module, "main", lambda *a, **k: 0)

    start_all_calls: dict[str, list[str]] = {}

    def fake_start_all_main(argv):
        start_all_calls["argv"] = list(argv)
        return 0

    monkeypatch.setattr(start_all_module, "main", fake_start_all_main)

    monkeypatch.setattr(config_module, "load_config", lambda *a, **k: {})
    monkeypatch.setattr(config_module, "find_config_file", lambda: None)
    monkeypatch.setattr(logging_utils, "log_startup", lambda *a, **k: None)
    monkeypatch.setattr(logging_utils, "rotate_preflight_log", lambda *a, **k: None)

    code = startup_mod.main(["--foo"])

    assert code == 0
    assert recorded["rest_arg"] == ["--foo"]
    assert start_all_calls["argv"][0] == "--foo"
    assert "--foreground" in start_all_calls["argv"]
