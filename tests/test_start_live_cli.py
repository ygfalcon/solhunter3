from __future__ import annotations

import logging
import os
from pathlib import Path

import pytest

import tomllib

from solhunter_zero import primary_entry_point
from solhunter_zero.cli import start_live
from solhunter_zero.config import ConfigFileNotFound
from solhunter_zero.ui import UIStartupError


def test_start_live_console_script_registered() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text())
    scripts = data.get("project", {}).get("scripts", {})
    assert scripts.get("start_live") == "solhunter_zero.cli.start_live:main"


def test_start_live_sets_new_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NEW_PIPELINE", raising=False)

    def fake_main(args: list[str] | None) -> int:
        assert os.environ.get("NEW_PIPELINE") == "1"
        return 0

    monkeypatch.setattr(primary_entry_point, "main", fake_main)

    exit_code = start_live.main([])

    assert exit_code == 0


def test_start_live_requires_config(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc:
        start_live.main([])
    out = capsys.readouterr()
    assert exc.value.code != 0
    assert "--config" in out.err


def test_start_live_reports_missing_config(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    missing_config = "/path/to/missing-config.toml"

    class FakeRuntime:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - trivial
            pass

        def run_forever(self) -> None:
            raise ConfigFileNotFound(missing_config)

    caplog.set_level(logging.ERROR, logger="solhunter_zero.primary_entry_point")
    monkeypatch.setattr(primary_entry_point, "TradingRuntime", FakeRuntime)

    exit_code = start_live.main(["--config", missing_config])

    assert exit_code == 1
    assert any(
        "Unable to start trading runtime" in record.getMessage()
        and missing_config in record.getMessage()
        for record in caplog.records
    )


def test_start_live_reports_ui_startup_error(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ui_error_message = "UI failed to bind port"

    class FakeRuntime:
        def __init__(self, *args, **kwargs) -> None:  # pragma: no cover - trivial
            pass

        def run_forever(self) -> None:
            raise UIStartupError(ui_error_message)

    caplog.set_level(logging.ERROR, logger="solhunter_zero.primary_entry_point")
    monkeypatch.setattr(primary_entry_point, "TradingRuntime", FakeRuntime)

    exit_code = start_live.main(["--config", "/path/to/config.toml"])

    assert exit_code == 1
    assert any(
        "Unable to start trading runtime UI" in record.getMessage()
        and ui_error_message in record.getMessage()
        for record in caplog.records
    )
