from __future__ import annotations

import logging
from pathlib import Path

import pytest

import tomllib

from solhunter_zero.cli import start_live
from solhunter_zero import primary_entry_point
from solhunter_zero.config import ConfigFileNotFound


def test_start_live_console_script_registered() -> None:
    data = tomllib.loads(Path("pyproject.toml").read_text())
    scripts = data.get("project", {}).get("scripts", {})
    assert scripts.get("start_live") == "solhunter_zero.cli.start_live:main"


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
