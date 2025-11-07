from __future__ import annotations

from pathlib import Path

import pytest

import tomllib

from solhunter_zero.cli import start_live


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
