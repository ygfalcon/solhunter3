"""Smoke tests for the runtime launch CLI."""

from __future__ import annotations

import subprocess
import sys


def test_launch_cli_help() -> None:
    """Ensure the launch CLI imports successfully and shows help."""

    result = subprocess.run(
        [sys.executable, "-m", "solhunter_zero.runtime.launch", "--help"],
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert "Launch SolHunter runtime" in result.stdout
