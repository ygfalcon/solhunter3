import os
import subprocess
import sys
from pathlib import Path

import torch

from solhunter_zero import device as device_module


def test_get_default_device_mps(monkeypatch):
    monkeypatch.setattr(torch.cuda, "is_available", lambda: False)
    monkeypatch.setattr(torch.backends.mps, "is_available", lambda: True)
    assert getattr(device_module.get_default_device(), "type", None) == "mps"


def test_cli_detects_mps(tmp_path):
    stubs = tmp_path / "stubs"
    stubs.mkdir()

    (stubs / "platform.py").write_text("def system():\n    return 'Darwin'\n")
    (stubs / "torch.py").write_text(
        """
class _Cuda:
    @staticmethod
    def is_available():
        return False

class _Mps:
    @staticmethod
    def is_available():
        return True

class _Backends:
    mps = _Mps()

cuda = _Cuda()
backends = _Backends()
"""
    )

    env = os.environ.copy()
    env["PYTHONPATH"] = f"{stubs}:{Path.cwd()}:{env.get('PYTHONPATH', '')}"

    result = subprocess.run(
        [sys.executable, "-m", "solhunter_zero.device", "--check-gpu"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0
