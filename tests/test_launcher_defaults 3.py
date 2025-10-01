import importlib
import sys


def test_inject_defaults_handles_assigned_values():
    launcher = importlib.import_module("solhunter_zero.launcher")
    inject = launcher._inject_defaults
    assert inject(["--one-click=false"]) == ["--full-deps", "--one-click=false"]
    assert inject(["--full-deps=yes"]) == ["--one-click", "--full-deps=yes"]
    assert inject(["--one-click=false", "--full-deps=no"]) == ["--one-click=false", "--full-deps=no"]
    sys.modules.pop("solhunter_zero.launcher", None)
