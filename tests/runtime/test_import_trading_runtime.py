"""Ensure trading runtime imports without triggering circular references."""

import importlib

# Import the runtime test helpers to install optional dependency stubs before
# importing the trading runtime module.
import tests.runtime.test_trading_runtime  # noqa: F401


def test_trading_runtime_imports_clean() -> None:
    importlib.import_module("solhunter_zero.runtime.trading_runtime")
