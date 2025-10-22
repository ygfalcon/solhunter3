"""Import smoke tests for runtime modules."""

import importlib
import pkgutil

import solhunter_zero.runtime as runtime_pkg


def test_runtime_imports_clean() -> None:
    """Ensure all runtime modules import without raising exceptions."""

    for module in pkgutil.iter_modules(runtime_pkg.__path__, runtime_pkg.__name__ + "."):
        importlib.import_module(module.name)
