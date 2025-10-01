import pytest

from solhunter_zero.optional_imports import try_import


def test_try_import_stub():
    sentinel = object()
    mod = try_import("some_missing_mod_xyz", stub=sentinel)
    assert mod is sentinel


def test_try_import_raises():
    with pytest.raises(ImportError):
        try_import("some_missing_mod_xyz")
