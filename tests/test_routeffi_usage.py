import pytest
import importlib

import solhunter_zero.arbitrage as arb


def _build_prices():
    return {"dex1": 1.0, "dex2": 1.2, "dex3": 1.1}


@pytest.fixture
def ensure_ffi(monkeypatch):
    from pathlib import Path
    import subprocess

    lib_path = Path(__file__).resolve().parents[1] / "route_ffi/target/release/libroute_ffi.so"
    if not lib_path.exists():
        subprocess.run(
            [
                "cargo",
                "build",
                "--manifest-path",
                str(Path(__file__).resolve().parents[1] / "route_ffi/Cargo.toml"),
                "--release",
            ],
            check=True,
        )
    monkeypatch.setenv("ROUTE_FFI_LIB", str(lib_path))
    importlib.reload(arb._routeffi)
    importlib.reload(arb)
    yield


def test_ffi_called(monkeypatch, ensure_ffi):
    called = {}
    real = arb._routeffi.best_route

    def wrapper(*a, **k):
        called['used'] = True
        return real(*a, **k)

    monkeypatch.setattr(arb._routeffi, 'best_route', wrapper)
    prices = _build_prices()
    arb._best_route(prices, 1.0)
    assert called.get('used', False)


def test_ffi_matches_python(monkeypatch, ensure_ffi):
    prices = _build_prices()
    py_res = arb._best_route_py(prices, 1.0)
    ffi_res = arb._best_route(prices, 1.0)
    assert ffi_res == py_res


def test_default_uses_ffi(monkeypatch, ensure_ffi):
    monkeypatch.delenv("USE_FFI_ROUTE", raising=False)
    importlib.reload(arb._routeffi)
    importlib.reload(arb)
    assert arb.USE_FFI_ROUTE

