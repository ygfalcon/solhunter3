import importlib
import sys
import types
import asyncio

import pytest


def _make_torch(available=True, mps_available=False):
    """Return a minimal torch stub."""

    torch_mod = types.ModuleType("torch")
    torch_mod.__spec__ = importlib.machinery.ModuleSpec("torch", None)
    torch_mod.cuda = types.SimpleNamespace(is_available=lambda: available)
    torch_mod.backends = types.SimpleNamespace(
        mps=types.SimpleNamespace(is_available=lambda: mps_available, is_built=lambda: True)
    )
    torch_mod.ones = lambda *a, **k: types.SimpleNamespace(cpu=lambda: None)
    torch_mod.Tensor = type("Tensor", (), {})
    return torch_mod


def _make_cupy(available=True):
    cp_mod = types.ModuleType("cupy")
    cp_mod.__spec__ = importlib.machinery.ModuleSpec("cupy", None)
    runtime = types.SimpleNamespace(getDeviceCount=lambda: 1 if available else 0)
    cp_mod.cuda = types.SimpleNamespace(runtime=runtime)
    return cp_mod


def test_simulation_auto_enables_gpu(monkeypatch):
    monkeypatch.delenv("USE_GPU_SIM", raising=False)
    torch_mod = _make_torch(True)
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setattr("solhunter_zero.device.torch", torch_mod, raising=False)
    monkeypatch.setitem(sys.modules, "cupy", _make_cupy(False))
    import solhunter_zero.simulation as sim
    importlib.reload(sim)
    assert sim.USE_GPU_SIM is True
    assert sim._GPU_BACKEND == "torch"


def test_simulation_env_disables_gpu(monkeypatch):
    monkeypatch.setenv("USE_GPU_SIM", "0")
    torch_mod = _make_torch(True)
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setattr("solhunter_zero.device.torch", torch_mod, raising=False)
    monkeypatch.setitem(sys.modules, "cupy", _make_cupy(True))
    import solhunter_zero.simulation as sim
    importlib.reload(sim)
    assert sim.USE_GPU_SIM is False


def test_mps_detected_disables_gpu_index(monkeypatch):
    monkeypatch.delenv("FORCE_CPU_INDEX", raising=False)
    monkeypatch.delenv("GPU_MEMORY_INDEX", raising=False)

    torch_mod = _make_torch(available=False, mps_available=True)
    monkeypatch.setitem(sys.modules, "torch", torch_mod)
    monkeypatch.setattr("solhunter_zero.device.torch", torch_mod, raising=False)
    monkeypatch.setattr("solhunter_zero.device.platform.system", lambda: "Darwin")
    monkeypatch.setattr("solhunter_zero.device.platform.machine", lambda: "arm64")
    faiss_stub = types.ModuleType("faiss")
    monkeypatch.setitem(sys.modules, "faiss", faiss_stub)
    st_stub = types.ModuleType("sentence_transformers")
    st_stub.SentenceTransformer = object
    monkeypatch.setitem(sys.modules, "sentence_transformers", st_stub)
    import solhunter_zero.advanced_memory as am
    importlib.reload(am)
    assert am._detect_gpu() is True
    assert am._gpu_index_enabled() is False
    monkeypatch.delitem(sys.modules, "solhunter_zero.advanced_memory", raising=False)
