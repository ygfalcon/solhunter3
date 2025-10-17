import os
import platform
import subprocess
import sys
import types

import pytest

os.environ.setdefault("TORCH_METAL_VERSION", "0")
os.environ.setdefault("TORCHVISION_METAL_VERSION", "0")
dummy_config = types.SimpleNamespace(load_config=lambda: {})
sys.modules["solhunter_zero.config"] = dummy_config
from solhunter_zero import device as device_module
from solhunter_zero.device import METAL_EXTRA_INDEX, load_torch_metal_versions

TORCH_METAL_VERSION, TORCHVISION_METAL_VERSION = load_torch_metal_versions()


def test_run_with_timeout_success(monkeypatch):
    def fake_run(cmd, check, timeout, **kwargs):  # pragma: no cover - simplified
        return None

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = device_module._run_with_timeout(["echo"], timeout=1)
    assert result.success is True
    assert result.message == "echo"


def test_run_with_timeout_timeout(monkeypatch):
    def fake_run(cmd, check, timeout, **kwargs):  # pragma: no cover - simplified
        raise subprocess.TimeoutExpired(cmd, timeout)

    monkeypatch.setattr(subprocess, "run", fake_run)
    result = device_module._run_with_timeout(["echo"], timeout=1)
    assert result.success is False
    assert "timed out" in result.message


def test_load_torch_metal_versions_env(monkeypatch):
    monkeypatch.setenv("TORCH_METAL_VERSION", "1.2.3")
    monkeypatch.setenv("TORCHVISION_METAL_VERSION", "4.5.6")
    assert device_module.load_torch_metal_versions() == ("1.2.3", "4.5.6")


def test_load_torch_metal_versions_config(monkeypatch):
    monkeypatch.delenv("TORCH_METAL_VERSION", raising=False)
    monkeypatch.delenv("TORCHVISION_METAL_VERSION", raising=False)
    monkeypatch.setattr(
        sys.modules["solhunter_zero.config"],
        "load_config",
        lambda: {"torch": {"torch_metal_version": "7.7", "torchvision_metal_version": "8.8"}},
    )
    assert device_module.load_torch_metal_versions() == ("7.7", "8.8")

def test_load_torch_metal_versions_missing(monkeypatch, caplog):
    monkeypatch.delenv("TORCH_METAL_VERSION", raising=False)
    monkeypatch.delenv("TORCHVISION_METAL_VERSION", raising=False)
    monkeypatch.setattr(sys.modules["solhunter_zero.config"], "load_config", lambda: {})
    monkeypatch.setitem(
        sys.modules,
        "solhunter_zero.macos_setup",
        types.SimpleNamespace(_resolve_metal_versions=lambda: ("9.9", "8.8")),
    )
    with caplog.at_level("WARNING"):
        assert device_module.load_torch_metal_versions() == ("9.9", "8.8")
    assert "Torch Metal versions not specified" in caplog.text


def test_detect_gpu_and_get_default_device_mps(monkeypatch):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True, is_built=lambda: True)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: True),
        device=lambda name: types.SimpleNamespace(type=name),
        ones=lambda *a, **k: types.SimpleNamespace(cpu=lambda: None),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    assert device_module.detect_gpu() is True
    dev = device_module.get_default_device("auto")
    assert getattr(dev, "type", None) == "mps"


def test_detect_gpu_rosetta(monkeypatch, caplog):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "x86_64")
    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True, is_built=lambda: True)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: True),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    with caplog.at_level("WARNING"):
        assert device_module.detect_gpu() is False
    assert "Rosetta" in caplog.text


def test_detect_gpu_mps_install_hint(monkeypatch, caplog):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_built=lambda: False, is_available=lambda: False)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: False),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    with caplog.at_level("WARNING"):
        assert device_module.detect_gpu() is False
    assert "MPS backend not built" in caplog.text


def test_detect_gpu_tensor_failure(monkeypatch, caplog):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")

    def failing_ones(*args, **kwargs):
        raise RuntimeError("boom")

    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True, is_built=lambda: True)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: True),
        ones=failing_ones,
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    with caplog.at_level("WARNING"):
        assert device_module.detect_gpu() is False
    assert "Tensor operation failed" in caplog.text


def test_get_gpu_backend_warns_on_torch_error(monkeypatch, caplog):
    def raising_cuda_available():
        raise RuntimeError("boom")

    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: False)
        ),
        cuda=types.SimpleNamespace(is_available=raising_cuda_available),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    cp_stub = types.SimpleNamespace(
        cuda=types.SimpleNamespace(
            runtime=types.SimpleNamespace(getDeviceCount=lambda: 0)
        )
    )
    monkeypatch.setitem(sys.modules, "cupy", cp_stub)
    with caplog.at_level("WARNING"):
        assert device_module.get_gpu_backend() is None
    assert "GPU backend probe failed" in caplog.text


def test_ensure_torch_with_metal_failure_does_not_write_sentinel(
    monkeypatch, tmp_path, caplog
):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    sentinel = tmp_path / "sentinel"
    monkeypatch.setattr(device_module, "MPS_SENTINEL", sentinel)
    monkeypatch.setattr(device_module, "torch", None, raising=False)

    def fake_run_with_timeout(cmd, timeout):
        return device_module.InstallStatus(False, "boom")

    monkeypatch.setattr(device_module, "_run_with_timeout", fake_run_with_timeout)
    manual_cmd = (
        f"{sys.executable} -m pip install "
        f"torch=={TORCH_METAL_VERSION} torchvision=={TORCHVISION_METAL_VERSION} "
        + " ".join(METAL_EXTRA_INDEX)
    )
    with caplog.at_level("ERROR"):
        with pytest.raises(RuntimeError):
            device_module.ensure_torch_with_metal()
    assert manual_cmd in caplog.text
    assert not sentinel.exists()


@pytest.mark.skipif(platform.system() != "Darwin", reason="MPS is only available on macOS")
def test_configure_gpu_env_mps(monkeypatch):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: True)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: False),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("PYTORCH_ENABLE_MPS_FALLBACK", raising=False)
    monkeypatch.delenv("SOLHUNTER_GPU_AVAILABLE", raising=False)
    monkeypatch.delenv("SOLHUNTER_GPU_DEVICE", raising=False)
    env = device_module.ensure_gpu_env()
    assert env["TORCH_DEVICE"] == "mps"
    assert env["PYTORCH_ENABLE_MPS_FALLBACK"] == "1"
    assert env["SOLHUNTER_GPU_AVAILABLE"] == "1"
    assert env["SOLHUNTER_GPU_DEVICE"] == "mps"
    assert os.environ["TORCH_DEVICE"] == "mps"
    assert os.environ["PYTORCH_ENABLE_MPS_FALLBACK"] == "1"
    assert os.environ["SOLHUNTER_GPU_AVAILABLE"] == "1"
    assert os.environ["SOLHUNTER_GPU_DEVICE"] == "mps"


@pytest.mark.skipif(platform.system() != "Darwin", reason="MPS is only available on macOS")
def test_configure_gpu_env_mps_unavailable(monkeypatch):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    torch_stub = types.SimpleNamespace(
        backends=types.SimpleNamespace(
            mps=types.SimpleNamespace(is_available=lambda: False)
        ),
        cuda=types.SimpleNamespace(is_available=lambda: False),
    )
    monkeypatch.setattr(device_module, "torch", torch_stub, raising=False)
    monkeypatch.delenv("TORCH_DEVICE", raising=False)
    monkeypatch.delenv("PYTORCH_ENABLE_MPS_FALLBACK", raising=False)
    monkeypatch.delenv("SOLHUNTER_GPU_AVAILABLE", raising=False)
    monkeypatch.delenv("SOLHUNTER_GPU_DEVICE", raising=False)
    env = device_module.ensure_gpu_env()
    assert env["SOLHUNTER_GPU_AVAILABLE"] == "0"
    assert env["SOLHUNTER_GPU_DEVICE"] == "cpu"
    assert env["TORCH_DEVICE"] == "cpu"
    assert os.environ["TORCH_DEVICE"] == "cpu"
    assert "PYTORCH_ENABLE_MPS_FALLBACK" not in os.environ


def test_initialize_gpu_installs_before_verify(monkeypatch, tmp_path):
    call_order: list[str] = []

    def fake_install() -> None:
        call_order.append("install")

    def fake_verify() -> None:
        call_order.append("verify")

    def fake_env() -> dict[str, str]:
        return {"SOLHUNTER_GPU_AVAILABLE": "1", "SOLHUNTER_GPU_DEVICE": "mps", "TORCH_DEVICE": "mps"}

    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(device_module, "_sentinel_matches", lambda: False)
    monkeypatch.setattr(device_module, "MPS_SENTINEL", tmp_path / "sentinel")
    monkeypatch.setattr(device_module, "ensure_torch_with_metal", fake_install)
    monkeypatch.setattr(device_module, "verify_gpu", fake_verify)
    monkeypatch.setattr(device_module, "ensure_gpu_env", fake_env)
    monkeypatch.setattr(device_module, "_GPU_LOGGED", True)

    device_module.initialize_gpu()
    assert call_order == ["install", "verify"]


def test_initialize_gpu_install_failure(monkeypatch, tmp_path):
    def fake_install() -> None:
        raise RuntimeError("boom")

    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    monkeypatch.setattr(device_module, "_sentinel_matches", lambda: False)
    monkeypatch.setattr(device_module, "MPS_SENTINEL", tmp_path / "sentinel")
    monkeypatch.setattr(device_module, "ensure_torch_with_metal", fake_install)
    monkeypatch.setattr(device_module, "_GPU_LOGGED", True)

    with pytest.raises(RuntimeError, match="scripts/mac_setup.py"):
        device_module.initialize_gpu()


def test_initialize_gpu_stale_sentinel_installs_once(monkeypatch, tmp_path):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "arm64")
    sentinel = tmp_path / "sentinel"
    sentinel.write_text("1.0.0\n1.0.0\n")
    monkeypatch.setattr(device_module, "MPS_SENTINEL", sentinel)

    calls: list[str] = []

    def fake_install() -> None:
        assert not sentinel.exists()
        calls.append("install")

    monkeypatch.setattr(device_module, "ensure_torch_with_metal", fake_install)
    monkeypatch.setattr(device_module, "verify_gpu", lambda: None)
    monkeypatch.setattr(
        device_module,
        "ensure_gpu_env",
        lambda: {
            "SOLHUNTER_GPU_AVAILABLE": "1",
            "SOLHUNTER_GPU_DEVICE": "mps",
            "TORCH_DEVICE": "mps",
        },
    )
    monkeypatch.setattr(device_module, "_GPU_LOGGED", True)

    device_module.initialize_gpu()
    assert calls == ["install"]


def test_initialize_gpu_rosetta_error(monkeypatch):
    monkeypatch.setattr(device_module.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(device_module.platform, "machine", lambda: "x86_64")
    with pytest.raises(RuntimeError, match="arch -arm64"):
        device_module.initialize_gpu()
