from scripts import preflight


def test_check_gpu_no_backend(monkeypatch):
    monkeypatch.delenv("SOLHUNTER_GPU_AVAILABLE", raising=False)
    monkeypatch.delenv("SOLHUNTER_GPU_DEVICE", raising=False)
    monkeypatch.setattr("solhunter_zero.device.detect_gpu", lambda: False)
    ok, msg = preflight.check_gpu()
    assert ok is False
    assert "prepare_macos_env" in msg
