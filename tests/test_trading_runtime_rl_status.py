import json
import sys
import types

torch_stub = sys.modules.setdefault("torch", types.ModuleType("torch"))
nn_stub = sys.modules.setdefault("torch.nn", types.ModuleType("torch.nn"))
functional_stub = sys.modules.setdefault(
    "torch.nn.functional", types.ModuleType("torch.nn.functional")
)
setattr(nn_stub, "functional", functional_stub)
setattr(torch_stub, "nn", nn_stub)

datasets_mod = sys.modules.setdefault(
    "solhunter_zero.datasets", types.ModuleType("solhunter_zero.datasets")
)
sample_ticks_mod = sys.modules.setdefault(
    "solhunter_zero.datasets.sample_ticks",
    types.ModuleType("solhunter_zero.datasets.sample_ticks"),
)
setattr(sample_ticks_mod, "load_sample_ticks", lambda *args, **kwargs: [])
setattr(sample_ticks_mod, "DEFAULT_PATH", "")
setattr(datasets_mod, "sample_ticks", sample_ticks_mod)

from solhunter_zero.runtime import trading_runtime


class DummyResponse:
    def __init__(self, status: int = 200) -> None:
        self.status = status

    def __enter__(self) -> "DummyResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def getcode(self) -> int:
        return self.status


def test_probe_rl_daemon_health_running(tmp_path, monkeypatch):
    path = tmp_path / "rl_daemon.health.json"
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_PATH", path)
    payload = {"url": "http://127.0.0.1:7070/health"}
    path.write_text(json.dumps(payload), encoding="utf-8")

    response = DummyResponse(status=200)

    def fake_urlopen(url, timeout=0.5):
        assert url == payload["url"]
        assert timeout <= 0.5
        return response

    monkeypatch.setattr(trading_runtime.urllib.request, "urlopen", fake_urlopen)

    result = trading_runtime._probe_rl_daemon_health(timeout=0.01)
    assert result["detected"] is True
    assert result["running"] is True
    assert result["url"] == payload["url"]
    assert result["error"] is None


def test_probe_rl_daemon_health_failure(tmp_path, monkeypatch):
    path = tmp_path / "rl_daemon.health.json"
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_PATH", path)
    payload = {"url": "http://127.0.0.1:8080/health"}
    path.write_text(json.dumps(payload), encoding="utf-8")

    def fake_urlopen(url, timeout=0.5):
        raise trading_runtime.urllib.error.URLError("boom")

    monkeypatch.setattr(trading_runtime.urllib.request, "urlopen", fake_urlopen)

    result = trading_runtime._probe_rl_daemon_health(timeout=0.02)
    assert result["detected"] is True
    assert result["running"] is False
    assert result["error"]


def test_probe_rl_daemon_health_missing(monkeypatch, tmp_path):
    path = tmp_path / "rl_daemon.health.json"
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_PATH", path)
    result = trading_runtime._probe_rl_daemon_health(timeout=0.01)
    assert result["detected"] is False
    assert result["running"] is False
    assert result["url"] is None
