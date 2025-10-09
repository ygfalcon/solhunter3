import asyncio
import json
import sys
import types

import pytest

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


@pytest.fixture
def anyio_backend():
    return "asyncio"


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


@pytest.mark.anyio("asyncio")
async def test_rl_status_watcher_detects_external_daemon(monkeypatch, tmp_path):
    health_path = tmp_path / "rl_daemon.health.json"
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_PATH", health_path)
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_INITIAL_INTERVAL", 0.05)
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_MAX_INTERVAL", 0.5)

    response = DummyResponse(status=200)

    def fake_urlopen(url, timeout=0.5):
        return response

    monkeypatch.setattr(trading_runtime.urllib.request, "urlopen", fake_urlopen)

    runtime = trading_runtime.TradingRuntime()

    async def fake_prepare_configuration(self):
        self.cfg = {}
        self.runtime_cfg = {}
        self.depth_proc = None
        self.status.depth_service = False

    async def fake_start_event_bus(self):
        self.status.event_bus = True

    async def fake_start_ui(self):
        return None

    async def fake_start_agents(self):
        self._rl_status_info.update(
            {
                "enabled": False,
                "running": False,
                "source": None,
                "url": None,
                "error": None,
                "detected": False,
                "configured": False,
            }
        )
        self.status.rl_daemon = False

    async def fake_start_loop(self):
        self.status.trading_loop = True

    monkeypatch.setattr(
        trading_runtime.TradingRuntime,
        "_prepare_configuration",
        fake_prepare_configuration,
    )
    monkeypatch.setattr(
        trading_runtime.TradingRuntime,
        "_start_event_bus",
        fake_start_event_bus,
    )
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_ui", fake_start_ui)
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_agents", fake_start_agents)
    monkeypatch.setattr(trading_runtime.TradingRuntime, "_start_loop", fake_start_loop)

    await runtime.start()

    assert runtime.status.rl_daemon is False

    await asyncio.sleep(0.05)
    payload = {"url": "http://127.0.0.1:9999/health"}
    health_path.write_text(json.dumps(payload), encoding="utf-8")

    async def wait_for_online(timeout=1.0):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while loop.time() < deadline:
            if runtime.status.rl_daemon:
                return
            await asyncio.sleep(0.01)
        raise AssertionError("RL status watcher did not detect external daemon in time")

    await wait_for_online()

    assert runtime._rl_status_info["detected"] is True
    assert runtime._rl_status_info["running"] is True
    assert runtime._rl_status_info["source"] == "external"
    assert runtime._rl_status_info["url"] == payload["url"]

    await runtime.stop()
