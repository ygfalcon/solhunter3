import asyncio
import json
import sys
import time
import types

import pytest


def _install_sqlalchemy_stub() -> None:
    def _noop(*_args, **_kwargs):
        return None

    class _SQLAlchemyModule(types.ModuleType):
        def __getattr__(self, name: str):  # type: ignore[override]
            if name == "event":
                return types.SimpleNamespace(listens_for=lambda *a, **k: (lambda func: func))
            return _noop

    stub = _SQLAlchemyModule("sqlalchemy")
    event_module = types.ModuleType("sqlalchemy.event")
    event_module.listens_for = lambda *a, **k: (lambda func: func)
    sys.modules["sqlalchemy.event"] = event_module
    sys.modules["sqlalchemy"] = stub

    stub.create_engine = _noop
    stub.Column = lambda *a, **k: None
    stub.Integer = object
    stub.Float = object
    stub.String = object
    stub.DateTime = object
    stub.Text = object
    stub.ForeignKey = lambda *a, **k: None
    stub.Index = lambda *a, **k: None
    stub.event = event_module

    orm_stub = types.ModuleType("sqlalchemy.orm")
    orm_stub.declarative_base = lambda *a, **k: type("Base", (), {})
    orm_stub.sessionmaker = lambda *a, **k: lambda **_kw: None
    sys.modules["sqlalchemy.orm"] = orm_stub


_install_sqlalchemy_stub()


def _install_base58_stub() -> None:
    module = types.ModuleType("base58")
    module.b58decode = lambda *a, **k: b""
    module.b58encode = lambda *a, **k: b""
    sys.modules["base58"] = module


_install_base58_stub()

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


@pytest.mark.chaos
def test_probe_rl_daemon_health_failure(tmp_path, monkeypatch, chaos_remediator):
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

    chaos_remediator(
        component="RL daemon",
        failure="Trading runtime cannot reach the RL health endpoint",
        detection=(
            "`_probe_rl_daemon_health` raises urllib errors for the URL recorded in "
            "`rl_daemon.health.json`, setting `running` to False."
        ),
        impact=(
            "The runtime treats the RL service as offline and skips RL-assisted policy updates."
        ),
        remediation=[
            f"Verify the RL health endpoint `{payload['url']}` is reachable from the runtime host (e.g. `curl {payload['url']}`).",
            "Restart the RL daemon with `python scripts/run_rl_daemon.py` or restart the managed service to re-register the health server.",
            "After restart, re-run `python scripts/healthcheck.py` to confirm the RL check reports PASS.",
        ],
        verification=(
            f"`curl {payload['url']}` returns HTTP 200 and `_probe_rl_daemon_health()` reports `running: True`."
        ),
        severity="high",
        tags=["runtime", "healthcheck", "rl"],
        metadata={"url": payload["url"], "error": result["error"]},
    )


@pytest.mark.chaos
def test_probe_rl_daemon_health_missing(monkeypatch, tmp_path, chaos_remediator):
    path = tmp_path / "rl_daemon.health.json"
    monkeypatch.setattr(trading_runtime, "_RL_HEALTH_PATH", path)
    result = trading_runtime._probe_rl_daemon_health(timeout=0.01)
    assert result["detected"] is False
    assert result["running"] is False
    assert result["url"] is None

    chaos_remediator(
        component="RL daemon",
        failure="The runtime cannot find `rl_daemon.health.json`",
        detection="`_probe_rl_daemon_health()` returns `detected: False` with no URL recorded.",
        impact="The RL watcher never enables external policy updates because no daemon is registered.",
        remediation=[
            "Start the RL daemon via `python scripts/run_rl_daemon.py` so it writes `rl_daemon.health.json` into the runtime root.",
            "If the RL daemon runs remotely, mount or sync the generated health file onto the trading runtime host.",
            "Confirm the health file exists and rerun `python scripts/healthcheck.py` or restart the runtime process.",
        ],
        verification=(
            "`ls rl_daemon.health.json` succeeds and `_probe_rl_daemon_health()` subsequently reports `detected: True`."
        ),
        severity="medium",
        tags=["runtime", "healthcheck", "rl"],
        metadata={"expected_path": str(path)},
    )


def test_rl_gate_blocks_stale_weights(monkeypatch):
    runtime = trading_runtime.TradingRuntime()
    pipeline_calls: list[bool] = []
    manager_calls: list[tuple[bool, str | None]] = []

    runtime.pipeline = types.SimpleNamespace(
        set_rl_enabled=lambda enabled: pipeline_calls.append(enabled),
        set_rl_weights=lambda *_a, **_k: None,
    )
    runtime.agent_manager = types.SimpleNamespace(
        set_rl_disabled=lambda disabled, reason=None: manager_calls.append(
            (bool(disabled), reason)
        )
    )
    runtime._rl_window_sec = 0.4
    runtime._rl_status_info.update({"enabled": True, "running": True})
    runtime.status.heartbeat_ts = time.time() - 2.0

    runtime._update_rl_gate()

    assert runtime._rl_gate_active is False
    assert manager_calls[-1] == (True, "stale_heartbeat")
    assert pipeline_calls[-1] is False

    runtime.status.heartbeat_ts = time.time()
    runtime._update_rl_gate()

    assert runtime._rl_gate_active is True
    assert manager_calls[-1] == (False, None)
    assert pipeline_calls[-1] is True


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
