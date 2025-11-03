import os
from types import SimpleNamespace

import pytest

from solhunter_zero.runtime import trading_runtime


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_prepare_configuration_uses_configured_modes(monkeypatch):
    runtime = trading_runtime.TradingRuntime(config_path="config.toml")

    preview_cfg = {
        "offline": True,
        "dry_run": False,
        "memory_path": "sqlite:///memory.db",
        "portfolio_path": "portfolio.json",
    }

    async def fake_startup(path, *, offline, dry_run):
        assert offline is True
        assert dry_run is False
        return preview_cfg, SimpleNamespace(), None

    monkeypatch.setattr(trading_runtime, "load_config", lambda path: preview_cfg)
    monkeypatch.setattr(trading_runtime, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(trading_runtime, "perform_startup_async", fake_startup)
    monkeypatch.setattr(trading_runtime, "set_env_from_config", lambda cfg: None)

    monkeypatch.delenv("SOLHUNTER_OFFLINE", raising=False)
    monkeypatch.delenv("DRY_RUN", raising=False)

    await runtime._prepare_configuration()

    assert runtime.cfg == preview_cfg
    assert os.getenv("SOLHUNTER_OFFLINE") == "1"
    assert os.getenv("DRY_RUN") == "0"


@pytest.mark.anyio("asyncio")
async def test_prepare_configuration_prefers_environment(monkeypatch):
    runtime = trading_runtime.TradingRuntime(config_path="config.toml")

    preview_cfg = {
        "offline": False,
        "dry_run": False,
        "memory_path": "sqlite:///memory.db",
        "portfolio_path": "portfolio.json",
    }

    async def fake_startup(path, *, offline, dry_run):
        assert offline is True
        assert dry_run is True
        return preview_cfg, SimpleNamespace(), None

    monkeypatch.setattr(trading_runtime, "load_config", lambda path: preview_cfg)
    monkeypatch.setattr(trading_runtime, "apply_env_overrides", lambda cfg: cfg)
    monkeypatch.setattr(trading_runtime, "perform_startup_async", fake_startup)
    monkeypatch.setattr(trading_runtime, "set_env_from_config", lambda cfg: None)

    monkeypatch.setenv("SOLHUNTER_OFFLINE", "1")
    monkeypatch.setenv("DRY_RUN", "1")

    await runtime._prepare_configuration()

    assert runtime.cfg == preview_cfg
    assert os.getenv("SOLHUNTER_OFFLINE") == "1"
    assert os.getenv("DRY_RUN") == "1"
