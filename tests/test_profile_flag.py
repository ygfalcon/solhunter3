import sys
import types
import runpy
import asyncio
from pathlib import Path


def _stub(name, attrs=None):
    mod = types.ModuleType(name)
    if attrs:
        for k, v in attrs.items():
            setattr(mod, k, v)
    sys.modules[name] = mod


async def _async(*_a, **_k):
    return None


class _Sub:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        pass


def test_profile_flag_creates_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    _stub("solhunter_zero.token_scanner", {"scan_tokens_async": _async})
    _stub(
        "solhunter_zero.onchain_metrics",
        {"async_top_volume_tokens": _async, "fetch_dex_metrics_async": _async},
    )
    _stub("solhunter_zero.market_ws", {"listen_and_trade": _async})
    _stub("solhunter_zero.simulation", {"run_simulations": lambda *a, **k: []})
    _stub(
        "solhunter_zero.decision",
        {"should_buy": lambda *_a, **_k: False, "should_sell": lambda *_a, **_k: False},
    )
    _stub("solhunter_zero.prices", {"fetch_token_prices_async": _async, "warm_cache": _async})
    _stub("solhunter_zero.order_book_ws", {})
    _stub(
        "solhunter_zero.memory",
        {
            "Memory": type("Memory", (), {"__init__": lambda self, *a, **k: None}),
            "load_snapshot": lambda *a, **k: None,
        },
    )
    _stub(
        "solhunter_zero.portfolio",
        {
            "Portfolio": type(
                "Portfolio", (), {"__init__": lambda self, *a, **k: None}
            ),
            "calculate_order_size": lambda *a, **k: 1,
            "dynamic_order_size": lambda *a, **k: 1,
        },
    )
    _stub("solhunter_zero.exchange", {"place_order_async": _async})
    _stub(
        "solhunter_zero.strategy_manager",
        {
            "StrategyManager": type(
                "StrategyManager", (), {"__init__": lambda self, *a, **k: None}
            )
        },
    )
    _stub(
        "solhunter_zero.agent_manager",
        {
            "AgentManager": type(
                "AgentManager", (), {"from_config": classmethod(lambda cls, cfg: None)}
            )
        },
    )
    _stub("solhunter_zero.agents", {})
    _stub(
        "solhunter_zero.agents.discovery",
        {"DiscoveryAgent": type("DiscoveryAgent", (), {"discover_tokens": _async})},
    )
    _stub("solhunter_zero.agents.conviction", {"predict_price_movement": _async})
    _stub("solhunter_zero.risk", {"RiskManager": type("RiskManager", (), {})})
    _stub("solhunter_zero.arbitrage", {"detect_and_execute_arbitrage": _async})
    _stub("solhunter_zero.depth_client", {"listen_depth_ws": _async})
    _stub(
        "solhunter_zero.event_bus",
        {
            "start_ws_server": _async,
            "stop_ws_server": _async,
            "_reload_bus": lambda _x: None,
            "subscription": lambda *_a, **_k: _Sub(),
            "publish": lambda *_a, **_k: None,
        },
    )
    _stub(
        "solhunter_zero.data_pipeline",
        {"start_depth_snapshot_listener": lambda *_a, **_k: lambda: None},
    )
    _stub(
        "solhunter_zero.wallet",
        {"load_selected_keypair": lambda: None, "load_keypair": lambda p: None},
    )
    sys.modules["aiohttp"] = types.ModuleType("aiohttp")

    monkeypatch.setattr(asyncio, "run", lambda coro: None)
    monkeypatch.setattr(
        sys, "argv", ["solhunter_zero.main", "--profile", "--iterations", "0"]
    )

    runpy.run_module("solhunter_zero.main", run_name="__main__")
    assert (tmp_path / "profile.out").is_file()
