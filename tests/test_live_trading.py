"""Helpers to run the trading loop in an isolated environment.

The real :mod:`solhunter_zero.main` module interacts with a large portion of
the Solana ecosystem.  For the unit tests we only need to exercise the control
flow of ``main.main`` without performing any network or disk heavy operations.

``setup_live_trading_env`` prepares such a lightweight environment by patching
out the components that would normally communicate with external services.

It returns the patched ``solhunter_zero.main`` module together with a list that
records any trades logged through the :class:`~solhunter_zero.memory.Memory`
stub.  Tests can then invoke ``main.main`` and make assertions based on the
recorded trades.
"""

from __future__ import annotations

from typing import Any
from types import SimpleNamespace

import solhunter_zero.main as main_module
from solhunter_zero.agents.discovery import DiscoveryAgent as _DiscoveryAgent


def setup_live_trading_env(monkeypatch):
    """Prepare a minimal environment for running the trading loop offline.

    Parameters
    ----------
    monkeypatch:
        Pytest fixture used to apply temporary patches.

    Returns
    -------
    tuple
        The patched ``main`` module and a list capturing logged trades.
    """

    # Ensure required environment variables are present but point to harmless
    # local values so ``main.main`` can run without accessing the network.
    monkeypatch.setenv("SOLANA_RPC_URL", "http://localhost:8899")
    monkeypatch.setenv("KEYPAIR_PATH", "keypairs/assistant.json")
    monkeypatch.setenv("USE_DEPTH_STREAM", "0")
    monkeypatch.setenv("AGENTS", "")

    # Stub out startup routines which would normally verify connectivity or
    # spawn background services.
    monkeypatch.setattr(main_module, "_start_depth_service", lambda cfg: None)
    monkeypatch.setattr(main_module, "ensure_connectivity", lambda **_: None)
    monkeypatch.setattr(
        main_module, "ensure_connectivity_async", lambda **_: None
    )

    # The event bus normally requires a running broker; replace the key
    # functions with no-ops so that publishing and server management become
    # trivial.
    monkeypatch.setattr(
        main_module.event_bus, "start_ws_server", lambda *a, **k: None
    )
    monkeypatch.setattr(
        main_module.event_bus, "stop_ws_server", lambda *a, **k: None
    )
    monkeypatch.setattr(
        main_module.event_bus, "get_event_bus_url", lambda: "ws://dummy",
        raising=False,
    )
    monkeypatch.setattr(
        main_module.event_bus, "publish", lambda *a, **k: None
    )

    async def _verify_broker_connection() -> bool:
        return True

    monkeypatch.setattr(
        main_module.event_bus, "verify_broker_connection", _verify_broker_connection
    )

    # Provide simple stand‑ins for routing and depth queries by attaching
    # lightweight objects to the main module.
    monkeypatch.setattr(
        main_module,
        "routeffi",
        SimpleNamespace(best_route=lambda *a, **k: (["SOL"], 0.0)),
        raising=False,
    )
    monkeypatch.setattr(
        main_module,
        "depth_client",
        SimpleNamespace(snapshot=lambda *a, **k: ({}, 0.0)),
        raising=False,
    )

    # The discovery agent normally scans the chain for tokens.  Here it simply
    # returns a single static token symbol.
    async def discover_stub(self, *args: Any, **kwargs: Any) -> list[str]:
        return ["SOL"]

    monkeypatch.setattr(_DiscoveryAgent, "discover_tokens", discover_stub)
    monkeypatch.setattr(main_module, "DiscoveryAgent", _DiscoveryAgent, raising=False)

    # Replace the strategy manager with a lightweight stub that always proposes
    # one mock trade so the trading loop has something to execute.
    class DummyStrategyManager:
        def __init__(self, *args: Any, **kwargs: Any) -> None:  # pragma: no cover - trivial
            pass

        async def evaluate(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
            return [{"token": "SOL", "side": "buy", "amount": 1.0, "price": 0.0}]

    monkeypatch.setattr(main_module, "StrategyManager", DummyStrategyManager)

    # Avoid the overhead of agent manager initialisation.
    monkeypatch.setattr(
        main_module.AgentManager, "from_default", classmethod(lambda cls: None)
    )
    monkeypatch.setattr(
        main_module.AgentManager, "from_config", classmethod(lambda cls, cfg: None)
    )

    # Record trades through a custom ``Memory.log_trade`` implementation.
    trades: list[dict[str, Any]] = []

    async def log_trade_stub(self, *args: Any, **kwargs: Any) -> int:
        trades.append(kwargs)
        main_module._first_trade_recorded = True
        return 1

    monkeypatch.setattr(main_module.Memory, "log_trade", log_trade_stub)

    # Ensure place_order_async marks the trade flag even in dry-run mode.
    from solhunter_zero import loop as _loop

    async def fake_place_order_async(*args: Any, **kwargs: Any):
        _loop._first_trade_recorded = True
        main_module._first_trade_recorded = True
        return {"dry_run": True}

    monkeypatch.setattr(_loop, "place_order_async", fake_place_order_async)
    monkeypatch.setattr(main_module, "place_order_async", fake_place_order_async)

    # Ensure the flag checked by tests starts cleared.
    main_module._first_trade_recorded = False

    return main_module, trades


__all__ = ["setup_live_trading_env"]

