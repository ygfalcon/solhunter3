import asyncio
import hashlib
import json
import random
import sys
import time
import types
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping

import pytest

from tests import stubs as _test_stubs

_test_stubs.install_stubs()

sqlalchemy_mod = sys.modules.get("sqlalchemy")
if sqlalchemy_mod is not None and not hasattr(sqlalchemy_mod, "Index"):
    sqlalchemy_mod.Index = lambda *args, **kwargs: None  # type: ignore[attr-defined]
if sqlalchemy_mod is not None and not hasattr(sqlalchemy_mod, "event"):
    sqlalchemy_mod.event = types.SimpleNamespace(  # type: ignore[attr-defined]
        listens_for=lambda *a, **k: (lambda func: func)
    )

if "base58" not in sys.modules:
    base58_stub = types.ModuleType("base58")
    base58_stub.b58decode = lambda *args, **kwargs: b""
    base58_stub.b58encode = lambda *args, **kwargs: b""
    sys.modules["base58"] = base58_stub

from solhunter_zero import event_bus
from solhunter_zero.golden_pipeline.bus import InMemoryBus
from solhunter_zero.golden_pipeline.contracts import STREAMS
from solhunter_zero.golden_pipeline.kv import InMemoryKeyValueStore
from solhunter_zero.runtime.runtime_wiring import RuntimeWiring, initialise_runtime_wiring
from solhunter_zero.ui import UIState


class BridgedBus(InMemoryBus):
    """In-memory bus that also forwards events to the global event bus."""

    async def publish(self, stream: str, payload: Mapping[str, object]) -> None:  # type: ignore[override]
        await super().publish(stream, payload)
        event_bus.publish(stream, dict(payload), _broadcast=False)


@dataclass
class SynthRuntime:
    """Container exposing UI providers backed by ``RuntimeWiring`` collectors."""

    ui_state: UIState
    wiring: RuntimeWiring
    status: Dict[str, Any] = field(
        default_factory=lambda: {
            "event_bus": False,
            "trading_loop": False,
            "loop_state": "stopped",
        }
    )
    websocket_state: Dict[str, Dict[str, Any]] = field(
        default_factory=lambda: {
            "rl": {"connected": False, "last_heartbeat": None},
            "events": {"connected": False, "last_heartbeat": None},
            "logs": {"connected": False, "last_heartbeat": None},
        }
    )
    metrics: Dict[str, float] = field(
        default_factory=lambda: {
            "bus_latency_ms": None,
            "ohlcv_lag_ms": None,
            "depth_lag_ms": None,
            "golden_lag_ms": None,
        }
    )
    paper_positions: List[Dict[str, Any]] = field(default_factory=list)
    summary: Dict[str, Any] = field(
        default_factory=lambda: {
            "suggestions_5m": 0,
            "acceptance_rate": 0.0,
            "open_vote_windows": 0,
            "golden_hashes": 0,
            "paper_pnl": {
                "pnl_1d": 0.0,
                "drawdown_pct": 0.0,
                "turnover_usd": 0.0,
            },
        }
    )

    def status_snapshot(self) -> Dict[str, Any]:
        payload = dict(self.status)
        payload.update(self.metrics)
        payload["websockets"] = {
            channel: {
                "connected": data.get("connected", False),
                "last_heartbeat": data.get("last_heartbeat"),
            }
            for channel, data in self.websocket_state.items()
        }
        return payload

    def shadow_snapshot(self) -> Dict[str, Any]:
        base = self.wiring.collectors.shadow_snapshot()
        snapshot = dict(base)
        snapshot["paper_positions"] = [dict(pos) for pos in self.paper_positions]
        return snapshot

    def summary_snapshot(self) -> Dict[str, Any]:
        return json.loads(json.dumps(self.summary))

    async def wait_for_golden(self, timeout: float = 5.0) -> None:
        await self.wiring.wait_for_topic(STREAMS.golden_snapshot, timeout=timeout)

    def wait_for_websockets(self, timeout: float = 3.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if all(
                self.websocket_state[channel].get("connected")
                for channel in ("rl", "events", "logs")
            ):
                return
            time.sleep(0.05)
        raise AssertionError("websockets did not report connected")


@pytest.fixture
def bus() -> BridgedBus:
    return BridgedBus()


@pytest.fixture
def kv() -> InMemoryKeyValueStore:
    return InMemoryKeyValueStore()


@pytest.fixture
def runtime(bus: BridgedBus, kv: InMemoryKeyValueStore) -> Iterable[SynthRuntime]:
    event_bus.BUS.reset()
    ui_state = UIState()
    wiring = initialise_runtime_wiring(ui_state)
    runtime = SynthRuntime(ui_state=ui_state, wiring=wiring)
    ui_state.status_provider = runtime.status_snapshot
    ui_state.summary_provider = runtime.summary_snapshot

    def _shadow_provider() -> Dict[str, Any]:
        return runtime.shadow_snapshot()

    ui_state.shadow_provider = _shadow_provider

    try:
        yield runtime
    finally:
        wiring.close()
        event_bus.BUS.reset()


async def _seed_runtime(
    runtime: SynthRuntime,
    bus: BridgedBus,
    kv: InMemoryKeyValueStore,
    *,
    now_ts: float,
    seed: int,
) -> None:
    rnd = random.Random(seed)
    base_ts = float(now_ts)

    def publish_event(topic: str, payload: Mapping[str, Any] | Dict[str, Any]) -> None:
        event_bus.publish(topic, dict(payload), _broadcast=False)

    volume_baselines = {
        "SOL": 600_000.0,
        "USDC": 900_000.0,
        "BONK": 150_000.0,
        "SPX": 80_000.0,
        "SIGMA": 90_000.0,
        "PUMP1": 40_000.0,
        "PUMP2": 60_000.0,
    }

    volatility_map = {
        "SOL": 48.0,
        "USDC": 3.0,
        "BONK": 112.0,
        "SPX": 36.0,
        "SIGMA": 64.0,
        "PUMP1": 180.0,
        "PUMP2": 150.0,
    }

    drift_map = {
        "SOL": 0.0012,
        "USDC": 0.0,
        "BONK": 0.005,
        "SPX": 0.0018,
        "SIGMA": 0.003,
        "PUMP1": 0.012,
        "PUMP2": 0.008,
    }

    decimals_map = {
        "SOL": 9,
        "USDC": 6,
        "BONK": 5,
        "SPX": 2,
        "SIGMA": 6,
        "PUMP1": 6,
        "PUMP2": 6,
    }

    sources = {
        "SOL": "das",
        "USDC": "das",
        "BONK": "amm_watch",
        "SPX": "amm_watch",
        "SIGMA": "amm_watch",
        "PUMP1": "mint_stream",
        "PUMP2": "mint_stream",
    }

    spreads = {
        "SOL": 6.0,
        "USDC": 1.0,
        "BONK": 18.0,
        "SPX": 12.0,
        "SIGMA": 15.0,
        "PUMP1": 28.0,
        "PUMP2": 24.0,
    }

    depth_usd = {
        "SOL": 4_100_000.0,
        "USDC": 12_000_000.0,
        "BONK": 380_000.0,
        "SPX": 210_000.0,
        "SIGMA": 330_000.0,
        "PUMP1": 55_000.0,
        "PUMP2": 72_000.0,
    }

    token_specs = [
        {
            "symbol": "SOL",
            "mint": "So11111111111111111111111111111111111111112",
            "price": 158.23,
            "liq": 12_500_000.0,
            "vol_1m": 820_000.0,
        },
        {
            "symbol": "USDC",
            "mint": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9",
            "price": 1.0003,
            "liq": 80_000_000.0,
            "vol_1m": 1_200_000.0,
        },
        {
            "symbol": "BONK",
            "mint": "DeZBonkMint11111111111111111111111111111111",
            "price": 0.0000214,
            "liq": 4_300_000.0,
            "vol_1m": 290_000.0,
        },
        {
            "symbol": "SPX",
            "mint": "SPXWrmhl1111111111111111111111111111111111",
            "price": 512.80,
            "liq": 1_900_000.0,
            "vol_1m": 110_000.0,
        },
        {
            "symbol": "SIGMA",
            "mint": "SiGmAT0ken1111111111111111111111111111111",
            "price": 3.42,
            "liq": 2_600_000.0,
            "vol_1m": 180_000.0,
        },
        {
            "symbol": "PUMP1",
            "mint": "PuMpFunMint1111111111111111111111111111111",
            "price": 0.0041,
            "liq": 220_000.0,
            "vol_1m": 95_000.0,
        },
        {
            "symbol": "PUMP2",
            "mint": "PuMpFunMint2222222222222222222222222222222",
            "price": 0.0079,
            "liq": 310_000.0,
            "vol_1m": 140_000.0,
        },
    ]

    golden_hashes: Dict[str, str] = {}
    suggestion_payloads: List[Dict[str, Any]] = []
    fill_payloads: List[Dict[str, Any]] = []

    async def _publish_discovery(mint: str, symbol: str, source: str, ts: float) -> None:
        candidate = {"mint": mint, "symbol": symbol, "source": source, "asof": ts}
        await bus.publish(STREAMS.discovery_candidates, candidate)
        publish_event(
            "token_discovered",
            {
                "mint": mint,
                "symbol": symbol,
                "source": source,
                "ts": ts,
            },
        )

    for spec in token_specs:
        symbol = spec["symbol"]
        mint = spec["mint"]
        price = float(spec["price"])
        liquidity = float(spec["liq"])
        vol_1m = float(spec["vol_1m"])
        source = sources[symbol]
        base_ts += 0.05
        await _publish_discovery(mint, symbol, source, base_ts)
        base_ts += 0.05
        publish_event(
            "price_update",
            {
                "token": mint,
                "symbol": symbol,
                "price": price,
                "source": "synth",
                "liquidity_usd": liquidity,
                "volume_1m_usd": vol_1m,
                "asof": base_ts,
            },
        )
        depth_payload = {
            mint: {
                "mid": price,
                "spread_bps": spreads[symbol],
                "depth": depth_usd[symbol],
                "venue": "synthetic",
                "ts": base_ts,
            }
        }
        publish_event("depth_update", depth_payload)

        drift = drift_map[symbol]
        bars: List[Dict[str, Any]] = []
        close_price = price
        for index in range(5):
            minute_ago = 4 - index
            tstamp = base_ts - minute_ago * 60.0
            factor = (1.0 + drift) ** -minute_ago
            adj_close = close_price * factor
            open_px = adj_close / (1.0 + drift) if drift else adj_close
            hi = max(open_px, adj_close) * (1.0 + 0.0005 * (1 + rnd.random()))
            lo = min(open_px, adj_close) * (1.0 - 0.0005 * (1 + rnd.random()))
            bars.append(
                {
                    "t": tstamp,
                    "o": open_px,
                    "h": hi,
                    "l": lo,
                    "c": adj_close,
                    "vol_usd": vol_1m,
                    "buyers": int(20 + rnd.random() * 10),
                    "trades": int(35 + rnd.random() * 12),
                }
            )
        latest_bar = bars[-1]
        await bus.publish(
            STREAMS.market_ohlcv,
            {
                "mint": mint,
                "o": latest_bar["o"],
                "h": latest_bar["h"],
                "l": latest_bar["l"],
                "c": latest_bar["c"],
                "vol_usd": vol_1m * 5.0,
                "buyers": latest_bar["buyers"],
                "trades": latest_bar["trades"],
                "zret": 0.0,
                "zvol": 0.0,
                "asof_close": latest_bar["t"],
            },
        )
        depth_snapshot = {
            "mint": mint,
            "venue": "synthetic",
            "mid_usd": price,
            "spread_bps": spreads[symbol],
            "depth_pct": {
                "1": depth_usd[symbol],
                "2": depth_usd[symbol] * 1.5,
                "5": depth_usd[symbol] * 2.2,
            },
            "asof": base_ts,
        }
        await bus.publish(STREAMS.market_depth, depth_snapshot)

        hash_input = f"{symbol}:{price:.6f}".encode("utf-8")
        golden_hash = hashlib.sha1(hash_input).hexdigest()
        golden_hashes[mint] = golden_hash
        volume_spike = vol_1m / volume_baselines[symbol]
        golden_payload = {
            "mint": mint,
            "asof": base_ts,
            "meta": {
                "symbol": symbol,
                "decimals": decimals_map[symbol],
                "source": source,
                "asof": base_ts,
            },
            "px": {
                "fair_price": price,
                "mid_usd": price,
                "spread_bps": spreads[symbol],
                "vol_1m_usd": vol_1m,
                "liquidity_usd": liquidity,
            },
            "liq": liquidity,
            "ohlcv5m": {
                "o": latest_bar["o"],
                "h": latest_bar["h"],
                "l": latest_bar["l"],
                "c": latest_bar["c"],
                "vol_usd": vol_1m * 5.0,
                "buyers": latest_bar["buyers"],
                "trades": latest_bar["trades"],
                "asof_close": latest_bar["t"],
            },
            "hash": golden_hash,
            "metrics": {
                "latency_ms": 140.0 + rnd.random() * 40.0,
                "depth_staleness_ms": 90.0 + rnd.random() * 30.0,
                "candle_age_ms": 100.0 + rnd.random() * 25.0,
                "volatility_5m_annualized": volatility_map[symbol],
                "volume_spike": float(f"{volume_spike:.2f}"),
                "integrity": {
                    "market_data": True,
                    "depth": True,
                    "ohlcv": True,
                    "price": True,
                },
            },
        }
        await bus.publish(STREAMS.golden_snapshot, golden_payload)

        await kv.set(
            f"x:mint.golden:{mint}",
            json.dumps(golden_payload),
        )
        await kv.set(
            f"x:market.ohlcv:{mint}",
            json.dumps(bars),
        )
        await kv.set(
            f"x:market.depth:{mint}",
            json.dumps(depth_snapshot),
        )

    runtime.status.update({"event_bus": True, "trading_loop": True, "loop_state": "running"})

    suggestions_spec = [
        {
            "agent": "MomentumAgent",
            "mint": token_specs[2]["mint"],  # BONK
            "symbol": "BONK",
            "side": "buy",
            "notional": 2_500.0,
            "edge": 0.018,
            "breakeven_bps": 30.0,
            "ttl": 45.0,
        },
        {
            "agent": "MicrocapAgent",
            "mint": token_specs[5]["mint"],  # PUMP1
            "symbol": "PUMP1",
            "side": "buy",
            "notional": 1_200.0,
            "edge": 0.036,
            "breakeven_bps": 45.0,
            "ttl": 60.0,
        },
        {
            "agent": "LiquidityArb",
            "mint": token_specs[4]["mint"],  # SIGMA
            "symbol": "SIGMA",
            "side": "sell",
            "notional": 3_000.0,
            "edge": 0.011,
            "breakeven_bps": 25.0,
            "ttl": 30.0,
        },
    ]

    for spec in suggestions_spec:
        mint = spec["mint"]
        suggestion = {
            "agent": spec["agent"],
            "mint": mint,
            "side": spec["side"],
            "notional_usd": spec["notional"],
            "edge": spec["edge"],
            "breakeven_bps": spec["breakeven_bps"],
            "max_slippage_bps": 50.0,
            "confidence": max(0.05, spec["edge"] * 10.0),
            "inputs_hash": golden_hashes[mint],
            "ttl_sec": spec["ttl"],
            "generated_at": base_ts + 0.01,
            "gating": {
                "edge_pass": True,
                "expected_edge_bps": spec["edge"] * 10_000,
                "breakeven_bps": spec["breakeven_bps"],
            },
            "risk": {
                "expected_edge_bps": spec["edge"] * 10_000,
                "breakeven_bps": spec["breakeven_bps"],
            },
        }
        suggestion_payloads.append(suggestion)
        await bus.publish(STREAMS.trade_suggested, suggestion)

    await kv.set("x:agent.suggestions", json.dumps(suggestion_payloads))

    window_scores = {
        suggestion_payloads[0]["mint"]: 0.64,
        suggestion_payloads[1]["mint"]: 0.71,
        suggestion_payloads[2]["mint"]: 0.55,
    }

    decisions: List[Dict[str, Any]] = []
    for mint, score in window_scores.items():
        decision = {
            "mint": mint,
            "side": "buy" if mint != suggestion_payloads[2]["mint"] else "sell",
            "score": score,
            "snapshot_hash": golden_hashes[mint],
            "client_order_id": hashlib.sha1((mint + str(score)).encode("utf-8")).hexdigest(),
            "ts": base_ts + 0.5,
        }
        decisions.append(decision)
        await bus.publish(STREAMS.vote_decisions, decision)

    shadow_fill = {
        "order_id": "PUMP1-shadow-fill",
        "mint": token_specs[5]["mint"],
        "side": "buy",
        "qty_base": 150_000.0,
        "price_usd": 0.00805,
        "fees_usd": 0.0,
        "slippage_bps": 42.0,
        "snapshot_hash": golden_hashes[token_specs[5]["mint"]],
        "route": "paper",
        "ts": base_ts + 0.75,
    }
    fill_payloads.append(shadow_fill)
    await bus.publish(STREAMS.virtual_fills, shadow_fill)
    await kv.set("x:shadow.fills", json.dumps(fill_payloads))

    paper_position = {
        "mint": token_specs[5]["mint"],
        "side": "long",
        "qty_base": 150_000.0,
        "avg_cost": 0.00805,
        "realized_usd": 0.0,
        "unrealized_usd": (0.00824 - 0.00805) * 150_000.0,
        "total_pnl_usd": (0.00824 - 0.00805) * 150_000.0,
    }
    runtime.paper_positions = [paper_position]
    await kv.set("x:paper.positions", json.dumps({paper_position["mint"]: paper_position}))

    # Logical time advance and BONK uptick
    base_ts += 30.0
    publish_event(
        "price_update",
        {
            "token": token_specs[2]["mint"],
            "symbol": "BONK",
            "price": 0.0000219,
            "source": "synth",
            "asof": base_ts,
        },
    )

    runtime.summary.update(
        {
            "suggestions_5m": 3,
            "acceptance_rate": 66.7,
            "open_vote_windows": 2,
            "golden_hashes": len(golden_hashes),
            "paper_pnl": {
                "pnl_1d": 64.0,
                "drawdown_pct": 0.3,
                "turnover_usd": 1_200.0,
            },
        }
    )

    runtime.metrics.update(
        {
            "bus_latency_ms": 14.0,
            "ohlcv_lag_ms": 220.0,
            "depth_lag_ms": 180.0,
            "golden_lag_ms": 95.0,
        }
    )
    await kv.set(
        "x:metrics/latency",
        json.dumps(runtime.metrics),
    )

    # Websocket heartbeats
    for offset in range(0, 31, 5):
        beat_ts = base_ts + offset
        for channel in ("rl", "events", "logs"):
            runtime.websocket_state[channel]["connected"] = True
            runtime.websocket_state[channel]["last_heartbeat"] = beat_ts

    publish_event(
        "amm_pair_created",
        {
            "mint": token_specs[6]["mint"],
            "venue": "raydium",
            "pool_liq_usd": 410_000.0,
            "score": 0.72,
        },
    )

    runtime.summary.setdefault("votes", {})
    summary_event = {
        "suggestions_5m": 3,
        "acceptance_rate": 66.7,
        "open_vote_windows": 2,
        "golden_hashes": len(golden_hashes),
    }
    publish_event("summary_report", summary_event)

    await runtime.wait_for_golden(timeout=5.0)


@pytest.fixture
def synth_seed(runtime: SynthRuntime, bus: BridgedBus, kv: InMemoryKeyValueStore):
    def _seed(*, now_ts: float = 1_734_768_000.0, seed: int = 42) -> None:
        asyncio.run(_seed_runtime(runtime, bus, kv, now_ts=now_ts, seed=seed))

    return _seed
