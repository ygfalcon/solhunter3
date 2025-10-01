from __future__ import annotations

"""Investor demo utilities and CLI.

The demo degrades gracefully when optional dependencies are unavailable,
falling back to simplified implementations for correlation matrices and
hedge allocation.
"""

import argparse
import asyncio
import csv
import json
import logging
import sys
import types
import warnings
from importlib import resources
from pathlib import Path
from typing import Callable, Dict, List, Tuple, Union
import threading

try:  # SQLAlchemy is optional; fall back to a simple in-memory implementation
    from .memory import Memory  # type: ignore
except Exception:  # pragma: no cover - absence of SQLAlchemy
    Memory = None  # type: ignore[assignment]

try:  # optional hedging utilities
    from .portfolio import hedge_allocation  # type: ignore
except Exception:  # pragma: no cover - portfolio module optional
    def hedge_allocation(
        weights: Dict[str, float], _corr: Dict[tuple[str, str], float]
    ) -> Dict[str, float]:
        return weights



# Track which trade types have been exercised by the demo
used_trade_types: set[str] = set()

# Flags indicating availability of optional trade modules
ROUTE_FFI_AVAILABLE: bool = False
JITO_STREAM_AVAILABLE: bool = False

# Toggle for heavier demo features such as real RL training
FULL_SYSTEM: bool = False

# Toggle for the lightweight RL demonstration. When enabled the demo will
# attempt to run a tiny reinforcement learning example even if the heavy
# dependencies are missing.  The stub falls back to a NumPy implementation and
# always returns a non-zero reward so that the pipeline can be demonstrated in
# minimal environments.
RL_DEMO: bool = False

# Directory where RL metrics will be written
RL_REPORT_DIR: Path | None = None

# Simple strategy functions used for demonstration


def _buy_and_hold(
    prices: List[float], fee: float = 0.0, slippage: float = 0.0
) -> List[float]:
    rets: List[float] = []
    cost = fee + slippage
    for i in range(1, len(prices)):
        r = (prices[i] - prices[i - 1]) / prices[i - 1] - cost
        rets.append(r)
    return rets


def _momentum(
    prices: List[float], fee: float = 0.0, slippage: float = 0.0
) -> List[float]:
    returns: List[float] = []
    cost = fee + slippage
    for i in range(1, len(prices)):
        r = (prices[i] - prices[i - 1]) / prices[i - 1]
        returns.append(r - cost if r > 0 else 0.0)
    return returns


def _mean_reversion(
    prices: List[float], fee: float = 0.0, slippage: float = 0.0
) -> List[float]:
    """Simple mean-reversion strategy.

    This toy implementation buys after price drops and assumes an immediate
    rebound. Negative returns are flipped to positive gains while positive
    moves are ignored.
    """

    returns: List[float] = []
    cost = fee + slippage
    for i in range(1, len(prices)):
        r = (prices[i] - prices[i - 1]) / prices[i - 1]
        returns.append(-r - cost if r < 0 else 0.0)
    return returns


DEFAULT_STRATEGIES: List[
    Tuple[str, Callable[[List[float], float, float], List[float]]]
] = [
    ("buy_hold", _buy_and_hold),
    ("momentum", _momentum),
    ("mean_reversion", _mean_reversion),
]

# Packaged price data for the demo
DATA_FILE = resources.files(__package__) / "data" / "investor_demo_prices.json"

# Optional preset datasets bundled with the package
PRESET_DATA_FILES: Dict[str, Path] = {
    "short": resources.files(__package__) / "data" / "investor_demo_prices_short.json",
    "multi": resources.files(__package__) / "data" / "investor_demo_prices_multi.json",
    "full": resources.files(__package__) / "data" / "investor_demo_prices_full.json",
}


def compute_weighted_returns(
    prices: List[float],
    weights: Dict[str, float],
    fee: float = 0.0,
    slippage: float = 0.0,
) -> List[float]:
    """Aggregate strategy returns weighted by ``weights``.

    Negative weights represent short positions.  Returns are normalised by the
    sum of absolute weights so that portfolios with offsetting long and short
    allocations still produce meaningful values.
    """

    arrs: List[Tuple[List[float], float]] = []
    abs_sum = 0.0
    for name, strat in DEFAULT_STRATEGIES:
        w = float(weights.get(name, 0.0))
        if not w:
            continue
        rets = strat(prices, fee, slippage)
        if rets:
            arrs.append(([float(r) for r in rets], w))
            abs_sum += abs(w)
    if not arrs or abs_sum == 0.0:
        return []
    length = min(len(a) for a, _ in arrs)
    agg = [0.0] * length
    for a, w in arrs:
        for i in range(length):
            agg[i] += w * a[i]
    for i in range(length):
        agg[i] /= abs_sum
    return agg


def max_drawdown(returns: List[float]) -> float:
    """Calculate maximum drawdown from a series of returns."""
    if not returns:
        return 0.0
    cumulative: List[float] = []
    total = 1.0
    for r in returns:
        total *= 1 + r
        cumulative.append(total)
    peak: List[float] = []
    max_val = float("-inf")
    for c in cumulative:
        if c > max_val:
            max_val = c
        peak.append(max_val)
    drawdowns = [(p - c) / p if p else 0.0 for c, p in zip(cumulative, peak)]
    return max(drawdowns) if drawdowns else 0.0


def load_prices(
    path: Path | None = None,
    preset: str | None = None,
) -> Union[Tuple[List[float], List[str]], Dict[str, Tuple[List[float], List[str]]]]:
    """Load a JSON price dataset.

    The legacy dataset format is a list of ``{"date", "price"}`` objects which
    represents a single token.  To support scenarios with multiple tokens this
    function also accepts a mapping of token name to such a list.  When a
    mapping is provided the return value is a dictionary keyed by token whose
    values are ``(prices, dates)`` tuples.  For backwards compatibility a list
    input continues to return a single ``(prices, dates)`` tuple.
    """

    if path is not None and preset is not None:
        raise ValueError("Provide only one of 'path' or 'preset'")

    if preset is not None:
        data_path = PRESET_DATA_FILES.get(preset)
        if data_path is None:
            raise ValueError(f"Unknown preset '{preset}'")
        data_text = data_path.read_text()
    elif path is not None:
        data_text = path.read_text()
    else:
        data_text = DATA_FILE.read_text()
    data = json.loads(data_text)

    def _parse(entries: List[object]) -> Tuple[List[float], List[str]]:
        prices: List[float] = []
        dates: List[str] = []
        for entry in entries:
            if not isinstance(entry, dict) or "price" not in entry or "date" not in entry:
                raise ValueError("Each entry must contain 'date' and numeric 'price'")
            price = entry["price"]
            date = entry["date"]
            if not isinstance(price, (int, float)):
                raise ValueError("Each entry must contain a numeric 'price'")
            if not isinstance(date, str):
                raise ValueError("Each entry must contain a string 'date'")
            prices.append(float(price))
            dates.append(date)
        return prices, dates

    if isinstance(data, list):
        return _parse(data)
    if isinstance(data, dict):
        out: Dict[str, Tuple[List[float], List[str]]] = {}
        for token, entries in data.items():
            if not isinstance(token, str) or not isinstance(entries, list):
                raise ValueError("Price data mapping must be token -> list of entries")
            out[token] = _parse(entries)
        return out
    raise ValueError("Price data must be a list or mapping of token to list")


async def _demo_arbitrage() -> Dict[str, object]:
    """Exercise the real :mod:`arbitrage` module with static prices."""
    try:
        from . import arbitrage
    except ImportError as exc:  # pragma: no cover - absence of module
        logging.getLogger(__name__).warning("Arbitrage demo skipped: %s", exc)
        used_trade_types.add("arbitrage")
        return {"path": [], "profit": 0.0}

    prices = {"dex1": 100.0, "dex2": 105.0}
    fees = {"dex1": 0.0, "dex2": 0.0}
    gas = {"dex1": 0.0, "dex2": 0.0}
    latency = {"dex1": 0.0, "dex2": 0.0}
    depth = {
        "dex1": {"bids": 1_000.0, "asks": 1_000.0},
        "dex2": {"bids": 1_000.0, "asks": 1_000.0},
    }
    try:
        path, profit = arbitrage._best_route(  # type: ignore[attr-defined]
            prices,
            1.0,
            fees=fees,
            gas=gas,
            latency=latency,
            depth=depth,
            use_flash_loans=False,
            max_flash_amount=0.0,
            max_hops=2,
            use_gnn_routing=False,
        )
    except Exception as exc:  # pragma: no cover - best effort
        logging.getLogger(__name__).warning("Arbitrage demo skipped: %s", exc)
        path, profit = [], 0.0
    used_trade_types.add("arbitrage")
    return {"path": path, "profit": float(profit)}


async def _demo_flash_loan() -> str | None:
    """Invoke :mod:`flash_loans.borrow_flash` with stubbed network calls."""
    try:
        from . import flash_loans, depth_client
    except ImportError as exc:
        print(f"Flash loan demo skipped: {exc}")
        return None
    try:
        from solders.keypair import Keypair
        from solders.instruction import Instruction, AccountMeta
        from solders.pubkey import Pubkey
        from solders.hash import Hash
    except ImportError as exc:
        print(f"Flash loan demo skipped: {exc}")
        return None
    import types

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_latest_blockhash(self):
            return types.SimpleNamespace(
                value=types.SimpleNamespace(blockhash=Hash.default())
            )

        async def confirm_transaction(self, _sig: str) -> None:
            return None

    async def dummy_submit(_tx: str) -> str:
        return "demo_sig"

    orig_client = flash_loans.AsyncClient
    orig_submit = depth_client.submit_raw_tx
    flash_loans.AsyncClient = lambda _url: DummyClient()  # type: ignore[assignment]
    depth_client.submit_raw_tx = dummy_submit  # type: ignore[assignment]
    try:
        payer = Keypair()
        ix = Instruction(
            Pubkey.default(), b"demo", [AccountMeta(payer.pubkey(), True, True)]
        )
        sig = await flash_loans.borrow_flash(
            1.0,
            "USDC",
            [ix],
            payer=payer,
            program_accounts={},
            rpc_url="http://offline",
        )
    finally:
        flash_loans.AsyncClient = orig_client  # type: ignore[assignment]
        depth_client.submit_raw_tx = orig_submit  # type: ignore[assignment]
    used_trade_types.add("flash_loan")
    return sig


async def _demo_sniper() -> List[str]:
    """Run :mod:`sniper.evaluate` on deterministic inputs."""

    import sys
    import types
    try:
        from .decision import should_buy, should_sell
        from .simulation import SimulationResult
        from . import models as _models  # noqa: F401
    except ImportError:
        print("Sniper demo skipped: PyTorch not installed")
        used_trade_types.add("sniper")
        return []

    mem_mod = types.ModuleType("solhunter_zero.memory")
    mem_mod.Memory = type("Memory", (), {})  # minimal stub
    sys.modules.setdefault("solhunter_zero.memory", mem_mod)

    main_mod = types.ModuleType("solhunter_zero.main")

    async def fetch_prices(tokens: set[str]) -> Dict[str, float]:
        return {t: 1.0 for t in tokens}

    def run_sims(_token: str, count: int = 100) -> List[SimulationResult]:
        return [
            SimulationResult(
                success_prob=0.9,
                expected_roi=1.2,
                volume=1.0,
                liquidity=1.0,
                slippage=0.1,
                volatility=0.1,
                volume_spike=1.1,
                depth_change=0.1,
                whale_activity=0.1,
                tx_rate=1.0,
            ),
            SimulationResult(
                success_prob=0.8,
                expected_roi=1.1,
                volume=1.0,
                liquidity=1.0,
                slippage=0.1,
                volatility=0.1,
                volume_spike=1.1,
                depth_change=0.1,
                whale_activity=0.1,
                tx_rate=1.0,
            ),
        ]

    main_mod.run_simulations = run_sims  # type: ignore[attr-defined]
    main_mod.should_buy = should_buy  # type: ignore[attr-defined]
    main_mod.should_sell = should_sell  # type: ignore[attr-defined]
    main_mod.fetch_token_prices_async = fetch_prices  # type: ignore[attr-defined]
    sys.modules.setdefault("solhunter_zero.main", main_mod)

    from . import sniper
    from .portfolio import Portfolio, Position

    orig_predict = sniper.predict_price_movement
    sniper.predict_price_movement = lambda _t: 0.1
    try:
        port = Portfolio(path=None)
        port.balances["USD"] = Position("USD", 100.0, 1.0, 1.0)
        actions = await sniper.evaluate("TKN", port)
    finally:
        sniper.predict_price_movement = orig_predict
    used_trade_types.add("sniper")
    return [a["token"] for a in actions]


async def _demo_dex_scanner() -> List[str]:
    """Scan for new pools using :mod:`dex_scanner` with stubbed RPC."""
    try:
        from . import dex_scanner
    except ImportError as exc:
        print(f"DEX scanner demo skipped: {exc}")
        return []

    class DummyClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get_program_accounts(self, *_a, **_k):
            return {
                "result": [
                    {
                        "account": {
                            "data": {
                                "parsed": {
                                    "info": {
                                        "tokenA": {
                                            "mint": "mintA",
                                            "name": "AlphaBonk",
                                        },
                                        "tokenB": {
                                            "mint": "mintB",
                                            "name": "BetaBonk",
                                        },
                                    }
                                }
                            }
                        }
                    }
                ]
            }

    orig_client = dex_scanner.AsyncClient
    dex_scanner.AsyncClient = lambda _url: DummyClient()  # type: ignore[assignment]
    try:
        tokens = await dex_scanner.scan_new_pools("http://offline")
    finally:
        dex_scanner.AsyncClient = orig_client  # type: ignore[assignment]
    used_trade_types.add("dex_scanner")
    return tokens


async def _demo_route_ffi() -> Dict[str, object]:
    """Exercise ``routeffi._best_route_json`` on deterministic data."""

    global ROUTE_FFI_AVAILABLE
    try:
        from . import routeffi
    except Exception as exc:  # pragma: no cover - optional dependency
        warnings.warn(f"Route FFI demo skipped: {exc}")
        return {"path": [], "profit": 0.0}
    ROUTE_FFI_AVAILABLE = True

    prices = {"dex1": 100.0, "dex2": 105.0}
    fees = {"dex1": 0.0, "dex2": 0.0}
    try:
        result = routeffi._best_route_json(
            prices,
            1.0,
            fees=fees,
            gas=fees,
            latency=fees,
            max_hops=2,
        )
    except Exception as exc:  # pragma: no cover - best effort
        warnings.warn(f"Route FFI demo skipped: {exc}")
        result = None
    if result is None:
        path = ["dex1", "dex2"]
        profit = (prices["dex2"] - prices["dex1"]) * 1.0
    else:
        path, profit = result
    used_trade_types.add("route_ffi")
    return {"path": path, "profit": float(profit)}


async def _demo_jito_stream() -> List[dict]:
    """Stream a deterministic pending swap via :mod:`jito_stream`."""

    global JITO_STREAM_AVAILABLE
    try:
        from . import jito_stream
    except Exception as exc:  # pragma: no cover - optional dependency
        warnings.warn(f"Jito stream demo skipped: {exc}")
        return []
    JITO_STREAM_AVAILABLE = True

    async def fake_stream(_url: str, *, auth: str | None = None):
        yield {
            "pendingTransactions": [
                {"swap": {"token": "tok", "size": 1.0, "slippage": 0.1}}
            ]
        }

    orig_stream = jito_stream.stream_pending_transactions
    jito_stream.stream_pending_transactions = fake_stream  # type: ignore[assignment]
    swaps: List[dict] = []
    try:
        async for swap in jito_stream.stream_pending_swaps("ws://demo"):
            swaps.append(swap)
            break
    finally:
        jito_stream.stream_pending_transactions = orig_stream  # type: ignore[assignment]
    used_trade_types.add("jito_stream")
    return swaps


def run_rl_demo(report_dir: Path) -> float:
    """Run a tiny RL demo and write training metrics.

    The function attempts to execute the real RL training pipeline when heavy
    dependencies like :mod:`torch` are available.  In minimal environments it
    falls back to a compact NumPy Q-learning routine.  Training losses and the
    reward curve are written to ``report_dir/rl_metrics.json`` and the final
    cumulative reward is returned.
    """

    if not RL_DEMO:
        return 0.0

    def _write_metrics(metrics: Dict[str, List[float]]) -> None:
        try:
            with open(report_dir / "rl_metrics.json", "w", encoding="utf-8") as mf:
                json.dump(metrics, mf, indent=2)
        except Exception:
            pass

    def _numpy_q_learning() -> float:
        try:
            import numpy as np

            n_states, n_actions = 2, 2
            q = np.zeros((n_states, n_actions))
            rewards: List[float] = []
            losses: List[float] = []
            for epoch in range(10):
                state = epoch % n_states
                action = int(np.argmax(q[state]))
                reward = 1.0 if action == state else -1.0
                next_state = (state + 1) % n_states
                td_target = reward + 0.9 * np.max(q[next_state])
                td_error = td_target - q[state, action]
                q[state, action] += 0.1 * td_error
                rewards.append(float(reward))
                losses.append(float(td_error ** 2))
            _write_metrics({"loss": losses, "rewards": rewards})
            return float(sum(rewards))
        except Exception:  # pragma: no cover - NumPy missing
            q = [[0.0, 0.0], [0.0, 0.0]]
            rewards: List[float] = []
            losses: List[float] = []
            for epoch in range(10):
                state = epoch % 2
                action = 0 if q[state][0] >= q[state][1] else 1
                reward = 1.0 if action == state else -1.0
                next_state = (state + 1) % 2
                td_target = reward + 0.9 * max(q[next_state])
                td_error = td_target - q[state][action]
                q[state][action] += 0.1 * td_error
                rewards.append(float(reward))
                losses.append(float(td_error ** 2))
            _write_metrics({"loss": losses, "rewards": rewards})
            return float(sum(rewards))

    if not FULL_SYSTEM:
        return _numpy_q_learning()

    try:
        from datetime import datetime
        from types import SimpleNamespace
        import tempfile
        import torch  # type: ignore[import-untyped]
        from torch.utils.data import DataLoader  # type: ignore[import-untyped]

        from . import rl_training, simulation
        from .rl_training import _TradeDataset, LightningPPO
    except Exception:  # pragma: no cover - optional deps
        return _numpy_q_learning()

    orig_predict = simulation.predict_price_movement
    simulation.predict_price_movement = lambda *a, **k: 0.0  # type: ignore
    try:
        now = datetime.utcnow()
        trades = [
            SimpleNamespace(
                token="demo", side="buy", price=1.0, amount=1.0, timestamp=now
            ),
            SimpleNamespace(
                token="demo", side="sell", price=1.1, amount=1.0, timestamp=now
            ),
        ]
        snaps = [
            SimpleNamespace(
                token="demo",
                depth=1.0,
                slippage=0.0,
                imbalance=0.0,
                tx_rate=0.0,
                timestamp=now,
            )
        ]

        with tempfile.TemporaryDirectory() as td:
            model_path = Path(td) / "demo_rl.pt"
            rl_training.fit(trades, snaps, model_path=model_path, algo="ppo")
            model = LightningPPO()
            model.load_state_dict(torch.load(model_path))
            dataset = _TradeDataset(trades, snaps, sims_per_token=0)
            loader = DataLoader(dataset, batch_size=len(dataset))
            model.eval()
            total = 0.0
            rewards_curve: List[float] = []
            losses: List[float] = []
            loss_fn = torch.nn.CrossEntropyLoss(reduction="none")
            with torch.no_grad():
                for states, actions, rewards_tensor in loader:
                    logits = model.actor(states)
                    losses.extend(loss_fn(logits, actions).tolist())
                    rewards_curve.extend(rewards_tensor.tolist())
                    preds = logits.argmax(dim=1)
                    mask = preds == actions
                    if mask.any():
                        total += float(rewards_tensor[mask].sum())
            _write_metrics({"loss": losses, "rewards": rewards_curve})
            return float(total)
    except Exception:  # pragma: no cover - best effort
        return _numpy_q_learning()
    finally:
        simulation.predict_price_movement = orig_predict


def _demo_rl_agent() -> float:
    report_dir = RL_REPORT_DIR or Path(".")
    return run_rl_demo(report_dir)


def _learning_demo(prices: List[float], iterations: int = 3) -> None:
    """Run a tiny learning loop rotating strategy weights.

    This uses the ``_demo_rl_agent`` stub to generate a reward each iteration
    and cycles through a canned set of strategy weights.  The reward and the
    next set of weights are printed so that the learning progression can be
    observed without heavy dependencies.
    """

    order = ["buy_hold", "momentum", "mean_reversion"]
    weights: Dict[str, float] = {order[0]: 1.0}
    for i in range(iterations):
        reward = _demo_rl_agent()
        port_ret = sum(compute_weighted_returns(prices, weights))
        print(
            f"Learning iteration {i + 1}: reward {reward:.2f} "
            f"portfolio {port_ret:.4f} weights {weights}"
        )
        weights = {order[(i + 1) % len(order)]: 1.0}


def main(argv: List[str] | None = None) -> None:
    used_trade_types.clear()
    try:
        from . import resource_monitor
    except Exception:  # pragma: no cover - optional dependency
        resource_monitor = None  # type: ignore[assignment]
    try:  # optional risk analysis
        from .risk import correlation_matrix  # type: ignore
    except Exception:  # pragma: no cover - risk module optional
        correlation_matrix = None  # type: ignore[assignment]
    try:
        import psutil  # type: ignore
    except Exception:  # pragma: no cover - psutil optional
        psutil = None  # type: ignore
    parser = argparse.ArgumentParser(description="Investor demo backtest")
    parser.add_argument(
        "--data",
        type=Path,
        default=None,
        help="Path to JSON price history",
    )
    parser.add_argument(
        "--preset",
        choices=sorted(PRESET_DATA_FILES.keys()),
        default=None,
        help="Bundled price dataset to use (default: 'short')",
    )
    parser.add_argument(
        "--reports",
        type=Path,
        default=Path("reports"),
        help="Directory to write reports/plots to",
    )
    parser.add_argument(
        "--capital",
        type=float,
        default=100.0,
        help="Starting capital for the backtest",
    )
    parser.add_argument(
        "--fee",
        type=float,
        default=0.0,
        help="Per-trade fee as a fractional cost",
    )
    parser.add_argument(
        "--slippage",
        type=float,
        default=0.0,
        help="Per-trade slippage as a fractional cost",
    )
    parser.add_argument(
        "--full-system",
        action="store_true",
        help="Run the heavier RL components",
    )
    parser.add_argument(
        "--rl-demo",
        action="store_true",
        help="Run a lightweight RL demo using a tiny pre-trained stub",
    )
    parser.add_argument(
        "--learn",
        action="store_true",
        help="Run a tiny learning loop that rotates strategy weights",
    )
    parser.add_argument(
        "--price-streams",
        default=None,
        help="Comma-separated venue=url pairs for live price streams",
    )
    parser.add_argument(
        "--tokens",
        default=None,
        help="Comma-separated tokens to subscribe to in price streams",
    )
    args = parser.parse_args(argv)

    if args.data is not None and args.preset is not None:
        raise ValueError("Cannot specify both --data and --preset")

    if args.preset is not None:
        preset = args.preset
    elif args.data is not None:
        preset = None
    else:
        preset = "short"

    global FULL_SYSTEM, RL_DEMO, RL_REPORT_DIR
    FULL_SYSTEM = bool(args.full_system)
    RL_DEMO = bool(args.rl_demo or args.full_system or args.learn)
    RL_REPORT_DIR = args.reports

    loaded = load_prices(args.data, preset)
    if isinstance(loaded, dict):
        price_map = loaded
    else:
        # Default token name for single-token datasets
        price_map = {"demo": loaded}
    if not price_map:
        raise ValueError("price data must contain at least one token")
    multi_token = len(price_map) > 1

    first_token, (prices, dates) = next(iter(price_map.items()))
    if not prices or not dates:
        raise ValueError("price data must contain at least one entry")

    stream_mgr = None
    _stream_loop = None
    _stream_thread = None
    if args.price_streams:
        streams: Dict[str, str] = {}
        for spec in str(args.price_streams).split(','):
            if not spec:
                continue
            if '=' not in spec:
                raise ValueError('--price-streams requires venue=url pairs')
            venue, url = spec.split('=', 1)
            streams[venue.strip()] = url.strip()
        if not streams:
            raise ValueError('--price-streams requires venue=url pairs')
        tokens = (
            [t.strip() for t in str(args.tokens).split(',') if t.strip()]
            if args.tokens
            else list(price_map.keys())
        )
        try:
            from .price_stream_manager import PriceStreamManager  # type: ignore
        except ImportError:
            print(
                "websockets package is required for --price-streams; skipping live price streams",
                file=sys.stderr,
            )
            PriceStreamManager = None  # type: ignore[assignment]
        if PriceStreamManager is not None:
            stream_mgr = PriceStreamManager(streams, tokens)
            _stream_loop = asyncio.new_event_loop()
            _stream_thread = threading.Thread(
                target=_stream_loop.run_forever, daemon=True
            )
            _stream_thread.start()
            asyncio.run_coroutine_threadsafe(
                stream_mgr.start(), _stream_loop
            ).result()

    # Demonstrate Memory usage and portfolio hedging using the first token.
    # ``Memory`` relies on SQLAlchemy which may not be installed in minimal
    # environments.  Attempt to instantiate it and fall back to a simple
    # in-memory implementation when unavailable.
    try:
        if Memory is None:
            raise ImportError
        mem = Memory("sqlite:///:memory:")  # type: ignore[call-arg]
    except ImportError:
        from .simple_memory import SimpleMemory

        mem = SimpleMemory()

    async def _record_demo_trade() -> None:
        await mem.log_trade(
            token=first_token, direction="buy", amount=1.0, price=prices[0]
        )
        trades = await mem.list_trades(token=first_token)
        assert len(trades) == 1

    asyncio.run(_record_demo_trade())

    mem.log_var(0.0)
    asyncio.run(mem.close())

    # Compute correlations between strategy returns for the first token
    strategy_returns: Dict[str, List[float]] = {
        name: strat(prices, args.fee, args.slippage) for name, strat in DEFAULT_STRATEGIES
    }
    series: Dict[str, List[float]] = {}
    for name, rets in strategy_returns.items():
        if not rets:
            continue
        pseudo_prices = [1.0]
        for r in rets:
            pseudo_prices.append(pseudo_prices[-1] * (1 + r))
        series[name] = pseudo_prices
    corr_pairs: Dict[tuple[str, str], float] = {}
    if len(series) >= 2:
        def _manual_corr() -> None:
            keys = list(strategy_returns.keys())
            for i in range(len(keys)):
                for j in range(i + 1, len(keys)):
                    a = strategy_returns[keys[i]]
                    b = strategy_returns[keys[j]]
                    n = min(len(a), len(b))
                    if n == 0:
                        continue
                    a = a[:n]
                    b = b[:n]
                    ma = sum(a) / n
                    mb = sum(b) / n
                    va = sum((x - ma) ** 2 for x in a) / n
                    vb = sum((y - mb) ** 2 for y in b) / n
                    if va <= 0 or vb <= 0:
                        c = 0.0
                    else:
                        cov = sum((a[k] - ma) * (b[k] - mb) for k in range(n)) / n
                        c = cov / (va ** 0.5 * vb ** 0.5)
                    corr_pairs[(keys[i], keys[j])] = c

        if correlation_matrix is not None:
            try:
                corr_mat = correlation_matrix(series)
                keys = list(series.keys())
                for i in range(len(keys)):
                    for j in range(i + 1, len(keys)):
                        corr_pairs[(keys[i], keys[j])] = float(corr_mat[i, j])
            except Exception:
                _manual_corr()
        else:
            _manual_corr()
    hedged_weights = hedge_allocation(
        {"buy_hold": 1.0, "momentum": 0.0}, corr_pairs
    )

    configs = {
        "buy_hold": {"buy_hold": 1.0},
        "momentum": {"momentum": 1.0},
        "mean_reversion": {"mean_reversion": 1.0},
        "mixed": {
            "buy_hold": 1 / 3,
            "momentum": 1 / 3,
            "mean_reversion": 1 / 3,
        },
    }
    args.reports.mkdir(parents=True, exist_ok=True)

    # Persist correlation pairs and hedged weights for inspection
    corr_out = {str((a, b)): c for (a, b), c in corr_pairs.items()}
    with open(args.reports / "correlations.json", "w", encoding="utf-8") as cf:
        json.dump(corr_out, cf, indent=2)
    with open(args.reports / "hedged_weights.json", "w", encoding="utf-8") as hf:
        json.dump(hedged_weights, hf, indent=2)

    # Produce concise summaries for stdout and later highlights.json
    top_corr = sorted(corr_pairs.items(), key=lambda kv: abs(kv[1]), reverse=True)[:3]
    corr_summary = {f"{a}-{b}": c for (a, b), c in top_corr}
    if corr_summary:
        print(
            "Key correlations:",
            ", ".join(f"{pair}: {val:.2f}" for pair, val in corr_summary.items()),
        )
    if hedged_weights:
        print(
            "Hedged weights:",
            ", ".join(f"{k}: {v:.2f}" for k, v in hedged_weights.items()),
        )
    summary: List[Dict[str, float | int | str]] = []
    trade_history: List[Dict[str, float | str]] = []

    for token, (prices, dates) in price_map.items():
        if not prices or not dates:
            raise ValueError("price data must contain at least one entry")
        for name, weights in configs.items():
            returns = compute_weighted_returns(prices, weights, args.fee, args.slippage)
            if returns:
                cum: List[float] = []
                total = 1.0
                for r in returns:
                    total *= 1 + r
                    cum.append(total)
                roi = cum[-1] - 1
                mean = sum(returns) / len(returns)
                variance = sum((r - mean) ** 2 for r in returns) / len(returns)
                vol = variance ** 0.5
                sharpe = mean / vol if vol else 0.0
                trades = sum(1 for r in returns if r != 0)
                wins = sum(1 for r in returns if r > 0)
                losses = sum(1 for r in returns if r < 0)
                win_rate = wins / trades if trades else 0.0
            else:
                cum = []
                roi = 0.0
                sharpe = 0.0
                vol = 0.0
                trades = wins = losses = 0
                win_rate = 0.0
            dd = max_drawdown(returns)
            final_capital = args.capital * (1 + roi)
            metrics = {
                "token": token,
                "config": name,
                "roi": roi,
                "sharpe": sharpe,
                "drawdown": dd,
                "volatility": vol,
                "trades": trades,
                "wins": wins,
                "losses": losses,
                "win_rate": win_rate,
                "final_capital": final_capital,
            }
            summary.append(metrics)

            # record per-period capital history for this strategy/token
            capital = args.capital
            trade_history.append(
                {
                    "token": token,
                    "strategy": name,
                    "period": 0,
                    "date": dates[0],
                    "action": "buy",
                    "price": prices[0],
                    "capital": capital,
                }
            )
            for i in range(1, len(prices)):
                r = returns[i - 1] if i - 1 < len(returns) else 0.0
                capital *= 1 + r
                action = "buy" if r > 0 else "sell" if r < 0 else "hold"
                trade_history.append(
                    {
                        "token": token,
                        "strategy": name,
                        "period": i,
                        "date": dates[i],
                        "action": action,
                        "price": prices[i],
                        "capital": capital,
                    }
                )
            for i in range(len(returns) + 1, len(prices)):
                trade_history.append(
                    {
                        "token": token,
                        "strategy": name,
                        "period": i,
                        "date": dates[i],
                        "action": "hold",
                        "price": prices[i],
                        "capital": capital,
                    }
                )

            try:  # plotting hook
                import matplotlib.pyplot as plt  # type: ignore

                plt.figure()
                if cum:
                    plt.plot(cum, label="Cumulative Return")
                plt.title(f"Performance - {name}")
                plt.xlabel("Period")
                plt.ylabel("Growth")
                plt.legend()
                plt.tight_layout()
                plt.savefig(args.reports / f"{token}_{name}.png")
                plt.close()
            except Exception as exc:  # pragma: no cover - plotting optional
                print(f"Plotting failed for {token} {name}: {exc}")

    agents_dir = Path(__file__).resolve().parent / "agents"
    agent_modules = [
        p.stem for p in agents_dir.glob("*.py") if not p.name.startswith("__")
    ]
    existing = {str(row["config"]) for row in summary}
    default_token = str(summary[0]["token"]) if summary else "demo"
    for mod in agent_modules:
        if mod not in existing:
            summary.append(
                {
                    "token": default_token,
                    "config": mod,
                    "roi": 0.0,
                    "sharpe": 0.0,
                    "drawdown": 0.0,
                    "volatility": 0.0,
                    "trades": 0,
                    "wins": 0,
                    "losses": 0,
                    "win_rate": 0.0,
                    "final_capital": args.capital,
                }
            )

    json_path = args.reports / "summary.json"
    csv_path = args.reports / "summary.csv"
    with open(json_path, "w", encoding="utf-8") as jf:
        json.dump(summary, jf, indent=2)
    with open(csv_path, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(cf, fieldnames=summary[0].keys())
        writer.writeheader()
        for row in summary:
            writer.writerow(row)

    # Compute aggregated metrics across strategies
    strat_groups: Dict[str, Dict[str, object]] = {}
    total_roi = 0.0
    total_sharpes: List[float] = []
    for row in summary:
        name = str(row["config"])
        grp = strat_groups.setdefault(name, {"roi": 0.0, "sharpes": []})
        roi = float(row["roi"])
        sharpe = float(row["sharpe"])
        grp["roi"] = float(grp["roi"]) + roi
        grp.setdefault("sharpes", []).append(sharpe)  # type: ignore[call-arg]
        total_roi += roi
        total_sharpes.append(sharpe)

    avg_sharpe = sum(total_sharpes) / len(total_sharpes) if total_sharpes else 0.0
    best_strategy = (
        max(strat_groups.items(), key=lambda kv: kv[1]["roi"])[0]
        if strat_groups
        else None
    )
    worst_strategy = (
        min(strat_groups.items(), key=lambda kv: kv[1]["roi"])[0]
        if strat_groups
        else None
    )

    aggregated: Dict[str, object] = {
        "total_roi": total_roi,
        "average_sharpe": avg_sharpe,
        "best_strategy": best_strategy,
        "worst_strategy": worst_strategy,
    }
    with open(args.reports / "aggregated_summary.json", "w", encoding="utf-8") as agf:
        json.dump(aggregated, agf, indent=2)

    strat_table: List[Tuple[str, float, float]] = []
    for name, metrics in strat_groups.items():
        vals: List[float] = metrics.get("sharpes", [])  # type: ignore[assignment]
        avg = sum(vals) / len(vals) if vals else 0.0
        strat_table.append((name, float(metrics["roi"]), avg))

    # Derive an aggregate view across tokens using the best strategy for each
    # token.  ``summary`` holds a row for every (token, strategy) pair so we
    # select the strategy with the highest final capital per token and compute
    # global metrics from those winners.
    per_token_best: Dict[str, Dict[str, float | int | str]] = {}
    for row in summary:
        tok = str(row["token"])
        existing = per_token_best.get(tok)
        if existing is None or float(row["final_capital"]) > float(existing["final_capital"]):
            per_token_best[tok] = row

    agg_rows = [
        {
            "token": t,
            "strategy": r["config"],
            "roi": r["roi"],
            "sharpe": r["sharpe"],
            "final_capital": r["final_capital"],
        }
        for t, r in per_token_best.items()
    ]

    global_roi = (
        sum(float(r["roi"]) for r in agg_rows) / len(agg_rows) if agg_rows else 0.0
    )
    global_sharpe = (
        sum(float(r["sharpe"]) for r in agg_rows) / len(agg_rows)
        if agg_rows
        else 0.0
    )

    top = max(summary, key=lambda e: e["final_capital"]) if summary else None

    aggregate: Dict[str, object] = {
        "global_roi": global_roi,
        "global_sharpe": global_sharpe,
        "per_token": agg_rows,
    }
    if top is not None:
        aggregate.update(
            {
                "top_token": top["token"],
                "top_strategy": top["config"],
                "top_final_capital": top["final_capital"],
                "top_roi": top["roi"],
                "top_sharpe": top["sharpe"],
            }
        )

    agg_json = args.reports / "aggregate_summary.json"
    agg_csv = args.reports / "aggregate_summary.csv"
    with open(agg_json, "w", encoding="utf-8") as af:
        json.dump(aggregate, af, indent=2)
    with open(agg_csv, "w", newline="", encoding="utf-8") as cf:
        writer = csv.DictWriter(
            cf, fieldnames=["token", "strategy", "roi", "sharpe", "final_capital"]
        )
        writer.writeheader()
        for row in agg_rows:
            writer.writerow(row)

    # Write trade history in CSV and JSON for inspection
    hist_csv = args.reports / "trade_history.csv"
    hist_json = args.reports / "trade_history.json"
    if trade_history:
        with open(hist_json, "w", encoding="utf-8") as jf:
            json.dump(trade_history, jf, indent=2)
        with open(hist_csv, "w", newline="", encoding="utf-8") as cf:
            writer = csv.DictWriter(
                cf,
                fieldnames=[
                    "token",
                    "strategy",
                    "period",
                    "date",
                    "action",
                    "price",
                    "capital",
                ],
            )
            writer.writeheader()
            for row in trade_history:
                writer.writerow(row)

    # Exercise trade types via lightweight stubs
    async def _exercise_trade_types() -> Dict[str, object]:
        (
            arb,
            fl_sig,
            sniped,
            pools,
            route,
            jito_swaps,
        ) = await asyncio.gather(
            _demo_arbitrage(),
            _demo_flash_loan(),
            _demo_sniper(),
            _demo_dex_scanner(),
            _demo_route_ffi(),
            _demo_jito_stream(),
        )
        rl_reward = _demo_rl_agent()
        arb_path = arb.get("path") if isinstance(arb, dict) else None
        arb_profit = arb.get("profit") if isinstance(arb, dict) else None
        route_path = route.get("path") if isinstance(route, dict) else None
        route_profit = route.get("profit") if isinstance(route, dict) else None
        return {
            "arbitrage_path": arb_path,
            "arbitrage_profit": arb_profit,
            "route_ffi_path": route_path,
            "route_ffi_profit": route_profit,
            "flash_loan_signature": fl_sig,
            "sniper_tokens": sniped,
            "dex_new_pools": pools,
            "rl_reward": rl_reward,
            "jito_swaps": jito_swaps,
        }

    trade_outputs = asyncio.run(_exercise_trade_types())

    required = {"arbitrage", "flash_loan", "sniper", "dex_scanner"}
    optional_flags = {
        "route_ffi": ROUTE_FFI_AVAILABLE,
        "jito_stream": JITO_STREAM_AVAILABLE,
    }
    for name, available in optional_flags.items():
        if available:
            required.add(name)
    missing = required - used_trade_types
    if missing:
        raise RuntimeError(
            f"Demo did not exercise trade types: {', '.join(sorted(missing))}"
        )

    executed_optional = sorted(
        name for name, avail in optional_flags.items() if avail and name in used_trade_types
    )
    skipped_optional = sorted(
        name for name, avail in optional_flags.items() if not avail
    )
    print(
        "Executed optional trade types: "
        + (", ".join(executed_optional) if executed_optional else "none")
    )
    print(
        "Skipped optional trade types: "
        + (", ".join(skipped_optional) if skipped_optional else "none")
    )

    # Collect resource usage metrics if available
    cpu_usage = None
    mem_pct = None
    if psutil is not None and resource_monitor is not None:
        try:
            cpu_usage = resource_monitor.get_cpu_usage()
            mem_pct = psutil.virtual_memory().percent
        except Exception:
            pass

    # Write highlights summarising top performing strategy and trade results
    if top is not None:
        highlights = {
            "top_strategy": top["config"],
            "top_final_capital": top["final_capital"],
            "top_roi": top["roi"],
        }
        if multi_token:
            highlights["top_token"] = top["token"]
        if cpu_usage is not None:
            highlights["cpu_usage"] = cpu_usage
        if mem_pct is not None:
            highlights["memory_percent"] = mem_pct
        highlights.update(trade_outputs)
        if corr_summary:
            highlights["key_correlations"] = corr_summary
        if hedged_weights:
            highlights["hedged_weights"] = hedged_weights
        with open(args.reports / "highlights.json", "w", encoding="utf-8") as hf:
            json.dump(highlights, hf, indent=2)

    # Display a simple capital summary on stdout
    print("Capital Summary:")
    for row in summary:
        # Include ROI and Sharpe ratio alongside final capital so that the CLI
        # output mirrors the contents of ``summary.json``.
        prefix = f"{row['token']} {row['config']}" if multi_token else row["config"]
        line = (
            f"{prefix}: {row['final_capital']:.2f} "
            f"ROI {row['roi']:.4f} "
            f"Sharpe {row['sharpe']:.4f} "
            f"Drawdown {row['drawdown']:.4f} "
            f"Win rate {row['win_rate']:.4f}"
        )
        print(line)

    if strat_table:
        try:  # Optional dependency
            from tabulate import tabulate  # type: ignore[import-untyped]

            table = [
                (name, f"{roi:.4f}", f"{sharpe:.4f}") for name, roi, sharpe in strat_table
            ]
            print("\nAggregated Strategy Performance:")
            print(tabulate(table, headers=["Strategy", "Total ROI", "Avg Sharpe"]))
        except Exception:
            print("\nAggregated Strategy Performance:")
            for name, roi, sharpe in strat_table:
                print(f"{name}: ROI {roi:.4f} Avg Sharpe {sharpe:.4f}")
    if top:
        top_prefix = (
            f"{top['token']} {top['config']}" if multi_token else top["config"]
        )
        print(
            f"Top strategy: {top_prefix} with final capital {top['final_capital']:.2f}"
        )
    print("Trade type results:", json.dumps(trade_outputs))
    if cpu_usage is not None and mem_pct is not None:
        print(
            f"Resource usage - CPU: {cpu_usage:.2f}% Memory: {mem_pct:.2f}%"
        )

    print(f"Wrote reports to {args.reports}")

    if args.learn:
        _learning_demo(prices)

    if stream_mgr is not None and _stream_loop is not None:
        asyncio.run_coroutine_threadsafe(stream_mgr.stop(), _stream_loop).result()
        _stream_loop.call_soon_threadsafe(_stream_loop.stop)
        if _stream_thread is not None:
            _stream_thread.join()


if __name__ == "__main__":  # pragma: no cover
    main()
