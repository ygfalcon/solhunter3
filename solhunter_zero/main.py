"""Runtime entry points and trading loop orchestration for SolHunter Zero."""

from __future__ import annotations

import asyncio
import contextlib
import datetime
import logging
import os
import subprocess
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import Mapping, Sequence
import cProfile

from .util import install_uvloop, parse_bool_env
from .agents.discovery import (
    DEFAULT_DISCOVERY_METHOD,
    resolve_discovery_method,
)
from .paths import ROOT
from .config import (
    load_config,
    apply_env_overrides,
    set_env_from_config,
    load_selected_config,
    get_active_config_name,
    CONFIG_DIR,
    initialize_event_bus,
)
from . import wallet
from .production.keypair import resolve_live_keypair
from . import metrics_aggregator
from . import arbitrage
from .bootstrap import bootstrap
from .config_runtime import Config
from .connectivity import ensure_connectivity_async, ensure_connectivity
from .services import start_depth_service as _start_depth_service
from .loop import (
    place_order_async,
    trading_loop,
    FirstTradeTimeoutError,
    ResourceBudgetExceeded,
    run_iteration as _run_iteration,
)
from .agents.discovery import DiscoveryAgent
from .main_state import TradingState
from .memory import Memory, load_snapshot
from .portfolio import Portfolio
from . import prices
from . import prices_bootstrap
from .feature_flags import get_feature_flags
from .strategy_manager import StrategyManager
from .agent_manager import AgentManager
from .onchain_metrics import fetch_dex_metrics_async
try:
    from .token_scanner import scan_tokens_async
except ImportError as exc:  # pragma: no cover - optional dependency guard
    logging.getLogger(__name__).warning(
        "token_scanner unavailable (%s); discovery CLI will use stubbed scan",
        exc,
    )

    async def scan_tokens_async(
        *,
        rpc_url: str | None = None,
        limit: int = 50,
        enrich: bool = True,
        api_key: str | None = None,
    ) -> list[str]:
        del rpc_url, enrich, api_key
        return []
from .simulation import run_simulations
from .decision import should_buy, should_sell
from . import event_bus
from . import depth_client

log = logging.getLogger(__name__)


def _log_active_keypair_path() -> None:
    keypair_env = os.getenv("KEYPAIR_PATH")
    source = "KEYPAIR_PATH"
    candidate = keypair_env
    if not candidate:
        alt = os.getenv("SOLANA_KEYPAIR")
        if alt:
            candidate = alt
            source = "SOLANA_KEYPAIR"
    flags = get_feature_flags()
    mode = flags.mode.lower()
    if not candidate:
        if mode == "paper":
            log.info(
                "Paper mode: running without signing key (ephemeral keypair will be used)"
            )
            return
        message = "Live trading requires KEYPAIR_PATH or SOLANA_KEYPAIR to be configured"
        log.error(message)
        raise SystemExit(message)
    path = Path(candidate).expanduser()
    if path.exists():
        log.info("Using keypair from %s (%s)", source, path)
        return
    if mode == "paper":
        log.info(
            "Paper mode: configured %s (%s) missing; proceeding with ephemeral keypair",
            source,
            path,
        )
        return
    message = f"Configured {source} points to missing path {path}"
    log.error(message)
    raise SystemExit(message)


def _ensure_live_keypair_ready(cfg: Mapping[str, object]) -> None:
    """Guarantee a signing keypair is available when running in live mode."""

    try:
        mode = get_feature_flags().mode.lower()
    except Exception:
        mode = "paper"
    if mode != "live":
        return

    resolved, pubkey = resolve_live_keypair(cfg, announce=False)
    if resolved and resolved.exists():
        log.info("Live keypair ready at %s (pubkey=%s)", resolved, pubkey)

_ORIGINAL_STRATEGY_MANAGER = StrategyManager

try:  # optional dependency; tests patch attributes when available
    import psutil  # type: ignore
except Exception:  # pragma: no cover - provide minimal stub
    import types as _types

    psutil = _types.SimpleNamespace(cpu_percent=lambda *_a, **_k: 0.0)


install_uvloop()

_DEFAULT_PRESET = Path(__file__).resolve().parent.parent / "config" / "default.toml"


def _normalize_token(value: object) -> str | None:
    if isinstance(value, str):
        token = value.strip()
        return token or None
    if value is None:
        return None
    token = str(value).strip()
    return token or None


def _collect_warmup_tokens(
    portfolio: Portfolio, snapshot_trades: Sequence[dict]
) -> tuple[str, ...]:
    ordered: dict[str, None] = {}
    for token in portfolio.balances.keys():
        normalized = _normalize_token(token)
        if normalized:
            ordered.setdefault(normalized, None)
    for trade in snapshot_trades:
        normalized = _normalize_token(trade.get("token"))
        if normalized:
            ordered.setdefault(normalized, None)
    return tuple(ordered.keys())


def _seed_price_cache_from_state(
    portfolio: Portfolio, snapshot_trades: Sequence[dict]
) -> set[str]:
    seeded: set[str] = set()
    for token, position in portfolio.balances.items():
        normalized = _normalize_token(token)
        if not normalized:
            continue
        entry_price = getattr(position, "entry_price", None)
        if isinstance(entry_price, (int, float)) and entry_price > 0:
            if prices.get_cached_price(normalized) is None:
                prices.update_price_cache(normalized, float(entry_price))
                seeded.add(normalized)
    for trade in snapshot_trades:
        normalized = _normalize_token(trade.get("token"))
        if not normalized or prices.get_cached_price(normalized) is not None:
            continue
        price = trade.get("price")
        if isinstance(price, (int, float)) and price > 0:
            prices.update_price_cache(normalized, float(price))
            seeded.add(normalized)
    return seeded


async def _warm_price_cache_background(
    tokens: Sequence[str], *, delay: float, batch_size: int
) -> None:
    if not tokens:
        return
    try:
        missing = [tok for tok in tokens if prices.get_cached_price(tok) is None]
    except Exception as exc:  # pragma: no cover - defensive guard
        logging.debug("Price warm-up cache lookup failed: %s", exc)
        return
    if not missing:
        return

    batch = max(1, int(batch_size) if batch_size else 1)
    sleep_delay = max(0.0, float(delay) if delay is not None else 0.0)

    for start in range(0, len(missing), batch):
        chunk = missing[start : start + batch]
        try:
            await prices.fetch_token_prices_async(chunk)
        except Exception as exc:  # pragma: no cover - best effort warming
            logging.debug("Price warm-up fetch failed for %s: %s", chunk, exc)
        if start + batch >= len(missing) or sleep_delay <= 0:
            continue
        try:
            await asyncio.sleep(sleep_delay)
        except TypeError as exc:
            logging.warning("Price warm-up sleep skipped: %s", exc)
            break
        except Exception as exc:  # pragma: no cover - defensive guard
            logging.debug("Price warm-up sleep failed: %s", exc)
            break


async def _run_trading_loop_with_warmup(
    *,
    warm_tokens: Sequence[str],
    warmup_delay: float,
    warmup_batch: int,
    trading_kwargs: dict,
) -> None:
    warm_task: asyncio.Task | None = None
    if warm_tokens:
        warm_task = asyncio.create_task(
            _warm_price_cache_background(
                warm_tokens, delay=warmup_delay, batch_size=warmup_batch
            )
        )
    try:
        await trading_loop(**trading_kwargs)
    finally:
        if warm_task is not None:
            with contextlib.suppress(Exception):
                await warm_task


async def perform_startup_async(
    config_path: str | None,
    *,
    offline: bool = False,
    dry_run: bool = False,
) -> tuple[dict, Config, subprocess.Popen | None]:
    """Load configuration, verify connectivity and start services."""
    offline = bool(
        offline
        or parse_bool_env("SOLHUNTER_OFFLINE", False)
        or parse_bool_env("SOLHUNTER_SKIP_CONNECTIVITY", False)
    )
    if offline:
        log.info("[boot] Offline mode enabled; skipping external connectivity checks where possible")
    start = time.perf_counter()
    cfg = apply_env_overrides(load_config(config_path))
    set_env_from_config(cfg)
    _ensure_live_keypair_ready(cfg)
    _log_active_keypair_path()
    try:
        resolved_ids = prices_bootstrap.ensure_env_has_pyth_price_ids()
    except Exception as exc:
        log.debug("Failed to resolve Pyth price ids automatically: %s", exc)
    else:
        log.info("[boot] PYTH_PRICE_IDS resolved: %s", resolved_ids)
    try:
        prices.validate_pyth_overrides_on_boot(network_required=not (offline or dry_run))
    except RuntimeError:
        raise
    except Exception as exc:  # pragma: no cover - validation best effort
        log.debug("Pyth override validation skipped: %s", exc)
    initialize_event_bus()
    runtime_cfg = Config.from_env(cfg)
    metrics_aggregator.publish(
        "startup_config_load_duration", time.perf_counter() - start
    )

    start = time.perf_counter()
    await ensure_connectivity_async(offline=offline or dry_run)
    metrics_aggregator.publish(
        "startup_connectivity_check_duration", time.perf_counter() - start
    )

    start = time.perf_counter()
    proc: subprocess.Popen | None = None
    if parse_bool_env("DEPTH_SERVICE", True):
        proc = _start_depth_service(cfg)
    metrics_aggregator.publish(
        "startup_depth_service_start_duration", time.perf_counter() - start
    )

    return cfg, runtime_cfg, proc


def perform_startup(
    config_path: str | None,
    *,
    offline: bool = False,
    dry_run: bool = False,
) -> tuple[dict, Config, subprocess.Popen | None]:
    """Synchronous wrapper for :func:`perform_startup_async`."""
    return asyncio.run(
        perform_startup_async(config_path, offline=offline, dry_run=dry_run)
    )


def main(
    memory_path: str = "sqlite:///memory.db",
    loop_delay: int = 60,
    min_delay: int | None = None,
    max_delay: int | None = None,
    cpu_low_threshold: float | None = None,
    cpu_high_threshold: float | None = None,
    depth_freq_low: float | None = None,
    depth_freq_high: float | None = None,
    depth_rate_limit: float = 0.1,
    *,
    iterations: int | None = None,
    testnet: bool = False,
    dry_run: bool = False,
    offline: bool = False,
    token_file: str | None = None,
    discovery_method: str | None = None,
    keypair_path: str | None = None,
    portfolio_path: str = "portfolio.json",
    config_path: str | None = None,
    stop_loss: float | None = None,
    take_profit: float | None = None,
    trailing_stop: float | None = None,
    max_drawdown: float | None = None,
    volatility_factor: float | None = None,
    risk_tolerance: float | None = None,
    max_allocation: float | None = None,
    risk_multiplier: float | None = None,
    market_ws_url: str | None = None,
    order_book_ws_url: str | None = None,
    arbitrage_threshold: float | None = None,
    arbitrage_amount: float | None = None,
    arbitrage_tokens: list[str] | None = None,
    strategies: list[str] | None = None,
    rl_daemon: bool = False,
    rl_interval: float = 3600.0,
    dynamic_concurrency: bool = False,
    strategy_rotation_interval: int | None = None,
    weight_config_paths: list[str] | None = None,
    live_discovery: bool | None = None,
) -> None:
    """Run the trading loop."""

    from .wallet import load_keypair

    prev_agents = os.environ.get("AGENTS")
    prev_weights = os.environ.get("AGENT_WEIGHTS")
    try:
        cfg, runtime_cfg, proc = perform_startup(
            config_path, offline=offline, dry_run=dry_run
        )
    except Exception as exc:
        logging.error("Failed to start depth_service: %s", exc)
        cfg = {"depth_service": False}
        runtime_cfg = Config.from_env(cfg)
        proc = None
        os.environ["DEPTH_SERVICE"] = "false"
    metrics_aggregator.start()

    if not asyncio.run(event_bus.verify_broker_connection()):
        logging.error("Message broker verification failed")
        if parse_bool_env("BROKER_VERIFY_ABORT", False):
            raise SystemExit(1)

    cfg_val = cfg.get("use_mev_bundles")
    if cfg_val is not None:
        use_bundles = str(cfg_val).strip().lower() in {"1", "true", "yes"}
    else:
        use_bundles = parse_bool_env("USE_MEV_BUNDLES", False)
    if use_bundles and (not os.getenv("JITO_RPC_URL") or not os.getenv("JITO_AUTH")):
        logging.warning("MEV bundles enabled but JITO_RPC_URL or JITO_AUTH is missing")

    proc_ref = [proc]

    if risk_tolerance is not None:
        os.environ["RISK_TOLERANCE"] = str(risk_tolerance)
    if max_allocation is not None:
        os.environ["MAX_ALLOCATION"] = str(max_allocation)
    if risk_multiplier is not None:
        os.environ["RISK_MULTIPLIER"] = str(risk_multiplier)

    if discovery_method is not None:
        resolved_method = resolve_discovery_method(discovery_method)
    else:
        resolved_method = resolve_discovery_method(cfg.get("discovery_method"))
        if resolved_method is None:
            resolved_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
    if resolved_method is None:
        resolved_method = DEFAULT_DISCOVERY_METHOD
    discovery_method = resolved_method

    if stop_loss is None:
        stop_loss = cfg.get("stop_loss")
    if take_profit is None:
        take_profit = cfg.get("take_profit")
    if trailing_stop is None:
        trailing_stop = cfg.get("trailing_stop")
    if max_drawdown is None:
        max_drawdown = float(cfg.get("max_drawdown", 1.0))
    if volatility_factor is None:
        volatility_factor = float(cfg.get("volatility_factor", 1.0))
    if arbitrage_threshold is None:
        arbitrage_threshold = float(cfg.get("arbitrage_threshold", 0.0))
    if arbitrage_amount is None:
        arbitrage_amount = float(cfg.get("arbitrage_amount", 0.0))
    if strategies is None:
        strategies = cfg.get("strategies")
        if isinstance(strategies, str):
            strategies = [s.strip() for s in strategies.split(",") if s.strip()]

    if market_ws_url is None:
        market_ws_url = cfg.get("market_ws_url")
    if market_ws_url is None:
        market_ws_url = os.getenv("MARKET_WS_URL")
    if order_book_ws_url is None:
        order_book_ws_url = cfg.get("order_book_ws_url")
    if order_book_ws_url is None:
        order_book_ws_url = os.getenv("ORDER_BOOK_WS_URL")
    if arbitrage_tokens is None:
        tokens_cfg = cfg.get("arbitrage_tokens")
        if isinstance(tokens_cfg, str):
            arbitrage_tokens = [t.strip() for t in tokens_cfg.split(",") if t.strip()]
        elif tokens_cfg:
            arbitrage_tokens = list(tokens_cfg)
    if arbitrage_tokens is None:
        env_tokens = os.getenv("ARBITRAGE_TOKENS")
        if env_tokens:
            arbitrage_tokens = [t.strip() for t in env_tokens.split(",") if t.strip()]

    if min_delay is None:
        min_delay = int(cfg.get("min_delay", 1))
    if max_delay is None:
        max_delay = int(cfg.get("max_delay", loop_delay))
    if cpu_low_threshold is None:
        cpu_low_threshold = float(cfg.get("cpu_low_threshold", 20.0))
    if cpu_high_threshold is None:
        cpu_high_threshold = float(cfg.get("cpu_high_threshold", 80.0))
    if depth_freq_low is None:
        depth_freq_low = float(cfg.get("depth_freq_low", 1.0))
    if depth_freq_high is None:
        depth_freq_high = float(cfg.get("depth_freq_high", 10.0))

    if strategy_rotation_interval is None:
        strategy_rotation_interval = int(cfg.get("strategy_rotation_interval", 0))
    if weight_config_paths is None:
        paths = cfg.get("weight_config_paths") or []
        if isinstance(paths, str):
            weight_config_paths = [p.strip() for p in paths.split(",") if p.strip()]
        else:
            weight_config_paths = list(paths) if paths else []

    snapshot_path = cfg.get("memory_snapshot_path")
    snapshot_trades = load_snapshot(snapshot_path) if snapshot_path else []
    memory = Memory(memory_path)
    if snapshot_trades:

        async def _seed(mem: Memory, trades: Sequence[dict]) -> None:
            for tr in trades:
                try:
                    await mem.log_trade(_broadcast=False, **tr)
                except Exception:
                    continue

        asyncio.run(_seed(memory, snapshot_trades))
    memory.start_writer()
    portfolio = Portfolio(path=portfolio_path)
    warm_tokens = _collect_warmup_tokens(portfolio, snapshot_trades)
    seeded_tokens = _seed_price_cache_from_state(portfolio, snapshot_trades)
    if seeded_tokens:
        logging.debug(
            "Seeded price cache from snapshots: %s", ", ".join(sorted(seeded_tokens))
        )
    warmup_delay_env = os.getenv("PRICE_WARMUP_DELAY")
    try:
        warmup_delay = float(warmup_delay_env) if warmup_delay_env is not None else 0.25
    except ValueError:
        warmup_delay = 0.25
    warmup_delay = max(0.0, warmup_delay)

    warmup_batch_env = os.getenv("PRICE_WARMUP_BATCH")
    try:
        warmup_batch = int(warmup_batch_env) if warmup_batch_env is not None else 16
    except ValueError:
        warmup_batch = 16
    if warmup_batch <= 0:
        warmup_batch = 1
    state = TradingState()

    recent_window = float(os.getenv("RECENT_TRADE_WINDOW", "0") or 0)
    if recent_window > 0:

        async def _load_recent_trades() -> None:
            trades = await memory.list_trades()
            for tr in trades:
                ts = getattr(tr, "created_at", None)
                if ts is not None:
                    prev = state.last_trade_times.get(tr.token)
                    if prev is None or ts > prev:
                        state.last_trade_times[tr.token] = ts

        try:
            asyncio.run(_load_recent_trades())
        except Exception as exc:
            logging.warning("Failed to load recent trade timestamps: %s", exc)

    agent_manager: AgentManager | None = None
    strategy_manager: StrategyManager | None = None

    patched_strategy_cls = StrategyManager
    try:
        patched_strategy_cls = getattr(sys.modules[__name__], "StrategyManager", StrategyManager)
    except Exception:
        patched_strategy_cls = StrategyManager

    if patched_strategy_cls is not _ORIGINAL_STRATEGY_MANAGER:
        strategy_manager = patched_strategy_cls(strategies)
    else:
        agents_cfg = cfg.get("agents")

        if agents_cfg:
            if weight_config_paths:
                cfg["weight_config_paths"] = weight_config_paths
            if strategy_rotation_interval is not None:
                cfg["strategy_rotation_interval"] = strategy_rotation_interval
            agent_manager = AgentManager.from_config(cfg)
            if agent_manager is None:
                strategy_manager = StrategyManager(strategies)
                missing = getattr(strategy_manager, "list_missing", lambda: [])()
                if missing:
                    logging.warning("Skipped strategies: %s", ", ".join(missing))
        elif "agents" in cfg:
            strategy_manager = StrategyManager(strategies)
            missing = getattr(strategy_manager, "list_missing", lambda: [])()
            if missing:
                logging.warning("Skipped strategies: %s", ", ".join(missing))
        else:
            agent_manager = AgentManager.from_default()
            if agent_manager is None:
                strategy_manager = StrategyManager(strategies)
                missing = getattr(strategy_manager, "list_missing", lambda: [])()
                if missing:
                    logging.warning("Skipped strategies: %s", ", ".join(missing))

    if keypair_path:
        keypair = load_keypair(keypair_path)
    else:
        try:
            keypair = wallet.load_selected_keypair()
        except Exception as exc:
            print(
                f"Wallet interaction failed: {exc}\n"
                "Run 'solhunter-wallet' manually or set the MNEMONIC environment variable.",
                file=sys.stderr,
            )
            raise SystemExit(1)

    if not offline and not dry_run:
        try:
            _ct_timeout_env = os.getenv("CONNECTIVITY_TEST_TIMEOUT")
            try:
                _ct_timeout = float(_ct_timeout_env) if _ct_timeout_env is not None else 5.0
            except ValueError:
                _ct_timeout = 5.0
            asyncio.run(
                place_order_async(
                    "So11111111111111111111111111111111111111112",
                    "buy",
                    0.001,
                    0.0,
                    testnet=testnet,
                    dry_run=True,
                    connectivity_test=True,
                    max_retries=1,
                    timeout=_ct_timeout,
                )
            )
        except Exception as exc:
            logging.error("Connectivity test order failed: %s", exc)
            raise SystemExit(1)

    try:
        while True:
            try:
                if live_discovery is None:
                    live_discovery = parse_bool_env("LIVE_DISCOVERY", True)
                trading_kwargs = dict(
                    cfg=cfg,
                    runtime_cfg=runtime_cfg,
                    memory=memory,
                    portfolio=portfolio,
                    state=state,
                    loop_delay=loop_delay,
                    min_delay=min_delay,
                    max_delay=max_delay,
                    cpu_low_threshold=cpu_low_threshold,
                    cpu_high_threshold=cpu_high_threshold,
                    depth_freq_low=depth_freq_low,
                    depth_freq_high=depth_freq_high,
                    depth_rate_limit=depth_rate_limit,
                    iterations=iterations,
                    testnet=testnet,
                    dry_run=dry_run,
                    offline=offline,
                    token_file=token_file,
                    discovery_method=discovery_method,
                    keypair=keypair,
                    stop_loss=stop_loss,
                    take_profit=take_profit,
                    trailing_stop=trailing_stop,
                    max_drawdown=max_drawdown,
                    volatility_factor=volatility_factor,
                    arbitrage_threshold=arbitrage_threshold,
                    arbitrage_amount=arbitrage_amount,
                    strategy_manager=strategy_manager,
                    agent_manager=agent_manager,
                    market_ws_url=market_ws_url,
                    order_book_ws_url=order_book_ws_url,
                    arbitrage_tokens=arbitrage_tokens,
                    rl_daemon=rl_daemon,
                    rl_interval=rl_interval,
                    proc_ref=proc_ref,
                    live_discovery=live_discovery,
                )
                asyncio.run(
                    _run_trading_loop_with_warmup(
                        warm_tokens=warm_tokens,
                        warmup_delay=warmup_delay,
                        warmup_batch=warmup_batch,
                        trading_kwargs=trading_kwargs,
                    )
                )
                break
            except ResourceBudgetExceeded as exc:
                logging.error("Resource budget triggered shutdown: %s", exc)
                raise SystemExit(str(exc)) from exc
            except FirstTradeTimeoutError:
                logging.error("Retrying trading loop after first trade timeout")
                continue
    finally:
        if proc_ref[0]:
            with contextlib.suppress(Exception):
                proc_ref[0].terminate()
            with contextlib.suppress(Exception):
                proc_ref[0].wait(timeout=1)
        if prev_agents is None:
            os.environ.pop("AGENTS", None)
        else:
            os.environ["AGENTS"] = prev_agents
        if prev_weights is None:
            os.environ.pop("AGENT_WEIGHTS", None)
        else:
            os.environ["AGENT_WEIGHTS"] = prev_weights


def run_auto(**kwargs) -> None:
    """Start trading with selected config or default preset."""
    bootstrap(one_click=True)
    try:
        cfg = load_selected_config()
    except Exception:
        cfg = {}
    cfg_path = None
    if cfg:
        name = get_active_config_name()
        cfg_path = os.path.join(CONFIG_DIR, name) if name else None
    elif _DEFAULT_PRESET.is_file():
        cfg_path = str(_DEFAULT_PRESET)
        cfg = load_config(cfg_path)
    cfg = apply_env_overrides(cfg)
    prev_agents = os.environ.get("AGENTS")
    prev_weights = os.environ.get("AGENT_WEIGHTS")
    prev_keypair_path = os.environ.get("KEYPAIR_PATH")
    set_env_from_config(cfg)
    initialize_event_bus()

    try:
        from . import data_sync

        asyncio.run(data_sync.sync_recent())
    except Exception as exc:  # pragma: no cover - ignore sync errors
        logging.getLogger(__name__).warning("data sync failed: %s", exc)

    active_name = wallet.get_active_keypair_name()
    if active_name and not os.getenv("KEYPAIR_PATH"):
        os.environ["KEYPAIR_PATH"] = os.path.join(
            wallet.KEYPAIR_DIR, active_name + ".json"
        )

    try:
        # Respect environment toggles for common flags when not explicitly set
        if "dry_run" not in kwargs:
            kwargs["dry_run"] = parse_bool_env("DRY_RUN", False)
        if "offline" not in kwargs:
            kwargs["offline"] = parse_bool_env("SOLHUNTER_OFFLINE", False)
        main(config_path=cfg_path, **kwargs)
    finally:
        if prev_agents is None:
            os.environ.pop("AGENTS", None)
        else:
            os.environ["AGENTS"] = prev_agents
        if prev_weights is None:
            os.environ.pop("AGENT_WEIGHTS", None)
        else:
            os.environ["AGENT_WEIGHTS"] = prev_weights
        if prev_keypair_path is None:
            os.environ.pop("KEYPAIR_PATH", None)
        else:
            os.environ["KEYPAIR_PATH"] = prev_keypair_path


if __name__ == "__main__":
    parser = ArgumentParser(description="Run SolHunter Zero bot")
    parser.add_argument(
        "--memory-path",
        default="sqlite:///memory.db",
        help="Database URL for storing trades",
    )
    parser.add_argument(
        "--loop-delay",
        type=int,
        default=60,
        help="Delay between iterations in seconds",
    )
    parser.add_argument(
        "--min-delay",
        type=int,
        default=None,
        help="Minimum delay between trade iterations",
    )
    parser.add_argument(
        "--max-delay",
        type=int,
        default=None,
        help="Maximum delay between trade iterations",
    )
    parser.add_argument(
        "--iterations",
        type=int,
        default=None,
        help="Number of iterations to run before exiting",
    )
    parser.add_argument(
        "--testnet",
        action="store_true",
        help="Use testnet DEX endpoints",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Do not submit orders, just simulate",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Use a static token list and skip network requests",
    )
    parser.add_argument(
        "--token-list",
        dest="token_file",
        help="Load token addresses from FILE (one per line)",
    )
    parser.add_argument(
        "--discovery-method",
        default=None,
        choices=["websocket", "onchain", "mempool", "pools", "file"],
        help="Token discovery method",
    )
    parser.add_argument(
        "--keypair",
        default=os.getenv("KEYPAIR_PATH"),
        help="Path to a JSON keypair for signing transactions",
    )
    parser.add_argument(
        "--min-balance",
        type=float,
        default=float(os.getenv("MIN_STARTING_BALANCE", "0") or 0),
        help="Minimum starting balance in SOL",
    )
    parser.add_argument(
        "--portfolio-path",
        default="portfolio.json",
        help="Path to a JSON file for persisting portfolio state",
    )
    parser.add_argument(
        "--config",
        default=None,
        help="Path to a configuration file",
    )
    parser.add_argument(
        "--stop-loss",
        type=float,
        default=None,
        help="Stop loss threshold as a fraction (e.g. 0.1 for 10%)",
    )
    parser.add_argument(
        "--take-profit",
        type=float,
        default=None,
        help="Take profit threshold as a fraction",
    )
    parser.add_argument(
        "--trailing-stop",
        type=float,
        default=None,
        help="Trailing stop percentage",
    )
    parser.add_argument(
        "--max-drawdown",
        type=float,
        default=None,
        help="Maximum allowed portfolio drawdown",
    )
    parser.add_argument(
        "--volatility-factor",
        type=float,
        default=None,
        help="Scaling factor for volatility in position sizing",
    )
    parser.add_argument(
        "--risk-tolerance",
        type=float,
        default=None,
        help="Base risk tolerance for position sizing",
    )
    parser.add_argument(
        "--max-allocation",
        type=float,
        default=None,
        help="Maximum portfolio allocation per trade",
    )
    parser.add_argument(
        "--risk-multiplier",
        type=float,
        default=None,
        help="Multiplier applied to risk parameters",
    )
    parser.add_argument(
        "--market-ws-url",
        default=None,
        help="Websocket URL for real-time market events",
    )
    parser.add_argument(
        "--order-book-ws-url",
        default=None,
        help="Websocket URL for order book depth updates",
    )
    parser.add_argument(
        "--arbitrage-threshold",
        type=float,
        default=None,
        help="Minimum price diff fraction for arbitrage",
    )
    parser.add_argument(
        "--arbitrage-amount",
        type=float,
        default=None,
        help="Trade size when executing arbitrage",
    )
    parser.add_argument(
        "--arbitrage-tokens",
        default=None,
        help="Comma-separated list of token addresses to monitor",
    )
    parser.add_argument(
        "--strategies",
        default=None,
        help="Comma-separated list of strategy modules",
    )
    parser.add_argument(
        "--rl-daemon", action="store_true", help="Start RL training daemon"
    )
    parser.add_argument(
        "--rl-interval",
        type=float,
        default=3600.0,
        help="Seconds between RL training cycles",
    )
    parser.add_argument(
        "--dynamic-concurrency",
        action="store_true",
        help="Dynamically adjust ranking concurrency based on CPU usage",
    )
    parser.add_argument(
        "--strategy-rotation-interval",
        type=int,
        default=0,
        help="Iterations between weight config evaluations",
    )
    parser.add_argument(
        "--weight-config",
        dest="weight_configs",
        action="append",
        default=[],
        help="Configuration file with agent_weights to rotate",
    )
    # CLI flags for thresholds and depth polling, referenced downstream
    parser.add_argument(
        "--cpu-low-threshold",
        type=float,
        default=float(os.getenv("CPU_LOW_THRESHOLD", "20.0") or 20.0),
        help="Lower CPU threshold for dynamic depth frequency",
    )
    parser.add_argument(
        "--cpu-high-threshold",
        type=float,
        default=float(os.getenv("CPU_HIGH_THRESHOLD", "80.0") or 80.0),
        help="Upper CPU threshold for dynamic depth frequency",
    )
    parser.add_argument(
        "--depth-freq-low",
        type=float,
        default=float(os.getenv("DEPTH_FREQ_LOW", "1.0") or 1.0),
        help="Minimum depth polling frequency (Hz)",
    )
    parser.add_argument(
        "--depth-freq-high",
        type=float,
        default=float(os.getenv("DEPTH_FREQ_HIGH", "10.0") or 10.0),
        help="Maximum depth polling frequency (Hz)",
    )
    parser.add_argument(
        "--depth-rate-limit",
        type=float,
        default=float(os.getenv("DEPTH_RATE_LIMIT", "0.1") or 0.1),
        help="Rate-limit seconds between depth requests",
    )
    parser.add_argument(
        "--profile",
        action="store_true",
        help="Profile the trading loop and write stats to profile.out",
    )
    parser.add_argument(
        "--live-discovery",
        action="store_true",
        help="Enable live mempool discovery alongside the main loop",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Load selected config and start trading automatically",
    )
    args = parser.parse_args()
    os.environ["MIN_STARTING_BALANCE"] = str(args.min_balance)
    kwargs = dict(
        memory_path=args.memory_path,
        loop_delay=args.loop_delay,
        iterations=args.iterations,
        testnet=args.testnet,
        dry_run=args.dry_run,
        offline=args.offline,
        token_file=args.token_file,
        discovery_method=args.discovery_method,
        keypair_path=args.keypair,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        cpu_low_threshold=args.cpu_low_threshold,
        cpu_high_threshold=args.cpu_high_threshold,
        depth_freq_low=args.depth_freq_low,
        depth_freq_high=args.depth_freq_high,
        depth_rate_limit=args.depth_rate_limit,
        portfolio_path=args.portfolio_path,
        config_path=args.config,
        stop_loss=args.stop_loss,
        take_profit=args.take_profit,
        trailing_stop=args.trailing_stop,
        max_drawdown=args.max_drawdown,
        volatility_factor=args.volatility_factor,
        risk_tolerance=args.risk_tolerance,
        max_allocation=args.max_allocation,
        risk_multiplier=args.risk_multiplier,
        market_ws_url=args.market_ws_url,
        order_book_ws_url=args.order_book_ws_url,
        arbitrage_threshold=args.arbitrage_threshold,
        arbitrage_amount=args.arbitrage_amount,
        arbitrage_tokens=args.arbitrage_tokens.split(",")
        if args.arbitrage_tokens
        else None,
        strategies=args.strategies.split(",") if args.strategies else None,
        rl_daemon=args.rl_daemon,
        rl_interval=args.rl_interval,
        dynamic_concurrency=args.dynamic_concurrency,
        strategy_rotation_interval=args.strategy_rotation_interval,
        weight_config_paths=args.weight_configs,
        live_discovery=args.live_discovery,
    )
    config_override = kwargs.pop("config_path", None)
    call_kwargs = dict(kwargs)
    if config_override is not None:
        call_kwargs["config_path"] = config_override
    if args.profile:
        cProfile.runctx(
            "main(**call_kwargs)",
            globals(),
            {"call_kwargs": call_kwargs},
            filename="profile.out",
        )
    elif args.auto:
        if config_override:
            os.environ["SOLHUNTER_CONFIG"] = config_override
        run_auto(**kwargs)
    else:
        main(**call_kwargs)
