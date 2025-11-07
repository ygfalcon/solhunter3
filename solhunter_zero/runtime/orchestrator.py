from __future__ import annotations

import argparse
import asyncio
import logging
import os
import signal
import sys
from dataclasses import dataclass
from typing import Any, Optional

from ..util import install_uvloop, parse_bool_env
from ..agents.discovery import DEFAULT_DISCOVERY_METHOD, resolve_discovery_method
from .. import event_bus
from .. import discovery_state
from ..config import (
    initialize_event_bus,
    apply_env_overrides,
    load_config,
    load_selected_config,
)
from ..main import perform_startup_async
from ..main_state import TradingState
from ..memory import Memory
from ..portfolio import Portfolio
from ..strategy_manager import StrategyManager
from ..agent_manager import AgentManager
from ..loop import trading_loop as _trading_loop
from ..ui import create_app as _create_ui_app, start_websockets as _start_ui_ws


install_uvloop()

log = logging.getLogger(__name__)


@dataclass
class RuntimeHandles:
    ui_app: Any | None = None
    ui_threads: dict[str, Any] | None = None
    ui_server: Any | None = None
    bus_started: bool = False
    tasks: list[asyncio.Task] | None = None
    depth_proc: Any | None = None


class RuntimeOrchestrator:
    """Owns startup sequencing for UI → EventBus → Agents (+ RL).

    This runs only when NEW_RUNTIME=1 to preserve the current behavior by default.
    """

    def __init__(
        self,
        *,
        config_path: str | None = None,
        run_http: bool = True,
        dry_run: bool | None = None,
        testnet: bool | None = None,
    ) -> None:
        self.config_path = config_path
        self.run_http = run_http
        self.handles = RuntimeHandles(tasks=[])
        self._closed = False
        self._stopping = False
        self._executor_dry_run = dry_run
        self._executor_testnet = testnet

    async def _publish_stage(self, stage: str, ok: bool, detail: str = "") -> None:
        try:
            event_bus.publish("runtime.stage_changed", {"stage": stage, "ok": ok, "detail": detail})
        except Exception:
            pass
        if os.getenv("ORCH_VERBOSE", "").lower() in {"1", "true", "yes"}:
            log.info("stage=%s ok=%s detail=%s", stage, ok, detail)

    async def start_bus(self) -> None:
        await self._publish_stage("bus:init", True)
        # Choose event-bus WS port early and export URLs so init sees them
        ws_port = int(os.getenv("EVENT_BUS_WS_PORT", "8779") or 8779)
        original_event_bus_url = os.environ.get("EVENT_BUS_URL")
        original_broker_ws_urls = os.environ.get("BROKER_WS_URLS")
        os.environ.setdefault("EVENT_BUS_URL", f"ws://127.0.0.1:{ws_port}")
        os.environ.setdefault("BROKER_WS_URLS", f"ws://127.0.0.1:{ws_port}")
        # Load config early so event bus has proper env/urls
        try:
            from ..config import get_broker_urls, set_env_from_config
            cfg = apply_env_overrides(load_selected_config() or load_config(self.config_path))
            set_env_from_config(cfg)
        except Exception:
            cfg = None
        # Opportunistically ensure local redis if configured to localhost
        try:
            from ..redis_util import ensure_local_redis_if_needed

            urls = []
            if cfg is not None:
                try:
                    urls = get_broker_urls(cfg)
                except Exception:
                    urls = []
            ensure_local_redis_if_needed(urls)
        except Exception:
            pass
        initialize_event_bus()
        # Prefer a dedicated local WS port to avoid conflicts
        local_ws_bound = False
        try:
            await event_bus.start_ws_server("localhost", ws_port)
            local_ws_bound = True
            self.handles.bus_started = True
            await self._publish_stage("bus:ws", True, f"port={ws_port}")
        except Exception as exc:
            detail = f"{exc}; keeping remote broker configuration"
            await self._publish_stage("bus:ws", False, detail)
            log.info("Local event bus websocket unavailable; %s", detail)

        if local_ws_bound:
            # Ensure peers point to the local WS
            os.environ["BROKER_WS_URLS"] = f"ws://127.0.0.1:{ws_port}"
            os.environ["EVENT_BUS_URL"] = f"ws://127.0.0.1:{ws_port}"
        else:
            # Restore original broker configuration when local WS is unavailable
            if original_broker_ws_urls is None:
                os.environ.pop("BROKER_WS_URLS", None)
            else:
                os.environ["BROKER_WS_URLS"] = original_broker_ws_urls
            if original_event_bus_url is None:
                os.environ.pop("EVENT_BUS_URL", None)
            else:
                os.environ["EVENT_BUS_URL"] = original_event_bus_url
        ok = await event_bus.verify_broker_connection(timeout=1.0)
        if not ok:
            await self._publish_stage("bus:verify", False, "broker roundtrip failed")
            if parse_bool_env("BROKER_VERIFY_ABORT", False):
                raise SystemExit(1)
        else:
            await self._publish_stage("bus:verify", True)
        # continue even if local ws failed (peers may still be available)

    async def start_ui(self) -> None:
        await self._publish_stage("ui:init", True)
        app = _create_ui_app(auto_start=False)
        threads = _start_ui_ws()
        self.handles.ui_app = app
        self.handles.ui_threads = threads
        await self._publish_stage("ui:ws", True)
        if self.run_http and str(os.getenv("UI_DISABLE_HTTP_SERVER", "")).lower() not in {"1", "true", "yes"}:
            # Start Flask server in a background thread using werkzeug only if available.
            import threading

            def _serve():
                try:
                    host = os.getenv("UI_HOST", "127.0.0.1")
                    port = int(os.getenv("UI_PORT", os.getenv("PORT", "5000") or 5000))
                    app.run(host=host, port=port)
                except Exception:
                    log.exception("UI HTTP server failed")

            t = threading.Thread(target=_serve, daemon=True)
            t.start()
            await self._publish_stage("ui:http", True, f"host={os.getenv('UI_HOST','127.0.0.1')} port={os.getenv('UI_PORT', os.getenv('PORT','5000'))}")

    async def start_agents(self) -> bool:
        # Use existing startup path to ensure consistent connectivity + depth_service
        await self._publish_stage("agents:startup", True)
        cfg, runtime_cfg, proc = await perform_startup_async(self.config_path, offline=False, dry_run=False)
        self.handles.depth_proc = proc

        # Build runtime services
        memory_path = os.getenv("MEMORY_PATH", "sqlite:///memory.db")
        portfolio_path = os.getenv("PORTFOLIO_PATH", "portfolio.json")
        memory = Memory(memory_path)
        memory.start_writer()
        portfolio = Portfolio(path=portfolio_path)
        state = TradingState()

        # Strategy/agents selection mirrors main.main
        agent_manager: AgentManager | None = None
        strategies = cfg.get("strategies")
        if isinstance(strategies, str):
            strategies = [s.strip() for s in strategies.split(",") if s.strip()]
        strategy_manager: StrategyManager | None = None
        if cfg.get("agents"):
            agent_manager = AgentManager.from_config(cfg)
            if agent_manager is None:
                strategy_manager = StrategyManager(strategies)
        else:
            agent_manager = AgentManager.from_default()
            strategy_manager = None if agent_manager is not None else StrategyManager(strategies)

        active_manager = agent_manager or AgentManager.from_default()
        if active_manager is None:
            await self._publish_stage(
                "agents:loaded",
                False,
                "no agents available; configure agents or install defaults",
            )
            return False

        # Announce loaded agents
        try:
            names = [a.name for a in active_manager.agents]
            await self._publish_stage("agents:loaded", True, f"count={len(names)} names={','.join(names[:10])}{'...' if len(names)>10 else ''}")
        except Exception:
            pass

        # Choose mode: event-driven vs classic loop
        event_driven = parse_bool_env("EVENT_DRIVEN", True)
        # Live drill: simulate executions on live data without broadcasting trades
        try:
            import os as _os
            if bool(cfg.get("live_drill", False)):
                _os.environ["LIVE_DRILL"] = "1"
                await self._publish_stage("drill:on", True, "live trade broadcasting disabled; simulation active")
        except Exception:
            pass

        # Trading loop task (classic mode: reuses adaptive loop with evolve/cull)
        loop_delay = int(cfg.get("loop_delay", int(os.getenv("LOOP_DELAY", "60") or 60)))
        min_delay = int(cfg.get("min_delay", 1))
        max_delay = int(cfg.get("max_delay", loop_delay))
        cpu_low_threshold = float(cfg.get("cpu_low_threshold", 20.0))
        cpu_high_threshold = float(cfg.get("cpu_high_threshold", 80.0))
        depth_freq_low = float(cfg.get("depth_freq_low", 1.0))
        depth_freq_high = float(cfg.get("depth_freq_high", 10.0))
        depth_rate_limit = float(cfg.get("depth_rate_limit", 0.1))
        rl_daemon = bool(cfg.get("rl_auto_train", False)) or parse_bool_env("RL_DAEMON", False)
        rl_interval = float(cfg.get("rl_interval", 3600.0))

        # Derive discovery & arbitrage
        discovery_method = resolve_discovery_method(cfg.get("discovery_method"))
        if discovery_method is None:
            discovery_method = resolve_discovery_method(os.getenv("DISCOVERY_METHOD"))
        if discovery_method is None:
            discovery_method = DEFAULT_DISCOVERY_METHOD
        arbitrage_threshold = float(cfg.get("arbitrage_threshold", 0.0))
        arbitrage_amount = float(cfg.get("arbitrage_amount", 0.0))
        arbitrage_tokens = None
        tokens_cfg = cfg.get("arbitrage_tokens")
        if isinstance(tokens_cfg, str):
            arbitrage_tokens = [t.strip() for t in tokens_cfg.split(",") if t.strip()]
        elif tokens_cfg:
            arbitrage_tokens = list(tokens_cfg)

        async def _run_classic():
            await _trading_loop(
                cfg,
                runtime_cfg,
                memory,
                portfolio,
                state,
                loop_delay=loop_delay,
                min_delay=min_delay,
                max_delay=max_delay,
                cpu_low_threshold=cpu_low_threshold,
                cpu_high_threshold=cpu_high_threshold,
                depth_freq_low=depth_freq_low,
                depth_freq_high=depth_freq_high,
                depth_rate_limit=depth_rate_limit,
                iterations=None,
                testnet=False,
                dry_run=False,
                offline=False,
                token_file=None,
                discovery_method=discovery_method,
                keypair=None,
                stop_loss=cfg.get("stop_loss"),
                take_profit=cfg.get("take_profit"),
                trailing_stop=cfg.get("trailing_stop"),
                max_drawdown=float(cfg.get("max_drawdown", 1.0)),
                volatility_factor=float(cfg.get("volatility_factor", 1.0)),
                arbitrage_threshold=arbitrage_threshold,
                arbitrage_amount=arbitrage_amount,
                strategy_manager=strategy_manager,
                agent_manager=active_manager,
                market_ws_url=cfg.get("market_ws_url"),
                order_book_ws_url=cfg.get("order_book_ws_url"),
                arbitrage_tokens=arbitrage_tokens,
                rl_daemon=rl_daemon,
                rl_interval=rl_interval,
                proc_ref=[self.handles.depth_proc],
                live_discovery=cfg.get("live_discovery"),
            )

        if not event_driven:
            task = asyncio.create_task(_run_classic(), name="trading_loop")
            self.handles.tasks.append(task)
            await self._publish_stage("agents:loop", True)
            return True

        # Event-driven mode: start agent runtime, swarm coordinator and executor
        from ..agents.runtime import AgentRuntime
        from ..exec_service import TradeExecutor
        from ..agents.discovery import DiscoveryAgent
        from ..loop import _init_rl_training as _init_rl_training  # type: ignore

        aruntime = AgentRuntime(active_manager, portfolio)
        await aruntime.start()
        def _maybe_bool(value: Any) -> bool | None:
            if value is None:
                return None
            if isinstance(value, bool):
                return value
            if isinstance(value, str):
                lowered = value.strip().lower()
                if lowered in {"1", "true", "yes", "on"}:
                    return True
                if lowered in {"0", "false", "no", "off"}:
                    return False
            try:
                return bool(value)
            except Exception:
                return None

        def _resolve_flag(override: bool | None, env_name: str, cfg_key: str) -> bool:
            if override is not None:
                return bool(override)
            if os.getenv(env_name) is not None:
                return parse_bool_env(env_name, False)
            candidate: Any | None = None
            if isinstance(cfg, dict):
                candidate = cfg.get(cfg_key)
            if candidate is None and isinstance(runtime_cfg, dict):
                candidate = runtime_cfg.get(cfg_key)
            maybe_val = _maybe_bool(candidate)
            return bool(maybe_val) if maybe_val is not None else False

        executor_dry_run = _resolve_flag(self._executor_dry_run, "DRY_RUN", "dry_run")
        executor_testnet = _resolve_flag(self._executor_testnet, "TESTNET", "testnet")

        execu = TradeExecutor(
            memory,
            portfolio,
            testnet=executor_testnet,
            dry_run=executor_dry_run,
        )
        execu.start()

        async def _discovery_loop():
            agent = DiscoveryAgent()
            previous_tokens: list[str] | None = None
            while True:
                method = discovery_state.current_method(
                    config=cfg, explicit=discovery_method
                )
                try:
                    tokens = await agent.discover_tokens(method=method, offline=False)
                    if tokens:
                        seq = [str(token) for token in tokens if isinstance(token, str) and token]
                        if seq:
                            metadata_refresh = False
                            changed_tokens: list[str] = []
                            if previous_tokens is not None:
                                prev_set = {str(token) for token in previous_tokens if token}
                                curr_set = set(seq)
                                if curr_set == prev_set:
                                    metadata_refresh = True
                                    changed_tokens = list(seq)
                            event_bus.publish(
                                "token_discovered",
                                {
                                    "tokens": list(seq),
                                    "metadata_refresh": metadata_refresh,
                                    "changed_tokens": list(changed_tokens),
                                },
                            )
                            previous_tokens = list(seq)
                except Exception:
                    pass
                await asyncio.sleep(max(5, min(60, loop_delay)))

        async def _evolve_loop():
            # Evolve/mutate/cull continually based on success metrics
            interval = int(getattr(active_manager, "evolve_interval", 30) or 30)
            while True:
                try:
                    if active_manager is not None:
                        await active_manager.evolve(
                            threshold=getattr(active_manager, "mutation_threshold", 0.0)
                        )
                        await active_manager.update_weights()
                        active_manager.save_weights()
                except Exception:
                    pass
                await asyncio.sleep(max(5, interval))

        # Start RL training if enabled
        rl_task = await _init_rl_training(cfg, rl_daemon=rl_daemon, rl_interval=rl_interval)
        if rl_task is not None:
            self.handles.tasks.append(rl_task)

        # MEV bundles readiness hint
        use_bundles = bool(cfg.get("use_mev_bundles", False))
        if use_bundles and (not os.getenv("JITO_RPC_URL") or not os.getenv("JITO_AUTH")):
            await self._publish_stage("mev:warn", False, "MEV bundles enabled but JITO credentials missing")

        self.handles.tasks.append(asyncio.create_task(_discovery_loop(), name="discovery_loop"))
        self.handles.tasks.append(asyncio.create_task(_evolve_loop(), name="evolve_loop"))
        await self._publish_stage("agents:event_runtime", True)
        return True

    async def start(self) -> None:
        # Make orchestrator subscribe to control messages
        def _ctl(payload: dict) -> None:
            cmd = (payload or {}).get("cmd")
            if cmd == "stop":
                asyncio.get_event_loop().create_task(self.stop_all())

        event_bus.subscribe("control", _ctl)

        # 1) UI, 2) Bus, 3) Agents
        try:
            await self.start_ui()
            await self.start_bus()
            # Start decision metrics aggregator
            try:
                from ..metrics.decision_metrics import DecisionMetrics

                self._dec_metrics = DecisionMetrics()
                self._dec_metrics.start()
            except Exception:
                self._dec_metrics = None
            # Start adaptive risk controller
            try:
                from ..risk_controller import RiskController

                self._risk_ctl = RiskController()
                self._risk_ctl.start()
            except Exception:
                self._risk_ctl = None
            agents_ready = await self.start_agents()
            if not agents_ready:
                raise RuntimeError("agent startup aborted")
        except Exception:
            await self.stop_all()
            raise
        await self._publish_stage("runtime:ready", True)

    async def stop_all(self) -> None:
        if self._stopping:
            return
        if self._closed:
            return
        self._stopping = True
        try:
            self._closed = True
            await self._publish_stage("runtime:stopping", True)
            # Cancel tasks
            for t in list(self.handles.tasks or []):
                t.cancel()
            for t in list(self.handles.tasks or []):
                try:
                    await t
                except Exception:
                    pass
            self.handles.tasks = []
            # Stop bus server
            if self.handles.bus_started:
                try:
                    await event_bus.stop_ws_server()
                except Exception:
                    pass
                finally:
                    self.handles.bus_started = False
            # Stop metrics
            try:
                m = getattr(self, "_dec_metrics", None)
                if m is not None:
                    m.stop()
            except Exception:
                pass
            # Stop risk controller
            try:
                rc = getattr(self, "_risk_ctl", None)
                if rc is not None:
                    rc.stop()
            except Exception:
                pass
            # Close UI WS threads are daemonic; HTTP server stops with process
            try:
                from ..http import close_session

                await close_session()
            except Exception:
                pass
            await self._publish_stage("runtime:stopped", True)
        finally:
            self._stopping = False


def _parse_cli(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SolHunter Zero Runtime Orchestrator")
    p.add_argument("--config", default=None, help="Path to config file")
    p.add_argument("--no-http", action="store_true", help="Do not run UI HTTP server")
    p.add_argument("--verbose-stages", action="store_true", help="Log stage changes verbosely")
    return p.parse_args(argv)


async def _amain(argv: list[str] | None = None) -> int:
    args = _parse_cli(argv)
    run_http = not args.no_http
    if args.verbose_stages:
        os.environ["ORCH_VERBOSE"] = "1"
    orch = RuntimeOrchestrator(config_path=args.config, run_http=run_http)

    # Graceful shutdown on SIGINT/SIGTERM
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: asyncio.create_task(orch.stop_all()))
        except NotImplementedError:
            pass

    await orch.start()
    # Keep process alive while trading loop runs
    while True:
        await asyncio.sleep(3600)


def main(argv: list[str] | None = None) -> None:
    if os.getenv("NEW_RUNTIME", "").lower() not in {"1", "true", "yes"}:
        print("NEW_RUNTIME is not enabled; aborting orchestrator.")
        raise SystemExit(2)
    try:
        asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        pass
