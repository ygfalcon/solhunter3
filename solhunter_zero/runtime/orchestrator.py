from __future__ import annotations

import argparse
import asyncio
import errno
import logging
import os
import signal
import socket
import sys
from contextlib import closing, suppress
from dataclasses import dataclass
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Optional

from werkzeug.serving import make_server

from ..util import install_uvloop, parse_bool_env
from ..agents.discovery import DEFAULT_DISCOVERY_METHOD, resolve_discovery_method
from .. import event_bus
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
from .. import ui as _ui_module

if hasattr(_ui_module, "create_app"):
    _create_ui_app = _ui_module.create_app  # type: ignore[attr-defined]
else:  # pragma: no cover - fallback used only when UI module lacks factory
    def _create_ui_app(_state: Any | None = None) -> Any:
        try:
            from flask import Flask

            return Flask(__name__)
        except Exception:
            def _wsgi_app(environ, start_response):
                start_response("200 OK", [("Content-Type", "text/plain")])
                return [b"UI disabled"]

            return _wsgi_app

if hasattr(_ui_module, "start_websockets"):
    _start_ui_ws = getattr(_ui_module, "start_websockets")
else:  # pragma: no cover - websockets optional in lightweight UI builds
    def _start_ui_ws() -> dict[str, Any]:
        logging.getLogger(__name__).warning(
            "UI websockets not available; continuing with HTTP polling only"
        )
        return {}


install_uvloop()

log = logging.getLogger(__name__)


def _runtime_artifact_dir() -> Path:
    """Return the directory where runtime artifacts should be written."""

    root = Path(os.getenv("RUNTIME_ARTIFACT_ROOT", "artifacts"))
    run_id_raw = (
        os.getenv("RUNTIME_RUN_ID")
        or os.getenv("RUN_ID")
        or os.getenv("MODE")
        or "prelaunch"
    )
    run_id_norm = run_id_raw.replace("\\", "/").strip()
    parts = [part for part in run_id_norm.split("/") if part and part not in {".", ".."}]
    if not parts:
        parts = ["prelaunch"]
    artifact_dir = root.joinpath(*parts)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    return artifact_dir


def _publish_ui_url_to_redis(ui_url: str) -> None:
    """Publish the UI URL to Redis using durable and TTL keys."""

    redis_url = os.getenv("REDIS_URL") or "redis://127.0.0.1:6379/0"
    if not redis_url:
        return
    try:
        import redis  # type: ignore[import-not-found]
    except Exception:  # pragma: no cover - redis optional in some builds
        log.debug("Redis client unavailable; skipping UI URL publish")
        return

    try:
        client = redis.Redis.from_url(redis_url, socket_timeout=1.0)  # type: ignore[attr-defined]
        client.set("solhunter:ui:url", ui_url)
        client.setex("solhunter:ui:url:latest", 600, ui_url)
    except Exception as exc:  # pragma: no cover - redis connectivity issues
        log.warning("Failed to publish UI URL to redis at %s: %s", redis_url, exc)


@dataclass
class RuntimeHandles:
    ui_app: Any | None = None
    ui_threads: dict[str, Any] | None = None
    ui_server: Any | None = None
    bus_started: bool = False
    tasks: list[asyncio.Task] | None = None
    ui_state: Any | None = None
    depth_proc: Any | None = None


class RuntimeOrchestrator:
    """Owns startup sequencing for UI → EventBus → Agents (+ RL).

    This runs only when NEW_RUNTIME=1 to preserve the current behavior by default.
    """

    def __init__(self, *, config_path: str | None = None, run_http: bool = True) -> None:
        self.config_path = config_path
        self.run_http = run_http
        self.handles = RuntimeHandles(tasks=[])
        self._closed = False

    def _emit_ui_ready(self, host: str, port: int) -> None:
        """Log and persist UI readiness details for downstream consumers."""

        url_host = "127.0.0.1" if host in {"0.0.0.0", "::"} else host
        scheme = os.getenv("UI_HTTP_SCHEME") or os.getenv("UI_SCHEME") or "http"
        ui_url = f"{scheme}://{url_host}:{port}"
        ws_urls: dict[str, str] = {}
        if hasattr(_ui_module, "get_ws_urls"):
            try:
                ws_urls = _ui_module.get_ws_urls()  # type: ignore[attr-defined]
            except Exception:
                ws_urls = {}
        rl_url = ws_urls.get("rl") or "-"
        events_url = ws_urls.get("events") or "-"
        logs_url = ws_urls.get("logs") or "-"
        readiness_line = (
            "UI_READY "
            f"url={ui_url} "
            f"rl_ws={rl_url} "
            f"events_ws={events_url} "
            f"logs_ws={logs_url}"
        )
        log.info(readiness_line)
        artifact_dir: Path | None = None
        try:
            artifact_dir = _runtime_artifact_dir()
        except Exception as exc:  # pragma: no cover - filesystem issues
            log.warning("Failed to prepare UI artifact directory: %s", exc)
        if artifact_dir is not None:
            try:
                (artifact_dir / "ui_url.txt").write_text(ui_url, encoding="utf-8")
            except Exception as exc:  # pragma: no cover - filesystem issues
                log.warning("Failed to write UI URL artifact: %s", exc)
        try:
            _publish_ui_url_to_redis(ui_url)
        except Exception as exc:  # pragma: no cover - defensive
            log.warning("Error publishing UI URL to redis: %s", exc)

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
        try:
            await event_bus.start_ws_server("localhost", ws_port)
            self.handles.bus_started = True
            await self._publish_stage("bus:ws", True, f"port={ws_port}")
        except Exception as exc:
            await self._publish_stage("bus:ws", False, f"{exc}")
        # Ensure peers point to the local WS
        os.environ["BROKER_WS_URLS"] = f"ws://127.0.0.1:{ws_port}"
        os.environ["EVENT_BUS_URL"] = f"ws://127.0.0.1:{ws_port}"
        ok = await event_bus.verify_broker_connection(timeout=5.0)
        if not ok:
            await self._publish_stage("bus:verify", False, "broker roundtrip failed")
            if parse_bool_env("BROKER_VERIFY_ABORT", False):
                raise SystemExit(1)
        else:
            await self._publish_stage("bus:verify", True)
        # continue even if local ws failed (peers may still be available)

    async def start_ui(self) -> None:
        await self._publish_stage("ui:init", True)
        state_obj = None
        if hasattr(_ui_module, "UIState"):
            try:
                state_obj = _ui_module.UIState()
            except Exception:
                state_obj = None
        app = _create_ui_app(state_obj)
        threads = _start_ui_ws() if callable(_start_ui_ws) else {}
        if hasattr(_ui_module, "get_ws_urls"):
            try:
                ws_urls = _ui_module.get_ws_urls()  # type: ignore[attr-defined]
            except Exception:
                ws_urls = {}
            else:
                env_map = {
                    "events": ("UI_EVENTS_WS", "UI_EVENTS_WS_URL", "UI_WS_URL"),
                    "rl": ("UI_RL_WS", "UI_RL_WS_URL"),
                    "logs": ("UI_LOGS_WS", "UI_LOG_WS_URL"),
                }
                for channel, keys in env_map.items():
                    url = ws_urls.get(channel)
                    if not url:
                        continue
                    for key in keys:
                        os.environ.setdefault(key, url)
        self.handles.ui_app = app
        self.handles.ui_threads = threads
        self.handles.ui_state = state_obj
        await self._publish_stage("ui:ws", True)
        if self.run_http and str(os.getenv("UI_DISABLE_HTTP_SERVER", "")).lower() not in {"1", "true", "yes"}:
            # Start Flask server in a background thread using werkzeug only if available.
            import threading

            def _select_listen_port(host: str, requested_port: int) -> tuple[int, bool]:
                """Return a usable port and whether it differs from the request."""

                if requested_port <= 0:
                    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                        sock.bind((host, 0))
                        return sock.getsockname()[1], requested_port != 0

                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    try:
                        sock.bind((host, requested_port))
                        return requested_port, False
                    except OSError as exc:
                        if exc.errno not in {errno.EADDRINUSE, errno.EACCES, errno.EADDRNOTAVAIL}:
                            raise

                # Requested port unavailable; ask OS for an open port
                with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
                    sock.bind((host, 0))
                    return sock.getsockname()[1], True

            def _serve(port_queue: Queue[Any]) -> None:
                server = None
                try:
                    host = os.getenv("UI_HOST", "127.0.0.1")
                    port_env = os.getenv("UI_PORT", os.getenv("PORT", "5000") or 5000)
                    try:
                        requested_port = int(port_env)
                    except (TypeError, ValueError):
                        requested_port = 0

                    port, changed = _select_listen_port(host, requested_port)
                    if changed:
                        log.warning(
                            "Requested UI port %s unavailable; using %s instead", requested_port, port
                        )
                    os.environ["UI_PORT"] = str(port)

                    app = self.handles.ui_app or _create_ui_app(self.handles.ui_state)
                    server = make_server(host, port, app)
                    server.daemon_threads = True
                    self.handles.ui_server = server
                    port_queue.put(port)
                    try:
                        self._emit_ui_ready(host, port)
                    except Exception:
                        log.exception("Failed to emit UI readiness signal")
                    server.serve_forever()
                except Exception:
                    with suppress(Exception):
                        port_queue.put(sys.exc_info()[1] or RuntimeError("ui serve failed"))
                    log.exception("UI HTTP server failed")
                finally:
                    self.handles.ui_server = None
                    if server is not None:
                        with suppress(Exception):
                            server.shutdown()
                        with suppress(Exception):
                            server.server_close()

            port_queue: Queue[Any] = Queue(maxsize=1)
            t = threading.Thread(target=_serve, args=(port_queue,), daemon=True)
            t.start()
            actual_port: str | None = None
            try:
                result = port_queue.get(timeout=5)
            except Empty:
                await self._publish_stage(
                    "ui:http",
                    False,
                    "ui server did not report readiness within timeout",
                )
            else:
                if isinstance(result, Exception):
                    await self._publish_stage("ui:http", False, str(result))
                else:
                    actual_port = str(result)
                    await self._publish_stage(
                        "ui:http",
                        True,
                        f"host={os.getenv('UI_HOST','127.0.0.1')} port={actual_port}",
                    )

    async def start_agents(self) -> None:
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
        elif not strategies:
            strategies = []
        if cfg.get("agents"):
            agent_manager = AgentManager.from_config(cfg)
            strategy_manager = None
            if agent_manager is None:
                strategy_manager = StrategyManager(strategies)
        else:
            agent_manager = AgentManager.from_default()
            strategy_manager = None if agent_manager is not None else StrategyManager(strategies)

        # Announce loaded agents
        try:
            names = [a.name for a in (agent_manager.agents if agent_manager else [])]
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
                agent_manager=agent_manager,
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
            return

        # Event-driven mode: start agent runtime, swarm coordinator and executor
        from ..agents.runtime import AgentRuntime
        from ..exec_service import TradeExecutor
        from ..agents.discovery import DiscoveryAgent
        from ..loop import _init_rl_training as _init_rl_training  # type: ignore

        aruntime = AgentRuntime(agent_manager or AgentManager.from_default(), portfolio)
        await aruntime.start()
        execu = TradeExecutor(memory, portfolio)
        execu.start()

        async def _discovery_loop():
            agent = DiscoveryAgent()
            method = discovery_method
            while True:
                try:
                    tokens = await agent.discover_tokens(method=method, offline=False)
                    if tokens:
                        event_bus.publish("token_discovered", list(tokens))
                except Exception:
                    pass
                await asyncio.sleep(max(5, min(60, loop_delay)))

        async def _evolve_loop():
            # Evolve/mutate/cull continually based on success metrics
            interval = int(getattr(agent_manager, "evolve_interval", 30) or 30)
            while True:
                try:
                    if agent_manager is not None:
                        await agent_manager.evolve(
                            threshold=getattr(agent_manager, "mutation_threshold", 0.0)
                        )
                        await agent_manager.update_weights()
                        agent_manager.save_weights()
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

    async def start(self) -> None:
        # Make orchestrator subscribe to control messages
        def _ctl(payload: dict) -> None:
            cmd = (payload or {}).get("cmd")
            if cmd == "stop":
                asyncio.get_event_loop().create_task(self.stop_all())

        event_bus.subscribe("control", _ctl)

        # 1) UI, 2) Bus, 3) Agents
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
        await self.start_agents()
        await self._publish_stage("runtime:ready", True)

    async def stop_all(self) -> None:
        if self._closed:
            return
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
    while not orch._closed:
        await asyncio.sleep(1)

    return 0


def main(argv: list[str] | None = None) -> None:
    if os.getenv("NEW_RUNTIME", "").lower() not in {"1", "true", "yes"}:
        print("NEW_RUNTIME is not enabled; aborting orchestrator.")
        raise SystemExit(2)
    try:
        asyncio.run(_amain(argv))
    except KeyboardInterrupt:
        pass
