#!/usr/bin/env python3
"""Launch one or more sequential RL daemons with a shared health endpoint."""

from __future__ import annotations

import argparse
import asyncio
import errno
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Iterable, List

from aiohttp import web

import solhunter_zero.device as device_mod
from solhunter_zero.agents.dqn import DQNAgent
from solhunter_zero.agents.memory import MemoryAgent
from solhunter_zero.agents.ppo_agent import PPOAgent
from solhunter_zero.memory import Memory
from solhunter_zero.paths import ROOT
from solhunter_zero.health_runtime import check_redis, wait_for
from solhunter_zero.port_utils import find_available_port
from solhunter_zero.rl_daemon import RLDaemon
from solhunter_zero.rl_training import TrainingConfig


logger = logging.getLogger(__name__)


HEALTH_INFO_PATH = ROOT / "rl_daemon.health.json"


class RLHealth:
    """Thread-safe health state shared between RL workers and HTTP server."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started = time.time()
        self._status = "starting"
        self._last_heartbeat: float | None = None
        self._last_training: float | None = None
        self._last_error: str | None = None
        self._iterations = 0
        self._summary: dict[str, int] = {}

    def touch(self) -> None:
        with self._lock:
            self._last_heartbeat = time.time()
            if self._status == "starting":
                self._status = "running"

    def record_training(self, algo: str, trades: int, snaps: int) -> None:
        with self._lock:
            self._status = "running"
            self._last_training = time.time()
            self._iterations += 1
            self._summary.setdefault(algo, 0)
            self._summary[algo] += 1
            self._last_error = None

    def record_error(self, message: str) -> None:
        with self._lock:
            self._status = "error"
            self._last_error = message

    def snapshot(self) -> dict[str, object]:
        with self._lock:
            return {
                "status": self._status,
                "started": self._started,
                "last_heartbeat": self._last_heartbeat,
                "last_training": self._last_training,
                "iterations": self._iterations,
                "summary": dict(self._summary),
                "last_error": self._last_error,
            }


async def _start_health_server(
    state: RLHealth, host: str, port: int
) -> tuple[web.AppRunner, int]:
    async def _health(request: web.Request) -> web.Response:
        return web.json_response(state.snapshot())

    attempts = 0
    current_port = int(port)

    while attempts < 32:
        app = web.Application()
        app.router.add_get("/health", _health)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, host, current_port)
        try:
            await site.start()
        except OSError as exc:
            await runner.cleanup()
            if getattr(exc, "errno", None) not in {errno.EADDRINUSE, errno.EACCES}:
                raise
            attempts += 1
            next_port = find_available_port(host, current_port + 1)
            if next_port == current_port:
                next_port += 1
            logger.info(
                "RL health server port %s busy, retrying on %s",
                current_port,
                next_port,
            )
            current_port = next_port
            continue

        sockets = getattr(site._server, "sockets", None)
        if sockets:
            try:
                current_port = sockets[0].getsockname()[1]
            except Exception:  # pragma: no cover - socket APIs vary
                pass
        return runner, current_port

    raise RuntimeError("Unable to bind RL health server; no ports available")


def _parse_algorithms(items: Iterable[str] | None) -> List[str]:
    if not items:
        return ["ppo"]
    algos: List[str] = []
    for item in items:
        parts = [p.strip() for p in str(item).split(",") if p.strip()]
        algos.extend(parts or ["ppo"])
    if not algos:
        algos.append("ppo")
    return algos


async def main() -> None:
    parser = argparse.ArgumentParser(description="Run sequential RL daemon")
    parser.add_argument("--memory", default="sqlite:///memory.db")
    parser.add_argument("--data", default="offline_data.db")
    parser.add_argument("--interval", type=float, default=3600.0)
    parser.add_argument("--device", default="auto")
    parser.add_argument("--event-bus")
    parser.add_argument("--algo", action="append", help="RL algorithm to train (can repeat)")
    parser.add_argument("--dqn-model", default="dqn_model.pt")
    parser.add_argument("--ppo-model", default="ppo_model.pt")
    parser.add_argument(
        "--health-port",
        type=int,
        default=int(os.getenv("RL_HEALTH_PORT", "7070") or 7070),
    )
    parser.add_argument(
        "--health-host",
        default=os.getenv("RL_HEALTH_HOST", "127.0.0.1"),
    )
    args = parser.parse_args()

    dev = device_mod.get_default_device(args.device)
    if args.event_bus:
        os.environ["EVENT_BUS_URL"] = args.event_bus

    redis_urls: list[str] = []
    for key in ("BROKER_URLS", "BROKER_URL", "EVENT_BUS_URL"):
        val = os.getenv(key)
        if not val:
            continue
        if key == "BROKER_URLS":
            parts = [u.strip() for u in val.split(",") if u.strip()]
        else:
            parts = [val]
        for url in parts:
            if url.startswith(("redis://", "rediss://")):
                redis_urls.append(url)

    if redis_urls:

        def _redis_probe() -> tuple[bool, str]:
            last_msg = "no redis URL checked"
            for redis_url in redis_urls:
                ok, msg = check_redis(redis_url)
                if ok:
                    return True, f"redis ok ({redis_url})"
                last_msg = f"{redis_url}: {msg}"
            return False, last_msg

        ok, msg = wait_for(_redis_probe, retries=30, sleep=1.0)
        if ok:
            logger.info("Redis broker ready: %s", msg)
        else:
            logger.warning(
                "Redis broker unavailable (%s); continuing in degraded mode",
                msg,
            )

    memory = Memory(args.memory)
    mem_agent = MemoryAgent(memory)

    algos = _parse_algorithms(args.algo)
    training_config = TrainingConfig(device=str(dev))
    health = RLHealth()

    daemons: List[RLDaemon] = []
    for algo in algos:
        if algo.lower() == "dqn":
            agent = DQNAgent(
                memory_agent=mem_agent,
                model_path=args.dqn_model,
                replay_url="sqlite:///replay.db",
                device=dev,
            )
            model_path = Path(args.dqn_model)
        else:
            agent = PPOAgent(
                memory_agent=mem_agent,
                data_url=f"sqlite:///{args.data}",
                model_path=args.ppo_model,
                device=dev,
            )
            model_path = Path(args.ppo_model)

        daemon = RLDaemon(
            memory_path=args.memory,
            data_path=args.data,
            model_path=model_path,
            algo=algo,
            interval=args.interval,
            health_state=health,
            training_config=training_config,
        )
        daemon.register_agent(agent)
        daemons.append(daemon)

    async def _shutdown() -> None:
        logger.info("Shutting down RL daemon")
        for daemon in daemons:
            daemon.stop()
        await asyncio.gather(*(d.close() for d in daemons))
        try:
            HEALTH_INFO_PATH.unlink()
        except FileNotFoundError:
            pass

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        health.record_error("terminated")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _signal_handler)

    runner, port = await _start_health_server(health, args.health_host, args.health_port)
    HEALTH_INFO_PATH.write_text(
        json.dumps({
            "host": args.health_host,
            "port": port,
            "url": f"http://{args.health_host}:{port}/health",
            "updated": time.time(),
        }),
        encoding="utf-8",
    )

    tasks = [daemon.start() for daemon in daemons]

    try:
        await stop_event.wait()
    finally:
        await _shutdown()
        await runner.cleanup()


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s %(message)s")
    asyncio.run(main())

