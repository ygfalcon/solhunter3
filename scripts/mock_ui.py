"""Mock UI service for local testing and CI connectivity checks.

This lightweight server exposes the same surface area that the runtime UI
health probes expect:

* HTTP health endpoints with event bus and trading loop status markers
* WebSocket channels for ``events``, ``rl``, and ``logs`` that emit ``hello``
  frames followed by example payloads
* Ready-line logging that mirrors the runtime ``UI_READY`` / ``UI_WS_READY``
  markers that ``launch_live.sh`` watches for during startup

The service is intentionally dependency-light and uses ``aiohttp`` plus
``websockets``, both of which are already part of the project's Python
requirements.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import os
import signal
import time
from dataclasses import dataclass
from pathlib import Path

from aiohttp import web
import websockets

LOGGER = logging.getLogger("mock_ui")
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
)

UI_SCHEMA_VERSION = 1
DEFAULT_CHANNELS = ("events", "rl", "logs")
ASSET_ROOT = Path(__file__).resolve().parent / "mock_ui_assets"


@dataclass
class MockUIConfig:
    host: str
    port: int
    public_host: str
    ws_path_template: str
    ready_line: bool
    panel_count: int

    @property
    def base_http_url(self) -> str:
        return f"http://{self.public_host}:{self.port}" if self.port else f"http://{self.public_host}"

    @property
    def base_ws_url(self) -> str:
        return f"ws://{self.public_host}:{self.port}" if self.port else f"ws://{self.public_host}"

    def ws_url(self, channel: str) -> str:
        path = self.ws_path_template.format(channel=channel)
        if not path.startswith("/"):
            path = "/" + path
        return f"{self.base_ws_url}{path}"


class MockUIPublisher:
    def __init__(self, app: web.Application, config: MockUIConfig) -> None:
        self.app = app
        self.config = config
        self.clients: dict[str, set[web.WebSocketResponse]] = {channel: set() for channel in DEFAULT_CHANNELS}
        self._heartbeat_task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self._heartbeat_task = asyncio.create_task(self._heartbeat())

    async def stop(self) -> None:
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._heartbeat_task
        for channel_clients in self.clients.values():
            for client in set(channel_clients):
                await client.close(code=1001, message="server shutdown")

    async def register(self, channel: str, ws: web.WebSocketResponse) -> None:
        if channel not in self.clients:
            self.clients[channel] = set()
        self.clients[channel].add(ws)
        await self._send_handshake(channel, ws)
        await self._send_panel_snapshot(channel, ws)

    async def unregister(self, channel: str, ws: web.WebSocketResponse) -> None:
        self.clients.get(channel, set()).discard(ws)

    async def _send_handshake(self, channel: str, ws: web.WebSocketResponse) -> None:
        await ws.send_json({"event": "hello", "channel": channel, "schema": UI_SCHEMA_VERSION})

    async def _send_panel_snapshot(self, channel: str, ws: web.WebSocketResponse) -> None:
        panels = [
            {"name": "portfolio", "status": "ok"},
            {"name": "orders", "status": "ok"},
            {"name": "risk", "status": "ok"},
            {"name": "market_depth", "status": "ok"},
        ]
        payload = {
            "event": "ui_snapshot",
            "channel": channel,
            "panels": panels[: self.config.panel_count],
            "generated_ts": time.time(),
        }
        await ws.send_json(payload)

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(5)
            for channel, channel_clients in self.clients.items():
                stale: list[web.WebSocketResponse] = []
                for client in channel_clients:
                    try:
                        await client.send_json({"event": "keepalive", "channel": channel, "ts": time.time()})
                    except Exception:
                        stale.append(client)
                for client in stale:
                    channel_clients.discard(client)


async def _health(_: web.Request, config: MockUIConfig) -> web.Response:
    status = {"event_bus": True, "trading_loop": True}
    payload = {
        "ok": True,
        "status": status,
        "panels": {
            "portfolio": "ok",
            "orders": "ok",
            "risk": "ok",
            "market_depth": "ok",
        },
        "ws": {channel: config.ws_url(channel) for channel in DEFAULT_CHANNELS},
    }
    return web.json_response(payload)


async def _root(_: web.Request, config: MockUIConfig) -> web.Response:
    message = {
        "message": "mock ui running",
        "health": f"{config.base_http_url}/health",
        "ws": {channel: config.ws_url(channel) for channel in DEFAULT_CHANNELS},
    }
    return web.json_response(message)


async def _websocket_handler(request: web.Request, publisher: MockUIPublisher, config: MockUIConfig) -> web.StreamResponse:
    channel = request.match_info.get("channel", "events")
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    await publisher.register(channel, ws)

    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            data: str = msg.data
            if data.strip().lower() == "ping":
                await ws.send_str("pong")
            else:
                await ws.send_json({"event": "echo", "channel": channel, "payload": data})
        elif msg.type == web.WSMsgType.ERROR:
            LOGGER.warning("WebSocket error on %s: %s", channel, ws.exception())
            break
    await publisher.unregister(channel, ws)
    return ws


def _build_config() -> MockUIConfig:
    host = os.getenv("MOCK_UI_HOST", "0.0.0.0")
    port = int(os.getenv("MOCK_UI_PORT", "5001"))
    public_host = os.getenv("MOCK_UI_PUBLIC_HOST", os.getenv("UI_HOST", "127.0.0.1")) or "127.0.0.1"
    ws_path_template = os.getenv("MOCK_UI_WS_PATH_TEMPLATE", "/ws/{channel}")
    ready_line = os.getenv("MOCK_UI_READY_LINE", "1").strip().lower() not in {"0", "false", "no"}
    panel_count = int(os.getenv("MOCK_UI_PANEL_COUNT", "4"))
    return MockUIConfig(
        host=host,
        port=port,
        public_host=public_host,
        ws_path_template=ws_path_template,
        ready_line=ready_line,
        panel_count=max(1, panel_count),
    )


def _announce_ready(config: MockUIConfig) -> None:
    if not config.ready_line:
        return
    print(
        "UI_READY "
        f"url={config.base_http_url} "
        f"rl_ws={config.ws_url('rl')} "
        f"events_ws={config.ws_url('events')} "
        f"logs_ws={config.ws_url('logs')}",
        flush=True,
    )
    print("UI_WS_READY status=ok channels=events,rl,logs", flush=True)


def _prepare_app(config: MockUIConfig) -> web.Application:
    app = web.Application()
    publisher = MockUIPublisher(app, config)
    app["publisher"] = publisher

    app.router.add_get("/", lambda request: _root(request, config))
    app.router.add_get("/health", lambda request: _health(request, config))
    app.router.add_get("/healthz", lambda request: _health(request, config))
    ws_route = config.ws_path_template
    if "{channel}" not in ws_route:
        ws_route = ws_route.rstrip("/") + "/{channel}"
    if not ws_route.startswith("/"):
        ws_route = "/" + ws_route
    app.router.add_get(ws_route, lambda request: _websocket_handler(request, publisher, config))

    async def _on_startup(_: web.Application) -> None:
        asset_path = os.getenv("CONNECTIVITY_UI_ASSET_PATHS")
        if not asset_path:
            os.environ["CONNECTIVITY_UI_ASSET_PATHS"] = str(ASSET_ROOT)
        await publisher.start()
        _announce_ready(config)

    async def _on_cleanup(_: web.Application) -> None:
        await publisher.stop()

    app.on_startup.append(_on_startup)
    app.on_cleanup.append(_on_cleanup)
    return app


async def _run_app(config: MockUIConfig) -> None:
    app = _prepare_app(config)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, host=config.host, port=config.port)
    await site.start()
    LOGGER.info("Mock UI listening on %s:%s", config.host, config.port)

    stop_event = asyncio.Event()

    def _handle_signal(*_: object) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _handle_signal)

    await stop_event.wait()
    LOGGER.info("Shutting down mock UI")
    await runner.cleanup()


def main() -> int:
    config = _build_config()
    try:
        asyncio.run(_run_app(config))
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
