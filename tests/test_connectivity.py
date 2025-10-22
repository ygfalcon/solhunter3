import asyncio
import json

import pytest
import websockets
from aiohttp import web

from solhunter_zero.production.connectivity import ConnectivityChecker


@pytest.mark.asyncio
async def test_probe_ui_ws_hello():
    async def handler(websocket, _path):
        await websocket.send(json.dumps({"channel": "events", "event": "hello"}))
        await websocket.wait_closed()

    server = await websockets.serve(handler, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        checker = ConnectivityChecker(env={})
        result = await checker._probe_ws("ui-ws", f"ws://127.0.0.1:{port}/ws/events")
        assert result.ok
    finally:
        server.close()
        await server.wait_closed()


def _make_ui_app():
    app = web.Application()

    async def health(request):
        payload = request.app["payload"]
        return web.json_response(payload)

    app["payload"] = {"ok": True, "status": {"event_bus": True, "trading_loop": True}}
    app.router.add_get("/health", health)
    app.router.add_head("/health", health)
    return app


@pytest.mark.asyncio
async def test_probe_ui_http_success(monkeypatch):
    app = _make_ui_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        checker = ConnectivityChecker(env={})
        result = await checker._probe_http("ui-http", f"http://127.0.0.1:{port}/health")
        assert result.ok
    finally:
        await runner.cleanup()


@pytest.mark.asyncio
async def test_probe_ui_http_detects_event_bus_down(monkeypatch):
    app = _make_ui_app()
    app["payload"] = {"ok": False, "status": {"event_bus": False, "trading_loop": True}}
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        checker = ConnectivityChecker(env={})
        result = await checker._probe_http("ui-http", f"http://127.0.0.1:{port}/health")
        assert not result.ok
        assert result.error and "event bus" in result.error
    finally:
        await runner.cleanup()
