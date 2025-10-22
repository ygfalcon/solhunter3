import json
import types

import pytest
import websockets
from aiohttp import web

from solhunter_zero.production.connectivity import ConnectivityChecker


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.mark.anyio("asyncio")
async def test_probe_ui_ws_hello(monkeypatch):
    from solhunter_zero.production import connectivity as connectivity_mod

    class DummyWS:
        async def ping(self) -> None:
            return None

        async def recv(self) -> str:
            return json.dumps({"channel": "events", "event": "hello"})

    class DummyConnect:
        def __init__(self) -> None:
            self.ws = DummyWS()

        async def __aenter__(self) -> DummyWS:
            return self.ws

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    def dummy_connect(*_args, **_kwargs):
        return DummyConnect()

    monkeypatch.setattr(
        connectivity_mod,
        "websockets",
        types.SimpleNamespace(connect=dummy_connect),
    )

    checker = ConnectivityChecker(env={})
    result = await checker._probe_ws("ui-ws", "ws://example/ws/events")
    assert result.ok


def _make_ui_app():
    app = web.Application()

    async def health(request):
        payload = request.app["payload"]
        return web.json_response(payload)

    app["payload"] = {"ok": True, "status": {"event_bus": True, "trading_loop": True}}
    app.router.add_get("/health", health)
    return app


@pytest.mark.anyio("asyncio")
async def test_probe_ui_http_success():
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


@pytest.mark.anyio("asyncio")
async def test_probe_ui_http_detects_event_bus_down():
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
