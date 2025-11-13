import json
import types

import pytest
import websockets
from aiohttp import web

from solhunter_zero.production.connectivity import ConnectivityChecker, ConnectivityResult


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


def test_skip_ui_targets_via_env():
    checker = ConnectivityChecker(env={"CONNECTIVITY_SKIP_UI_PROBES": "true", "UI_HOST": "dummy"})
    names = {target["name"] for target in checker.targets}
    assert "ui-ws" not in names
    assert "ui-http" not in names


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
async def test_connectivity_soak_refreshes_ui_override(monkeypatch):
    for key in ("UI_HEALTH_PATH", "UI_HEALTH_URL", "UI_HTTP_URL", "UI_HOST", "UI_PORT"):
        monkeypatch.delenv(key, raising=False)

    checker = ConnectivityChecker(env={"UI_HOST": "127.0.0.1", "UI_PORT": "6311"})
    initial_target = next(target for target in checker.targets if target["name"] == "ui-http")
    assert initial_target["url"].endswith("/health")

    monkeypatch.setenv("UI_HEALTH_PATH", "/ui/meta")

    captured_urls: list[str] = []

    async def _ok_http(name: str, url: str) -> ConnectivityResult:
        captured_urls.append(url)
        return ConnectivityResult(name=name, target=url, ok=True)

    async def _ok_ws(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    async def _ok_redis(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    monkeypatch.setattr(checker, "_probe_http", _ok_http)
    monkeypatch.setattr(checker, "_probe_ws", _ok_ws)
    monkeypatch.setattr(checker, "_probe_redis", _ok_redis)

    await checker.run_soak(duration=0.05, interval=0.01)

    assert any(url.endswith("/ui/meta") for url in captured_urls)


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
