import asyncio
import json
import types
from pathlib import Path

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
        def __init__(self, channel: str, *, fail: bool = False) -> None:
            self.channel = channel
            self.fail = fail

        async def ping(self) -> None:
            if self.fail:
                raise RuntimeError("ping failed")

        async def recv(self) -> str:
            if self.fail:
                raise asyncio.TimeoutError
            return json.dumps({"channel": self.channel, "event": "hello"})

    class DummyConnect:
        def __init__(self, channel: str, *, fail: bool = False) -> None:
            self.channel = channel
            self.fail = fail

        async def __aenter__(self) -> DummyWS:
            if self.fail:
                raise RuntimeError("connect failed")
            return DummyWS(self.channel, fail=self.fail)

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    def dummy_connect(url, *_args, **_kwargs):
        if "rl" in url:
            return DummyConnect("rl")
        return DummyConnect("events")

    monkeypatch.setattr(
        connectivity_mod,
        "websockets",
        types.SimpleNamespace(connect=dummy_connect),
    )

    checker = ConnectivityChecker(env={})
    urls = {"events": "ws://example/ws/events", "rl": "ws://example/ws/rl"}
    result = await checker._probe_ws("ui-ws", urls)
    assert result.ok
    assert result.status == "ok"
    assert result.latency_ms is not None


@pytest.mark.anyio("asyncio")
async def test_probe_ui_ws_partial_outage(monkeypatch):
    from solhunter_zero.production import connectivity as connectivity_mod

    class DummyWS:
        def __init__(self, channel: str, *, fail: bool = False) -> None:
            self.channel = channel
            self.fail = fail

        async def ping(self) -> None:
            if self.fail:
                raise RuntimeError("ping failed")

        async def recv(self) -> str:
            if self.fail:
                raise asyncio.TimeoutError
            return json.dumps({"channel": self.channel, "event": "hello"})

    class DummyConnect:
        def __init__(self, channel: str, *, fail: bool = False) -> None:
            self.channel = channel
            self.fail = fail

        async def __aenter__(self) -> DummyWS:
            if self.fail:
                raise RuntimeError("connect failed")
            return DummyWS(self.channel, fail=self.fail)

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    def dummy_connect(url, *_args, **_kwargs):
        if "logs" in url:
            return DummyConnect("logs", fail=True)
        return DummyConnect("events")

    monkeypatch.setattr(
        connectivity_mod,
        "websockets",
        types.SimpleNamespace(connect=dummy_connect),
    )

    checker = ConnectivityChecker(env={})
    urls = {
        "events": "ws://example/ws/events",
        "logs": "ws://example/ws/logs",
    }
    result = await checker._probe_ws("ui-ws", urls)
    assert result.ok
    assert result.status == "degraded"
    assert result.error and "logs" in result.error


@pytest.mark.anyio("asyncio")
async def test_probe_ui_ws_total_outage(monkeypatch):
    from solhunter_zero.production import connectivity as connectivity_mod

    class DummyConnect:
        def __init__(self, channel: str) -> None:
            self.channel = channel

        async def __aenter__(self):
            raise RuntimeError("connect failed")

        async def __aexit__(self, exc_type, exc, tb) -> bool:
            return False

    def dummy_connect(url, *_args, **_kwargs):
        return DummyConnect("events")

    monkeypatch.setattr(
        connectivity_mod,
        "websockets",
        types.SimpleNamespace(connect=dummy_connect),
    )

    checker = ConnectivityChecker(env={})
    urls = {"events": "ws://example/ws/events"}
    result = await checker._probe_ws("ui-ws", urls)
    assert not result.ok
    assert result.status == "error"
    assert "events" in (result.error or "")


def test_skip_ui_targets_via_env():
    checker = ConnectivityChecker(env={"CONNECTIVITY_SKIP_UI_PROBES": "true", "UI_HOST": "dummy"})
    names = {target["name"] for target in checker.targets}
    assert "ui-ws" not in names
    assert "ui-http" not in names


def test_connectivity_targets_use_canonical_ui_http_url():
    checker = ConnectivityChecker(
        env={
            "UI_HTTP_URL": "https://0.0.0.0:8443",
            "UI_HTTP_SCHEME": "https",
            "UI_HOST": "0.0.0.0",
            "UI_PORT": "5001",
            "UI_HEALTH_PATH": "/ui/meta",
        }
    )

    ui_target = next(target for target in checker.targets if target["name"] == "ui-http")
    assert ui_target["url"] == "https://127.0.0.1:8443/ui/meta"


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


def test_resolve_ui_ws_urls_prefers_runtime_log(tmp_path: Path):
    log_path = tmp_path / "runtime.log"
    log_path.write_text(
        "\n".join(
            [
                "[ts] other line",
                "[ts] UI_WS_READY status=ok rl_ws=ws://localhost:9101/ws/rl events_ws=ws://localhost:9100/ws/events logs_ws=-",
            ]
        )
    )
    checker = ConnectivityChecker(env={"CONNECTIVITY_RUNTIME_LOG": str(log_path)})
    urls = checker._resolve_ui_ws_urls()
    assert urls["events"] == "ws://localhost:9100/ws/events"
    assert urls["rl"] == "ws://localhost:9101/ws/rl"
    assert "logs" not in urls


def test_resolve_ui_ws_urls_falls_back_to_manifest(tmp_path: Path):
    manifest_path = tmp_path / "ui_manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "events_ws": "ws://127.0.0.1:9000/ws/events",
                "logs_ws": "ws://127.0.0.1:9002/ws/logs",
            }
        )
    )
    checker = ConnectivityChecker(env={"CONNECTIVITY_UI_MANIFEST": str(manifest_path)})
    urls = checker._resolve_ui_ws_urls()
    assert urls["events"] == "ws://127.0.0.1:9000/ws/events"
    assert urls["logs"] == "ws://127.0.0.1:9002/ws/logs"
