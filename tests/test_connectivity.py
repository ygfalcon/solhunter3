import asyncio
import json
import logging
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


def _write_dummy_assets(root: Path) -> Path:
    root.mkdir(parents=True, exist_ok=True)
    bundle = root / "assets"
    bundle.mkdir(parents=True, exist_ok=True)
    (bundle / "app.js").write_text("console.log('ok')", encoding="utf-8")
    return root


@pytest.mark.anyio("asyncio")
async def test_probe_ui_http_success(tmp_path: Path):
    app = _make_ui_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        assets = _write_dummy_assets(tmp_path / "ui-assets")
        checker = ConnectivityChecker(env={"CONNECTIVITY_UI_ASSET_PATHS": str(assets)})
        result = await checker._probe_http("ui-http", f"http://127.0.0.1:{port}/health")
        assert result.ok
    finally:
        await runner.cleanup()


@pytest.mark.anyio("asyncio")
async def test_probe_ui_http_fails_when_assets_missing(tmp_path: Path, caplog: pytest.LogCaptureFixture):
    app = _make_ui_app()
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    caplog.set_level(logging.ERROR)
    try:
        port = site._server.sockets[0].getsockname()[1]
        missing = tmp_path / "ui-assets-missing"
        checker = ConnectivityChecker(env={"CONNECTIVITY_UI_ASSET_PATHS": str(missing)})
        result = await checker._probe_http("ui-http", f"http://127.0.0.1:{port}/health")
        assert not result.ok
        assert "UI assets missing" in (result.error or "")
        assert str(missing) in caplog.text
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
async def test_connectivity_soak_restarts_and_verifies_reconnections(monkeypatch):
    checker = ConnectivityChecker(env={"UI_HOST": "127.0.0.1", "UI_PORT": "6001"})

    async def ok_http(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    async def ok_ws(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    async def ok_redis(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    monkeypatch.setattr(checker, "_probe_http", ok_http)
    monkeypatch.setattr(checker, "_probe_ws", ok_ws)
    monkeypatch.setattr(checker, "_probe_redis", ok_redis)

    restart_calls: list[str] = []

    async def restart_services() -> dict[str, bool]:
        restart_calls.append("redis")
        await asyncio.sleep(0)
        return {
            "panels_reconnected": True,
            "stale_indicators_cleared": True,
            "alerts_fired": True,
        }

    summary = await checker.run_soak(
        duration=0.05, interval=0.01, restart_backing_services=[restart_services]
    )

    assert restart_calls == ["redis"]
    assert summary.reconnect_count == 0


@pytest.mark.anyio("asyncio")
async def test_connectivity_soak_fails_without_reconnection_signals(monkeypatch):
    checker = ConnectivityChecker(env={"UI_HOST": "127.0.0.1", "UI_PORT": "6002"})

    async def ok_http(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    async def ok_ws(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    async def ok_redis(name: str, url: str) -> ConnectivityResult:
        return ConnectivityResult(name=name, target=url, ok=True)

    monkeypatch.setattr(checker, "_probe_http", ok_http)
    monkeypatch.setattr(checker, "_probe_ws", ok_ws)
    monkeypatch.setattr(checker, "_probe_redis", ok_redis)

    async def restart_services() -> dict[str, bool]:
        return {"panels_reconnected": False, "stale_indicators_cleared": True}

    with pytest.raises(RuntimeError, match="reconnection checks failed"):
        await checker.run_soak(
            duration=0.02, interval=0.01, restart_backing_services=[restart_services]
        )


@pytest.mark.anyio("asyncio")
async def test_probe_ui_http_detects_event_bus_down(tmp_path: Path):
    app = _make_ui_app()
    app["payload"] = {"ok": False, "status": {"event_bus": False, "trading_loop": True}}
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    try:
        port = site._server.sockets[0].getsockname()[1]
        assets = _write_dummy_assets(tmp_path / "ui-assets")
        checker = ConnectivityChecker(env={"CONNECTIVITY_UI_ASSET_PATHS": str(assets)})
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


def test_build_targets_include_jito_and_mempool_endpoints():
    checker = ConnectivityChecker(
        env={
            "SOLANA_RPC_URL": "https://rpc.example",
            "SOLANA_WS_URL": "wss://ws.example",
            "JITO_RPC_URL": "https://jito.rpc",
            "JITO_WS_URL": "wss://jito.ws",
            "MEMPOOL_STREAM_WS_URL": "wss://mempool/ws",
            "MEMPOOL_STREAM_REDIS_URL": "redis://mempool/2",
        }
    )
    names = {target["name"] for target in checker.targets}
    assert {
        "jito-rpc",
        "jito-ws",
        "mempool-stream-ws",
        "mempool-stream-redis",
    } <= names
