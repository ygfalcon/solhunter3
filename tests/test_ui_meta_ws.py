import asyncio
import importlib
import json
import sys
import threading
import time
import types
import urllib.request
from urllib.parse import urlparse

import pytest
from werkzeug.serving import make_server


@pytest.mark.timeout(30)
def test_ui_meta_websocket_smoke(monkeypatch):
    for name in list(sys.modules):
        if name.startswith("websockets"):
            sys.modules.pop(name, None)
    ws_mod = pytest.importorskip("websockets")

    sys.modules.setdefault("sqlparse", types.SimpleNamespace())
    sys.modules.setdefault(
        "solhunter_zero.wallet", types.ModuleType("solhunter_zero.wallet")
    )
    base58_mod = types.ModuleType("base58")
    base58_mod.b58decode = lambda *a, **k: b""
    base58_mod.b58encode = lambda *a, **k: b""
    sys.modules.setdefault("base58", base58_mod)
    sa = sys.modules.get("sqlalchemy")
    if sa is not None:
        class _Index:
            def __init__(self, *args, **kwargs):
                self.args = args
                self.kwargs = kwargs

        setattr(sa, "Index", getattr(sa, "Index", _Index))

    sys.modules.pop("solhunter_zero.ui", None)
    import solhunter_zero.ui as ui
    importlib.reload(ui)

    assert ui.websockets is not None

    monkeypatch.setenv("UI_HOST", "127.0.0.1")
    monkeypatch.setenv("UI_PORT", "6200")
    monkeypatch.setenv("UI_RL_WS_PORT", "0")
    monkeypatch.setenv("UI_EVENT_WS_PORT", "0")
    monkeypatch.setenv("UI_LOG_WS_PORT", "0")

    threads: dict[str, threading.Thread | None] = {}
    try:
        threads = ui.start_websockets()
        app = ui.create_app()
        server = make_server("127.0.0.1", 0, app)
        port = server.server_port
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/ui/meta", timeout=5
            ) as response:
                meta = json.loads(response.read().decode("utf-8"))
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/api/manifest", timeout=5
            ) as response:
                manifest = json.loads(response.read().decode("utf-8"))
            assert meta["url"].startswith("http://127.0.0.1:")
            assert meta["events_ws"].startswith("ws://")
            status_block = meta.get("status")
            assert isinstance(status_block, dict)
            assert status_block.get("rl_ws") == "ok"
            assert status_block.get("events_ws") == "ok"
            assert status_block.get("logs_ws") == "ok"
            assert manifest["ui_port"] == 6200
            from solhunter_zero.production.connectivity import ConnectivityChecker

            checker = ConnectivityChecker(env={"UI_HOST": "127.0.0.1", "UI_PORT": "6200"})
            ui_http_targets = [t for t in checker.targets if t.get("name") == "ui-http"]
            assert ui_http_targets, "expected ui-http target from connectivity checker"
            assert ui_http_targets[0]["url"].startswith("http://127.0.0.1:6200/")
            ui_ws_targets = [t for t in checker.targets if t.get("name") == "ui-ws"]
            assert ui_ws_targets, "expected ui-ws target from connectivity checker"
            ws_target = ui_ws_targets[0]["url"]
            if isinstance(ws_target, dict):
                candidate_url = ws_target.get("events") or next(iter(ws_target.values()), None)
            else:
                candidate_url = ws_target
            assert candidate_url, "expected at least one websocket url"
            parsed_ws = urlparse(candidate_url)
            assert parsed_ws.scheme == "ws"
            assert parsed_ws.hostname in {"127.0.0.1", "localhost"}
            assert parsed_ws.port is not None
            assert parsed_ws.path.startswith("/ws/")

            ui.push_event({"event": "heartbeat", "ts": time.time()})
            time.sleep(0.2)

            async def _collect() -> tuple[str, dict[str, object] | None, dict[str, object] | None]:
                async with ws_mod.connect(meta["events_ws"]) as conn:
                    meta_frame = await asyncio.wait_for(conn.recv(), timeout=5)
                    await conn.send(json.dumps({"event": "hello", "client": "tests", "version": 3}))
                    hello_obj: dict[str, object] | None = None
                    payload_obj: dict[str, object] | None = None
                    deadline = time.time() + 5.0
                    while (hello_obj is None or payload_obj is None) and time.time() < deadline:
                        raw = await asyncio.wait_for(conn.recv(), timeout=5)
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue
                        if not isinstance(obj, dict):
                            continue
                        event_name = str(obj.get("event") or obj.get("type") or "").lower()
                        if event_name == "hello" and hello_obj is None:
                            hello_obj = obj
                        elif obj.get("event") == "heartbeat" and payload_obj is None:
                            payload_obj = obj
                    return meta_frame, hello_obj, payload_obj

            meta_msg, hello_obj, payload_obj = asyncio.run(_collect())
            assert hello_obj is not None, "expected hello frame"
            assert hello_obj.get("event") == "hello"
            assert hello_obj.get("channel") == "events"
            assert payload_obj is not None, "expected heartbeat payload"
            meta_obj = json.loads(meta_msg)
            assert meta_obj.get("type") == "UI_META"
            assert meta_obj.get("v") == ui.UI_SCHEMA_VERSION
            version_block = meta_obj.get("version") or {}
            if not version_block:
                pytest.skip("UI meta version payload unavailable in this environment")
            assert version_block.get("schema_version") == ui.UI_SCHEMA_VERSION
            assert version_block.get("schema_hash")
            assert version_block.get("mode") == "paper"
            broker_info = meta_obj.get("broker") or {}
            assert broker_info.get("channel") == meta_obj.get("channel")
            assert broker_info.get("kind")
            event_bus_info = meta_obj.get("event_bus") or {}
            assert event_bus_info.get("url_ws") == "ws://127.0.0.1:8779"
            lag_block = meta_obj.get("lag") or {}
            for key in ("bus_ms", "ohlcv_ms", "depth_ms", "golden_ms", "suggestions_ms", "votes_ms", "rl_ms"):
                assert key in lag_block
            ttl_block = meta_obj.get("ttl") or {}
            assert ttl_block.get("depth_s") == pytest.approx(10.0, rel=1e-6)
            assert ttl_block.get("ohlcv_5m_s") == pytest.approx(600.0, rel=1e-6)
            features_block = meta_obj.get("features") or {}
            for required_key in (
                "discovery_enabled",
                "golden_pipeline_enabled",
                "depth_service_enabled",
                "rl_shadow_enabled",
                "sentiment_enabled",
            ):
                assert required_key in features_block
            providers = meta_obj.get("providers") or {}
            for provider_key in (
                "solana_rpc",
                "helius_das",
                "jupiter_ws",
                "bird_eye",
                "dexscreener",
                "raydium",
                "meteora",
                "phoenix",
                "pump_fun",
            ):
                assert provider_key in providers
                assert providers[provider_key]["status"] in {"ok", "degraded", "down"}
            streams = meta_obj.get("streams") or []
            stream_names = {entry.get("name") for entry in streams if isinstance(entry, dict)}
            assert {"discovery", "market_ohlcv", "market_depth", "golden_snapshot", "agent_suggestions", "vote_decisions", "rl_weights", "shadow_execution"}.issubset(stream_names)
            schemas = meta_obj.get("schemas") or []
            assert schemas and all(entry.get("schema_id") for entry in schemas)
            assert all("types" in entry for entry in schemas if entry.get("required"))
            bootstrap = meta_obj.get("bootstrap") or {}
            providers_list = bootstrap.get("price_providers") or []
            assert providers_list[:4] == ["pyth", "dexscreener", "birdeye", "synthetic"]
            assert bootstrap.get("price_provider_order") == providers_list
            timeouts_ms = bootstrap.get("price_provider_timeouts_ms") or {}
            for provider_name in providers_list[:4]:
                assert timeouts_ms.get(provider_name), f"timeout missing for {provider_name}"
                assert timeouts_ms[provider_name] > 0
            seeds = bootstrap.get("seed_tokens") or []
            assert {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}.issubset(set(seeds))
            seed_metadata = bootstrap.get("seed_token_metadata") or []
            assert seed_metadata, "expected seed token metadata"
            metadata_mints = {entry.get("mint") for entry in seed_metadata if isinstance(entry, dict)}
            assert {"So11111111111111111111111111111111111111112", "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZsaAkJ9", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}.issubset(metadata_mints)
            resilience = meta_obj.get("resilience") or {}
            ws_resilience = resilience.get("ws") or {}
            assert ws_resilience.get("ping_interval_s") == pytest.approx(20.0, rel=1e-6)
            assert ws_resilience.get("ping_timeout_s") == pytest.approx(20.0, rel=1e-6)
            das_resilience = resilience.get("das") or {}
            assert das_resilience.get("rps") == pytest.approx(1.0, rel=1e-6)
            assert das_resilience.get("timeout_threshold") == pytest.approx(3.0, rel=1e-6)
            assert das_resilience.get("timeout_total_s") == pytest.approx(9.0, rel=1e-6)
            assert das_resilience.get("circuit_open_s") == pytest.approx(90.0, rel=1e-6)
            provider_resilience = resilience.get("price_providers") or {}
            for provider_name in providers_list[:3]:
                assert provider_resilience.get(provider_name)
                assert provider_resilience[provider_name]["timeout_ms"] > 0
            contracts = meta_obj.get("test_contracts") or {}
            assert contracts.get("handshake") == "tests/test_ui_meta_ws.py"
            execution_block = meta_obj.get("execution") or {}
            assert execution_block.get("paper") is True
            assert execution_block.get("keypair_mode") in {"ephemeral", "missing"}
            assert execution_block.get("rl_mode") in {"shadow", "applied", "live"}
            depth_info = execution_block.get("depth_service") or {}
            assert depth_info.get("enabled") is True
            assert depth_info.get("ready") in {True, False}
            assert depth_info.get("rpc_url")
            assert depth_info.get("reason") in {None, "paper-mode", "starting", "disabled"}
            self_check = meta_obj.get("self_check") or {}
            assert isinstance(self_check, dict)
            assert self_check.get("status")
            assert payload_obj.get("event") == "heartbeat"
            if isinstance(meta.get("meta"), dict):
                assert meta["meta"].get("channel") == meta_obj.get("channel")
                assert meta["meta"].get("broker") == meta_obj.get("broker")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        ui.stop_websockets()
        for t in threads.values():
            if t is not None:
                t.join(timeout=1)
