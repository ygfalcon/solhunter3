import asyncio
import importlib
import json
import sys
import threading
import time
import types
import urllib.request

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
            assert meta["url"].startswith("http://127.0.0.1:")
            assert meta["events_ws"].startswith("ws://")

            ui.push_event({"event": "heartbeat", "ts": time.time()})
            time.sleep(0.2)

            async def _collect() -> tuple[str, str, str]:
                async with ws_mod.connect(meta["events_ws"]) as conn:
                    hello = await asyncio.wait_for(conn.recv(), timeout=5)
                    await conn.send(json.dumps({"event": "hello", "client": "tests", "version": 3}))
                    meta_frame = await asyncio.wait_for(conn.recv(), timeout=5)
                    payload = await asyncio.wait_for(conn.recv(), timeout=5)
                    return hello, meta_frame, payload

            hello_msg, meta_msg, payload_msg = asyncio.run(_collect())
            hello_obj = json.loads(hello_msg)
            assert hello_obj.get("event") == "hello"
            assert hello_obj.get("channel") == "events"
            meta_obj = json.loads(meta_msg)
            assert meta_obj.get("type") == "UI_META"
            assert meta_obj.get("v") == ui.UI_SCHEMA_VERSION
            version_block = meta_obj.get("version") or {}
            assert version_block.get("schema_version") == ui.UI_SCHEMA_VERSION
            assert version_block.get("schema_hash")
            assert version_block.get("mode") in {"paper", "live"}
            broker_info = meta_obj.get("broker") or {}
            assert broker_info.get("channel") == meta_obj.get("channel")
            assert broker_info.get("kind")
            event_bus_info = meta_obj.get("event_bus") or {}
            assert (event_bus_info.get("url_ws") or "").startswith("ws://")
            lag_block = meta_obj.get("lag") or {}
            for key in ("bus", "ohlcv", "depth", "golden", "suggestions", "decisions", "rl"):
                assert key in lag_block
            ttl_block = meta_obj.get("ttl") or {}
            assert ttl_block.get("depth_s") == pytest.approx(10.0, rel=1e-6)
            assert ttl_block.get("ohlcv_5m_s") == pytest.approx(600.0, rel=1e-6)
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
            bootstrap = meta_obj.get("bootstrap") or {}
            assert bootstrap.get("price_providers")
            resilience = meta_obj.get("resilience") or {}
            ws_resilience = resilience.get("ws") or {}
            assert ws_resilience.get("ping_interval_s") == pytest.approx(20.0, rel=1e-6)
            assert ws_resilience.get("ping_timeout_s") == pytest.approx(20.0, rel=1e-6)
            contracts = meta_obj.get("test_contracts") or {}
            assert contracts.get("handshake") == "tests/test_ui_meta_ws.py"
            payload_obj = json.loads(payload_msg)
            assert payload_obj.get("event") == "heartbeat"
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)
    finally:
        ui.stop_websockets()
        for t in threads.values():
            if t is not None:
                t.join(timeout=1)
