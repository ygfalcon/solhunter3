import asyncio
import json
import websockets
import pytest
import sys
import types
import importlib.util
import importlib.machinery

if importlib.util.find_spec("solders") is None:
    mod = types.ModuleType("solders")
    mod.__spec__ = importlib.machinery.ModuleSpec("solders", None)
    sys.modules.setdefault("solders", mod)
    sys.modules.setdefault("solders.keypair", types.SimpleNamespace(Keypair=type("Keypair", (), {})))
    sys.modules.setdefault("solders.pubkey", types.SimpleNamespace(Pubkey=object))
    sys.modules.setdefault("solders.instruction", types.SimpleNamespace(Instruction=object))
if importlib.util.find_spec("aiofiles") is None:
    aiof = types.ModuleType("aiofiles")
    aiof.__spec__ = importlib.machinery.ModuleSpec("aiofiles", None)
    sys.modules.setdefault("aiofiles", aiof)

from solhunter_zero.price_stream_manager import PriceStreamManager
from solhunter_zero.agents.arbitrage import ArbitrageAgent
from solhunter_zero.portfolio import Portfolio
from solhunter_zero.event_bus import subscribe
import os


@pytest.mark.asyncio
async def test_price_stream_manager_integration():
    async def orca_handler(ws):
        await ws.send(json.dumps({"token": "TOK", "price": 1.0}))
        await asyncio.sleep(0.1)

    async def ray_handler(ws):
        await ws.send(json.dumps({"token": "TOK", "price": 1.3}))
        await asyncio.sleep(0.1)

    server1 = await websockets.serve(orca_handler, "localhost", 0)
    port1 = server1.sockets[0].getsockname()[1]
    server2 = await websockets.serve(ray_handler, "localhost", 0)
    port2 = server2.sockets[0].getsockname()[1]

    events = []
    unsub = subscribe("price_update", lambda p: (events.append(p)))
    os.environ["MEASURE_DEX_LATENCY"] = "0"

    agent = ArbitrageAgent(threshold=0.1, amount=5, feeds={})
    mgr = PriceStreamManager(
        {"orca": f"ws://localhost:{port1}", "raydium": f"ws://localhost:{port2}"},
        ["TOK"],
    )
    await mgr.start()
    await asyncio.sleep(0.2)
    pf = Portfolio(path=None)
    actions = await agent.propose_trade("TOK", pf)

    unsub()
    await mgr.stop()
    server1.close()
    await server1.wait_closed()
    server2.close()
    await server2.wait_closed()

    assert any(e.get("venue") == "orca" for e in events)
    assert any(e.get("venue") == "raydium" for e in events)
    assert len(actions) == 2
    assert actions[0]["side"] == "buy"
    assert actions[1]["side"] == "sell"
