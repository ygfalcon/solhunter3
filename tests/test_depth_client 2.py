import asyncio
import json
import struct
import os
import subprocess
import time
import base64
import pytest
import aiohttp
from aiohttp import web

from solhunter_zero import depth_client


@pytest.fixture(autouse=True)
def _reset_pool():
    asyncio.run(depth_client.close_ipc_clients())
    yield
    asyncio.run(depth_client.close_ipc_clients())


class FakeReader:
    def __init__(self, data: bytes):
        self._data = data

    async def read(self):
        return self._data


class FakeWriter:
    def __init__(self):
        self.data = b""
        self.closed = False
        self.waited = False

    def write(self, data: bytes):
        self.data += data

    async def drain(self):
        pass

    def close(self):
        self.closed = True

    def is_closing(self):
        return self.closed

    async def wait_closed(self):
        self.waited = True


def _encode_entry(obj):
    tx_rate = float(obj.get("tx_rate", 0.0))
    dex = {
        k: {
            "bids": float(v.get("bids", 0.0)),
            "asks": float(v.get("asks", 0.0)),
            "tx_rate": float(v.get("tx_rate", 0.0)),
        }
        for k, v in obj.items()
        if isinstance(v, dict)
    }
    bids = sum(d["bids"] for d in dex.values())
    asks = sum(d["asks"] for d in dex.values())
    buf = bytearray()
    buf.extend(struct.pack("<Q", len(dex)))
    for name, info in dex.items():
        nb = name.encode()
        buf.extend(struct.pack("<Q", len(nb)))
        buf.extend(nb)
        buf.extend(struct.pack("<ddd", info["bids"], info["asks"], info["tx_rate"]))
    buf.extend(struct.pack("<dddq", bids, asks, tx_rate, 0))
    return bytes(buf)


def build_index(path, entries, adj=None):
    header = bytearray(b"IDX1")
    all_entries = dict(entries)
    if adj:
        for t, mat in adj.items():
            all_entries[f"adj_{t}"] = mat
    header.extend(struct.pack("<I", len(all_entries)))
    header_size = 8 + sum(2 + len(k) + 8 for k in all_entries)
    data = bytearray()

    def _encode_matrix(mat):
        buf = bytearray()
        venues = mat["venues"]
        matrix = mat["matrix"]
        buf.extend(struct.pack("<Q", len(venues)))
        for name in venues:
            nb = name.encode()
            buf.extend(struct.pack("<Q", len(nb)))
            buf.extend(nb)
        buf.extend(struct.pack("<Q", len(matrix)))
        buf.extend(struct.pack(f"<{len(matrix)}d", *matrix))
        return bytes(buf)

    for token, obj in all_entries.items():
        if token.startswith("adj_"):
            b = _encode_matrix(obj)
        else:
            b = _encode_entry(obj)
        header.extend(struct.pack("<H", len(token)))
        header.extend(token.encode())
        header.extend(struct.pack("<II", header_size + len(data), len(b)))
        data.extend(b)

    path.write_bytes(bytes(header + data))


def test_snapshot(tmp_path, monkeypatch):
    data = {
        "tok": {
            "tx_rate": 2.5,
            "dex1": {"bids": 10, "asks": 5},
            "dex2": {"bids": 1},
        }
    }
    path = tmp_path / "depth.mmap"
    build_index(path, data)
    monkeypatch.setattr(depth_client, "MMAP_PATH", str(path))

    venues, rate = depth_client.snapshot("tok")

    assert rate == pytest.approx(2.5)
    assert venues == {
        "dex1": {"bids": 10.0, "asks": 5.0},
        "dex2": {"bids": 1.0, "asks": 0.0},
    }


def test_snapshot_cache(tmp_path, monkeypatch):
    data = {"tok": {"tx_rate": 1.0, "dex": {"bids": 1, "asks": 1}}}
    path = tmp_path / "depth.mmap"
    build_index(path, data)
    monkeypatch.setattr(depth_client, "MMAP_PATH", str(path))
    depth_client.SNAPSHOT_CACHE.clear()
    monkeypatch.setattr(depth_client, "DEPTH_CACHE_TTL", 0.5)

    import builtins

    calls = []

    original_open = builtins.open

    def fake_open(file, *args, **kwargs):
        if file == str(path):
            calls.append(1)
        return original_open(file, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", fake_open)
    times = [0.0, 0.1]

    def fake_monotonic():
        return times.pop(0)

    monkeypatch.setattr(depth_client.time, "monotonic", fake_monotonic)

    venues1, rate1 = depth_client.snapshot("tok")
    venues2, rate2 = depth_client.snapshot("tok")
    monkeypatch.setattr(builtins, "open", original_open)

    assert venues1 == venues2
    assert rate1 == rate2
    assert len(calls) == 1


def test_snapshot_json_fallback(tmp_path, monkeypatch):
    data = {"tok": {"tx_rate": 3.0, "dex": {"bids": 2, "asks": 4}}}
    path = tmp_path / "depth.mmap"
    path.write_text(json.dumps(data))
    monkeypatch.setattr(depth_client, "MMAP_PATH", str(path))
    depth_client.SNAPSHOT_CACHE.clear()

    venues, rate = depth_client.snapshot("tok")

    assert rate == pytest.approx(3.0)
    assert venues == {"dex": {"bids": 2.0, "asks": 4.0}}


def test_get_adjacency_matrix(tmp_path, monkeypatch):
    data = {"tok": {"tx_rate": 1.0}}
    adj = {
        "tok": {
            "venues": ["dex1", "dex2"],
            "matrix": [0.0, 1.0, 2.0, 0.0],
        }
    }
    path = tmp_path / "depth.mmap"
    build_index(path, data, adj)
    monkeypatch.setattr(depth_client, "MMAP_PATH", str(path))

    res = depth_client.get_adjacency_matrix("tok")

    assert res == (["dex1", "dex2"], [[0.0, 1.0], [2.0, 0.0]])


def test_token_offset_cache(tmp_path, monkeypatch):
    data = {
        "a": {"tx_rate": 1.0},
        "b": {"tx_rate": 2.0},
    }
    path = tmp_path / "depth.mmap"
    build_index(path, data)
    monkeypatch.setattr(depth_client, "MMAP_PATH", str(path))
    depth_client.SNAPSHOT_CACHE.clear()
    depth_client.TOKEN_OFFSETS.clear()
    monkeypatch.setattr(depth_client, "DEPTH_CACHE_TTL", 0.0)

    calls = []
    orig = depth_client._build_token_offsets

    def spy(buf):
        calls.append(True)
        orig(buf)

    monkeypatch.setattr(depth_client, "_build_token_offsets", spy)

    depth_client.snapshot("a")
    depth_client.snapshot("b")
    assert len(calls) == 1

    data["c"] = {"tx_rate": 3.0}
    build_index(path, data)
    depth_client.snapshot("c")
    assert len(calls) == 2


def test_submit_signed_tx(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"signature": "sig"}).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.submit_signed_tx("TX", socket_path="sock")

    sig = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {"cmd": "signed_tx", "tx": "TX"}
    assert sig == "sig"


def test_prepare_signed_tx(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"tx": "AAA"}).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.prepare_signed_tx(
            "MSG", priority_fee=7, socket_path="sock"
        )

    tx = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {"cmd": "prepare", "msg": "MSG", "priority_fee": 7}
    assert tx == "AAA"


def test_submit_tx_batch(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps(["a", "b"]).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.submit_tx_batch(["t1", "t2"], socket_path="sock")

    sigs = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {"cmd": "batch", "txs": ["t1", "t2"]}
    assert sigs == ["a", "b"]


def test_submit_raw_tx(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"signature": "sig"}).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.submit_raw_tx(
            "TX",
            socket_path="sock",
            priority_rpc=["u1", "u2"],
            priority_fee=7,
        )

    sig = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {
        "cmd": "raw_tx",
        "tx": "TX",
        "priority_rpc": ["u1", "u2"],
        "priority_fee": 7,
    }
    assert sig == "sig"


def test_auto_exec(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"ok": True}).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.auto_exec(
            "TOK",
            1.2,
            ["AA"],
            socket_path="sock",
        )

    res = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {
        "cmd": "auto_exec",
        "token": "TOK",
        "threshold": 1.2,
        "txs": ["AA"],
    }
    assert res is True


def test_best_route(monkeypatch):
    captured = {}

    async def fake_conn(path):
        captured["socket"] = path
        writer = FakeWriter()
        resp = {"path": ["a", "b"], "profit": 1.0, "slippage": 0.2}
        reader = FakeReader(json.dumps(resp).encode())
        captured["writer"] = writer
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        return await depth_client.best_route("TOK", 2.0, socket_path="sock", max_hops=4)

    res = asyncio.run(run())
    payload = json.loads(captured["writer"].data.decode())

    assert captured["socket"] == "sock"
    assert payload == {
        "cmd": "route",
        "token": "TOK",
        "amount": 2.0,
        "max_hops": 4,
    }
    assert res == (["a", "b"], 1.0, 0.2)


def test_listen_depth_ws(monkeypatch):
    msgs = [{"tok": {"bids": 1, "asks": 2, "tx_rate": 1.0}}]

    class FakeMsg:
        def __init__(self, data):
            self.type = aiohttp.WSMsgType.TEXT
            self.data = json.dumps(data)

    class FakeWS:
        def __init__(self, messages):
            self.messages = list(messages)

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.messages:
                return FakeMsg(self.messages.pop(0))
            raise StopAsyncIteration

    class FakeSession:
        def __init__(self, messages):
            self.messages = messages

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            pass

        def ws_connect(self, url):
            self.url = url
            return FakeWS(self.messages)

    monkeypatch.setattr("aiohttp.ClientSession", lambda: FakeSession(msgs))
    events = []
    monkeypatch.setattr(depth_client, "publish", lambda t, d: events.append((t, d)))

    asyncio.run(depth_client.listen_depth_ws(max_updates=1))

    assert events[0][0] == "depth_service_status"
    assert events[0][1]["status"] == "connected"
    assert events[1] == ("depth_update", msgs[0])
    assert (
        events[2][0] == "depth_service_status"
        and events[2][1]["status"] == "disconnected"
    )


def test_connection_pool_reuse(monkeypatch):
    calls = []

    async def fake_conn(path):
        calls.append(path)
        writer = FakeWriter()
        reader = FakeReader(json.dumps({"ok": True}).encode())
        return reader, writer

    monkeypatch.setattr(asyncio, "open_unix_connection", fake_conn)

    async def run():
        await depth_client.auto_exec("TOK", 1.0, ["A"], socket_path="sock")
        await depth_client.best_route("TOK", 1.0, socket_path="sock")

    asyncio.run(run())

    assert calls == ["sock"]


@pytest.mark.asyncio
async def test_priority_rpc_concurrent(tmp_path):
    try:
        subprocess.run(
            ["cargo", "build", "--manifest-path", "depth_service/Cargo.toml"],
            check=True,
        )
    except subprocess.CalledProcessError:
        pytest.skip("cargo build failed")

    def make_handler(name, delay, times):
        async def handler(request):
            data = await request.json()
            method = data.get("method")
            if method == "getLatestBlockhash":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "result": {
                            "context": {"slot": 1},
                            "value": {
                                "blockhash": "11111111111111111111111111111111",
                                "lastValidBlockHeight": 1,
                            },
                        },
                        "id": data.get("id"),
                    }
                )
            elif method == "getVersion":
                return web.json_response(
                    {
                        "jsonrpc": "2.0",
                        "result": {"solana-core": "1.18.0"},
                        "id": data.get("id"),
                    }
                )
            elif method == "sendTransaction":
                times[name] = time.monotonic()
                await asyncio.sleep(delay)
                return web.json_response(
                    {"jsonrpc": "2.0", "result": name, "id": data.get("id")}
                )
            return web.json_response({"jsonrpc": "2.0", "result": None, "id": data.get("id")})

        return handler

    times: dict[str, float] = {}

    async def start_server(handler):
        app = web.Application()
        app.router.add_post("/", handler)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "localhost", 0)
        await site.start()
        port = site._server.sockets[0].getsockname()[1]
        return runner, port

    default_runner, default_port = await start_server(make_handler("default", 0.0, times))
    slow_runner, slow_port = await start_server(make_handler("slow", 0.4, times))
    fast_runner, fast_port = await start_server(make_handler("fast", 0.0, times))

    sock = tmp_path / "svc.sock"
    env = os.environ.copy()
    env.update(
        {
            "SOLANA_RPC_URL": f"http://localhost:{default_port}",
            "DEPTH_SERVICE_SOCKET": str(sock),
            "DEPTH_HEARTBEAT_INTERVAL": "1",
        }
    )

    proc = await asyncio.create_subprocess_exec(
        "depth_service/target/debug/depth_service",
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )

    for _ in range(50):
        if sock.exists():
            break
        await asyncio.sleep(0.1)

    tx_b64 = (
        "AYwq8aR+5Py3ToGbLJmYpJXtWdUKgI0sf7fY1Vssmtq7suSGoy+hXKH1kTR0M0RloR49SFhcFpB1GaIO"
        "+bPuXwSAAQABAp7j7fK3ZSJ7A3dW6xIB71a87kRBNxOHO8FARfwL2zazAAAAAAAAAAAAAAAAAAAAAAAA"
        "AAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAEBAAAA"
    )

    start = time.monotonic()
    try:
        sig_ret = await asyncio.wait_for(
            depth_client.submit_raw_tx(
                tx_b64,
                socket_path=str(sock),
                priority_rpc=[
                    f"http://localhost:{slow_port}",
                    f"http://localhost:{fast_port}",
                ],
            ),
            5,
        )
    except Exception as e:
        proc.kill()
        await proc.wait()
        await default_runner.cleanup()
        await slow_runner.cleanup()
        await fast_runner.cleanup()
        pytest.skip(f"service error: {e}")
    duration = time.monotonic() - start

    proc.kill()
    await proc.wait()

    await default_runner.cleanup()
    await slow_runner.cleanup()
    await fast_runner.cleanup()

    assert sig_ret == "fast"
    assert duration < 0.4
    assert "slow" in times and "fast" in times
    assert abs(times["slow"] - times["fast"]) < 0.2

