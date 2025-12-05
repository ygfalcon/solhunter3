import importlib
import os
import socket
import subprocess
import sys
import threading

import pytest


def test_ui_cli_reports_bind_failure() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        host, port = sock.getsockname()

        env = os.environ.copy()
        env["UI_ALLOW_OFFLINE_REDIS"] = "1"
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "solhunter_zero.ui",
                "--host",
                host,
                "--port",
                str(port),
            ],
            capture_output=True,
            text=True,
            env=env,
        )

    assert result.returncode != 0
    stderr = result.stderr
    assert "Failed to start UI server" in stderr
    assert f"{host}:{port}" in stderr


def test_ui_cli_starts_and_stops_websockets(monkeypatch, capsys):
    module = importlib.import_module("solhunter_zero.ui")
    events: list[str] = []

    class DummyServer:
        def __init__(self, state, *, host, port):
            self.state = state
            self.host = host
            self.port = port
            self.ready_timeout = 0.1
            self._event = threading.Event()

        def start(self):
            events.append("server-start")
            self._event.set()

        def stop(self):
            events.append("server-stop")

        @property
        def serve_forever_started(self):
            return self._event

    def fake_start_websockets():
        events.append("ws-start")
        return {"events": object()}

    def fake_stop_websockets():
        events.append("ws-stop")

    def fake_manifest():
        return {
            "rl_ws": "ws://example/rl",
            "events_ws": "ws://example/events",
            "logs_ws": "ws://example/logs",
            "rl_ws_available": True,
            "events_ws_available": True,
            "logs_ws_available": True,
        }

    monkeypatch.setattr(module, "build_ui_manifest", fake_manifest)

    def stop_after_first_sleep(_seconds):
        raise KeyboardInterrupt

    monkeypatch.setattr(module, "UIServer", DummyServer)
    monkeypatch.setattr(module, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module.time, "sleep", stop_after_first_sleep)

    module.main(["--host", "127.0.0.1", "--port", "0"])

    captured = capsys.readouterr()
    assert "Solsniper Zero UI listening" in captured.out
    assert events == ["server-start", "ws-start", "ws-stop", "server-stop"]


def test_ui_cli_websocket_failure(monkeypatch, capsys):
    module = importlib.import_module("solhunter_zero.ui")
    events: list[str] = []

    class DummyServer:
        def __init__(self, state, *, host, port):
            self.state = state
            self.host = host
            self.port = port
            self.ready_timeout = 0.1
            self._event = threading.Event()

        def start(self):
            events.append("server-start")
            self._event.set()

        def stop(self):
            events.append("server-stop")

        @property
        def serve_forever_started(self):
            return self._event

    def fail_start_websockets():
        raise RuntimeError("no webs")

    monkeypatch.setattr(module, "UIServer", DummyServer)
    monkeypatch.setattr(module, "start_websockets", fail_start_websockets)

    with pytest.raises(SystemExit) as excinfo:
        module.main(["--host", "127.0.0.1", "--port", "0"])

    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "Failed to start UI websockets" in captured.err
    assert events == ["server-start", "server-stop"]


def test_ui_cli_websocket_readiness_failure(monkeypatch, capsys):
    module = importlib.import_module("solhunter_zero.ui")
    events: list[str] = []

    class DummyServer:
        def __init__(self, state, *, host, port):
            self.state = state
            self.host = host
            self.port = port
            self.ready_timeout = 0.1
            self._event = threading.Event()

        def start(self):
            events.append("server-start")
            self._event.set()

        def stop(self):
            events.append("server-stop")

        @property
        def serve_forever_started(self):
            return self._event

    def fake_start_websockets():
        events.append("ws-start")
        return {"events": object()}

    def fake_stop_websockets():
        events.append("ws-stop")

    def fake_manifest():
        return {
            "rl_ws": None,
            "events_ws": "ws://example/events",
            "logs_ws": "ws://example/logs",
            "rl_ws_available": False,
            "events_ws_available": True,
            "logs_ws_available": True,
        }

    def fake_status():
        return (
            {"rl_ws": "failed", "events_ws": "ok", "logs_ws": "ok"},
            {"rl_ws": "websocket dependency missing"},
        )

    monkeypatch.setattr(module, "UIServer", DummyServer)
    monkeypatch.setattr(module, "start_websockets", fake_start_websockets)
    monkeypatch.setattr(module, "stop_websockets", fake_stop_websockets)
    monkeypatch.setattr(module, "build_ui_manifest", fake_manifest)
    monkeypatch.setattr(module, "summarize_ws_status", lambda optional=False: fake_status())

    with pytest.raises(SystemExit) as excinfo:
        module.main(["--host", "127.0.0.1", "--port", "0"])

    assert excinfo.value.code == 1
    captured = capsys.readouterr()
    assert "UI websocket readiness failed" in captured.err
    assert "rl websocket unavailable" in captured.err
    assert events == ["server-start", "ws-start", "server-stop", "ws-stop"]
